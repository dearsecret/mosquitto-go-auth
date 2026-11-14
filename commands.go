package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	bes "github.com/iegomez/mosquitto-go-auth/backends"
	redis "github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

type RedisStreamConsumer struct {
	client           redis.UniversalClient
	stream           string
	group            string
	consumer         string
	block            time.Duration
	count            int64
	claimIdle        time.Duration
	claimInterval    time.Duration
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	startOnce        sync.Once
	maxDeliveryCount int64
}

type MessageCleanup struct {
	key       string
	topic     string
	timestamp string
}

func streamIntOption(opts map[string]string, key string, fallback int64) int64 {
	value := strings.TrimSpace(opts[key])
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		log.Warnf("invalid %s=%q, using %d", key, value, fallback)
		return fallback
	}

	return parsed
}

func NewRedisStreamConsumer(opts map[string]string) (*RedisStreamConsumer, error) {
	host := opts["redis_host"]
	if host == "" {
		host = "localhost"
	}

	port := opts["redis_port"]
	if port == "" {
		port = "6379"
	}

	password := opts["redis_password"]

	block := time.Duration(
		streamIntOption(opts, "redis_stream_block_ms", 5000),
	) * time.Millisecond

	stream := strings.TrimSpace(opts["redis_stream_name"])
	if stream == "" {
		stream = "mosquitto:commands"
	}

	group := strings.TrimSpace(opts["redis_stream_group"])
	if group == "" {
		group = "mosquitto-workers"
	}

	consumer := strings.TrimSpace(opts["redis_stream_consumer"])
	if consumer == "" {
		hostname, err := os.Hostname()
		if err != nil || hostname == "" {
			hostname = "mosquitto"
		}
		consumer = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}

	count := streamIntOption(opts, "redis_stream_count", 10)

	claimIdle := time.Duration(
		streamIntOption(opts, "redis_stream_claim_idle_ms", 60000),
	) * time.Millisecond

	claimInterval := time.Duration(
		streamIntOption(opts, "redis_stream_claim_interval_ms", 30000),
	) * time.Millisecond

	readTimeout := block + 5*time.Second

	var client redis.UniversalClient

	maxDeliveryCount := streamIntOption(
		opts,
		"redis_stream_max_delivery",
		5,
	)

	if opts["redis_mode"] == "cluster" {
		addressesOpt := strings.TrimSpace(opts["redis_cluster_addresses"])
		if addressesOpt == "" {
			return nil, fmt.Errorf("redis stream: missing redis_cluster_addresses")
		}

		addresses := strings.Split(addressesOpt, ",")
		for i := range addresses {
			addresses[i] = strings.TrimSpace(addresses[i])
		}

		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:       addresses,
			Password:    password,
			ReadTimeout: readTimeout,
		})
	} else {
		db := streamIntOption(opts, "redis_db", 1)

		client = redis.NewClient(&redis.Options{
			Addr:        fmt.Sprintf("%s:%s", host, port),
			Password:    password,
			DB:          int(db),
			ReadTimeout: readTimeout,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &RedisStreamConsumer{
		client:           client,
		stream:           stream,
		group:            group,
		consumer:         consumer,
		block:            block,
		count:            count,
		claimIdle:        claimIdle,
		claimInterval:    claimInterval,
		ctx:              ctx,
		cancel:           cancel,
		maxDeliveryCount: maxDeliveryCount,
	}, nil
}

func (c *RedisStreamConsumer) Start() {
	c.startOnce.Do(func() {
		c.wg.Add(1)
		go c.run()
	})
}

func (c *RedisStreamConsumer) Stop() {
	c.cancel()
	c.wg.Wait()

	if err := c.client.Close(); err != nil {
		log.Errorf("redis stream close failed: %v", err)
	}
}

func (c *RedisStreamConsumer) run() {
	defer c.wg.Done()

	for {
		if c.ctx.Err() != nil {
			return
		}

		if err := c.client.Ping(c.ctx).Err(); err != nil {
			log.Errorf("redis stream ping failed: %v", err)
			if !c.waitRetry() {
				return
			}
			continue
		}

		err := c.client.XGroupCreateMkStream(
			c.ctx,
			c.stream,
			c.group,
			"0",
		).Err()

		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			log.Errorf("redis stream group creation failed: %v", err)
			if !c.waitRetry() {
				return
			}
			continue
		}

		log.Infof(
			"redis stream consumer started: stream=%s group=%s consumer=%s",
			c.stream,
			c.group,
			c.consumer,
		)

		c.consume()
		return
	}
}

func (c *RedisStreamConsumer) waitRetry() bool {
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case <-c.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (c *RedisStreamConsumer) consume() {
	lastClaim := time.Now()

	for {
		if c.ctx.Err() != nil {
			return
		}

		if time.Since(lastClaim) >= c.claimInterval {
			c.claimPending()
			lastClaim = time.Now()
		}

		streams, err := c.client.XReadGroup(
			c.ctx,
			&redis.XReadGroupArgs{
				Group:    c.group,
				Consumer: c.consumer,
				Streams:  []string{c.stream, ">"},
				Count:    c.count,
				Block:    c.block,
			},
		).Result()

		if err == redis.Nil {
			continue
		}

		if err != nil {
			if c.ctx.Err() != nil {
				return
			}

			log.Errorf("redis stream read failed: %v", err)

			if !c.waitRetry() {
				return
			}
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				c.processMessage(message)
			}
		}
	}
}

func (c *RedisStreamConsumer) claimPending() {
	cursor := "0-0"

	for {
		messages, next, err := c.client.XAutoClaim(
			c.ctx,
			&redis.XAutoClaimArgs{
				Stream:   c.stream,
				Group:    c.group,
				Consumer: c.consumer,
				MinIdle:  c.claimIdle,
				Start:    cursor,
				Count:    c.count,
			},
		).Result()

		if err != nil {
			if c.ctx.Err() == nil {
				log.Errorf("redis stream auto claim failed: %v", err)
			}
			return
		}

		for _, message := range messages {
			c.processMessage(message)
		}

		if next == "0-0" || next == cursor {
			return
		}

		cursor = next
	}
}

func streamString(value interface{}) string {
	if value == nil {
		return ""
	}

	return fmt.Sprint(value)
}

const COMPLETE_COMMAND = `
if ARGV[3] ~= '' then
    local current = redis.call('HGET', KEYS[1], ARGV[3])

    if current and current == ARGV[4] then
        redis.call('HDEL', KEYS[1], ARGV[3])
    end
end

redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[2], ARGV[2])

return 1
`

func (c *RedisStreamConsumer) completeMessage(
	messageID string,
	cleanup *MessageCleanup,
) error {
	key := c.stream
	topic := ""
	timestamp := ""

	if cleanup != nil {
		key = cleanup.key
		topic = cleanup.topic
		timestamp = cleanup.timestamp
	}

	return c.client.Eval(
		c.ctx,
		COMPLETE_COMMAND,
		[]string{key, c.stream},
		c.group,
		messageID,
		topic,
		timestamp,
	).Err()
}

func (c *RedisStreamConsumer) processMessage(message redis.XMessage) {
	success, err, cleanup := c.processCommand(message)

	if !success {
		pending, pendingErr := c.client.XPendingExt(
			c.ctx,
			&redis.XPendingExtArgs{
				Stream: c.stream,
				Group:  c.group,
				Start:  message.ID,
				End:    message.ID,
				Count:  1,
			},
		).Result()

		if pendingErr != nil {
			log.Errorf(
				"redis stream pending check failed: id=%s err=%v",
				message.ID,
				pendingErr,
			)
			return
		}

		if len(pending) == 0 {
			return
		}

		retryCount := pending[0].RetryCount

		log.Errorf(
			"redis stream command failed: id=%s delivery=%d/%d err=%v",
			message.ID,
			retryCount,
			c.maxDeliveryCount,
			err,
		)

		if retryCount >= c.maxDeliveryCount {
			log.Errorf(
				"redis stream max delivery exceeded: id=%s delivery=%d",
				message.ID,
				retryCount,
			)

			if err := c.completeMessage(message.ID, nil); err != nil {
				log.Errorf(
					"redis stream completion failed: id=%s err=%v",
					message.ID,
					err,
				)
			}
		}

		return
	}

	if err := c.completeMessage(message.ID, cleanup); err != nil {
		log.Errorf(
			"redis stream completion failed: id=%s err=%v",
			message.ID,
			err,
		)
	}
}

func (c *RedisStreamConsumer) Make(
    key, topic string,
) (string, error) {
    value, err := c.client.HGet(c.ctx, key, topic).Result()
    if err == redis.Nil {
        return "", nil
    }
    if err != nil {
        return "", err
    }

    if _, err := strconv.ParseInt(value, 10, 64); err != nil {
        return "", fmt.Errorf("invalid timestamp: %w", err)
    }

    return value, nil
}


func (c *RedisStreamConsumer) processCommand(
	message redis.XMessage,
) (bool, error, *MessageCleanup) {
	command := strings.ToLower(
		strings.TrimSpace(streamString(message.Values["command"])),
	)

	switch command {
	case "kick":
		username := strings.TrimSpace(
			streamString(message.Values["username"]),
		)

		if username == "" {
			return false, fmt.Errorf("kick missing username"), nil
		}

		if authPlugin.useCache && authPlugin.cache != nil {
			if err := authPlugin.cache.DeleteAuthRecord(
				authPlugin.ctx,
				username,
			); err != nil {
				return false, err, nil
			}
		}

		if err := bes.KickClientByUsername(username); err != nil {
			log.Errorf(
				"redis stream kick failed: id=%s username=%s err=%v",
				message.ID,
				username,
				err,
			)
			return false, err, nil
		}

		return true, nil, nil

	case "dispatch":
		topic := strings.TrimSpace(
			streamString(message.Values["topic"]),
		)

		if topic == "" {
			return false, fmt.Errorf("dispatch missing topic"), nil
		}

		payload := []byte(streamString(message.Values["payload"]))

		qos := 0
		if value := streamString(message.Values["qos"]); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 0 || parsed > 2 {
				return false, fmt.Errorf("invalid qos: %q", value), nil
			}
			qos = parsed
		}

		retain := false
		if value := streamString(message.Values["retain"]); value != "" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return false, fmt.Errorf("invalid retain: %q", value), nil
			}
			retain = parsed
		}

		expiry := uint32(0)
		if value := streamString(message.Values["expiry_seconds"]); value != "" {
			parsed, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return false, fmt.Errorf("invalid expiry: %q", value), nil
			}
			expiry = uint32(parsed)
		}

		if err := bes.Dispatch(topic, payload, qos, retain, expiry); err != nil {
			log.Errorf(
				"redis stream dispatch failed: id=%s topic=%s err=%v",
				message.ID,
				topic,
				err,
			)
			return false, err, nil
		}

		log.Infof(
			"redis stream command processed: id=%s command=dispatch topic=%s qos=%d retain=%t expiry=%ds",
			message.ID,
			topic,
			qos,
			retain,
			expiry,
		)

		return true, nil, nil
	case "make":
		key := strings.TrimSpace(streamString(message.Values["key"]))
		topic := strings.TrimSpace(streamString(message.Values["topic"]))

		if key == "" || topic == "" {
			return false, fmt.Errorf("make missing key or topic"), nil
		}

		qos := 0
		if value := streamString(message.Values["qos"]); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 0 || parsed > 2 {
				return false, fmt.Errorf("invalid qos: %q", value), nil
			}
			qos = parsed
		}

		retain := true
		if value := streamString(message.Values["retain"]); value != "" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return false, fmt.Errorf("invalid retain: %q", value), nil
			}
			retain = parsed
		}

		expiry := uint32(0)
		if value := streamString(message.Values["expiry_seconds"]); value != "" {
			parsed, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return false, fmt.Errorf("invalid expiry: %q", value), nil
			}
			expiry = uint32(parsed)
		}

		createdAt, rawTimestamp, err := c.Make(key, topic)
		if err != nil {
			return false, err, nil
		}

		if createdAt == "" {
			return true, nil, nil
		}

		if err := bes.Dispatch(
			topic,
			[]byte(createdAt),
			qos,
			retain,
			expiry,
		); err != nil {
			return false, fmt.Errorf("make dispatch failed: %w", err), nil
		}

		log.Infof(
			"redis stream command processed: id=%s command=make topic=%s qos=%d retain=%t expiry=%ds",
			message.ID,
			topic,
			qos,
			retain,
			expiry,
		)

		return true, nil, &MessageCleanup{
			key:       key,
			topic:     topic,
			timestamp: rawTimestamp,
		}
	default:
		return false, fmt.Errorf("unsupported command: %q", command), nil
	}
}
