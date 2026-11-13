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
	client        redis.UniversalClient
	stream        string
	group         string
	consumer      string
	block         time.Duration
	count         int64
	claimIdle     time.Duration
	claimInterval time.Duration
	maxRetries    int64
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
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
		client:        client,
		stream:        stream,
		group:         group,
		consumer:      consumer,
		block:         block,
		count:         count,
		claimIdle:     claimIdle,
		claimInterval: claimInterval,
		ctx:           ctx,
		cancel:        cancel,
		maxRetries:    streamIntOption(opts, "redis_stream_max_retries", 3),
	}, nil
}

func (c *RedisStreamConsumer) Start() {
	c.wg.Add(1)
	go c.run()
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

func (c *RedisStreamConsumer) writeResult(
	message redis.XMessage,
	status string,
	err error,
) error {
	responseStream := strings.TrimSpace(
		streamString(message.Values["response_stream"]),
	)

	// 응답 요청이 없으면 아무것도 하지 않음
	if responseStream == "" {
		return nil
	}

	originalID := streamString(message.Values["original_id"])
	if originalID == "" {
		originalID = message.ID
	}

	values := map[string]interface{}{
		"original_id": originalID,
		"status":      status,
	}

	if err != nil {
		values["error"] = err.Error()
	}

	return c.client.XAdd(c.ctx, &redis.XAddArgs{
		Stream: responseStream,
		Values: values,
	}).Err()
}

func (c *RedisStreamConsumer) processMessage(message redis.XMessage) {
	success, err := c.processCommand(message)

	// response_stream이 있으면 동기 요청이므로 재시도하지 않음
	responseStream := strings.TrimSpace(
		streamString(message.Values["response_stream"]),
	)

	if responseStream != "" {
		status := "success"
		if !success {
			status = "error"
		}

		if resultErr := c.writeResult(message, status, err); resultErr != nil {
			log.Errorf(
				"redis stream result write failed: id=%s err=%v",
				message.ID,
				resultErr,
			)
			return
		}

		// 결과 기록 성공 후 ACK
		_ = c.ackMessage(message.ID)
		return
	}

	// 여기부터는 비동기 요청: 기존 재시도 정책 유지
	if success {
		_ = c.ackMessage(message.ID)
		return
	}

	retryCount, _ := strconv.ParseInt(
		streamString(message.Values["retry_count"]),
		10,
		64,
	)

	if retryCount < c.maxRetries {
		if retryErr := c.retryMessage(message, retryCount+1); retryErr != nil {
			log.Errorf(
				"redis stream retry handoff failed: id=%s err=%v",
				message.ID,
				retryErr,
			)
		}
		return
	}

	log.Errorf(
		"redis stream max retries exceeded: id=%s retries=%d err=%v",
		message.ID,
		retryCount,
		err,
	)

	_ = c.ackMessage(message.ID)
}

func (c *RedisStreamConsumer) ackMessage(messageID string) error {
	if err := c.client.XAck(
		c.ctx,
		c.stream,
		c.group,
		messageID,
	).Err(); err != nil {
		log.Errorf("redis stream ack failed: id=%s err=%v", messageID, err)
		return err
	}
	return nil
}

func (c *RedisStreamConsumer) retryMessage(
	message redis.XMessage,
	retryCount int64,
) error {
	values := make(map[string]interface{}, len(message.Values)+2)

	for key, value := range message.Values {
		values[key] = value
	}

	values["retry_count"] = retryCount
	values["original_id"] = streamString(
		message.Values["original_id"],
	)

	if values["original_id"] == "" {
		values["original_id"] = message.ID
	}

	newID, err := c.client.XAdd(
		c.ctx,
		&redis.XAddArgs{
			Stream: c.stream,
			Values: values,
		},
	).Result()

	if err != nil {
		return fmt.Errorf("retry XADD failed: %w", err)
	}

	log.Warnf(
		"redis stream retry scheduled: original_id=%s new_id=%s retry=%d/%d",
		values["original_id"],
		newID,
		retryCount,
		c.maxRetries,
	)

	if err := c.ackMessage(message.ID); err != nil {
		return fmt.Errorf(
			"retry message created but original ACK failed: %w",
			err,
		)
	}

	return nil
}

func (c *RedisStreamConsumer) processCommand(message redis.XMessage) (bool, error) {
	command := strings.ToLower(
		strings.TrimSpace(streamString(message.Values["command"])),
	)

	switch command {
	case "kick":
		username := strings.TrimSpace(
			streamString(message.Values["username"]),
		)

		if username == "" {
			return false, fmt.Errorf("kick missing username")
		}

		if authPlugin.useCache && authPlugin.cache != nil {
			if err := authPlugin.cache.DeleteAuthRecord(
				authPlugin.ctx,
				username,
			); err != nil {
				return false, err
			}
		}

		if err := bes.KickClientByUsername(username); err != nil {
			log.Errorf(
				"redis stream kick failed: id=%s username=%s err=%v",
				message.ID,
				username,
				err,
			)
			return false, err
		}

		return true, nil

	case "dispatch":
		topic := strings.TrimSpace(
			streamString(message.Values["topic"]),
		)

		if topic == "" {
			return false, fmt.Errorf("dispatch missing topic")
		}

		payload := []byte(streamString(message.Values["payload"]))

		qos := 0
		if value := streamString(message.Values["qos"]); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 0 || parsed > 2 {
				return false, fmt.Errorf("invalid qos: %q", value)
			}
			qos = parsed
		}

		retain := false
		if value := streamString(message.Values["retain"]); value != "" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return false, fmt.Errorf("invalid retain: %q", value)
			}
			retain = parsed
		}

		expiry := uint32(0)
		if value := streamString(message.Values["expiry_seconds"]); value != "" {
			parsed, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return false, fmt.Errorf("invalid expiry: %q", value)
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
			return false, err
		}

		log.Infof(
			"redis stream command processed: id=%s command=dispatch topic=%s qos=%d retain=%t expiry=%ds",
			message.ID,
			topic,
			qos,
			retain,
			expiry,
		)

		return true, nil

	default:
		return false, fmt.Errorf("unsupported command: %q", command)
	}
}
