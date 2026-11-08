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

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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

func (c *RedisStreamConsumer) processMessage(message redis.XMessage) {
	command := strings.ToLower(
		strings.TrimSpace(streamString(message.Values["command"])),
	)

	switch command {
	case "kick":
		username := strings.TrimSpace(
			streamString(message.Values["username"]),
		)

		if username == "" {
			log.Errorf("redis stream kick missing username: id=%s", message.ID)
			return
		}

		if err := bes.KickClientByUsername(username); err != nil {
			log.Errorf(
				"redis stream kick failed: id=%s username=%s err=%v",
				message.ID,
				username,
				err,
			)
			return
		}

		if authPlugin.useCache && authPlugin.cache != nil {
			if err := authPlugin.cache.DeleteAuthRecord(
				authPlugin.ctx,
				username,
			); err != nil {
				log.Errorf(
					"redis stream auth cache invalidation failed: id=%s username=%s err=%v",
					message.ID,
					username,
					err,
				)
				return
			}
		}

		log.Infof(
			"redis stream command processed: id=%s command=kick username=%s",
			message.ID,
			username,
		)

	case "dispatch":
		topic := strings.TrimSpace(
			streamString(message.Values["topic"]),
		)

		if topic == "" {
			log.Errorf("redis stream dispatch missing topic: id=%s", message.ID)
			return
		}

		payload := []byte(streamString(message.Values["payload"]))

		qos := 0
		if value := streamString(message.Values["qos"]); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 0 || parsed > 2 {
				log.Errorf(
					"redis stream dispatch invalid qos: id=%s qos=%q",
					message.ID,
					value,
				)
				return
			}
			qos = parsed
		}

		retain := false
		if value := streamString(message.Values["retain"]); value != "" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				log.Errorf(
					"redis stream dispatch invalid retain: id=%s retain=%q",
					message.ID,
					value,
				)
				return
			}
			retain = parsed
		}

		expiry := uint32(0)

		if value := streamString(message.Values["expiry_seconds"]); value != "" {
			parsed, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				log.Errorf(
					"redis stream dispatch invalid expiry: id=%s expiry=%q",
					message.ID,
					value,
				)
				return
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
			return
		}

		log.Infof(
			"redis stream command processed: id=%s command=dispatch topic=%s qos=%d retain=%t expiry=%ds",
			message.ID,
			topic,
			qos,
			retain,
			expiry,
		)

	default:
		log.Errorf(
			"unsupported redis stream command: id=%s command=%q",
			message.ID,
			command,
		)
		return
	}

	if err := c.client.XAck(
		c.ctx,
		c.stream,
		c.group,
		message.ID,
	).Err(); err != nil {
		log.Errorf(
			"redis stream ack failed: id=%s err=%v",
			message.ID,
			err,
		)
		return
	}
}