package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	redisOptions = make(map[string]string)
	redisClient  *redis.Client
	redisMu      sync.RWMutex

	onlineKey = "onlines"
	onlineTTL = 3600
)

func getRedisOption(key, fallback string) string {
	value := redisOptions[key]
	if value == "" {
		return fallback
	}
	return value
}

//export GoSetRedisOption
func GoSetRedisOption(key *C.char, value *C.char) {
	if key == nil || value == nil {
		return
	}

	redisOptions[C.GoString(key)] = C.GoString(value)
}

//export GoInitRedis
func GoInitRedis() C.int {
	host := redisOptions["redis_host"]
	port := getRedisOption("redis_port", "6379")
	password := redisOptions["redis_password"]

	if host == "" {
		log.Println("[mosquitto-disconnect] Redis host missing")
		return 1
	}

	dbNumber := 3
	if db := redisOptions["redis_db"]; db != "" {
		parsed, err := strconv.Atoi(db)
		if err != nil {
			log.Printf("[mosquitto-disconnect] Invalid Redis DB: %v", err)
			return 1
		}
		dbNumber = parsed
	}

	if key := redisOptions["redis_online_key"]; key != "" {
		onlineKey = key
	}

	if ttl := redisOptions["redis_online_ttl"]; ttl != "" {
		parsed, err := strconv.Atoi(ttl)
		if err != nil || parsed <= 0 {
			log.Printf("[mosquitto-disconnect] Invalid online TTL: %q", ttl)
			return 1
		}
		onlineTTL = parsed
	}

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Password: password,
		DB:       dbNumber,
	})

	redisMu.Lock()
	redisClient = client
	redisMu.Unlock()

	log.Printf(
		"[mosquitto-disconnect] Redis initialized: %s DB=%d key=%s ttl=%d",
		host,
		dbNumber,
		onlineKey,
		onlineTTL,
	)

	return 0
}

//export GoOnConnect
func GoOnConnect(username *C.char, clientID *C.char) {
	user := ""

	if username != nil {
		user = C.GoString(username)
	}

	redisMu.RLock()
	rdb := redisClient
	redisMu.RUnlock()

	if rdb == nil {
		log.Println("[mosquitto-disconnect] Redis client unavailable")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	timestamp := time.Now().Unix()

	if err := rdb.HSet(ctx, onlineKey, user, timestamp).Err(); err != nil {
		log.Printf("[mosquitto-disconnect] HSET failed: %v", err)
		return
	}

	log.Printf(
		"[mosquitto-disconnect] online: username=%q timestamp=%d",
		user,
		timestamp,
	)
}

//export GoOnOffline
func GoOnOffline(username *C.char, clientID *C.char) {
	user := ""

	if username != nil {
		user = C.GoString(username)
	}

	redisMu.RLock()
	rdb := redisClient
	redisMu.RUnlock()

	if rdb == nil {
		log.Println("[mosquitto-disconnect] Redis client unavailable")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	timestamp := -time.Now().Unix()

	err := rdb.HSetEXWithArgs(ctx, onlineKey, &redis.HSetEXOptions{
		Condition:      redis.HSetEXFXX,
		ExpirationType: redis.HSetEXExpirationEX,
		ExpirationVal:  int64(onlineTTL),
	}, user, timestamp).Err()

	if err != nil {
		log.Printf("[mosquitto-disconnect] HSETEX offline failed: %v", err)
		return
	}

	log.Printf(
		"[mosquitto-disconnect] offline: username=%q timestamp=%d ttl=%d",
		user,
		timestamp,
		onlineTTL,
	)
}

//export GoOnDisconnect
func GoOnDisconnect(username *C.char, clientID *C.char) {
	user := ""

	if username != nil {
		user = C.GoString(username)
	}

	redisMu.RLock()
	rdb := redisClient
	redisMu.RUnlock()

	if rdb == nil {
		log.Println("[mosquitto-disconnect] Redis client unavailable")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.HDel(ctx, onlineKey, user).Err(); err != nil {
		log.Printf("[mosquitto-disconnect] HDEL failed: %v", err)
		return
	}

	log.Printf("[mosquitto-disconnect] disconnect: username=%q", user)
}

//export GoCloseRedis
func GoCloseRedis() {
	redisMu.Lock()
	defer redisMu.Unlock()

	if redisClient != nil {
		if err := redisClient.Close(); err != nil {
			log.Printf("[mosquitto-disconnect] Redis close failed: %v", err)
		}
		redisClient = nil
	}
}

func main() {}