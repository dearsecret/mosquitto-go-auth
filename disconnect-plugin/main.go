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


	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Password: password,
		DB:       dbNumber,
	})

	redisMu.Lock()
	redisClient = client
	redisMu.Unlock()

	log.Printf(
		"[mosquitto-disconnect] Redis initialized: %s DB=%d key=%s",
		host,
		dbNumber,
		onlineKey,
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

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	if err := rdb.HSet(ctx, onlineKey, user, timestamp).Err(); err != nil {
		log.Printf("[mosquitto-disconnect] HSET failed: %v", err)
		return
	}

	log.Printf(
		"[mosquitto-disconnect] online: username=%q timestamp=%s",
		user,
		timestamp,
	)
}

//export GoOnSessionUpdate
func GoOnSessionUpdate(username *C.char, eventTime C.longlong, expiryTime C.longlong) {
	user := ""

	if username != nil {
		user = C.GoString(username)
	}

	event := int64(eventTime)
	expiry := int64(expiryTime)

	if user == "" || event <= 0 || expiry <= time.Now().Unix() {
		log.Printf(
			"[mosquitto-disconnect] invalid session update: username=%q event=%d expiry=%d",
			user,
			event,
			expiry,
		)
		return
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

	timestamp := strconv.FormatInt(event, 10)

	err := rdb.HSetEXWithArgs(ctx, onlineKey, &redis.HSetEXOptions{
		Condition:      redis.HSetEXFXX,
		ExpirationType: redis.HSetEXExpirationEXAT,
		ExpirationVal:  expiry,
	}, user, timestamp).Err()

	if err != nil {
		log.Printf("[mosquitto-disconnect] HSETEX session update failed: %v", err)
		return
	}

	log.Printf(
		"[mosquitto-disconnect] session updated: username=%q event=%d expiry=%d",
		user,
		event,
		expiry,
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


// MOSQ_EVT_PERSIST_CLIENT_ADD	세션 추가
// MOSQ_EVT_PERSIST_CLIENT_UPDATE	세션 정보 변경
// MOSQ_EVT_PERSIST_CLIENT_DELETE	세션 삭제
// MOSQ_EVT_PERSIST_SUBSCRIPTION_ADD	구독 추가
// MOSQ_EVT_PERSIST_SUBSCRIPTION_DELETE	구독 삭제
// MOSQ_EVT_SUBSCRIBE
// MOSQ_EVT_UNSUBSCRIBE