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
)

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
	port := redisOptions["redis_port"]
	password := redisOptions["redis_password"]
	db := redisOptions["redis_db"]
	stream := redisOptions["redis_event_stream"]

	if host == "" || stream == "" {
		log.Println("[mosquitto-disconnect] Redis host or event stream missing")
		return 1
	}

	if port == "" {
		port = "6379"
	}

	dbNumber := 3
	if db != "" {
		parsed, err := strconv.Atoi(db)
		if err != nil {
			log.Printf("[mosquitto-disconnect] Invalid Redis DB: %v", err)
			return 1
		}
		dbNumber = parsed
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
		"[mosquitto-disconnect] Redis initialized: %s DB=%d stream=%s",
		host,
		dbNumber,
		stream,
	)

	return 0
}

//export GoOnConnect
func GoOnConnect(username *C.char, clientID *C.char) {
	user := ""
	client := ""

	if username != nil {
		user = C.GoString(username)
	}

	if clientID != nil {
		client = C.GoString(clientID)
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

	stream := redisOptions["redis_event_stream"]

	err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"event":     "connect",
			"username":  user,
			"client_id": client,
			"timestamp": time.Now().UnixMilli(),
		},
	}).Err()

	if err != nil {
		log.Printf("[mosquitto-disconnect] Redis XADD failed: %v", err)
		return
	}

	log.Printf(
		"[mosquitto-disconnect] connect event published: username=%q client_id=%q",
		user,
		client,
	)
}

//export GoOnDisconnect
func GoOnDisconnect(
	username *C.char,
	clientID *C.char,
	reason C.int,
) {
	user := ""
	client := ""

	if username != nil {
		user = C.GoString(username)
	}

	if clientID != nil {
		client = C.GoString(clientID)
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

	stream := redisOptions["redis_event_stream"]

	err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"event":     "disconnect",
			"username":  user,
			"client_id": client,
			"reason":    int(reason),
			"timestamp": time.Now().UnixMilli(),
		},
	}).Err()

	if err != nil {
		log.Printf("[mosquitto-disconnect] Redis XADD failed: %v", err)
		return
	}

	log.Printf(
		"[mosquitto-disconnect] disconnect event published: username=%q client_id=%q",
		user,
		client,
	)
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