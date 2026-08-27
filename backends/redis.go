package backends

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v8"
	. "github.com/iegomez/mosquitto-go-auth/backends/constants"
	"github.com/iegomez/mosquitto-go-auth/backends/topics"
	"github.com/iegomez/mosquitto-go-auth/hashing"
	log "github.com/sirupsen/logrus"
)

type RedisClient interface {
	Get(ctx context.Context, key string) *goredis.StringCmd
	HGet(ctx context.Context, key, field string) *goredis.StringCmd
	HKeys(ctx context.Context, key string) *goredis.StringSliceCmd
	Ping(ctx context.Context) *goredis.StatusCmd
	Close() error
	FlushDB(ctx context.Context) *goredis.StatusCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.StatusCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *goredis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd
	ReloadState(ctx context.Context)
}

type SingleRedisClient struct {
	*goredis.Client
}

func (c SingleRedisClient) ReloadState(ctx context.Context) {
	// NO-OP
}

type Redis struct {
	Host             string
	Port             string
	Password         string
	SaltEncoding     string
	DB               int32
	conn             RedisClient
	disableSuperuser bool
	ctx              context.Context
	hasher           hashing.HashComparer
}

func NewRedis(authOpts map[string]string, logLevel log.Level, hasher hashing.HashComparer) (Redis, error) {

	log.SetLevel(logLevel)

	var redis = Redis{
		Host:         "localhost",
		Port:         "6379",
		DB:           1,
		SaltEncoding: "base64",
		ctx:          context.Background(),
		hasher:       hasher,
	}

	if authOpts["redis_disable_superuser"] == "true" {
		redis.disableSuperuser = true
	}

	if redisHost, ok := authOpts["redis_host"]; ok {
		redis.Host = redisHost
	}

	if redisPort, ok := authOpts["redis_port"]; ok {
		redis.Port = redisPort
	}

	if redisPassword, ok := authOpts["redis_password"]; ok {
		redis.Password = redisPassword
	}

	if redisDB, ok := authOpts["redis_db"]; ok {
		db, err := strconv.ParseInt(redisDB, 10, 32)
		if err == nil {
			redis.DB = int32(db)
		}
	}

	if authOpts["redis_mode"] == "cluster" {

		addressesOpt := authOpts["redis_cluster_addresses"]
		if addressesOpt == "" {
			return redis, fmt.Errorf("redis backend: missing Redis Cluster addresses")
		}

		// Take the given addresses and trim spaces from them.
		addresses := strings.Split(addressesOpt, ",")
		for i := 0; i < len(addresses); i++ {
			addresses[i] = strings.TrimSpace(addresses[i])
		}

		clusterClient := goredis.NewClusterClient(
			&goredis.ClusterOptions{
				Addrs:    addresses,
				Password: redis.Password,
			})
		redis.conn = clusterClient
	} else {
		addr := fmt.Sprintf("%s:%s", redis.Host, redis.Port)

		redisClient := goredis.NewClient(&goredis.Options{
			Addr:     addr,
			Password: redis.Password,
			DB:       int(redis.DB),
		})
		redis.conn = &SingleRedisClient{redisClient}
	}

	for {
		if _, err := redis.conn.Ping(redis.ctx).Result(); err != nil {
			log.Errorf("ping redis error, will retry in 2s: %s", err)
			time.Sleep(2 * time.Second)
		} else {
			break
		}
	}

	return redis, nil

}

// Checks if an error was caused by a moved record in a cluster.
func isMovedError(err error) bool {
	s := err.Error()
	if strings.HasPrefix(s, "MOVED ") || strings.HasPrefix(s, "ASK ") {
		return true
	}

	return false
}

//GetUser checks that the username exists and the given password hashes to the same password.
func (o Redis) GetUser(username, password, _ string) (bool, error) {
	ok, err := o.getUser(username, password)
	if err == nil {
		return ok, nil
	}

	//If using Redis Cluster, reload state and attempt once more.
	if isMovedError(err) {
		o.conn.ReloadState(o.ctx)

		//Retry once.
		ok, err = o.getUser(username, password)
	}

	if err != nil {
		log.Debugf("redis get user error: %s", err)
	}
	return ok, err
}

func (o Redis) getUser(username, password string) (bool, error) {
	pwHash, err := o.conn.Get(o.ctx, username).Result()
	if err == goredis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if o.hasher.Compare(password, pwHash) {
		return true, nil
	}

	return false, nil
}

//GetSuperuser checks that the key username:su exists and has value "true".
func (o Redis) GetSuperuser(username string) (bool, error) {
	if o.disableSuperuser {
		return false, nil
	}

	ok, err := o.getSuperuser(username)
	if err == nil {
		return ok, nil
	}

	//If using Redis Cluster, reload state and attempt once more.
	if isMovedError(err) {
		o.conn.ReloadState(o.ctx)

		//Retry once.
		ok, err = o.getSuperuser(username)
	}

	if err != nil {
		log.Debugf("redis get superuser error: %s", err)
	}

	return ok, err
}

func (o Redis) getSuperuser(username string) (bool, error) {
	isSuper, err := o.conn.Get(o.ctx, fmt.Sprintf("%s:su", username)).Result()
	if err == goredis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if isSuper == "true" {
		return true, nil
	}

	return false, nil
}

func (o Redis) CheckAcl(username, topic, clientid string, acc int32) (bool, error) {
	ok, err := o.checkAcl(username, topic, clientid, acc)
	if err == nil {
		return ok, nil
	}

	//If using Redis Cluster, reload state and attempt once more.
	if isMovedError(err) {
		o.conn.ReloadState(o.ctx)

		//Retry once.
		ok, err = o.checkAcl(username, topic, clientid, acc)
	}

	if err != nil {
		log.Debugf("redis check acl error: %s", err)
	}
	return ok, err
}

//CheckAcl gets all acls for the username and tries to match against topic, acc, and username/clientid if needed.
func (o Redis) checkAcl(username, topic, clientid string, acc int32) (bool, error) {
	// WRITE는 common:wacls만 사용
	if acc == MOSQ_ACL_WRITE {
		return o.matchCommonWriteAcl(username, clientid, topic)
	}
	// 개인 topic
	if o.containsUsername(topic, username) {
		return o.matchBuiltinAcl(username, topic, acc), nil
	}

	// wildcard subscription
	if o.isWildcard(topic) {
		return o.matchCommonWildcardAcl(topic, acc)
	}

	// 일반 topic → 개인 ACL exact
	var aclKeys []string

	switch acc {
	case MOSQ_ACL_SUBSCRIBE:
		aclKeys = []string{
			fmt.Sprintf("%s:sacls", username),
		}

	case MOSQ_ACL_READ:
		aclKeys = []string{
			fmt.Sprintf("%s:racls", username),
			fmt.Sprintf("%s:rwacls", username),
		}

	default:
		return false, nil
	}

	for _, key := range aclKeys {
		matched, err := o.matchAcl(key, topic)
		if err != nil {
			return false, err
		}

		if matched {
			return true, nil
		}
	}

	return false, nil
}


func (o Redis) containsUsername(topic, username string) bool {
	for _, level := range strings.Split(topic, "/") {
		if level == username {
			return true
		}
	}
	return false
}

func (o Redis) isWildcard(topic string) bool {
	return strings.ContainsAny(topic, "+#")
}



func (o Redis) matchAcl(key, topic string) (bool, error) {
	_, err := o.conn.HGet(o.ctx, key, topic).Result()

	if err == nil {
		return true, nil
	}

	if err == goredis.Nil {
		return false, nil
	}

	return false, err
}


func (o Redis) matchCommonWildcardAcl(topic string, acc int32) (bool, error) {
	key := o.getCommonAclKey(acc)
	if key == "" {
		return false, nil
	}

	acls, err := o.conn.HKeys(o.ctx, key).Result()
	if err != nil {
		return false, err
	}

	for _, acl := range acls {
		if !strings.ContainsAny(acl, "+#") {
			continue
		}

		if topics.Match(acl, topic) {
			return true, nil
		}
	}

	return false, nil
}

func (o Redis) GetName() string {
	return "Redis"
}

func (o Redis) Halt() {
	if o.conn != nil {
		err := o.conn.Close()
		if err != nil {
			log.Errorf("Redis cleanup error: %s", err)
		}
	}
}


func (o Redis) matchBuiltinAcl(username, topic string, acc int32) bool {
	matched := topics.Match(username+"/#", topic)
	switch acc {
	case MOSQ_ACL_SUBSCRIBE, MOSQ_ACL_READ:
		return matched
	}

	return false
}


func (o Redis) getCommonAclKey(acc int32) string {
	switch acc {
	case MOSQ_ACL_SUBSCRIBE:
		return "common:sacls"

	case MOSQ_ACL_READ:
		return "common:racls"

	case MOSQ_ACL_WRITE:
		return "common:wacls"

	default:
		return ""
	}
}


func (o Redis) matchCommonWriteAcl(
	username string,
	clientid string,
	topic string,
) (bool, error) {
	acls, err := o.conn.HKeys(o.ctx, "common:wacls").Result()
	if err != nil {
		return false, err
	}

	for _, acl := range acls {
		aclTopic := strings.ReplaceAll(acl, "%u", username)
		aclTopic = strings.ReplaceAll(aclTopic, "%c", clientid)

		if topics.Match(aclTopic, topic) {
			return true, nil
		}
	}

	return false, nil
}