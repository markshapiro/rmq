package rmq

import (
	"time"

	"github.com/go-redis/redis"
)

type RedisClient interface {
	GetClient() *redis.Client

	// simple keys
	Set(key string, value interface{}, expiration time.Duration) bool
	Get(key string) (error, interface{})
	Del(key string) (affected int, ok bool)      // default affected: 0
	TTL(key string) (ttl time.Duration, ok bool) // default ttl: 0

	// lists
	LLen(key string) (affected int, ok bool)

	// sets
	SAdd(key, value string) bool
	SMembers(key string) (members []string)         // default members: []string{}
	SRem(key, value string) (affected int, ok bool) // default affected: 0

	SCount(key string) (affected int, ok bool)

	RunShaScript(shaScriptKey string, keys []string, args ...interface{}) *redis.Cmd

	// special
	FlushDb()
}
