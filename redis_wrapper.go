package rmq

import (
	"log"
	"time"

	"github.com/go-redis/redis"
)

type RedisWrapper struct {
	rawClient *redis.Client
	scripts   map[string]string
}

func (wrapper RedisWrapper) GetClient() *redis.Client {
	return wrapper.rawClient
}

func (wrapper RedisWrapper) Set(key string, value interface{}, expiration time.Duration) bool {
	return checkErr(wrapper.rawClient.Set(key, value, expiration).Err())
}

func (wrapper RedisWrapper) Get(key string) (error, interface{}) {
	res, err := wrapper.rawClient.Get(key).Result()
	if err != nil {
		return err, ""
	}
	return nil, res
}

func (wrapper RedisWrapper) Del(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.Del(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) TTL(key string) (ttl time.Duration, ok bool) {
	ttl, err := wrapper.rawClient.TTL(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return ttl, ok
}

func (wrapper RedisWrapper) LLen(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.LLen(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) SAdd(key, value string) bool {
	return checkErr(wrapper.rawClient.SAdd(key, value).Err())
}

func (wrapper RedisWrapper) SMembers(key string) []string {
	members, err := wrapper.rawClient.SMembers(key).Result()
	if ok := checkErr(err); !ok {
		return []string{}
	}
	return members
}

func (wrapper RedisWrapper) SRem(key, value string) (affected int, ok bool) {
	n, err := wrapper.rawClient.SRem(key, value).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) SCount(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.ZCount(key, "-inf", "+inf").Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) FlushDb() {
	wrapper.rawClient.FlushDB()
}

func (wrapper RedisWrapper) RunShaScript(shaScriptKey string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := wrapper.rawClient.EvalSha(wrapper.scripts[shaScriptKey], keys, args...)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		log.Println("RunShaScript Error: operation=" + shaScriptKey + " message=" + cmd.Err().Error())
	}
	return cmd
}

// checkErr returns true if there is no error, false if the result error is nil and panics if there's another error
func checkErr(err error) (ok bool) {
	switch err {
	case nil:
		return true
	case redis.Nil:
		return false
	default:
		log.Panicf("rmq redis error is not nil %s", err)
		return false
	}
}
