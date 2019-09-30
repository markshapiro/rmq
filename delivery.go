package rmq

import (
	"fmt"

	"github.com/go-redis/redis"
)

type Delivery interface {
	Id() int
	Payload() string
	Ack() bool
	Reject() bool
	Push() bool
}

type wrapDelivery struct {
	id          int
	payload     string
	unackedKey  string
	rejectedKey string
	pushKey     string
	redisClient RedisClient
}

func newDelivery(id int, payload, unackedKey, rejectedKey, pushKey string, redisClient RedisClient) *wrapDelivery {
	return &wrapDelivery{
		id:          id,
		payload:     payload,
		unackedKey:  unackedKey,
		rejectedKey: rejectedKey,
		pushKey:     pushKey,
		redisClient: redisClient,
	}
}

func (delivery *wrapDelivery) String() string {
	return fmt.Sprintf("[%s %s]", delivery.payload, delivery.unackedKey)
}

func (delivery *wrapDelivery) Id() int {
	return delivery.id
}

func (delivery *wrapDelivery) Payload() string {
	return delivery.payload
}

func (delivery *wrapDelivery) Ack() bool {
	cmd := delivery.redisClient.RunShaScript("ack", []string{delivery.unackedKey}, delivery.id)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return false
	}
	count, err := cmd.Int()
	if err != nil {
		return false
	}
	return count == 1
}

func (delivery *wrapDelivery) Reject() bool {
	return delivery.move(delivery.rejectedKey)
}

func (delivery *wrapDelivery) Push() bool {
	if delivery.pushKey != "" {
		return delivery.move(delivery.pushKey)
	} else {
		return delivery.move(delivery.rejectedKey)
	}
}

func (delivery *wrapDelivery) move(key string) bool {
	cmd := delivery.redisClient.RunShaScript("move", []string{delivery.unackedKey, key}, delivery.id)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return false
	}
	return true
}
