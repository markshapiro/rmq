package rmq

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adjust/uniuri"
	"github.com/go-redis/redis"
)

const (
	connectionsKey                   = "rmq::connections"                                           // Set of connection names
	connectionHeartbeatTemplate      = "rmq::connection::{connection}::heartbeat"                   // expires after {connection} died
	connectionQueuesTemplate         = "rmq::connection::{connection}::queues"                      // Set of queues consumers of {connection} are consuming
	connectionQueueConsumersTemplate = "rmq::connection::{connection}::queue::[{queue}]::consumers" // Set of all consumers from {connection} consuming from {queue}
	connectionQueueUnackedTemplate   = "rmq::connection::{connection}::queue::[{queue}]::unacked"   // List of deliveries consumers of {connection} are currently consuming

	queuesKey             = "rmq::queues"                     // Set of all open queues
	queueReadyTemplate    = "rmq::queue::[{queue}]::ready"    // List of deliveries in that {queue} (right is first and oldest, left is last and youngest)
	queueRejectedTemplate = "rmq::queue::[{queue}]::rejected" // List of rejected deliveries from that {queue}
	queuePriorityTemplate = "rmq::queue::[{queue}]::priority"

	phConnection = "{connection}" // connection name
	phQueue      = "{queue}"      // queue name
	phConsumer   = "{consumer}"   // consumer name (consisting of tag and token)

	defaultBatchTimeout = time.Second
)

type Queue interface {
	Publish(payload string, priority int) bool
	PublishBytes(payload []byte, priority int) bool
	SetPushQueue(pushQueue Queue)
	StartConsuming(prefetchLimit int, pollDuration time.Duration) bool
	StopConsuming() <-chan struct{}
	AddConsumer(tag string, consumer Consumer) string
	AddConsumerFunc(tag string, consumerFunc ConsumerFunc) string
	AddBatchConsumer(tag string, batchSize int, consumer BatchConsumer) string
	AddBatchConsumerWithTimeout(tag string, batchSize int, timeout time.Duration, consumer BatchConsumer) string
	PurgeReady() int
	PurgeRejected() int
	ReturnRejected(count int) int
	ReturnAllRejected() int
	ReturnAllUnacked() int
	Close() bool
}

type redisQueue struct {
	name             string
	connectionName   string
	queuesKey        string // key to list of queues consumed by this connection
	consumersKey     string // key to set of consumers using this connection
	readyKey         string // key to list of ready deliveries
	rejectedKey      string // key to list of rejected deliveries
	priorityKey      string
	unackedKey       string // key to list of currently consuming deliveries
	pushKey          string // key to list of pushed deliveries
	redisClient      RedisClient
	deliveryChan     chan Delivery // nil for publish channels, not nil for consuming channels
	prefetchLimit    int           // max number of prefetched deliveries number of unacked can go up to prefetchLimit + numConsumers
	pollDuration     time.Duration
	consumingStopped int32 // queue status, 1 for stopped, 0 for consuming
	stopWg           sync.WaitGroup
}

func newQueue(name, connectionName, queuesKey string, redisClient RedisClient) *redisQueue {
	consumersKey := strings.Replace(connectionQueueConsumersTemplate, phConnection, connectionName, 1)
	consumersKey = strings.Replace(consumersKey, phQueue, name, 1)

	readyKey := strings.Replace(queueReadyTemplate, phQueue, name, 1)
	rejectedKey := strings.Replace(queueRejectedTemplate, phQueue, name, 1)
	priorityKey := strings.Replace(queuePriorityTemplate, phQueue, name, 1)

	unackedKey := strings.Replace(connectionQueueUnackedTemplate, phConnection, connectionName, 1)
	unackedKey = strings.Replace(unackedKey, phQueue, name, 1)

	queue := &redisQueue{
		name:             name,
		connectionName:   connectionName,
		queuesKey:        queuesKey,
		consumersKey:     consumersKey,
		readyKey:         readyKey,
		rejectedKey:      rejectedKey,
		priorityKey:      priorityKey,
		unackedKey:       unackedKey,
		redisClient:      redisClient,
		consumingStopped: 1, // start with stopped status
	}

	return queue
}

func (queue *redisQueue) String() string {
	return fmt.Sprintf("[%s conn:%s]", queue.name, queue.connectionName)
}

// Publish adds a delivery with the given payload to the queue
func (queue *redisQueue) Publish(payload string, priority int) bool {
	cmd := queue.redisClient.RunShaScript("publish", []string{queue.readyKey, queue.priorityKey}, payload, priority)

	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return false
	}

	return true
}

// PublishBytes just casts the bytes and calls Publish
func (queue *redisQueue) PublishBytes(payload []byte, priority int) bool {
	return queue.Publish(string(payload), priority)
}

// PurgeReady removes all ready deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeReady() int {
	cmd := queue.redisClient.RunShaScript("purge", []string{queue.readyKey, queue.priorityKey})
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return 0
	}
	countPurged, err := cmd.Int()
	if err != nil {
		return 0
	}
	return countPurged
}

// PurgeRejected removes all rejected deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeRejected() int {
	cmd := queue.redisClient.RunShaScript("purge", []string{queue.rejectedKey})
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return 0
	}
	countPurged, err := cmd.Int()
	if err != nil {
		return 0
	}
	return countPurged
}

// Close purges and removes the queue from the list of queues
func (queue *redisQueue) Close() bool {
	queue.PurgeRejected()
	queue.PurgeReady()
	count, _ := queue.redisClient.SRem(queuesKey, queue.name)
	return count > 0
}

func (queue *redisQueue) ReadyCount() int {
	count := queue.ReadyNormalCount()
	countPriority := queue.ReadyPriorityCount()
	return count + countPriority
}

func (queue *redisQueue) ReadyNormalCount() int {
	count, _ := queue.redisClient.LLen(queue.readyKey)
	return count
}

func (queue *redisQueue) ReadyPriorityCount() int {
	count, _ := queue.redisClient.SCount(queue.priorityKey)
	return count
}

func (queue *redisQueue) UnackedCount() int {
	count, _ := queue.redisClient.LLen(queue.unackedKey)
	return count
}

func (queue *redisQueue) RejectedCount() int {
	count, _ := queue.redisClient.LLen(queue.rejectedKey)
	return count
}

// ReturnAllUnacked moves all unacked deliveries back to the ready
// queue and deletes the unacked key afterwards, returns number of returned
// deliveries
func (queue *redisQueue) ReturnAllUnacked() int {
	cmd := queue.redisClient.RunShaScript("return", []string{queue.unackedKey, queue.readyKey, queue.priorityKey})
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return 0
	}
	countReturned, err := cmd.Int()
	if err != nil {
		return 0
	}
	return countReturned
}

// ReturnAllRejected moves all rejected deliveries back to the ready
// list and returns the number of returned deliveries
func (queue *redisQueue) ReturnAllRejected() int {
	cmd := queue.redisClient.RunShaScript("return", []string{queue.rejectedKey, queue.readyKey, queue.priorityKey})
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return 0
	}
	countReturned, err := cmd.Int()
	if err != nil {
		return 0
	}
	return countReturned
}

// ReturnRejected tries to return count rejected deliveries back to
// the ready list and returns the number of returned deliveries
func (queue *redisQueue) ReturnRejected(count int) int {
	cmd := queue.redisClient.RunShaScript("return", []string{queue.rejectedKey, queue.readyKey, queue.priorityKey}, count)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return 0
	}
	countReturned, err := cmd.Int()
	if err != nil {
		return 0
	}
	return countReturned
}

// CloseInConnection closes the queue in the associated connection by removing all related keys
func (queue *redisQueue) CloseInConnection() {
	queue.redisClient.Del(queue.unackedKey)
	queue.redisClient.Del(queue.consumersKey)
	queue.redisClient.SRem(queue.queuesKey, queue.name)
}

func (queue *redisQueue) SetPushQueue(pushQueue Queue) {
	redisPushQueue, ok := pushQueue.(*redisQueue)
	if !ok {
		return
	}

	queue.pushKey = redisPushQueue.readyKey
}

// StartConsuming starts consuming into a channel of size prefetchLimit
// must be called before consumers can be added!
// pollDuration is the duration the queue sleeps before checking for new deliveries
func (queue *redisQueue) StartConsuming(prefetchLimit int, pollDuration time.Duration) bool {
	if queue.deliveryChan != nil {
		return false // already consuming
	}

	// add queue to list of queues consumed on this connection
	if ok := queue.redisClient.SAdd(queue.queuesKey, queue.name); !ok {
		log.Panicf("rmq queue failed to start consuming %s", queue)
	}

	queue.prefetchLimit = prefetchLimit
	queue.pollDuration = pollDuration
	queue.deliveryChan = make(chan Delivery, prefetchLimit)
	atomic.StoreInt32(&queue.consumingStopped, 0)
	// log.Printf("rmq queue started consuming %s %d %s", queue, prefetchLimit, pollDuration)
	go queue.consume()
	return true
}

func (queue *redisQueue) StopConsuming() <-chan struct{} {
	finishedChan := make(chan struct{})
	if queue.deliveryChan == nil || atomic.LoadInt32(&queue.consumingStopped) == int32(1) {
		close(finishedChan) // not consuming or already stopped
		return finishedChan
	}

	// log.Printf("rmq queue stopping %s", queue)
	atomic.StoreInt32(&queue.consumingStopped, 1)
	go func() {
		queue.stopWg.Wait()
		close(finishedChan)
		// log.Printf("rmq queue stopped consuming %s", queue)
	}()

	return finishedChan
}

// AddConsumer adds a consumer to the queue and returns its internal name
// panics if StartConsuming wasn't called before!
func (queue *redisQueue) AddConsumer(tag string, consumer Consumer) string {
	queue.stopWg.Add(1)
	name := queue.addConsumer(tag)
	go queue.consumerConsume(consumer)
	return name
}

func (queue *redisQueue) AddConsumerFunc(tag string, consumerFunc ConsumerFunc) string {
	return queue.AddConsumer(tag, consumerFunc)
}

// AddBatchConsumer is similar to AddConsumer, but for batches of deliveries
func (queue *redisQueue) AddBatchConsumer(tag string, batchSize int, consumer BatchConsumer) string {
	return queue.AddBatchConsumerWithTimeout(tag, batchSize, defaultBatchTimeout, consumer)
}

// Timeout limits the amount of time waiting to fill an entire batch
// The timer is only started when the first message in a batch is received
func (queue *redisQueue) AddBatchConsumerWithTimeout(tag string, batchSize int, timeout time.Duration, consumer BatchConsumer) string {
	queue.stopWg.Add(1)
	name := queue.addConsumer(tag)
	go queue.consumerBatchConsume(batchSize, timeout, consumer)
	return name
}

func (queue *redisQueue) GetConsumers() []string {
	return queue.redisClient.SMembers(queue.consumersKey)
}

func (queue *redisQueue) RemoveConsumer(name string) bool {
	count, _ := queue.redisClient.SRem(queue.consumersKey, name)
	return count > 0
}

func (queue *redisQueue) addConsumer(tag string) string {
	if queue.deliveryChan == nil {
		log.Panicf("rmq queue failed to add consumer, call StartConsuming first! %s", queue)
	}

	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))

	// add consumer to list of consumers of this queue
	if ok := queue.redisClient.SAdd(queue.consumersKey, name); !ok {
		log.Panicf("rmq queue failed to add consumer %s %s", queue, tag)
	}

	// log.Printf("rmq queue added consumer %s %s", queue, name)
	return name
}

func (queue *redisQueue) RemoveAllConsumers() int {
	count, _ := queue.redisClient.Del(queue.consumersKey)
	return count
}

func (queue *redisQueue) consume() {
	for {
		batchSize := queue.batchSize()
		wantMore := queue.consumeBatch(batchSize)

		if !wantMore {
			time.Sleep(queue.pollDuration)
		}

		if atomic.LoadInt32(&queue.consumingStopped) == int32(1) {
			// log.Printf("rmq queue stopped consuming %s", queue)
			close(queue.deliveryChan)
			// log.Printf("rmq queue stopped fetching %s", queue)
			return
		}
	}
}

func (queue *redisQueue) batchSize() int {
	prefetchCount := len(queue.deliveryChan)
	prefetchLimit := queue.prefetchLimit - prefetchCount
	// TODO: ignore ready count here and just return prefetchLimit?
	if readyCount := queue.ReadyCount(); readyCount < prefetchLimit {
		return readyCount
	}
	return prefetchLimit
}

// consumeBatch tries to read batchSize deliveries, returns true if any and all were consumed
func (queue *redisQueue) consumeBatch(batchSize int) bool {

	if batchSize == 0 {
		return false
	}

	for i := 0; i < batchSize; i++ {

		cmd := queue.redisClient.RunShaScript("consume", []string{queue.readyKey, queue.unackedKey, queue.priorityKey})

		if cmd.Err() != nil && cmd.Err() != redis.Nil {
			return false
		}

		id, err := cmd.Int()

		if err != nil {
			return false
		}

		err, val := queue.redisClient.Get(strconv.Itoa(id) + "_value")
		if err != nil {
			return false
		}

		// debug(fmt.Sprintf("consume %d/%d %s %s", i, batchSize, id, queue)) // COMMENTOUT
		queue.deliveryChan <- newDelivery(id, val.(string), queue.unackedKey, queue.rejectedKey, queue.pushKey, queue.redisClient)
	}

	// debug(fmt.Sprintf("rmq queue consumed batch %s %d", queue, batchSize)) // COMMENTOUT
	return true
}

func (queue *redisQueue) consumerConsume(consumer Consumer) {
	for delivery := range queue.deliveryChan {
		// debug(fmt.Sprintf("consumer consume %s %s", delivery, consumer)) // COMMENTOUT
		consumer.Consume(delivery)
	}
	queue.stopWg.Done()
}

func (queue *redisQueue) consumerBatchConsume(batchSize int, timeout time.Duration, consumer BatchConsumer) {
	defer queue.stopWg.Done()
	batch := []Delivery{}
	for {
		// Wait for first delivery
		delivery, ok := <-queue.deliveryChan
		if !ok {
			// debug("batch channel closed") // COMMENTOUT
			return
		}
		batch = append(batch, delivery)
		// debug(fmt.Sprintf("batch consume added delivery %d", len(batch))) // COMMENTOUT
		batch, ok = queue.batchTimeout(batchSize, batch, timeout)
		consumer.Consume(batch)
		if !ok {
			// debug("batch channel closed") // COMMENTOUT
			return
		}
		batch = batch[:0] // reset batch
	}
}

func (queue *redisQueue) batchTimeout(batchSize int, batch []Delivery, timeout time.Duration) (fullBatch []Delivery, ok bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			// debug("batch timer fired") // COMMENTOUT
			// debug(fmt.Sprintf("batch consume consume %d", len(batch))) // COMMENTOUT
			return batch, true
		case delivery, ok := <-queue.deliveryChan:
			if !ok {
				// debug("batch channel closed") // COMMENTOUT
				return batch, false
			}
			batch = append(batch, delivery)
			// debug(fmt.Sprintf("batch consume added delivery %d", len(batch))) // COMMENTOUT
			if len(batch) >= batchSize {
				// debug(fmt.Sprintf("batch consume wait %d < %d", len(batch), batchSize)) // COMMENTOUT
				return batch, true
			}
		}
	}
}

func debug(message string) {
	// log.Printf("rmq debug: %s", message) // COMMENTOUT
}
