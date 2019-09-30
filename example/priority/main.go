package main

import (
	"log"
	"time"

	"github.com/markshapiro/rmq"
)

const (
	unackedLimit = 1
)

func main() {

	// NOTE: in this example tasks will not be neccesarily consumed in correct order since we start consuming before
	// publishing and some tasks may be consumed from ready queue before the next are published into it

	connection := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 2, false)
	queue := connection.OpenQueue("things")

	queue.StartConsuming(unackedLimit, 500*time.Millisecond)
	queue.AddConsumer("consumer", NewConsumer())

	queue.Publish("task1", 0)
	queue.Publish("task2", 1)
	queue.Publish("task3", 2)
	queue.Publish("task4", 4)
	queue.Publish("task5", 0)
	queue.Publish("task6", 2)
	queue.Publish("task7", 3)
	queue.Publish("task8", 1)
	queue.Publish("task9", 5)
	queue.Publish("task10", 0)
	queue.Publish("task11", 2)
	queue.Publish("task12", 1)
	queue.Publish("task13", 2)
	queue.Publish("task14", 5)
	queue.Publish("task15", 4)

	select {}
}

type Consumer struct{}

func NewConsumer() *Consumer {
	return &Consumer{}
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	log.Printf("consuming %s", delivery.Payload())
	delivery.Ack()
}
