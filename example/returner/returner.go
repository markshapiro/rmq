package main

import (
	"log"

	"github.com/markshapiro/rmq"
)

func main() {
	connection := rmq.OpenConnection("returner", "tcp", "localhost:6379", 2, false)
	queue := connection.OpenQueue("things")
	returned := queue.ReturnAllRejected()
	log.Printf("queue returner returned %d rejected deliveries", returned)
}
