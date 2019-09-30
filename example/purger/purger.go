package main

import (
	"github.com/markshapiro/rmq"
)

func main() {
	connection := rmq.OpenConnection("cleaner", "tcp", "localhost:6379", 2, false)
	queue := connection.OpenQueue("things")
	queue.PurgeReady()
}
