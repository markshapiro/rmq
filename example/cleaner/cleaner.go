package main

import (
	"time"

	"github.com/markshapiro/rmq"
)

func main() {
	connection := rmq.OpenConnection("cleaner", "tcp", "localhost:6379", 2, false)
	cleaner := rmq.NewCleaner(connection)

	for _ = range time.Tick(time.Second) {
		cleaner.Clean()
	}
}
