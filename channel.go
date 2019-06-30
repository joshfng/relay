package main

import "github.com/joshfng/joy4/av/pubsub"

// Channel holds connection information and packet queue
type Channel struct {
	que *pubsub.Queue
}
