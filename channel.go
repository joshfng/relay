package main

import (
	"sync"

	"github.com/joshfng/joy4/av/pubsub"
)

// OutputStream holds info about outbound rtmp streams
type OutputStream struct {
	URL     string
	Channel chan bool
}

// Channel holds connection information and packet queue
type Channel struct {
	Queue         *pubsub.Queue
	Lock          *sync.RWMutex
	OutputStreams []OutputStream
}

// NewChannel creates a new channel struct
func NewChannel() *Channel {
	return &Channel{
		Queue: pubsub.NewQueue(),
		Lock:  &sync.RWMutex{},
	}
}
