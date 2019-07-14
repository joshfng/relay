package server

import (
	"encoding/json"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/go-redis/redis"
	"github.com/joshfng/joy4/av/avutil"
	"github.com/joshfng/joy4/av/pubsub"
	"github.com/joshfng/joy4/format/rtmp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// TODO:
// when a relay ends early, set something in redis to notify the user of the disconnect
// maybe retry?

var channels = make(map[string]*Channel)
var redisClient *redis.Client
var publishWaitGroup sync.WaitGroup

// Server holds config for the server
type Server struct {
	RedisAddr string
	RtmpAddr  string
	Lock      *sync.RWMutex
}

// PubSubMessage holds infomation on which stream to add/remove connections from
type PubSubMessage struct {
	ChannelURL      string `json:"channel_url"`
	OutputStreamURL string `json:"output_stream_url"`
	Action          string `json:"action"`
}

// OutputStream holds info about outbound rtmp streams
type OutputStream struct {
	PlayURL string
	URL     string
	Channel chan bool
}

// Channel holds connection information and packet queue
// as well as a list of outbound streams
type Channel struct {
	Queue         *pubsub.Queue
	Lock          *sync.RWMutex
	Conn          *rtmp.Conn
	OutputStreams []OutputStream
	WaitGroup     *sync.WaitGroup
}

// NewChannel is an incomming stream from a user
func (server Server) NewChannel() *Channel {
	return &Channel{
		Queue:     pubsub.NewQueue(),
		Lock:      server.Lock,
		WaitGroup: &sync.WaitGroup{},
	}
}

// StartServer starts the RTMP server and relay proxies
func (server Server) StartServer() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-c
		log.Info("got shutdown signal, closing relays and connections")
		for _, channel := range channels {
			for _, outputStream := range channel.OutputStreams {
				outputStream.Channel <- true
			}

			channel.WaitGroup.Wait()

			channel.Conn.WriteTrailer()
			channel.Conn.Close()
		}

		publishWaitGroup.Wait()

		os.Exit(1)
	}()

	redisClient = redis.NewClient(&redis.Options{
		Addr: server.RedisAddr,
	})
	if redisClient.Ping().Err() != nil {
		panic("Unable to connect to redis")
	}

	go server.subscribeToEvents()

	rtmpServer := &rtmp.Server{
		Addr: server.RtmpAddr,
	}

	rtmpServer.HandlePlay = server.HandlePlay
	rtmpServer.HandlePublish = server.HandlePublish

	log.Infof("server running %s", rtmpServer.Addr)

	rtmpServer.ListenAndServe()
}

func (server Server) relayConnection(channel *Channel, outputStream OutputStream) {
	channel.WaitGroup.Add(1)
	defer channel.WaitGroup.Done()

	playURL := strings.Join([]string{"rtmp://127.0.0.1:1935", outputStream.PlayURL}, "")

	log.Debugf("starting ffmpeg relay for %s", playURL)
	cmd := exec.Command(viper.GetString("FFMPEG_PATH"), "-i", playURL, "-c", "copy", "-f", "flv", outputStream.URL)

	log.Debugf("ffmpeg args %v", cmd.Args)

	err := cmd.Start()
	if err != nil {
		log.Infof("error starting ffmpeg %v", err)
		return
	}

	// add output stream to channel
	channel.Lock.Lock()
	channel.OutputStreams = append(channel.OutputStreams, outputStream)
	log.Debugf("channel %s now has %d output streams", channel.Conn.URL.Path, len(channel.OutputStreams))
	channel.Lock.Unlock()

	go func() {
		err := cmd.Wait()
		if err != nil && err.Error() != "signal: killed" {
			log.Infof("ffmpeg process exited %s", err)
		}

		outputStream.Channel <- true
	}()

	select {
	case <-outputStream.Channel:
		log.Debugf("shutting down relay for %s", outputStream.URL)
		cmd.Process.Signal(os.Kill)
	}
	close(outputStream.Channel)
	outputStream.Channel = nil

	// remove output stream from channel
	channel.Lock.Lock()

	newStreams := []OutputStream{}
	for _, stream := range channel.OutputStreams {
		if stream.URL == outputStream.URL {
			// channel.OutputStreams = append(channel.OutputStreams[:idx], channel.OutputStreams[idx+1:]...)
			continue
		} else {
			newStreams = append(newStreams, stream)
		}
	}

	channel.OutputStreams = newStreams
	log.Debugf("channel %s now has %d output streams", channel.Conn.URL.Path, len(channel.OutputStreams))
	channel.Lock.Unlock()
}

// StreamExists checks if the requested stream is allowed
func StreamExists(url string) bool {
	return redisClient.SIsMember("streams", url).Val()
}

// HandlePlay pushes incoming stream to outbound stream
func (server Server) HandlePlay(conn *rtmp.Conn) {
	log.Debug("got play ", conn.URL.Path)

	if !StreamExists(conn.URL.Path) {
		log.Infof("Unknown stream ID for %s; dropping connection", conn.URL.Path)
		conn.Close()
		return
	}

	server.Lock.RLock()
	ch, chExists := channels[conn.URL.Path]
	server.Lock.RUnlock()

	if !chExists {
		log.Infof("Channel not found for play %s; dropping connection", conn.URL.Path)
		conn.Close()
		return
	}

	log.Debug("play started ", conn.URL.Path)
	avutil.CopyFile(conn, ch.Queue.Latest())
	log.Debug("play stopped ", conn.URL.Path)
}

// HandlePublish handles an incoming stream
func (server Server) HandlePublish(conn *rtmp.Conn) {
	publishWaitGroup.Add(1)
	defer publishWaitGroup.Done()

	log.Info("starting publish for ", conn.URL.Path)

	if !StreamExists(conn.URL.Path) {
		log.Infof("Unknown stream ID for %s; dropping connection", conn.URL.Path)
		conn.Close()
		return
	}

	streams, _ := conn.Streams()

	server.Lock.Lock()
	ch := server.NewChannel()
	defer ch.Queue.Close()
	ch.Queue.WriteHeader(streams)
	channels[conn.URL.Path] = ch
	channels[conn.URL.Path].Conn = conn
	server.Lock.Unlock()

	for _, outputStreamURL := range redisClient.SMembers(conn.URL.Path).Val() {
		log.Debug("creating relay connections for ", conn.URL.Path)

		outputStream := OutputStream{
			PlayURL: conn.URL.Path,
			URL:     outputStreamURL,
			Channel: make(chan bool),
		}

		go server.relayConnection(ch, outputStream)
	}

	log.Debugf("stream started %s", conn.URL.Path)
	log.Debugf("server is now managing %d channels", len(channels))

	avutil.CopyPackets(ch.Queue, conn)

	log.Debugf("stream stopped %s", conn.URL.Path)

	for _, outputStream := range ch.OutputStreams {
		log.Debugf("sending stop signal to channel for output url %s", outputStream.URL)
		outputStream.Channel <- true
	}

	ch.WaitGroup.Wait()

	server.Lock.Lock()
	delete(channels, conn.URL.Path)
	server.Lock.Unlock()

	log.Info("stopped publish for ", conn.URL.Path)
	log.Debugf("server is now managing %d channels", len(channels))

}

func (server Server) subscribeToEvents() {
	redisChannel := "streamoutput-events"
	pubsub := redisClient.Subscribe(redisChannel)
	defer pubsub.Close()

	_, err := pubsub.Receive()
	if err != nil {
		log.Info(err)
	}

	log.Debugf("subscribed to %s channel", redisChannel)

	ch := pubsub.Channel()

	for msg := range ch {
		var message PubSubMessage
		err := json.Unmarshal([]byte(msg.Payload), &message)
		if err != nil {
			log.Infof("Unable to decode redis message %v", msg.Payload)
		}

		log.Debugf("got pubsub message %s, %v", msg.Channel, message)

		if message.Action == "remove" {
			server.Lock.Lock()
			ch, chExists := channels[message.ChannelURL]

			if chExists {
				if len(ch.OutputStreams) == 0 {
					server.Lock.Unlock()
					continue
				}

				log.Debugf("sending stop signal to channel %s for output url %s", message.ChannelURL, message.OutputStreamURL)

				for _, stream := range ch.OutputStreams {
					if stream.URL == message.OutputStreamURL {
						stream.Channel <- true
					}
				}
			}

			server.Lock.Unlock()
		}

		if message.Action == "add" {
			server.Lock.RLock()
			ch, chExists := channels[message.ChannelURL]
			server.Lock.RUnlock()

			if chExists {
				outputExists := false
				server.Lock.RLock()
				for _, stream := range ch.OutputStreams {
					if stream.URL == message.OutputStreamURL {
						outputExists = true
						break
					}
				}
				server.Lock.RUnlock()

				if outputExists {
					log.Debugf("start signal sent for stream %s %s but it already exists", message.ChannelURL, message.OutputStreamURL)
					continue
				}

				log.Debugf("sending start signal to channel %s for output url %s", message.ChannelURL, message.OutputStreamURL)

				outputStream := OutputStream{
					PlayURL: message.ChannelURL,
					URL:     message.OutputStreamURL,
					Channel: make(chan bool),
				}

				go server.relayConnection(ch, outputStream)
			}
		}
	}
}
