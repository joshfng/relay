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

// TODO: when a relay fails notify the user of the disconnect & maybe retry?

var channels = make(map[string]*Channel)
var redisClient *redis.Client
var publishWaitGroup sync.WaitGroup

// Server holds config for the server
type Server struct {
	RedisAddr string
	RtmpAddr  string
	Lock      *sync.RWMutex
	Conn      *rtmp.Conn
}

// NewServer returns a new relay server
func NewServer(rtmpURL string, redisURL string) *Server {
	return &Server{
		RtmpAddr:  rtmpURL,
		RedisAddr: redisURL,
		Lock:      &sync.RWMutex{},
	}
}

// PubSubMessage holds infomation on which stream to add/remove connections from
type PubSubMessage struct {
	ChannelURL      string `json:"channel_url"`
	OutputStreamURL string `json:"output_stream_url"`
	Action          string `json:"action"`
}

// OutputStream holds info about outbound rtmp streams
type OutputStream struct {
	URL     string
	Channel chan bool
}

// Channel holds connection information and packet queue
// as well as a list of outbound streams
type Channel struct {
	URL           string
	Queue         *pubsub.Queue
	Lock          *sync.RWMutex
	Conn          *rtmp.Conn
	OutputStreams []OutputStream
	WaitGroup     *sync.WaitGroup
}

// NewChannel is an incomming stream from a user
func (server Server) NewChannel(conn *rtmp.Conn) *Channel {
	return &Channel{
		Queue:     pubsub.NewQueue(),
		Lock:      server.Lock,
		WaitGroup: &sync.WaitGroup{},
		Conn:      conn,
		URL:       conn.URL.Path,
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

		os.Exit(0)
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

	err := rtmpServer.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

func (server Server) relayConnection(channel *Channel, currentOutputStream *OutputStream) {
	channel.WaitGroup.Add(1)
	defer channel.WaitGroup.Done()

	playURL := strings.Join([]string{"rtmp://127.0.0.1:1935", channel.URL}, "")

	log.Debugf("starting ffmpeg relay for %s", playURL)
	cmd := exec.Command(viper.GetString("FFMPEG_PATH"), "-i", playURL, "-c", "copy", "-f", "flv", currentOutputStream.URL)

	log.Debugf("ffmpeg args %v", cmd.Args)

	err := cmd.Start()
	if err != nil {
		log.Infof("error starting ffmpeg %v", err)
		return
	}

	// add output stream to channel
	channel.Lock.Lock()
	channel.OutputStreams = append(channel.OutputStreams, *currentOutputStream)
	log.Debugf("channel %s now has %d output streams", channel.URL, len(channel.OutputStreams))
	channel.Lock.Unlock()

	go func() {
		err := cmd.Wait()
		if err != nil && err.Error() != "signal: killed" {
			log.Infof("ffmpeg process exited %s", err)
		}

		currentOutputStream.Channel <- true
	}()

	<-currentOutputStream.Channel
	log.Debugf("shutting down relay for %s", currentOutputStream.URL)
	cmd.Process.Signal(os.Kill)

	close(currentOutputStream.Channel)
	currentOutputStream.Channel = nil

	// remove output stream from channel
	channel.Lock.Lock()

	newStreams := []OutputStream{}
	for _, outputStream := range channel.OutputStreams {
		if outputStream.URL == currentOutputStream.URL {
			continue
		} else {
			newStreams = append(newStreams, outputStream)
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

	log.Debug("play started ", ch.URL)
	avutil.CopyFile(conn, ch.Queue.Latest())
	log.Debug("play stopped ", ch.URL)
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
	ch := server.NewChannel(conn)
	defer ch.Queue.Close()
	ch.Queue.WriteHeader(streams)
	channels[ch.URL] = ch
	server.Lock.Unlock()

	for _, outputStreamURL := range redisClient.SMembers(ch.URL).Val() {
		log.Debug("creating relay connections for ", ch.URL)

		outputStream := OutputStream{
			URL:     outputStreamURL,
			Channel: make(chan bool),
		}

		go server.relayConnection(ch, &outputStream)
	}

	log.Debugf("stream started %s", ch.URL)
	log.Debugf("server is now managing %d channels", len(channels))

	avutil.CopyPackets(ch.Queue, conn)

	log.Debugf("stream stopped %s", ch.URL)

	for _, outputStream := range ch.OutputStreams {
		log.Debugf("sending stop signal to channel for output url %s", outputStream.URL)
		outputStream.Channel <- true
	}

	ch.WaitGroup.Wait()

	server.Lock.Lock()
	delete(channels, ch.URL)
	server.Lock.Unlock()

	log.Info("stopped publish for ", ch.URL)
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
					URL:     message.OutputStreamURL,
					Channel: make(chan bool),
				}

				go server.relayConnection(ch, &outputStream)
			}
		}
	}
}
