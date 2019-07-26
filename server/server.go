package server

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/go-redis/redis"
	"github.com/joshfng/joy4/av/avutil"
	"github.com/joshfng/joy4/av/pktque"
	"github.com/joshfng/joy4/av/pubsub"
	"github.com/joshfng/joy4/format/rtmp"
	log "github.com/sirupsen/logrus"
)

// TODO: when a relay fails notify the user of the disconnect & maybe retry?

var channels = make(map[string]*Channel)
var redisClient *redis.Client
var publishWaitGroup sync.WaitGroup

// Server holds config for the server
type Server struct {
	RedisAddr  string
	RtmpAddr   string
	FFMPEGPath string
	Lock       *sync.RWMutex
	Conn       *rtmp.Conn
}

// NewServer returns a new relay server
func NewServer(rtmpURL string, redisURL string, FFMPEGPath string) *Server {
	return &Server{
		RtmpAddr:   rtmpURL,
		RedisAddr:  redisURL,
		FFMPEGPath: FFMPEGPath,
		Lock:       &sync.RWMutex{},
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
	URL       string
	Channel   chan bool
	TxBitrate float64
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
	RxBitrate     int64
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

// Start starts the RTMP server and relay proxies
func (server Server) Start() {
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

	// TODO: youtube drops the connection trying to do this all in go
	// solution for now is to use ffmpeg to relay
	// dest, err := rtmp.Dial(currentOutputStream.URL)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	//
	// streams, _ := channel.Conn.Streams()
	// dest.WriteHeader(streams)
	// dest.Prepare()
	//
	// filters := pktque.Filters{}
	// filters = append(filters, &pktque.Walltime{})
	// demuxer := &pktque.FilterDemuxer{
	// 	Filter:  filters,
	// 	Demuxer: channel.Queue.Latest(),
	// }
	// err = avutil.CopyPackets(dest, demuxer)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// dest.WriteTrailer()
	// currentOutputStream.Channel <- true

	playURL := strings.Join([]string{"rtmp://127.0.0.1:1935", channel.URL}, "")

	log.Debugf("starting ffmpeg relay for %s", playURL)
	cmd := exec.Command(server.FFMPEGPath, "-i", playURL, "-c", "copy", "-f", "flv", currentOutputStream.URL)

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

		channel.Lock.RLock()
		for _, outputStream := range channel.OutputStreams {
			if outputStream.URL == currentOutputStream.URL {
				currentOutputStream.Channel <- true
			}
		}
		channel.Lock.RUnlock()
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

// ChannelAllowed checks if the requested stream is allowed
func ChannelAllowed(url string) bool {
	return redisClient.SIsMember("streams", url).Val()
}

// HandlePlay pushes incoming stream to outbound stream
func (server Server) HandlePlay(conn *rtmp.Conn) {
	log.Debug("got play ", conn.URL.Path)

	if !ChannelAllowed(conn.URL.Path) {
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

	// TODO: find a way to calculate outbound bitrate and attach OutputStream
	log.Debugf("play started %s", ch.URL)
	bytesCount, _ := avutil.CopyFile(conn, ch.Queue.Latest())
	log.Debugf("play stopped %s, copied %.2fmb", ch.URL, float64(bytesCount/1e+6))
}

// HandlePublish handles an incoming stream
func (server Server) HandlePublish(conn *rtmp.Conn) {
	publishWaitGroup.Add(1)
	defer publishWaitGroup.Done()

	log.Info("starting publish for ", conn.URL.Path)

	if !ChannelAllowed(conn.URL.Path) {
		log.Infof("Unknown stream ID for %s; dropping connection", conn.URL.Path)
		conn.Close()
		return
	}

	streams, _ := conn.Streams()

	ch := server.NewChannel(conn)
	defer ch.Queue.Close()
	ch.Queue.WriteHeader(streams)
	server.Lock.Lock()
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

	filters := pktque.Filters{}
	filters = append(filters, &CalcBitrate{Channel: ch})
	demuxer := &pktque.FilterDemuxer{
		Filter:  filters,
		Demuxer: conn,
	}
	bytesCount, _ := avutil.CopyPackets(ch.Queue, demuxer)
	//bytesCount, _ := avutil.CopyFile(ch.Queue, conn)

	log.Debugf("stream stopped %s, copied %.2fmb", ch.URL, float64(bytesCount/1e+6))

	ch.Lock.RLock()
	for _, outputStream := range ch.OutputStreams {
		log.Debugf("sending stop signal to channel for output url %s", outputStream.URL)
		outputStream.Channel <- true
	}
	ch.Lock.RUnlock()

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

		if message.Action == "remove-output" {
			server.Lock.RLock()
			ch, chExists := channels[message.ChannelURL]

			if chExists {
				if len(ch.OutputStreams) == 0 {
					server.Lock.RUnlock()
					continue
				}

				log.Debugf("sending stop signal to channel %s for output url %s", message.ChannelURL, message.OutputStreamURL)

				for _, stream := range ch.OutputStreams {
					if stream.URL == message.OutputStreamURL {
						stream.Channel <- true
					}
				}
			}

			server.Lock.RUnlock()
		}

		if message.Action == "add-output" {
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

		if message.Action == "remove-channel" {
			server.Lock.RLock()
			ch, chExists := channels[message.ChannelURL]
			server.Lock.RUnlock()

			if chExists {
				ch.Conn.Close()
			}
		}
	}
}
