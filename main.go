package main

import (
	"log"
	"os/exec"
	"strings"
	"sync"

	"github.com/go-redis/redis"
	"github.com/joshfng/joy4/av/avutil"
	"github.com/joshfng/joy4/av/pubsub"
	"github.com/joshfng/joy4/format/rtmp"
	"github.com/spf13/viper"
)

// NEED
// stream struct
// restream struct

func relayConnection(conn *rtmp.Conn, streamURL string, wg *sync.WaitGroup, stop chan bool) {
	defer wg.Done()

	if !StreamExists(conn.URL.Path) {
		log.Println("Unknown stream ID; dropping connection.")
		conn.Close()
		return
	}

	playURL := strings.Join([]string{"rtmp://127.0.0.1:1935", conn.URL.Path}, "")

	log.Println("starting ffmpeg relay for", playURL)
	cmd := exec.Command(viper.GetString("FFMPEG_PATH"), "-i", playURL, "-c", "copy", "-f", "flv", streamURL)

	// log.Println(cmd.Args)

	// var stdOut bytes.Buffer
	// var stdErr bytes.Buffer
	// cmd.Stdout = &stdOut
	// cmd.Stderr = &stdErr
	err := cmd.Start()
	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		err := cmd.Wait()
		log.Println("ffmpeg process exited")
		log.Println(err)
		// log.Printf("ffmpeg outout: %q\n", stdOut.String())
		// log.Printf("ffmpeg error: %q\n", stdErr.String())
	}()

	select {
	case <-stop:
		log.Println("shutting down stream for:", streamURL)
		// log.Printf("ffmpeg outout: %q\n", stdOut.String())
		// log.Printf("ffmpeg error: %q\n", stdErr.String())
		cmd.Process.Kill()
	}
}

// StreamExists checks if the requested stream is allowed
func StreamExists(url string) bool {
	return redisClient.SIsMember("streams", url).Val()
}

// HandlePlay pushes incoming stream to outbound stream
func HandlePlay(conn *rtmp.Conn) {
	log.Println("got play", conn.URL.Path)

	if !StreamExists(conn.URL.Path) {
		log.Println("Unknown stream ID; dropping connection.")
		conn.Close()
		return
	}

	lock.RLock()
	ch, chExists := channels[conn.URL.Path]
	lock.RUnlock()

	if chExists {
		log.Println("play started", conn.URL.Path)
		avutil.CopyFile(conn, ch.que.Latest())
		log.Println("play stopped", conn.URL.Path)
	}
}

// HandlePublish handles an incoming stream
func HandlePublish(conn *rtmp.Conn) {
	log.Println("got publish", conn.URL.Path)

	if !StreamExists(conn.URL.Path) {
		log.Println("Unknown stream ID; dropping connection.")
		conn.Close()
		return
	}

	streams, _ := conn.Streams()

	lock.Lock()
	ch := Channel{}
	ch.que = pubsub.NewQueue()
	ch.que.WriteHeader(streams)
	channels[conn.URL.Path] = &ch
	lock.Unlock()

	var wg sync.WaitGroup

	lock.Lock()
	for _, outputStream := range redisClient.SMembers(conn.URL.Path).Val() {
		wg.Add(1)

		log.Println("creating relay connections for", conn.URL.Path)
		stopChannel := make(chan bool)
		outputStreams[conn.URL.Path] = append(outputStreams[conn.URL.Path], stopChannel)
		go relayConnection(conn, outputStream, &wg, stopChannel)
	}
	lock.Unlock()

	log.Println("sending publish packets", conn.URL.Path)
	avutil.CopyPackets(ch.que, conn)

	log.Println("Stream stopped, sending kill sigs", conn.URL.Path)

	lock.Lock()
	for key, outputStream := range outputStreams {
		for idx, channel := range outputStream {
			log.Printf("sending stop signal to channel %d for output url %s", idx, key)
			channel <- true
			close(channel)
		}

		delete(outputStreams, conn.URL.Path)
	}
	lock.Unlock()

	wg.Wait()

	lock.Lock()
	delete(channels, conn.URL.Path)
	lock.Unlock()
	ch.que.Close()
}

var lock = &sync.RWMutex{}
var channels = make(map[string]*Channel)
var outputStreams = make(map[string][]chan bool)

var redisClient *redis.Client

func initConfig() {
	viper.SetConfigType("env")
	viper.SetConfigFile(".env")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	if viper.ReadInConfig() != nil {
		log.Println("No .env file found, assuming ENV is set")
	} else {
		if viper.GetBool("DEBUG") {
			log.Println(viper.AllSettings())
		}
	}
}

func main() {
	initConfig()

	redisClient = redis.NewClient(&redis.Options{
		Addr: viper.GetString("REDIS_URL"),
	})
	if redisClient.Ping().Err() != nil {
		panic("Unable to connect to redis")
	}

	server := &rtmp.Server{
		Addr: viper.GetString("RTMP_URL"),
	}

	server.HandlePlay = HandlePlay
	server.HandlePublish = HandlePublish

	log.Println("server running:", server.Addr)

	server.ListenAndServe()
}
