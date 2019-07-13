package main

import (
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/go-redis/redis"
	"github.com/joshfng/joy4/av/avutil"
	"github.com/joshfng/joy4/format/rtmp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func relayConnection(conn *rtmp.Conn, streamURL string, wg *sync.WaitGroup, stop chan bool) {
	defer wg.Done()

	playURL := strings.Join([]string{"rtmp://127.0.0.1:1935", conn.URL.Path}, "")

	log.Debug("starting ffmpeg relay for ", playURL)
	cmd := exec.Command(viper.GetString("FFMPEG_PATH"), "-i", playURL, "-c", "copy", "-f", "flv", streamURL)

	// log.Info(cmd.Args)

	// var stdOut bytes.Buffer
	// var stdErr bytes.Buffer
	// cmd.Stdout = &stdOut
	// cmd.Stderr = &stdErr
	err := cmd.Start()
	if err != nil {
		log.Info(err)
		return
	}

	go func() {
		err := cmd.Wait()
		log.Debug("ffmpeg process exited")
		log.Debug(err)
		// log.Printf("ffmpeg outout: %q\n", stdOut.String())
		// log.Printf("ffmpeg error: %q\n", stdErr.String())
	}()

	select {
	case <-stop:
		log.Debug("shutting down relay for", streamURL)
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
	log.Debug("got play", conn.URL.Path)

	if !StreamExists(conn.URL.Path) {
		log.Infof("Unknown stream ID for %s; dropping connection", conn.URL.Path)
		conn.Close()
		return
	}

	lock.RLock()
	ch, chExists := channels[conn.URL.Path]
	lock.RUnlock()

	if chExists {
		log.Debug("play started", conn.URL.Path)
		avutil.CopyFile(conn, ch.Queue.Latest())
		log.Debug("play stopped", conn.URL.Path)
	}
}

// HandlePublish handles an incoming stream
func HandlePublish(conn *rtmp.Conn) {
	log.Info("starting publish for ", conn.URL.Path)

	if !StreamExists(conn.URL.Path) {
		log.Infof("Unknown stream ID for %s; dropping connection", conn.URL.Path)
		conn.Close()
		return
	}

	streams, _ := conn.Streams()

	lock.Lock()
	ch := NewChannel()
	defer ch.Queue.Close()
	ch.Queue.WriteHeader(streams)
	channels[conn.URL.Path] = ch
	lock.Unlock()

	var wg sync.WaitGroup

	for _, outputStreamURL := range redisClient.SMembers(conn.URL.Path).Val() {
		wg.Add(1)

		log.Debug("creating relay connections for ", conn.URL.Path)
		stopChannel := make(chan bool)

		ch.Lock.Lock()
		outputStream := OutputStream{
			URL:     outputStreamURL,
			Channel: stopChannel,
		}

		channels[conn.URL.Path].OutputStreams = append(channels[conn.URL.Path].OutputStreams, outputStream)
		ch.Lock.Unlock()

		go relayConnection(conn, outputStreamURL, &wg, stopChannel)
	}

	log.Debug("sending publish packets ", conn.URL.Path)
	avutil.CopyPackets(ch.Queue, conn)

	log.Debug("Stream stopped, sending kill sigs ", conn.URL.Path)

	for _, outputStream := range ch.OutputStreams {
		log.Debugf("sending stop signal to channel for output url %s", outputStream.URL)
		outputStream.Channel <- true
		close(outputStream.Channel)
	}

	wg.Wait()

	lock.Lock()
	delete(channels, conn.URL.Path)
	lock.Unlock()

	log.Info("stopped publish for ", conn.URL.Path)
}

var lock = &sync.RWMutex{}
var channels = make(map[string]*Channel)

var redisClient *redis.Client

func initConfig() {
	viper.SetConfigType("env")
	viper.SetConfigFile(".env")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	viper.ReadInConfig()

	if viper.GetBool("DEBUG") {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.SetOutput(os.Stdout)
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

	log.Info("server running:", server.Addr)

	server.ListenAndServe()
}
