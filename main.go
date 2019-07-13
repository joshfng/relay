package main

import (
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/go-redis/redis"
	"github.com/joshfng/joy4/av/avutil"
	"github.com/joshfng/joy4/format/rtmp"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func relayConnection(conn *rtmp.Conn, streamURL string, wg *sync.WaitGroup, stop chan bool) {
	defer wg.Done()

	playURL := strings.Join([]string{"rtmp://127.0.0.1:1935", conn.URL.Path}, "")

	logger.Debug("starting ffmpeg relay for ", playURL)
	cmd := exec.Command(viper.GetString("FFMPEG_PATH"), "-i", playURL, "-c", "copy", "-f", "flv", streamURL)

	// logger.Info(cmd.Args)

	// var stdOut bytes.Buffer
	// var stdErr bytes.Buffer
	// cmd.Stdout = &stdOut
	// cmd.Stderr = &stdErr
	err := cmd.Start()
	if err != nil {
		logger.Info(err)
		return
	}

	go func() {
		err := cmd.Wait()
		logger.Debug("ffmpeg process exited")
		logger.Debug(err)
		// log.Printf("ffmpeg outout: %q\n", stdOut.String())
		// log.Printf("ffmpeg error: %q\n", stdErr.String())
	}()

	select {
	case <-stop:
		logger.Debug("shutting down relay for", streamURL)
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
	logger.Debug("got play", conn.URL.Path)

	if !StreamExists(conn.URL.Path) {
		logger.Infof("Unknown stream ID for %s; dropping connection", conn.URL.Path)
		conn.Close()
		return
	}

	lock.RLock()
	ch, chExists := channels[conn.URL.Path]
	lock.RUnlock()

	if chExists {
		logger.Debug("play started", conn.URL.Path)
		avutil.CopyFile(conn, ch.Queue.Latest())
		logger.Debug("play stopped", conn.URL.Path)
	}
}

// HandlePublish handles an incoming stream
func HandlePublish(conn *rtmp.Conn) {
	logger.Info("starting publish for ", conn.URL.Path)

	if !StreamExists(conn.URL.Path) {
		logger.Infof("Unknown stream ID for %s; dropping connection", conn.URL.Path)
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

		logger.Debug("creating relay connections for ", conn.URL.Path)
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

	logger.Debug("sending publish packets ", conn.URL.Path)
	avutil.CopyPackets(ch.Queue, conn)

	logger.Debug("Stream stopped, sending kill sigs ", conn.URL.Path)

	for _, outputStream := range ch.OutputStreams {
		logger.Debugf("sending stop signal to channel for output url %s", outputStream.URL)
		outputStream.Channel <- true
		close(outputStream.Channel)
	}

	wg.Wait()

	lock.Lock()
	delete(channels, conn.URL.Path)
	lock.Unlock()

	logger.Info("stopped publish for ", conn.URL.Path)
}

var lock = &sync.RWMutex{}
var channels = make(map[string]*Channel)

var redisClient *redis.Client
var logger *zap.SugaredLogger

func initConfig() {
	viper.SetConfigType("env")
	viper.SetConfigFile(".env")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	viper.ReadInConfig()
}

func initLogger() {
	atom := zap.NewAtomicLevel()
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	tmplogger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		atom,
	))
	logger = tmplogger.Sugar()
	defer logger.Sync()

	if viper.GetBool("DEBUG") {
		atom.SetLevel(zap.DebugLevel)
	} else {
		atom.SetLevel(zap.InfoLevel)
	}

	logger.Info("logger activated ", atom.Level().String())
}

func main() {
	initConfig()
	initLogger()
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

	logger.Info("server running:", server.Addr)

	server.ListenAndServe()
}
