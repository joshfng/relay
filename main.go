package main

import (
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/joshfng/joy4/av/avutil"
	"github.com/joshfng/joy4/av/pubsub"
	"github.com/joshfng/joy4/format/rtmp"
)

// NEED
// stream struct
// restream struct

func relayConnection(conn *rtmp.Conn, streamURL string, wg *sync.WaitGroup, streaming chan bool) {
	defer wg.Done()

	if !StreamExists(conn.URL.Path) {
		log.Println("Unknown stream ID; dropping connection.")
		conn.Close()
		return
	}

	playURL := strings.Join([]string{"rtmp://127.0.0.1:1935", conn.URL.Path}, "/")

	log.Println("starting ffmpeg relay for", playURL)
	cmd := exec.Command(os.Getenv("FFMPEG_PATH"), "-i", playURL, "-c", "copy", "-f", "flv", streamURL)

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

loop:
	for {
		select {
		case <-streaming:
			log.Println("shutting down stream for: ", streamURL)
			// log.Printf("ffmpeg outout: %q\n", stdOut.String())
			// log.Printf("ffmpeg error: %q\n", stdErr.String())
			cmd.Process.Kill()
			break loop
		default:
			time.Sleep(time.Second * 1)
		}
	}
}

// StreamExists checks if the requested stream is allowed
func StreamExists(url string) bool {
	exists := false

	for _, streamURL := range streams {
		if streamURL == url {
			exists = true
		}
	}

	return exists
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

	wg.Add(1)
	youtubeChannel := make(chan bool)
	go relayConnection(conn, os.Getenv("YOUTUBE_URL"), &wg, youtubeChannel)

	wg.Add(1)
	twitchChannel := make(chan bool)
	go relayConnection(conn, os.Getenv("TWITCH_URL"), &wg, twitchChannel)

	log.Println("sending publish packets", conn.URL.Path)
	avutil.CopyPackets(ch.que, conn)

	log.Println("Stream stopped, sending kill sigs", conn.URL.Path)
	youtubeChannel <- false
	close(youtubeChannel)
	twitchChannel <- false
	close(twitchChannel)

	wg.Wait()

	lock.Lock()
	delete(channels, conn.URL.Path)
	lock.Unlock()
	ch.que.Close()
}

var lock = &sync.RWMutex{}
var channels = make(map[string]*Channel)
var streams = []string{"/live/joshfng"}

func main() {
	server := &rtmp.Server{
		Addr: "0.0.0.0:1935",
	}

	server.HandlePlay = HandlePlay
	server.HandlePublish = HandlePublish

	log.Println("server running:", server.Addr)
	log.Println("ffmpeg path:", os.Getenv("FFMPEG_PATH"))
	server.ListenAndServe()
}
