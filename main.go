package main

import (
	"bytes"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/joshfng/joy4/av/avutil"
	"github.com/joshfng/joy4/av/pubsub"
	"github.com/joshfng/joy4/format/rtmp"
)

// NEED
// stream struct
// restream struct

func relayConnection(streamURL string, wg *sync.WaitGroup, streaming chan bool) {
	defer wg.Done()

	//time.Sleep(time.Second * 2)
	log.Println("starting ffmpeg relay")
	// cmd := exec.Command(os.Getenv("FFMPEG_PATH"), "-re", "-i", "rtmp://127.0.0.1:1935", "-c", "copy", "-f", "flv", streamURL)
	cmd := exec.Command(os.Getenv("FFMPEG_PATH"), "-i", "rtmp://127.0.0.1:1935", "-c", "copy", "-f", "flv", streamURL)

	log.Println(cmd.Args)

	var stdOut bytes.Buffer
	var stdErr bytes.Buffer
	cmd.Stdout = &stdOut
	cmd.Stderr = &stdErr
	err := cmd.Start()
	if err != nil {
		log.Println(err)
	}

	go func() {
		cmd.Wait()
		log.Print("ffmpeg process exited")
		// log.Printf("ffmpeg outout: %q\n", stdOut.String())
		// log.Printf("ffmpeg error: %q\n", stdErr.String())
	}()

loop:
	for {
		select {
		case <-streaming:
			log.Print("shutting down stream for: ", streamURL)
			// log.Printf("ffmpeg outout: %q\n", stdOut.String())
			// log.Printf("ffmpeg error: %q\n", stdErr.String())
			cmd.Process.Kill()
			break loop
		default:
			time.Sleep(time.Second * 1)
		}
	}
}

// HandlePlay pushes incoming stream to outbound stream
func HandlePlay(conn *rtmp.Conn) {
	log.Println("got play")
	lock.RLock()
	ch, chExists := channels[conn.URL.Path]
	lock.RUnlock()

	if chExists {
		log.Println("sending play packets")
		cursor := ch.que.Latest()
		avutil.CopyFile(conn, cursor)
		log.Println("play stopped")
	}
}

// HandlePublish handles an incoming stream
func HandlePublish(conn *rtmp.Conn) {
	log.Println("got publish")

	// TODO: don't allow unauthorized restreams
	exists := true
	if !exists {
		log.Println("Unknown stream ID; dropping connection.")
		conn.Close()
		return
	}

	streams, _ := conn.Streams()

	lock.Lock()

	ch := Channel{}
	ch.que = pubsub.NewQueue()
	ch.que.WriteHeader(streams)
	channels[conn.URL.Path] = ch
	lock.Unlock()

	var wg sync.WaitGroup

	wg.Add(1)
	youtubeChannel := make(chan bool)
	go relayConnection(os.Getenv("YOUTUBE_URL"), &wg, youtubeChannel)

	wg.Add(1)
	twitchChannel := make(chan bool)
	go relayConnection(os.Getenv("TWITCH_URL"), &wg, twitchChannel)

	log.Println("sending publish packets")
	avutil.CopyPackets(ch.que, conn)

	log.Println("Stream stopped, sending kill sigs")
	youtubeChannel <- false
	close(youtubeChannel)
	twitchChannel <- false
	close(twitchChannel)

	lock.Lock()
	delete(channels, conn.URL.Path)
	lock.Unlock()
	ch.que.Close()

	wg.Wait()
}

// Channel holds connection information and packet queue
type Channel struct {
	que *pubsub.Queue
}

var lock = &sync.RWMutex{}
var channels = make(map[string]Channel)

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
