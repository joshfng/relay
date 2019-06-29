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

func main() {
	l := &sync.RWMutex{}
	type Channel struct {
		que *pubsub.Queue
	}
	channels := map[string]*Channel{}

	server := &rtmp.Server{
		Addr: "0.0.0.0:1935",
	}

	server.HandlePlay = func(conn *rtmp.Conn) {
		log.Println("got play")
		l.RLock()
		ch := channels[conn.URL.Path]
		l.RUnlock()

		if ch != nil {
			log.Println("sending play packets")
			cursor := ch.que.Latest()
			avutil.CopyFile(conn, cursor)
			log.Println("play stopped")
		}
	}

	server.HandlePublish = func(conn *rtmp.Conn) {
		log.Println("got publish")

		// TODO: don't allow unauthorized restreams
		exists := true
		if !exists {
			log.Println("Unknown stream ID; dropping connection.")
			conn.Close()
			return
		}

		streams, _ := conn.Streams()

		l.Lock()
		ch := channels[conn.URL.Path]
		if ch == nil {
			ch = &Channel{}
			ch.que = pubsub.NewQueue()
			ch.que.WriteHeader(streams)
			channels[conn.URL.Path] = ch
		} else {
			ch = nil
		}
		l.Unlock()
		if ch == nil {
			return
		}

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

		l.Lock()
		delete(channels, conn.URL.Path)
		l.Unlock()
		ch.que.Close()

		wg.Wait()
	}

	log.Println("server running:", server.Addr)
	log.Println("ffmpeg path:", os.Getenv("FFMPEG_PATH"))
	server.ListenAndServe()
}
