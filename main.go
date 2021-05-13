package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/suspiciousmilk/relay/server"
)

func initConfig() {
	viper.SetConfigType("env")
	viper.SetConfigFile(".env")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	if viper.ReadInConfig() != nil {
		log.Info("Unable to load .env file, assuming ENV is set")
	}
}

func initLogging() {
	if viper.GetBool("DEBUG") {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
}

func main() {
	initConfig()
	initLogging()

	server.NewServer(
		viper.GetString("RTMP_URL"),
		viper.GetString("REDIS_URL"),
		viper.GetString("FFMPEG_PATH"),
		viper.GetString("S3_BUCKET"),
	).Start()
}
