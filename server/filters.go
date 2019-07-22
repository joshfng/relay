package server

import (
	"time"

	"github.com/joshfng/joy4/av"
	log "github.com/sirupsen/logrus"
)

// CalcBitrate holds information for a stream's inbound bitrate
type CalcBitrate struct {
	Bitrate int64

	bitCnt   int64
	lastTime int64

	Channel      *Channel
	OutputStream *OutputStream
}

// ModifyPacket accepts a filter to perform actions on the packet
func (bitrate *CalcBitrate) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	drop = false

	// if pkt.Idx != int8(videoidx) { // || !pkt.IsKeyFrame {
	// 	return
	// }

	// TODO: Pretty sure this is somewhat incorrect
	bitrate.bitCnt += int64(len(pkt.Data))

	// log kb/s ever 2 seconds
	if bitrate.lastTime+(2000000000) <= time.Now().UnixNano() {
		bitrate.lastTime = time.Now().UnixNano()

		bitrate.Bitrate = bitrate.bitCnt

		if bitrate.Channel != nil {
			bitrate.Channel.RxBitrate = bitrate.Bitrate

			log.Debugf("%s bitrate %.0f kb/s", bitrate.Channel.URL, float64(bitrate.Channel.RxBitrate/2/1000))
		}

		bitrate.bitCnt = 0
	}

	return
}
