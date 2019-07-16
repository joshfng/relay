package server

import (
	"time"

	"github.com/joshfng/joy4/av"
	log "github.com/sirupsen/logrus"
)

// CalcBitrate holds information for a stream's inbound bitrate
type CalcBitrate struct {
	Bitrate float64

	bitCnt   int64
	lastTime int64

	Channel      *Channel
	OutputStream *OutputStream
}

// ModifyPacket accepts a filter to perform actions on the packet
func (bitrate *CalcBitrate) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	// TODO: Pretty sure this is somewhat incorrect
	bitrate.bitCnt += int64(len(pkt.Data))

	if bitrate.lastTime+(1000000000) <= time.Now().UnixNano() {
		bitrate.Bitrate = float64(bitrate.bitCnt / 500) // this should be 1000
		bitrate.lastTime = time.Now().UnixNano()

		if bitrate.Channel != nil {
			bitrate.Channel.RxBitrate = bitrate.Bitrate

			log.Debugf("%s bitrate %.0f kb/s\n", bitrate.Channel.URL, bitrate.Channel.RxBitrate)
		}

		if bitrate.OutputStream != nil {
			bitrate.OutputStream.TxBitrate = bitrate.Bitrate

			log.Debugf("%s bitrate %.3f kb/s\n", bitrate.OutputStream.URL, bitrate.OutputStream.TxBitrate)
		}

		bitrate.bitCnt = 0
	}

	drop = false
	return
}
