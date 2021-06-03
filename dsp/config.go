package dsp

import "github.com/saveio/themis/common/log"

func (this *Dsp) ChannelNetworkProtocol() string {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service NetworkProtocol")
		return ""
	}
	return this.config.ChannelProtocol
}
