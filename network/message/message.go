package message

import (
	"github.com/gogo/protobuf/proto"
	"github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message/pb"
	"github.com/oniio/dsp-go-sdk/network/message/types/block"
	"github.com/oniio/dsp-go-sdk/network/message/types/file"
)

type Header struct {
	Version string
	Type    string
	Length  int32
}

type Payload interface {
	// Data() []byte
	XXX_Marshal(b []byte, deterministic bool) ([]byte, error)
}

type Message struct {
	Header  *Header
	Payload Payload
}

func ReadMessage(msg proto.Message) *Message {
	pbMsg, ok := msg.(*pb.Message)
	if !ok {
		return nil
	}
	newMsg := &Message{}
	if pbMsg.GetHeader() != nil {
		header := &Header{
			Version: pbMsg.GetHeader().GetVersion(),
			Type:    pbMsg.GetHeader().GetType(),
			Length:  pbMsg.GetHeader().GetLength(),
		}
		newMsg.Header = header
	}
	if pbMsg.GetPayload() != nil && len(pbMsg.GetPayload().GetData()) > 0 {
		data := pbMsg.GetPayload().GetData()
		msgType := pbMsg.GetHeader().GetType()
		switch msgType {
		case common.MSG_TYPE_BLOCK, common.MSG_TYPE_BLOCK_REQ:
			blk := &block.Block{}
			err := blk.XXX_Unmarshal(data)
			if err != nil {
				return nil
			}
			newMsg.Payload = blk
		case common.MSG_TYPE_FILE:
			file := &file.File{}
			err := file.XXX_Unmarshal(data)
			if err != nil {
				return nil
			}
			newMsg.Payload = file
		}
	}
	return newMsg
}

func (this *Message) ToProtoMsg() proto.Message {
	msg := new(pb.Message)
	if this.Header != nil {
		msg.Header = new(pb.Header)
		msg.Header.Version = this.Header.Version
		msg.Header.Type = this.Header.Type
		msg.Header.Length = this.Header.Length
	}
	if this.Payload != nil {
		msg.Payload = new(pb.Payload)
		data, _ := this.Payload.XXX_Marshal(nil, false)
		msg.Payload.Data = data
	}
	return msg
}
