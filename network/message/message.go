package message

import (
	"github.com/gogo/protobuf/proto"
	"github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message/pb"
	"github.com/oniio/dsp-go-sdk/network/message/types/block"
	"github.com/oniio/dsp-go-sdk/network/message/types/file"
)

type Header struct {
	Version   string
	Type      string
	MsgLength int32
}

type Message struct {
	Header  *Header
	Payload proto.Message
}

func ReadMessage(msg proto.Message) *Message {
	pbMsg, ok := msg.(*pb.Message)
	if !ok {
		return nil
	}
	newMsg := &Message{}
	if pbMsg.GetHeader() != nil {
		header := &Header{
			Version:   pbMsg.GetHeader().GetVersion(),
			Type:      pbMsg.GetHeader().GetType(),
			MsgLength: pbMsg.GetHeader().GetMsgLength(),
		}
		newMsg.Header = header
	}
	if pbMsg.GetData() != nil && len(pbMsg.GetData()) > 0 {
		data := pbMsg.GetData()
		msgType := pbMsg.GetHeader().GetType()
		switch msgType {
		case common.MSG_TYPE_BLOCK:
			blk := &block.Block{}
			err := proto.Unmarshal(data, blk)
			if err != nil {
				return nil
			}
			newMsg.Payload = blk
		case common.MSG_TYPE_FILE:
			file := &file.File{}
			err := proto.Unmarshal(data, file)
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
		msg.Header.MsgLength = this.Header.MsgLength
	}
	if this.Payload != nil {
		data, err := proto.Marshal(this.Payload)
		if err != nil {
			return nil
		}
		msg.Data = data
	}
	return msg
}
