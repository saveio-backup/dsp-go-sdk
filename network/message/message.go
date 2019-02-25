package message

import (
	"github.com/gogo/protobuf/proto"
	"github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message/pb"
	"github.com/oniio/dsp-go-sdk/network/message/types/block"
	"github.com/oniio/dsp-go-sdk/network/message/types/file"
	"github.com/oniio/dsp-go-sdk/network/message/types/payment"
)

type Header struct {
	Version   string
	Type      string
	MsgLength int32
}

type Error struct {
	Code    int32
	Message string
}

type Message struct {
	Header  *Header
	Payload proto.Message
	Error   *Error
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
		case common.MSG_TYPE_PAYMENT:
			pay := &payment.Payment{}
			err := proto.Unmarshal(data, pay)
			if err != nil {
				return nil
			}
			newMsg.Payload = pay
		}
	}
	if pbMsg.GetError() != nil {
		newMsg.Error = &Error{
			Code:    pbMsg.GetError().GetCode(),
			Message: pbMsg.GetError().GetMessage(),
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
	if this.Error != nil {
		msg.Error = new(pb.Error)
		msg.Error.Code = this.Error.Code
		msg.Error.Message = this.Error.Message
	}
	return msg
}
