package message

import (
	"crypto/sha256"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message/pb"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/dsp-go-sdk/network/message/types/progress"
	"github.com/saveio/dsp-go-sdk/utils/crypto"
	"github.com/saveio/themis/common/log"
)

type Header struct {
	Version   string
	Type      string
	MsgLength int32
}

type Error struct {
	Code    uint32
	Message string
}

type Signature struct {
	SigData   []byte
	PublicKey []byte
}

type Message struct {
	MessageId string
	Syn       string
	Header    *Header
	Payload   proto.Message
	Sig       *Signature
	Error     *Error
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
		case common.MSG_TYPE_BLOCK_FLIGHTS:
			blk := &block.BlockFlights{}
			err := proto.Unmarshal(data, blk)
			if err != nil {
				return nil
			}
			newMsg.Payload = blk
		case common.MSG_TYPE_FILE:
			// verify signature
			if valid := isMsgVerified(pbMsg); !valid {
				log.Debugf("file msg has wrong signature")
				return nil
			}
			file := &file.File{}
			err := proto.Unmarshal(data, file)
			if err != nil {
				return nil
			}
			if file.PayInfo == nil {
				log.Debugf("file msg missing pay info field")
				return nil
			}
			if err := crypto.PublicKeyMatchAddress(pbMsg.Sig.PublicKey, file.PayInfo.WalletAddress); err != nil {
				log.Debugf("receive a invalid file msg, err: %v", err)
				return nil
			}
			newMsg.Payload = file
		case common.MSG_TYPE_PAYMENT:
			// verify signature
			if valid := isMsgVerified(pbMsg); !valid {
				log.Debugf("payment msg has wrong signature")
				return nil
			}
			pay := &payment.Payment{}
			err := proto.Unmarshal(data, pay)
			if err != nil {
				return nil
			}
			if err := crypto.PublicKeyMatchAddress(pbMsg.Sig.PublicKey, pay.Sender); err != nil {
				log.Debugf("receive a invalid payment msg")
				return nil
			}
			newMsg.Payload = pay
		case common.MSG_TYPE_PROGRESS:
			// verify signature
			if valid := isMsgVerified(pbMsg); !valid {
				log.Debugf("payment msg has wrong signature")
				return nil
			}
			progress := &progress.Progress{}
			err := proto.Unmarshal(data, progress)
			if err != nil {
				return nil
			}
			if err := crypto.PublicKeyMatchAddress(pbMsg.Sig.PublicKey, progress.Sender); err != nil {
				log.Debugf("receive a invalid payment msg")
				return nil
			}
			newMsg.Payload = progress
		}
	}
	if pbMsg.GetError() != nil {
		newMsg.Error = &Error{
			Code:    pbMsg.GetError().GetCode(),
			Message: pbMsg.GetError().GetMessage(),
		}
	}
	newMsg.MessageId = pbMsg.GetMsgId()
	newMsg.Syn = pbMsg.GetSyn()
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
	if this.Sig != nil {
		msg.Sig = new(pb.Signature)
		msg.Sig.SigData = this.Sig.SigData
		msg.Sig.PublicKey = this.Sig.PublicKey
	}
	if this.Error != nil {
		msg.Error = new(pb.Error)
		msg.Error.Code = this.Error.Code
		msg.Error.Message = this.Error.Message
	}
	msg.MsgId = this.MessageId
	msg.Syn = this.Syn
	return msg
}

func isMsgVerified(pbMsg *pb.Message) bool {
	if pbMsg.Sig == nil || len(pbMsg.Sig.SigData) == 0 || len(pbMsg.Sig.PublicKey) == 0 {
		log.Debugf("receive a no signed file msg")
		return false
	}
	data := pbMsg.GetData()

	// msg data length too large
	if len(data) < common.MAX_SIG_DATA_LEN {
		err := crypto.VerifyMsg(pbMsg.Sig.PublicKey, data, pbMsg.Sig.SigData)
		if err != nil {
			log.Errorf("verified failed %x %x %x", pbMsg.Sig.PublicKey, data, pbMsg.Sig.SigData)
			return false
		}
		return true
	}

	hashData := sha256.Sum256(data[:common.MAX_SIG_DATA_LEN])
	err := crypto.VerifyMsg(pbMsg.Sig.PublicKey, hashData[:], pbMsg.Sig.SigData)
	if err != nil {
		log.Errorf("verified failed %x %x %x", pbMsg.Sig.PublicKey, hashData, pbMsg.Sig.SigData)
		return false
	}

	return true
}
