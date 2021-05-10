package message

import (
	"math/rand"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message/pb"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/dsp-go-sdk/network/message/types/progress"
	dspUtils "github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/themis/common/log"
)

var msgRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// https://github.com/golang/go/issues/21835
var mu = new(sync.Mutex)

func GenMessageId() string {
	mu.Lock()
	defer mu.Unlock()
	return dspUtils.GenIdByTimestamp(msgRand)
}

func MessageHeader() *Header {
	return &Header{
		Version: common.MESSAGE_VERSION,
	}
}

func NewEmptyMsg(opts ...Option) *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
	}
	for _, opt := range opts {
		mOpt, ok := opt.(MsgOption)
		if ok {
			mOpt.apply(msg)
			continue
		}
		sOpt, ok := opt.(SignOption)
		if ok {
			sOpt.sign(msg)
			continue
		}
	}
	msg.Header.Type = common.MSG_TYPE_NONE
	data, err := msg.ToProtoMsg().(*pb.Message).XXX_Marshal(nil, false)
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewBlockMsg block req msg
func NewBlockReqMsg(sessionId, fileHash, blockHash string, index uint64, walletAddress string, asset int32,
	opts ...Option) *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK
	b := &block.Block{
		SessionId: sessionId,
		Index:     index,
		FileHash:  fileHash,
		Hash:      blockHash,
		Operation: common.BLOCK_OP_GET,
		Payment: &payment.Payment{
			Sender: walletAddress,
			Asset:  asset,
		},
	}
	msg.Payload = b
	for _, opt := range opts {
		mOpt, ok := opt.(MsgOption)
		if ok {
			mOpt.apply(msg)
			continue
		}
		sOpt, ok := opt.(SignOption)
		if ok {
			sOpt.sign(msg)
			continue
		}
	}
	data, err := msg.ToProtoMsg().(*pb.Message).XXX_Marshal(nil, false)
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewBlockMsg block ack msg
func NewBlockMsg(sessionId string, index uint64, fileHash, hash string, blockData, tag []byte, offset int64,
	opts ...Option) *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK
	b := &block.Block{
		SessionId: sessionId,
		Index:     index,
		FileHash:  fileHash,
		Hash:      hash,
		Data:      blockData,
		Tag:       tag,
		Operation: common.BLOCK_OP_NONE,
		Offset:    offset,
	}
	msg.Payload = b
	for _, opt := range opts {
		mOpt, ok := opt.(MsgOption)
		if ok {
			mOpt.apply(msg)
			continue
		}
		sOpt, ok := opt.(SignOption)
		if ok {
			sOpt.sign(msg)
			continue
		}
	}
	data, err := proto.Marshal(msg.ToProtoMsg())
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewBlockFlightsReqMsg blockflights req msg
func NewBlockFlightsReqMsg(blocks []*block.Block, timeStamp int64, opts ...Option) *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK_FLIGHTS
	flights := &block.BlockFlights{
		TimeStamp: timeStamp,
		Blocks:    blocks,
	}
	msg.Payload = flights
	for _, opt := range opts {
		mOpt, ok := opt.(MsgOption)
		if ok {
			mOpt.apply(msg)
			continue
		}
		sOpt, ok := opt.(SignOption)
		if ok {
			sOpt.sign(msg)
			continue
		}
	}
	data, err := msg.ToProtoMsg().(*pb.Message).XXX_Marshal(nil, false)
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewBlockFlightsMsg block ack msg
func NewBlockFlightsMsg(flights *block.BlockFlights, opts ...Option) *Message {
	return NewBlockFlightsMsgWithError(flights, 0, "", opts...)
}

func NewBlockFlightsMsgWithError(flights *block.BlockFlights, errorCode uint32,
	errorMsg string, opts ...Option) *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK_FLIGHTS
	if errorCode != 0 {
		msg.Error = &Error{
			Code:    errorCode,
			Message: errorMsg,
		}
	}
	if flights != nil {
		msg.Payload = flights
	}
	for _, opt := range opts {
		mOpt, ok := opt.(MsgOption)
		if ok {
			mOpt.apply(msg)
			continue
		}
		sOpt, ok := opt.(SignOption)
		if ok {
			sOpt.sign(msg)
			continue
		}
	}
	data, err := proto.Marshal(msg.ToProtoMsg())
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewFileMsg file msg
func NewFileMsg(fileHashStr string, op int32, opts ...Option) *Message {
	return NewFileMsgWithError(fileHashStr, op, common.MSG_ERROR_CODE_NONE, "", opts...)
}

func NewFileMsgWithError(fileHashStr string, op int32, errorCode uint32, errorMsg string, opts ...Option) *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_FILE
	f := &file.File{
		Operation: op,
		Hash:      fileHashStr,
	}
	signOpts := make([]SignOption, 0)
	msgOpts := make([]MsgOption, 0)
	for _, opt := range opts {
		fOpt, ok := opt.(FileMsgOption)
		sOpt, ok2 := opt.(SignOption)
		mOpt, ok3 := opt.(MsgOption)
		if ok2 {
			signOpts = append(signOpts, sOpt)
			continue
		}
		if ok3 {
			msgOpts = append(msgOpts, mOpt)
			continue
		}
		if !ok {
			continue
		}
		fOpt.apply(f)
	}
	log.Debugf("new file msg id %s, type %d", msg.MessageId, f.Operation)
	msg.Payload = f
	if errorCode != common.MSG_ERROR_CODE_NONE {
		msg.Error = &Error{
			Code:    errorCode,
			Message: errorMsg,
		}
	}
	for _, opt := range signOpts {
		opt.sign(msg)
	}
	for _, opt := range msgOpts {
		opt.apply(msg)
	}
	if msg.Header.MsgLength > 0 {
		return msg
	}
	data, err := proto.Marshal(msg.ToProtoMsg())
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewPaymentMsg. new payment msg
func NewPaymentMsg(sender, receiver string, paymentId int32, asset int32, amount uint64, txHash string,
	opts ...Option) *Message {
	return NewPaymentMsgWithError(sender, receiver, paymentId, asset, amount,
		txHash, common.MSG_ERROR_CODE_NONE, opts...)
}

func NewPaymentMsgWithError(sender, receiver string, paymentId int32, asset int32, amount uint64, txHash string,
	errorCode uint32, opts ...Option) *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_PAYMENT
	pay := &payment.Payment{
		Sender:    sender,
		Receiver:  receiver,
		PaymentId: paymentId,
		Asset:     asset,
		Amount:    amount,
		TxHash:    txHash,
	}
	msg.Payload = pay
	if errorCode != common.MSG_ERROR_CODE_NONE {
		errorMsg, ok := common.MSG_ERROR_MSG[errorCode]
		if !ok {
			errorMsg = "error"
		}
		msg.Error = &Error{
			Code:    errorCode,
			Message: errorMsg,
		}
	}
	for _, opt := range opts {
		mOpt, ok := opt.(SignOption)
		if !ok {
			continue
		}
		mOpt.sign(msg)
	}
	if msg.Header.MsgLength > 0 {
		return msg
	}
	data, err := proto.Marshal(msg.ToProtoMsg())
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

func NewProgressMsg(sender, fileHash string, operation int32, infos []*progress.ProgressInfo,
	opts ...Option) *Message {
	return NewProgressMsgWithError(sender, fileHash, operation, infos, 0, opts...)
}

func NewProgressMsgWithError(sender, fileHash string, operation int32, infos []*progress.ProgressInfo,
	errorCode uint32, opts ...Option) *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_PROGRESS
	msg.Payload = &progress.Progress{
		Hash:      fileHash,
		Sender:    sender,
		Operation: operation,
		Infos:     infos,
	}
	if errorCode != common.MSG_ERROR_CODE_NONE {
		errorMsg, ok := common.MSG_ERROR_MSG[errorCode]
		if !ok {
			errorMsg = "error"
		}
		msg.Error = &Error{
			Code:    errorCode,
			Message: errorMsg,
		}
	}
	for _, opt := range opts {
		mOpt, ok := opt.(SignOption)
		if !ok {
			continue
		}
		mOpt.sign(msg)
	}
	if msg.Header.MsgLength > 0 {
		return msg
	}
	data, err := proto.Marshal(msg.ToProtoMsg())
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}
