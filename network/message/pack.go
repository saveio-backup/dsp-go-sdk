package message

import (
	"crypto/sha256"
	"math/rand"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message/pb"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/themis-go-sdk/utils"
	"github.com/saveio/themis/account"
	"github.com/saveio/themis/crypto/keypair"
)

func GenMessageId() uint64 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Uint64()
}

func MessageHeader() *Header {
	return &Header{
		Version: common.MESSAGE_VERSION,
	}
}

func NewEmptyMsg() *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
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
func NewBlockReqMsg(sessionId, fileHash, blockHash string, index int32, walletAddress string, asset int32) *Message {
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
	data, err := msg.ToProtoMsg().(*pb.Message).XXX_Marshal(nil, false)
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewBlockMsg block ack msg
func NewBlockMsg(sessionId string, index int32, fileHash, hash string, blockData, tag []byte, offset int64) *Message {
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
	data, err := proto.Marshal(msg.ToProtoMsg())
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewBlockFlightsReqMsg blockflights req msg
func NewBlockFlightsReqMsg(blocks []*block.Block, timeStamp int64) *Message {
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
	data, err := msg.ToProtoMsg().(*pb.Message).XXX_Marshal(nil, false)
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewBlockFlightsMsg block ack msg
func NewBlockFlightsMsg(flights *block.BlockFlights) *Message {
	msg := &Message{
		MessageId: GenMessageId(),
		Header:    MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK_FLIGHTS
	msg.Payload = flights
	data, err := proto.Marshal(msg.ToProtoMsg())
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

type Option interface{}

type FileMsgOption interface {
	apply(*file.File)
}

type PaymentMsgOption interface {
	apply(*payment.Payment)
}

type SignOption interface {
	sign(*Message)
}

type msgOptionFunc func(*Message)

func (f msgOptionFunc) sign(m *Message) {
	f(m)
}

type optionFunc func(*file.File)

func (f optionFunc) apply(o *file.File) {
	f(o)
}

type paymentOptionFunc func(*payment.Payment)

func (f paymentOptionFunc) apply(p *payment.Payment) {
	f(p)
}

func WithSign(acc *account.Account) SignOption {
	return msgOptionFunc(func(msg *Message) {
		data, err := proto.Marshal(msg.ToProtoMsg())
		if err != nil {
			return
		}
		msg.Header.MsgLength = int32(len(data))
		var sigData []byte
		if msg.Header.MsgLength < common.MAX_SIG_DATA_LEN {
			sigData, err = utils.Sign(acc, data)
			if err != nil {
				return
			}
		} else {
			hashData := sha256.Sum256(data)
			sigData, err = utils.Sign(acc, hashData[:])
			if err != nil {
				return
			}
		}
		msg.Sig = &Signature{
			SigData:   sigData,
			PublicKey: keypair.SerializePublicKey(acc.PublicKey),
		}
		return
	})
}

func WithSessionId(sessionId string) FileMsgOption {
	return optionFunc(func(f *file.File) {
		f.SessionId = sessionId
	})
}

func WithHash(hash string) FileMsgOption {
	return optionFunc(func(f *file.File) {
		f.Hash = hash
	})
}

func WithBlockHashes(blockHashes []string) FileMsgOption {
	return optionFunc(func(f *file.File) {
		f.BlockHashes = blockHashes
	})
}

func WithOperation(operation int32) FileMsgOption {
	return optionFunc(func(f *file.File) {
		f.Operation = operation
	})
}

func WithPrefix(prefix []byte) FileMsgOption {
	return optionFunc(func(f *file.File) {
		f.Prefix = prefix
	})
}

func WithChunkSize(chunkSize int32) FileMsgOption {
	return optionFunc(func(f *file.File) {
		f.ChunkSize = chunkSize
	})
}

func WithWalletAddress(walletAddr string) FileMsgOption {
	return optionFunc(func(f *file.File) {
		if f.PayInfo == nil {
			f.PayInfo = &file.Payment{}
		}
		f.PayInfo.WalletAddress = walletAddr
	})
}

func WithAsset(asset int32) FileMsgOption {
	return optionFunc(func(f *file.File) {
		if f.PayInfo == nil {
			f.PayInfo = &file.Payment{}
		}
		f.PayInfo.Asset = asset
	})
}

func WithUnitPrice(unitPrice uint64) FileMsgOption {
	return optionFunc(func(f *file.File) {
		if f.PayInfo == nil {
			f.PayInfo = &file.Payment{}
		}
		f.PayInfo.UnitPrice = unitPrice
	})
}

func WithTxHash(txHash string) FileMsgOption {
	return optionFunc(func(f *file.File) {
		if f.Tx == nil {
			f.Tx = &file.Tx{}
		}
		f.Tx.Hash = txHash
	})
}

func WithTxHeight(txHeight uint64) FileMsgOption {
	return optionFunc(func(f *file.File) {
		if f.Tx == nil {
			f.Tx = &file.Tx{}
		}
		f.Tx.Height = txHeight
	})
}

func WithBreakpointHash(hash string) FileMsgOption {
	return optionFunc(func(f *file.File) {
		if f.Breakpoint == nil {
			f.Breakpoint = &file.Breakpoint{}
		}
		f.Breakpoint.Hash = hash
	})
}

func WithBreakpointIndex(index uint64) FileMsgOption {
	return optionFunc(func(f *file.File) {
		if f.Breakpoint == nil {
			f.Breakpoint = &file.Breakpoint{}
		}
		f.Breakpoint.Index = index
	})
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
	for _, opt := range opts {
		fOpt, ok := opt.(FileMsgOption)
		if !ok {
			continue
		}
		fOpt.apply(f)
	}
	msg.Payload = f
	if errorCode != common.MSG_ERROR_CODE_NONE {
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

// NewPaymentMsg. new payment msg
func NewPaymentMsg(sender, receiver string, paymentId int32, asset int32, amount uint64, fileHash string, opts ...Option) *Message {
	return NewPaymentMsgWithError(sender, receiver, paymentId, asset, amount, fileHash, common.MSG_ERROR_CODE_NONE, opts...)
}

func NewPaymentMsgWithError(sender, receiver string, paymentId int32, asset int32, amount uint64, fileHash string, errorCode uint32, opts ...Option) *Message {
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
		FileHash:  fileHash,
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
