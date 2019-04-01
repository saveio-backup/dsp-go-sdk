package message

import (
	"github.com/gogo/protobuf/proto"
	"github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message/pb"
	"github.com/oniio/dsp-go-sdk/network/message/types/block"
	"github.com/oniio/dsp-go-sdk/network/message/types/file"
	"github.com/oniio/dsp-go-sdk/network/message/types/payment"
)

func MessageHeader() *Header {
	return &Header{
		Version: common.MESSAGE_VERSION,
	}
}

func NewEmptyMsg() *Message {
	msg := &Message{
		Header: MessageHeader(),
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
func NewBlockReqMsg(fileHash, blockHash string, index int32, walletAddress string, asset int32) *Message {
	msg := &Message{
		Header: MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK
	b := &block.Block{
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
func NewBlockMsg(index int32, fileHash, hash string, blockData, tag []byte, offset int64) *Message {
	msg := &Message{
		Header: MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK
	b := &block.Block{
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

// NewFileMsg file msg
func NewFileMsg(file *file.File, errorCode int32) *Message {
	msg := &Message{
		Header: MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_FILE
	msg.Payload = file
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
	data, err := proto.Marshal(msg.ToProtoMsg())
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}

// NewFileFetchAsk
func NewFileFetchAsk(hash string, blkHashes []string, walletAddr, prefix string) *Message {
	f := &file.File{
		Hash:        hash,
		BlockHashes: blkHashes,
		Operation:   common.FILE_OP_FETCH_ASK,
		Prefix:      prefix,
		PayInfo: &file.Payment{
			WalletAddress: walletAddr,
		},
	}
	return NewFileMsg(f, common.MSG_ERROR_CODE_NONE)
}

// NewFileFetchAck
func NewFileFetchAck(hash string) *Message {
	f := &file.File{
		Hash:      hash,
		Operation: common.FILE_OP_FETCH_ACK,
	}
	return NewFileMsg(f, common.MSG_ERROR_CODE_NONE)
}

// NewFileFetchRdy
func NewFileFetchRdy(hash string) *Message {
	f := &file.File{
		Hash:      hash,
		Operation: common.FILE_OP_FETCH_RDY,
	}
	return NewFileMsg(f, common.MSG_ERROR_CODE_NONE)
}

// NewFileDownloadAsk
func NewFileDownloadAsk(hash, walletAddr string, asset int32) *Message {
	f := &file.File{
		Hash:      hash,
		Operation: common.FILE_OP_DOWNLOAD_ASK,
		PayInfo: &file.Payment{
			WalletAddress: walletAddr,
			Asset:         asset,
		},
	}
	return NewFileMsg(f, common.MSG_ERROR_CODE_NONE)
}

// NewFileDownloadAck
func NewFileDownloadAck(hash string, blkHashes []string, walletAddr, prefix string, uintPrice uint64, errorCode int32) *Message {
	f := &file.File{
		Hash:        hash,
		BlockHashes: blkHashes,
		Operation:   common.FILE_OP_DOWNLOAD_ACK,
		Prefix:      prefix,
		PayInfo: &file.Payment{
			WalletAddress: walletAddr,
			UnitPrice:     uintPrice,
		},
	}
	return NewFileMsg(f, errorCode)
}

// NewFileDownload download file from server msg
func NewFileDownload(hash, walletAddr string, asset int32) *Message {
	f := &file.File{
		Hash:      hash,
		Operation: common.FILE_OP_DOWNLOAD,
		PayInfo: &file.Payment{
			WalletAddress: walletAddr,
			Asset:         asset,
		},
	}
	return NewFileMsg(f, common.MSG_ERROR_CODE_NONE)
}

func NewFileDownloadOk(hash, walletAddr string, asset int32) *Message {
	f := &file.File{
		Hash:      hash,
		Operation: common.FILE_OP_DOWNLOAD_OK,
		PayInfo: &file.Payment{
			WalletAddress: walletAddr,
			Asset:         asset,
		},
	}
	return NewFileMsg(f, common.MSG_ERROR_CODE_NONE)
}

// NewFileDelete
func NewFileDelete(hash, walletAddr string) *Message {
	f := &file.File{
		Hash:      hash,
		Operation: common.FILE_OP_DELETE,
		PayInfo: &file.Payment{
			WalletAddress: walletAddr,
		},
	}
	return NewFileMsg(f, common.MSG_ERROR_CODE_NONE)
}

// NewFileDeleteAck
func NewFileDeleteAck(hash string) *Message {
	f := &file.File{
		Hash:      hash,
		Operation: common.FILE_OP_DELETE_ACK,
	}
	return NewFileMsg(f, common.MSG_ERROR_CODE_NONE)
}

// NewPayment new payment msg
func NewPayment(sender, receiver string, paymentId int32, asset int32, amount uint64, fileHash string, errorCode int32) *Message {
	msg := &Message{
		Header: MessageHeader(),
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
	data, err := proto.Marshal(msg.ToProtoMsg())
	if err != nil {
		return nil
	}
	msg.Header.MsgLength = int32(len(data))
	return msg
}
