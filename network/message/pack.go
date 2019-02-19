package message

import (
	"github.com/gogo/protobuf/proto"
	"github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message/pb"
	"github.com/oniio/dsp-go-sdk/network/message/types/block"
	"github.com/oniio/dsp-go-sdk/network/message/types/file"
)

func MessageHeader() *Header {
	return &Header{
		Version: common.MESSAGE_VERSION,
	}
}

// NewBlockMsg block req msg
func NewBlockReqMsg(fileHash, blockHash string, index int32) *Message {
	msg := &Message{
		Header: MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK
	b := &block.Block{
		Index:     index,
		FileHash:  fileHash,
		Hash:      blockHash,
		Operation: common.BLOCK_OP_GET,
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
func NewFileMsg(hash string, blkHashes []string, op int32, walletAddr, prefix string, asset int32, pricePerBlk uint64, errorCode int32) *Message {
	msg := &Message{
		Header: MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_FILE
	f := &file.File{
		Hash:        hash,
		BlockHashes: blkHashes,
		Operation:   op,
		Prefix:      prefix,
		PayInfo: &file.Payment{
			WalletAddress: walletAddr,
			Asset:         asset,
			Price:         pricePerBlk,
		},
	}
	msg.Payload = f
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
	return NewFileMsg(hash, blkHashes, common.FILE_OP_FETCH_ASK, walletAddr, prefix, common.ASSET_NONE, 0, common.MSG_ERROR_CODE_NONE)
}

// NewFileFetchAck
func NewFileFetchAck(hash string) *Message {
	return NewFileMsg(hash, nil, common.FILE_OP_FETCH_ACK, "", "", common.ASSET_NONE, 0, common.MSG_ERROR_CODE_NONE)
}

// NewFileFetchRdy
func NewFileFetchRdy(hash string) *Message {
	return NewFileMsg(hash, nil, common.FILE_OP_FETCH_RDY, "", "", common.ASSET_NONE, 0, common.MSG_ERROR_CODE_NONE)
}

// NewFileDownload download file from server msg
func NewFileDownload(hash, walletAddr string, asset int32, pricePerBlk uint64) *Message {
	return NewFileMsg(hash, nil, common.FILE_OP_DOWNLOAD, walletAddr, "", asset, pricePerBlk, common.MSG_ERROR_CODE_NONE)
}

// NewFileDownloadAck
func NewFileDownloadAck(hash string, blkHashes []string, walletAddr, prefix string) *Message {
	return NewFileMsg(hash, blkHashes, common.FILE_OP_DOWNLOAD_ACK, walletAddr, prefix, common.ASSET_NONE, 0, common.MSG_ERROR_CODE_NONE)
}

// NewFileDownloadAckErr
func NewFileDownloadAckErr(hash string, errorCode int32) *Message {
	return NewFileMsg(hash, nil, common.FILE_OP_DOWNLOAD_ACK, "", "", common.ASSET_NONE, 0, errorCode)
}

// NewFileDelete
func NewFileDelete(hash, walletAddr string) *Message {
	return NewFileMsg(hash, nil, common.FILE_OP_DELETE, walletAddr, "", common.ASSET_NONE, 0, common.MSG_ERROR_CODE_NONE)
}

// NewFileDeleteAck
func NewFileDeleteAck(hash string) *Message {
	return NewFileMsg(hash, nil, common.FILE_OP_DELETE_ACK, "", "", common.ASSET_NONE, 0, common.MSG_ERROR_CODE_NONE)
}
