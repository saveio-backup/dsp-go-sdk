package message

import (
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
func NewBlockRepMsg(fileHash, blockHash string) *Message {
	msg := &Message{
		Header: MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK_REQ
	b := &block.Block{
		FileHash: fileHash,
		Hash:     blockHash,
	}
	data, err := b.XXX_Marshal(nil, false)
	if err != nil {
		return nil
	}
	msg.Header.Length = int32(len(data))
	if len(data) > 0 {
		payload := new(pb.Payload)
		payload.Data = data
		msg.Payload = payload
	}
	return msg
}

// NewBlockMsg block ack msg
func NewBlockMsg(index int32, fileHash, hash string, blockData, tag []byte) *Message {
	msg := &Message{
		Header: MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_BLOCK
	b := &block.Block{
		Index:    index,
		FileHash: fileHash,
		Hash:     hash,
		Data:     blockData,
		Tag:      tag,
	}
	data, err := b.XXX_Marshal(nil, false)
	if err != nil {
		return nil
	}
	msg.Header.Length = int32(len(data))
	if len(data) > 0 {
		payload := new(pb.Payload)
		payload.Data = data
		msg.Payload = payload
	}
	return msg
}

// NewFileMsg file msg
func NewFileMsg(hash string, blkHashes []string, op int32, asset int32, pricePerBlk uint64) *Message {
	msg := &Message{
		Header: MessageHeader(),
	}
	msg.Header.Type = common.MSG_TYPE_FILE
	f := &file.File{
		Hash:        hash,
		BlockHashes: blkHashes,
		Operation:   op,
		PayInfo: &file.Payment{
			Asset: asset,
			Price: pricePerBlk,
		},
	}
	data, err := f.XXX_Marshal(nil, false)
	if err != nil {
		return nil
	}
	msg.Header.Length = int32(len(data))
	if len(data) > 0 {
		payload := new(pb.Payload)
		payload.Data = data
		msg.Payload = payload
	}
	return msg
}

// NewFileGiveMsg give file to server msg
func NewFileGiveMsg(hash string, blkHashes []string) *Message {
	return NewFileMsg(hash, blkHashes, common.FILE_OP_GIVE, common.ASSET_NONE, 0)
}

// NewFileFetchMsg fetch file from client msg
func NewFileFetchMsg(hash string, blkHashes []string) *Message {
	return NewFileMsg(hash, blkHashes, common.FILE_OP_FETCH, common.ASSET_NONE, 0)
}

// NewFileDownloadMsg download file from server msg
func NewFileDownloadMsg(hash string, blkHashes []string, asset int32, pricePerBlk uint64) *Message {
	return NewFileMsg(hash, blkHashes, common.FILE_OP_DOWNLOAD, asset, pricePerBlk)
}
