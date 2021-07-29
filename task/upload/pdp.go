package upload

import (
	"bytes"

	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/savefs/pdp"
)

func (this *UploadTask) GeneratePdpTags(hashes []string, fileID pdp.FileID) ([]pdp.Tag, error) {
	p := pdp.NewPdp(0)
	tags := make([]pdp.Tag, 0)
	for index, hash := range hashes {
		block := this.Mgr.Fs().GetBlock(hash)
		blockData := this.Mgr.Fs().BlockDataOfAny(block)
		if len(blockData) == 0 {
			log.Warnf("get empty block of %s %d", hash, index)
		}

		tag, err := p.GenerateTag([]pdp.Block{blockData}, fileID)
		if err != nil {
			return nil, err
		}
		tags = append(tags, tag...)
	}
	return tags, nil
}

func (this *UploadTask) GetMerkleRootForTag(fileID pdp.FileID, tags []pdp.Tag) ([]byte, error) {
	p := pdp.NewPdp(0)
	nodes := make([]*pdp.MerkleNode, 0)
	for index, tag := range tags {
		node := pdp.InitNodeWithData(tag[:], uint64(index))
		nodes = append(nodes, node)
	}
	err := p.InitMerkleTreeForFile(fileID, nodes)
	if err != nil {
		return nil, err
	}
	return p.GetRootHashForFile(fileID)
}

func GetFileIDFromFileHash(fileHash string) pdp.FileID {
	var fileID pdp.FileID
	reader := bytes.NewReader(([]byte)(fileHash))
	reader.Read(fileID[:])
	return fileID
}
