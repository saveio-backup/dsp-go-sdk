package upload

import (
	"bytes"

	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/savefs/pdp"
)

type GenearatePdpProgress struct {
	Total     int
	Generated int
}

func (this *UploadTask) GeneratePdpTags(hashes []string, fileID pdp.FileID, notify chan GenearatePdpProgress) ([]pdp.Tag, error) {
	p := pdp.NewPdp(0)
	tags := make([]pdp.Tag, 0)
	fs := this.Mgr.Fs()
	total := len(hashes)
	for index, hash := range hashes {
		block := fs.GetBlock(hash)
		blockData := fs.BlockDataOfAny(block)
		if len(blockData) == 0 {
			log.Warnf("get empty block of %s %d", hash, index)
		}

		tag, err := p.GenerateTag([]pdp.Block{blockData}, fileID)
		if err != nil {
			return nil, err
		}
		if notify != nil {
			go func(generated int) {
				notify <- GenearatePdpProgress{
					Generated: generated,
					Total:     total,
				}
			}(index + 1)
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
