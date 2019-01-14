package dsp

import (
	"errors"
	"fmt"

	"github.com/oniio/dsp-go-sdk/common"
	fs "github.com/ontio/ont-ipfs-go-sdk"
)

// UploadFile upload file logic
func (this *Dsp) UploadFile(filePath string, opt *common.UploadOption, progress chan *common.UploadingInfo) error {
	if err := uploadOptValid(filePath, opt); err != nil {
		return err
	}
	root, nodes, err := fs.NodesFromFile(filePath, this.Chain.Native.Fs.DefAcc.Address.ToBase58(), opt.Encrypt, opt.EncryptPassword)
	fmt.Printf("root:%v, nodes:%v, err:%v", root, nodes, err)
	return nil
}

// uploadOptValid check upload opt valid
func uploadOptValid(filePath string, opt *common.UploadOption) error {
	if !common.FileExisted(filePath) {
		return errors.New("file not exist")
	}
	if opt.Encrypt && len(opt.EncryptPassword) == 0 {
		return errors.New("encrypt password is missing")
	}
	return nil
}
