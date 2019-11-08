package task

import "github.com/saveio/dsp-go-sdk/store"

type InfoOption interface {
	apply(*store.TaskInfo)
}

type OptionFunc func(*store.TaskInfo)

func (f OptionFunc) apply(info *store.TaskInfo) {
	f(info)
}

func FileName(fileName string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.FileName = fileName
	})
}

func FileHash(fileHash string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.FileHash = fileHash
	})
}

func Prefix(prefix string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.Prefix = []byte(prefix)
	})
}
func FileOwner(fileOwner string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.FileOwner = fileOwner
	})
}

func Walletaddr(walletAddr string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.WalletAddress = walletAddr
	})
}

func FilePath(filePath string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.FilePath = filePath
	})
}

func SimpleCheckSum(checksum string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.SimpleChecksum = checksum
	})
}

func StoreType(storeType uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.StoreType = storeType
	})
}

func CopyNum(copyNum uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.CopyNum = copyNum
	})
}

func Url(url string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.Url = url
	})
}

func Owner(owner string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.FileOwner = owner
	})
}

func Inorder(inOrder bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.InOrder = inOrder
	})
}

func OnlyBlock(onlyBlock bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.OnlyBlock = onlyBlock
	})
}

func TransferState(transferState uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.TranferState = transferState
	})
}

func TotalBlockCnt(cnt uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.TotalBlockCount = cnt
	})
}

func PrivateKey(priKey []byte) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.ProvePrivKey = priKey
	})
}

func StoreTx(tx string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.StoreTx = tx
	})
}

func WhiteListTx(whiteListTx string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.WhitelistTx = whiteListTx
	})
}

func RegUrlTx(regUrlTx string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.RegisterDNSTx = regUrlTx
	})
}

func BindUrlTx(bindUrlTx string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.BindDNSTx = bindUrlTx
	})
}

func (this *Task) SetInfoWithOptions(opts ...InfoOption) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	for _, opt := range opts {
		opt.apply(this.info)
	}
	return this.db.SaveFileInfo(this.info)
}
