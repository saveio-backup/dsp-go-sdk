package base

import "github.com/saveio/dsp-go-sdk/store"

type InfoOption interface {
	apply(*store.TaskInfo)
}

type OptionFunc func(*store.TaskInfo)

func (f OptionFunc) apply(info *store.TaskInfo) {
	f(info)
}

func TaskType(taskType store.TaskType) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.Type = taskType
	})
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

func BlocksRoot(blocksRoot string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.BlocksRoot = blocksRoot
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

func StoreType(storeType uint32) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.StoreType = storeType
	})
}

func CopyNum(copyNum uint32) InfoOption {
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

func InOrder(inOrder bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.InOrder = inOrder
	})
}

func OnlyBlock(onlyBlock bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.OnlyBlock = onlyBlock
	})
}

func TaskState(taskState store.TaskState) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.TaskState = taskState
	})
}

func TransferState(transferState uint32) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.TranferState = transferState
	})
}

func TotalBlockCnt(cnt uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.TotalBlockCount = cnt
	})
}

func ProveParams(params []byte) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.ProveParams = params
	})
}

func StoreTx(tx string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.StoreTx = tx
	})
}

func StoreTxHeight(storeTxHeight uint32) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.StoreTxHeight = storeTxHeight
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

func ReferId(referId string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.ReferId = referId
	})
}

func PrimaryNodes(pm []string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.PrimaryNodes = pm
	})
}

func CandidateNodes(cn []string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.CandidateNodes = cn
	})
}

func DecryptPwd(decryptPwd string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.DecryptPwd = decryptPwd
	})
}

func Asset(asset int32) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.Asset = asset
	})
}

func MaxPeerCnt(maxPeerCnt int) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.MaxPeerCnt = maxPeerCnt
	})
}

func Free(free bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.Free = free
	})
}

func SetFileName(setFileName bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.SetFileName = setFileName
	})
}

func RealFileSize(realFileSize uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.RealFileSize = realFileSize
	})
}

func FileSize(fileSize uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.FileSize = fileSize
	})
}

func ExpiredHeight(expiredHeight uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.ExpiredHeight = expiredHeight
	})
}

func ProveInterval(proveInterval uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.ProveInterval = proveInterval
	})
}

func ProveLevel(proveLevel uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.ProveLevel = proveLevel
	})
}

func Privilege(privilege uint64) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.Privilege = privilege
	})
}

func Encrypt(encrypt bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.Encrypt = encrypt
	})
}

func RegisterDNS(registerDNS bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.RegisterDNS = registerDNS
	})
}

func BindDNS(bindDNS bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.BindDNS = bindDNS
	})
}

func Share(share bool) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.Share = share
	})
}

func EncryptPassword(encryptPassword []byte) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.EncryptPassword = encryptPassword
	})
}

func WhiteList(whiteList []*store.WhiteList) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.WhiteList = whiteList
	})
}

func NodeHostAddrs(walletHostAddrs map[string]string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		info.NodeHostAddrs = walletHostAddrs
	})
}

func PeerToSessionIds(peerToSessionIds map[string]string) InfoOption {
	return OptionFunc(func(info *store.TaskInfo) {
		if info.PeerToSessionIds == nil {
			info.PeerToSessionIds = make(map[string]string)
		}
		for key, val := range peerToSessionIds {
			info.PeerToSessionIds[key] = val
		}
	})
}
