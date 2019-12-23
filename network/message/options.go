package message

import (
	"crypto/sha256"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/themis-go-sdk/utils"
	"github.com/saveio/themis/account"
	"github.com/saveio/themis/crypto/keypair"
)

type Option interface{}

type FileMsgOption interface {
	apply(*file.File)
}

type PaymentMsgOption interface {
	apply(*payment.Payment)
}

type MsgOption interface {
	apply(*Message)
}

type SignOption interface {
	sign(*Message)
}

type msgOptionFunc func(*Message)

func (f msgOptionFunc) sign(m *Message) {
	f(m)
}

func (f msgOptionFunc) apply(m *Message) {
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

func WithSyn(syn string) MsgOption {
	return msgOptionFunc(func(msg *Message) {
		msg.Syn = syn
	})
}

func WithSign(acc *account.Account) SignOption {
	return msgOptionFunc(func(msg *Message) {
		data, err := proto.Marshal(msg.Payload)
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
			hashData := sha256.Sum256(data[:common.MAX_SIG_DATA_LEN])
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

func WithTotalBlockCount(totalBlockCount int32) FileMsgOption {
	return optionFunc(func(f *file.File) {
		f.TotalBlockCount = totalBlockCount
	})
}

func ChainId(chainId uint32) FileMsgOption {
	return optionFunc(func(f *file.File) {
		if f.ChainInfo == nil {
			f.ChainInfo = &file.Chain{}
		}
		f.ChainInfo.Id = chainId
	})
}

func ChainHeight(blockHeight uint32) FileMsgOption {
	return optionFunc(func(f *file.File) {
		if f.ChainInfo == nil {
			f.ChainInfo = &file.Chain{}
		}
		f.ChainInfo.Height = blockHeight
	})
}
