package utils

import (
	"crypto/sha256"

	"github.com/saveio/carrier/crypto"
	"github.com/saveio/carrier/crypto/ed25519"
)

type accountReader struct {
	PublicKey []byte
}

func (this accountReader) Read(buf []byte) (int, error) {
	bufs := make([]byte, 0)
	hash := sha256.Sum256(this.PublicKey)
	bufs = append(bufs, hash[:]...)
	for i, _ := range buf {
		if i < len(bufs) {
			buf[i] = bufs[i]
			continue
		}
		buf[i] = 0
	}
	return len(buf), nil
}

func NewNetworkEd25519KeyPair(pubKey, salt []byte) *crypto.KeyPair {
	tkPub, tkPri, err := ed25519.GenerateKey(&accountReader{
		PublicKey: append(pubKey, salt...),
	})
	if err != nil {
		return nil
	}
	return &crypto.KeyPair{
		PublicKey:  tkPub,
		PrivateKey: tkPri,
	}
}
