package utils

import (
	"crypto/sha256"
	"fmt"

	"github.com/saveio/carrier/crypto"
	"github.com/saveio/carrier/crypto/ed25519"
	"github.com/saveio/themis-go-sdk/utils"
	"github.com/saveio/themis/core/types"
	"github.com/saveio/themis/crypto/keypair"
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

func VerifyMsg(pubKey, data, sig []byte) error {
	publicKey, err := keypair.DeserializePublicKey(pubKey)
	if err != nil {
		return err
	}
	return utils.Verify(publicKey, data, sig)
}

func PublicKeyMatchAddress(pubKey []byte, address string) error {
	if len(pubKey) == 0 || len(address) == 0 {
		return fmt.Errorf("address is empty %v %v", pubKey, address)
	}
	publicKey, err := keypair.DeserializePublicKey(pubKey)
	if err != nil {
		return err
	}
	addr := types.AddressFromPubKey(publicKey)
	if err != nil {
		return err
	}
	if addr.ToBase58() != address {
		return fmt.Errorf("publicKey address %s not match %s", addr.ToBase58(), address)
	}
	return nil
}
