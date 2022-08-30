package crypto

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	ethCom "github.com/ethereum/go-ethereum/common"
	ethCrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/saveio/carrier/crypto"
	"github.com/saveio/carrier/crypto/ed25519"
	"github.com/saveio/themis-go-sdk/utils"
	"github.com/saveio/themis/account"
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

func NewNetworkKeyPairWithAccount(acc *account.Account) *crypto.KeyPair {
	pub := keypair.SerializePublicKey(acc.PubKey())
	priv := keypair.SerializePrivateKey(acc.PrivKey())
	return &crypto.KeyPair{
		PublicKey:  pub,
		PrivateKey: priv,
	}
}

func AddressFromPubkeyHex(pubKeyHex string) string {
	pubKeyBuf, err := hex.DecodeString(pubKeyHex)
	if err != nil {
		return pubKeyHex
	}
	pubK, err := keypair.DeserializePublicKey(pubKeyBuf)
	if err != nil {
		return pubKeyHex
	}
	addr := types.AddressFromPubKey(pubK)
	return addr.ToBase58()
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

func VerifyMsgForETH(pubKey, data, sig []byte) error {
	hashData := sha256.Sum256(data[:ethCrypto.DigestLength])
	signature := ethCrypto.VerifySignature(pubKey, hashData[:], sig[:ethCrypto.RecoveryIDOffset])
	if !signature {
		return fmt.Errorf("verify signature failed")
	}
	return nil
}

func PublicKeyMatchAddress(pubKey []byte, address string) error {
	if len(pubKey) == 0 || len(address) == 0 {
		return fmt.Errorf("address is empty %v %v", pubKey, address)
	}

	// unmarshal ont address
	publicKey, err := keypair.DeserializePublicKey(pubKey)
	if err != nil {
		return err
	}
	addr := types.AddressFromPubKey(publicKey)
	if err != nil {
		return err
	}

	// unmarshal eth address
	ethAddr := ethCom.Address{}
	ethPubKey, err := ethCrypto.UnmarshalPubkey(pubKey)
	if err == nil {
		ethAddr = ethCrypto.PubkeyToAddress(*ethPubKey)
	}

	if addr.ToBase58() != address && ethAddr.String() != address {
		return fmt.Errorf("publicKey ont address %s, eth address %s, not match %s", addr.ToBase58(), ethAddr, address)
	}
	return nil
}

func StringToSha256Hex(str string) string {
	hash := sha256.Sum256([]byte(str))
	hexStr := hex.EncodeToString(hash[:])
	return hexStr
}
