package ethereum

import (
	"crypto/sha256"
	"github.com/itchyny/base58-go"
	"math/big"
)

func ETHAddressToBase58(address []byte) string {
	data := append([]byte{23}, address[:]...)
	temp := sha256.Sum256(data)
	temps := sha256.Sum256(temp[:])
	data = append(data, temps[0:4]...)

	bi := new(big.Int).SetBytes(data).String()
	encoded, _ := base58.BitcoinEncoding.Encode([]byte(bi))
	return string(encoded)
}
