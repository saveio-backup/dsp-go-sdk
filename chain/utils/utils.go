package utils

import (
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/core/signature"
	"github.com/oniio/oniChain/core/types"
	"github.com/oniio/oniChain/crypto/keypair"
	nvutils "github.com/oniio/oniChain/smartcontract/service/native/utils"
)

func TransactionFromHexString(rawTx string) (*types.Transaction, error) {
	txData, err := hex.DecodeString(rawTx)
	if err != nil {
		return nil, err
	}
	return types.TransactionFromRawBytes(txData)
}

func AddressFromHexString(s string) (common.Address, error) {
	return common.AddressFromHexString(s)
}

func AddressParseFromBytes(b []byte) (common.Address, error) {
	return common.AddressParseFromBytes(b)
}

func AddressFromBase58(s string) (common.Address, error) {
	return common.AddressFromBase58(s)
}

func Uint256ParseFromBytes(f []byte) (common.Uint256, error) {
	return common.Uint256ParseFromBytes(f)
}

func Uint256FromHexString(s string) (common.Uint256, error) {
	return common.Uint256FromHexString(s)
}

func GetContractAddress(contractCode string) (common.Address, error) {
	code, err := hex.DecodeString(contractCode)
	if err != nil {
		return common.ADDRESS_EMPTY, fmt.Errorf("hex.DecodeString error:%s", err)
	}
	return common.AddressFromVmCode(code), nil
}

func GetAssetAddress(asset string) (common.Address, error) {
	var contractAddress common.Address
	switch strings.ToUpper(asset) {
	case "ONT":
		contractAddress = nvutils.OntContractAddress
	case "ONG":
		contractAddress = nvutils.OngContractAddress
	default:
		return common.ADDRESS_EMPTY, fmt.Errorf("asset:%s not equal ont or ong", asset)
	}
	return contractAddress, nil
}

//IsFileExist return is file is exist
func IsFileExist(file string) bool {
	_, err := os.Stat(file)
	return err == nil || os.IsExist(err)
}

func HasAlreadySig(data []byte, pk keypair.PublicKey, sigDatas [][]byte) bool {
	for _, sigData := range sigDatas {
		err := signature.Verify(pk, data, sigData)
		if err == nil {
			return true
		}
	}
	return false
}

func PubKeysEqual(pks1, pks2 []keypair.PublicKey) bool {
	if len(pks1) != len(pks2) {
		return false
	}
	size := len(pks1)
	if size == 0 {
		return true
	}
	pkstr1 := make([]string, 0, size)
	for _, pk := range pks1 {
		pkstr1 = append(pkstr1, hex.EncodeToString(keypair.SerializePublicKey(pk)))
	}
	pkstr2 := make([]string, 0, size)
	for _, pk := range pks2 {
		pkstr2 = append(pkstr2, hex.EncodeToString(keypair.SerializePublicKey(pk)))
	}
	sort.Strings(pkstr1)
	sort.Strings(pkstr2)
	for i := 0; i < size; i++ {
		if pkstr1[i] != pkstr2[i] {
			return false
		}
	}
	return true
}
