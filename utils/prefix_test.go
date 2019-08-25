package utils

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/saveio/themis/common"
)

func TestGenPrefix(t *testing.T) {
	addr, _ := common.AddressFromBase58("AZDn74qWSGQk8cyfD7DXmkRoyeJkZ4RbSx")
	fileSize := uint64(4 * 1024 * 1024 * 1024)
	prefix := &FilePrefix{
		Version:    1,
		Encrypt:    true,
		EncryptPwd: "1234",
		Owner:      addr,
		FileSize:   fileSize,
	}
	buf := prefix.Serialize()
	fmt.Printf("prefix-len: %v, size: %d\n", len(buf), fileSize)

	bufStr := string(buf)
	fmt.Printf("hex str: %s, len: %d\n", hex.EncodeToString(buf), len(bufStr))

	type TestJSON struct {
		Prefix string `json:"prefix"`
	}
	json1 := &TestJSON{
		Prefix: string(buf),
	}
	json1Buf, _ := json.Marshal(json1)

	json2 := &TestJSON{}
	json.Unmarshal(json1Buf, json2)
	fmt.Printf("json2.prefix :%s\n", json2.Prefix)
	fmt.Printf("json2.prefix :%v\n", []byte(json2.Prefix))

	fmt.Printf("prefix1: %v\n", buf)
	fmt.Printf("prefix2: %v\n", []byte(bufStr))

	prefix2 := &FilePrefix{}
	prefix2.Deserialize(buf)
	fmt.Printf("version: %d\n", prefix2.Version)
	fmt.Printf("encrypt: %t\n", prefix2.Encrypt)
	fmt.Printf("salt: %v\n", prefix2.EncryptSalt)
	fmt.Printf("hash: %v\n", prefix2.EncryptHash)
	fmt.Printf("owner: %s\n", prefix2.Owner.ToBase58())
	fmt.Printf("fileSize: %d\n", prefix2.FileSize)
	verify := VerifyEncryptPassword("456", prefix2.EncryptSalt, prefix2.EncryptHash)
	fmt.Printf("verify : %t\n", verify)
}
