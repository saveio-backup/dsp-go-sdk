package utils

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
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

	prefix2 := &FilePrefix{}
	prefix2.Deserialize(buf)
	fmt.Printf("version: %d\n", prefix2.Version)
	fmt.Printf("encrypt: %t\n", prefix2.Encrypt)
	fmt.Printf("salt: %v\n", prefix2.EncryptSalt)
	fmt.Printf("hash: %v\n", prefix2.EncryptHash)
	fmt.Printf("owner: %s\n", prefix2.Owner.ToBase58())
	fmt.Printf("fileSize: %d\n", prefix2.FileSize)
	verify := VerifyEncryptPassword("1234", prefix2.EncryptSalt, prefix2.EncryptHash)
	fmt.Printf("verify : %t\n", verify)
}

func TestBase64EncodePrefix(t *testing.T) {
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
	encodedLen := base64.StdEncoding.EncodedLen(74)
	fmt.Printf("len: %d\n", encodedLen)
	fmt.Printf("prefix-len: %v, size: %d\n", len(buf), fileSize)
	base64Str := base64.StdEncoding.EncodeToString(buf)
	fmt.Printf("str: %s, len: %d\n", base64Str, len(base64Str))
	buf2, _ := base64.StdEncoding.DecodeString(base64Str)

	prefix2 := &FilePrefix{}
	prefix2.Deserialize(buf2)
	fmt.Printf("version: %d\n", prefix2.Version)
	fmt.Printf("encrypt: %t\n", prefix2.Encrypt)
	fmt.Printf("salt: %v\n", prefix2.EncryptSalt)
	fmt.Printf("hash: %v\n", prefix2.EncryptHash)
	fmt.Printf("owner: %s\n", prefix2.Owner.ToBase58())
	fmt.Printf("fileSize: %d\n", prefix2.FileSize)
	verify := VerifyEncryptPassword("456", prefix2.EncryptSalt, prefix2.EncryptHash)
	fmt.Printf("verify : %t\n", verify)
}

func TestMarshalPrefix(t *testing.T) {
	addr, _ := common.AddressFromBase58("AZDn74qWSGQk8cyfD7DXmkRoyeJkZ4RbSx")
	fileSize := uint64(4 * 1024 * 1024 * 1024)
	prefix := &FilePrefix{
		Version:    1,
		Encrypt:    true,
		EncryptPwd: "1234",
		Owner:      addr,
		FileSize:   fileSize,
	}
	rand.Read(prefix.EncryptSalt[:])
	toHash := make([]byte, 0)
	toHash = append(toHash, []byte(prefix.EncryptPwd)...)
	toHash = append(toHash, []byte(prefix.EncryptSalt[:])...)
	hash := sha256.Sum256(toHash)
	prefix.EncryptHash = hash
	buf, err := json.Marshal(prefix)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("buf len: %d\n", len(buf))

	prefix2 := &FilePrefix{}
	err = json.Unmarshal(buf, prefix2)
	if err != nil {
		t.Fatal(err)
	}
	// prefix2.Deserialize(buf2)
	fmt.Printf("version: %d\n", prefix2.Version)
	fmt.Printf("encrypt: %t\n", prefix2.Encrypt)
	fmt.Printf("salt: %v\n", prefix2.EncryptSalt)
	fmt.Printf("hash: %v\n", prefix2.EncryptHash)
	fmt.Printf("owner: %s\n", prefix2.Owner.ToBase58())
	fmt.Printf("fileSize: %d\n", prefix2.FileSize)
	verify := VerifyEncryptPassword("1234", prefix2.EncryptSalt, prefix2.EncryptHash)
	fmt.Printf("verify : %t\n", verify)
}

func TestBytesToBytesPrefix(t *testing.T) {
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
	base64Str := base64.StdEncoding.EncodeToString(buf)
	fmt.Printf("str: %s, len: %d\n", base64Str, len(base64Str))
	fmt.Printf("bytes: %v, len: %d\n", []byte(base64Str), len([]byte(base64Str)))
	buf2, _ := base64.StdEncoding.DecodeString(base64Str)

	prefix2 := &FilePrefix{}
	prefix2.Deserialize(buf2)
	fmt.Printf("version: %d\n", prefix2.Version)
	fmt.Printf("encrypt: %t\n", prefix2.Encrypt)
	fmt.Printf("salt: %v\n", prefix2.EncryptSalt)
	fmt.Printf("hash: %v\n", prefix2.EncryptHash)
	fmt.Printf("owner: %s\n", prefix2.Owner.ToBase58())
	fmt.Printf("fileSize: %d\n", prefix2.FileSize)
	verify := VerifyEncryptPassword("456", prefix2.EncryptSalt, prefix2.EncryptHash)
	fmt.Printf("verify : %t\n", verify)
}
