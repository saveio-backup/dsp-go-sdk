package prefix

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"testing"

	"github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

func TestGenPrefix(t *testing.T) {
	log.InitLog(1, os.Stdout)
	addr, _ := common.AddressFromBase58("AZDn74qWSGQk8cyfD7DXmkRoyeJkZ4RbSx")
	fileSize := uint64(4 * 1024 * 1024 * 1024)
	prefix := &FilePrefix{
		Version:    1,
		Encrypt:    false,
		EncryptPwd: "1234",
		Owner:      addr,
		FileSize:   fileSize,
		FileName:   " 哈哈@@%^#*)!?><|}{}>~!.txt ",
		FileType:   0,
	}
	prefix.MakeSalt()
	buf := prefix.Serialize()
	log.Infof("prefix-len: %v, buf %v, str %s", len(buf), buf, buf)

	prefix2 := &FilePrefix{}
	prefix2.Deserialize(buf)
	log.Infof("version: %d", prefix2.Version)
	log.Infof("file type: %d", prefix2.FileType)
	log.Infof("encrypt: %t", prefix2.Encrypt)
	log.Infof("salt: %v", prefix2.EncryptSalt)
	log.Infof("hash: %v", prefix2.EncryptHash)
	log.Infof("owner: %s", prefix2.Owner.ToBase58())
	log.Infof("fileSize: %d", prefix2.FileSize)
	log.Infof("fileNameLen: %d", prefix2.FileNameLen)
	log.Infof("fileName: %s", prefix2.FileName)
	verify := VerifyEncryptPassword("12345", prefix2.EncryptSalt, prefix2.EncryptHash)
	log.Infof("verify : %t", verify)
}

func TestBase64EncodePrefix(t *testing.T) {
	log.InitLog(1, os.Stdout)
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
	log.Infof("len: %d", encodedLen)
	log.Infof("prefix-len: %v, size: %d", len(buf), fileSize)
	base64Str := base64.StdEncoding.EncodeToString(buf)
	log.Infof("str: %s, len: %d", base64Str, len(base64Str))
	buf2, _ := base64.StdEncoding.DecodeString(base64Str)

	prefix2 := &FilePrefix{}
	prefix2.Deserialize(buf2)
	log.Infof("version: %d", prefix2.Version)
	log.Infof("encrypt: %t", prefix2.Encrypt)
	log.Infof("salt: %v", prefix2.EncryptSalt)
	log.Infof("hash: %v", prefix2.EncryptHash)
	log.Infof("owner: %s", prefix2.Owner.ToBase58())
	log.Infof("fileSize: %d", prefix2.FileSize)
	verify := VerifyEncryptPassword("456", prefix2.EncryptSalt, prefix2.EncryptHash)
	log.Infof("verify : %t", verify)
}

func TestMarshalPrefix(t *testing.T) {
	log.InitLog(1, os.Stdout)
	addr, _ := common.AddressFromBase58("AZDn74qWSGQk8cyfD7DXmkRoyeJkZ4RbSx")
	fileSize := uint64(4 * 1024 * 1024 * 1024)
	prefix := &FilePrefix{
		Version:    1,
		FileType:   1,
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
	log.Infof("buf len: %d", len(buf))

	prefix2 := &FilePrefix{}
	err = json.Unmarshal(buf, prefix2)
	if err != nil {
		t.Fatal(err)
	}
	// prefix2.Deserialize(buf2)
	log.Infof("version: %d", prefix2.Version)
	log.Infof("encrypt: %t", prefix2.Encrypt)
	log.Infof("salt: %v", prefix2.EncryptSalt)
	log.Infof("hash: %v", prefix2.EncryptHash)
	log.Infof("owner: %s", prefix2.Owner.ToBase58())
	log.Infof("fileSize: %d", prefix2.FileSize)
	verify := VerifyEncryptPassword("1234", prefix2.EncryptSalt, prefix2.EncryptHash)
	log.Infof("verify : %t", verify)
}

func TestBytesToBytesPrefix(t *testing.T) {
	log.InitLog(1, os.Stdout)
	addr, _ := common.AddressFromBase58("AZDn74qWSGQk8cyfD7DXmkRoyeJkZ4RbSx")
	fileSize := uint64(4 * 1024 * 1024 * 1024)
	prefix := &FilePrefix{
		Version:    1,
		FileType:   1,
		Encrypt:    true,
		EncryptPwd: "1234",
		Owner:      addr,
		FileSize:   fileSize,
	}
	buf := prefix.Serialize()
	log.Infof("prefix-len: %v, size: %d", len(buf), fileSize)
	base64Str := base64.StdEncoding.EncodeToString(buf)
	log.Infof("str: %s, len: %d", base64Str, len(base64Str))
	log.Infof("bytes: %v, len: %d", []byte(base64Str), len([]byte(base64Str)))
	buf2, _ := base64.StdEncoding.DecodeString(base64Str)

	prefix2 := &FilePrefix{}
	prefix2.Deserialize(buf2)
	log.Infof("version: %d", prefix2.Version)
	log.Infof("encrypt: %t", prefix2.Encrypt)
	log.Infof("salt: %v", prefix2.EncryptSalt)
	log.Infof("hash: %v", prefix2.EncryptHash)
	log.Infof("owner: %s", prefix2.Owner.ToBase58())
	log.Infof("fileSize: %d", prefix2.FileSize)
	verify := VerifyEncryptPassword("456", prefix2.EncryptSalt, prefix2.EncryptHash)
	log.Infof("verify : %t", verify)
}

func TestBase64DecodePrefix(t *testing.T) {
	log.InitLog(1, os.Stdout)
	base64Str := "AAAAXA==AQEjxjXTanvuuNJGpSgYkvnjHov1MFBZxr2uF9iL+3fNHQMeRJAViLqEl5/khp3wxmk6rjLUmVxnawAAAAAAACH4ABDkuK3mloflkI3lrZcubG9nAAAAAOEzhyI="
	prefix := &FilePrefix{}
	prefix.ParseFromString(base64Str)
	log.Infof("version: %d", prefix.Version)
	log.Infof("encrypt: %t", prefix.Encrypt)
	log.Infof("salt: %v", prefix.EncryptSalt)
	log.Infof("hash: %v", prefix.EncryptHash)
	log.Infof("owner: %s", prefix.Owner.ToBase58())
	log.Infof("fileSize: %d", prefix.FileSize)
	log.Infof("fileNameLen: %d", prefix.FileNameLen)
	log.Infof("fileName: %s", prefix.FileName)
}

func TestGetFilePrefixFromPath(t *testing.T) {
	log.InitLog(1, os.Stdout)
	paths := []string{
		"/Users/zhijie/Desktop/onchain/save-test/node4/Chain-12345/Downloads/AHjjdbVLhfTyiNFEq2X8mFnnirZY1yK8Rq/中文名字.ept",
		// "/Users/zhijie/Desktop/onchain/save-test/node4/Chain-12345/Downloads/AHjjdbVLhfTyiNFEq2X8mFnnirZY1yK8Rq/2020-02-18_10.57.09_LOG.ept",
		// "/Users/zhijie/Desktop/onchain/save-test/node4/Chain-12345/Downloads/AHjjdbVLhfTyiNFEq2X8mFnnirZY1yK8Rq/2020-02-17_18.31.20_LOG.ept",
		// "/Users/zhijie/Desktop/onchain/save-test/node4/Chain-12345/Downloads/AHjjdbVLhfTyiNFEq2X8mFnnirZY1yK8Rq/2020-02-17_18.16.59_LOG.log",
	}
	for _, path := range paths {
		prefix, prefixBuf, err := GetPrefixFromFile(path)
		if err != nil {
			// t.Fatal(err)
			log.Error(err)
			continue
		}
		log.Infof("version: %d", prefix.Version)
		log.Infof("encrypt: %t", prefix.Encrypt)
		log.Infof("salt: %v", prefix.EncryptSalt)
		log.Infof("hash: %v", prefix.EncryptHash)
		log.Infof("owner: %s", prefix.Owner.ToBase58())
		log.Infof("fileSize: %d", prefix.FileSize)
		log.Infof("fileNameLen: %d", prefix.FileNameLen)
		log.Infof("fileName: %s", prefix.FileName)

		log.Infof("prefix len %d", len(prefixBuf))
		// log.Infof("prefix str %s", prefixBuf)
		verify := VerifyEncryptPassword("12356", prefix.EncryptSalt, prefix.EncryptHash)
		log.Infof("verify : %t", verify)
	}

}

func TestEncodeTwoStr(t *testing.T) {
	log.InitLog(1, os.Stdout)
	totalSizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(totalSizeBuf, 110)

	totalSizeBuf2 := make([]byte, 4)
	binary.BigEndian.PutUint32(totalSizeBuf2, 112)

	result := make([]byte, 8)
	result = append(result, totalSizeBuf...)
	// result = append(result, totalSizeBuf2...)
	base64Result := make([]byte, base64.StdEncoding.EncodedLen(len(result)))
	base64.StdEncoding.Encode(base64Result, result)
	log.Infof("base64Result %s, len %d", base64Result, len(base64Result))

}
