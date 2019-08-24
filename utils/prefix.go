package utils

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"

	"github.com/saveio/themis/common"
)

const (
	PREFIX_VERSION = 1
	VERSION_LEN    = 1
	CRYPTO_LEN     = 1
	FILESIZE_LEN   = 8
	REVERSED_LEN   = 4
	CHECKSUM_LEN   = 4
	PREFIX_LEN     = 74
)

type FilePrefix struct {
	Version     uint8
	Encrypt     bool
	EncryptPwd  string
	EncryptSalt [4]byte
	EncryptHash [32]byte
	Owner       common.Address
	FileSize    uint64
	Reserved    [4]byte
}

// version + en/de + salt + hash + owner + fileSize + reserved + checksum
// 1 byte  + 1 byte + 4 byte + 32 byte + 20 byte + 8 byte + 4 byte +  4 byte = 74 byte

func (p *FilePrefix) Serialize() []byte {
	var versionBuf [1]byte
	versionBuf[0] = byte(p.Version)
	var cryptoBuf [1]byte
	var salt [4]byte
	var hash [32]byte
	if p.Encrypt {
		cryptoBuf[0] = byte(1)
		_, err := rand.Read(salt[:])
		if err != nil {
			return nil
		}
		encryptData := make([]byte, 0)
		encryptData = append(encryptData, []byte(p.EncryptPwd)...)
		encryptData = append(encryptData, salt[:]...)
		hash = sha256.Sum256(encryptData)
	}

	fileSizeBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(fileSizeBuf, p.FileSize)

	var result []byte
	result = append(result, versionBuf[0])
	result = append(result, cryptoBuf[0])
	result = append(result, salt[:]...)
	result = append(result, hash[:]...)
	result = append(result, p.Owner[:]...)
	result = append(result, fileSizeBuf[:]...)
	result = append(result, p.Reserved[:]...)

	checkSum := crc32.ChecksumIEEE(result)
	checkSumBuf := make([]byte, CHECKSUM_LEN)
	binary.BigEndian.PutUint32(checkSumBuf, checkSum)

	result = append(result, checkSumBuf...)
	return result
}

func (p *FilePrefix) Deserialize(buf []byte) {
	if len(buf) != PREFIX_LEN {
		return
	}

	payload := buf[:PREFIX_LEN-CHECKSUM_LEN]
	checkSum := crc32.ChecksumIEEE(payload)
	check := binary.BigEndian.Uint32(buf[PREFIX_LEN-CHECKSUM_LEN:])
	if checkSum != check {
		return
	}
	p.Version = buf[0]
	encryptEnd := VERSION_LEN + CRYPTO_LEN
	saltEnd := VERSION_LEN + CRYPTO_LEN + len(p.EncryptSalt)
	hashEnd := saltEnd + len(p.EncryptHash)
	if buf[1] == 1 {
		p.Encrypt = true
	}
	copy(p.EncryptSalt[:], buf[encryptEnd:saltEnd])
	copy(p.EncryptHash[:], buf[saltEnd:hashEnd])

	addrEnd := hashEnd + len(common.ADDRESS_EMPTY[:])
	addr := buf[hashEnd:addrEnd]
	copy(p.Owner[:], addr[:])

	fileSizeEnd := addrEnd + FILESIZE_LEN
	p.FileSize = binary.BigEndian.Uint64(buf[addrEnd:fileSizeEnd])

	copy(p.Reserved[:], buf[fileSizeEnd:fileSizeEnd+REVERSED_LEN])
}

func (p *FilePrefix) String() string {
	buf := p.Serialize()
	return string(buf)
}

func VerifyEncryptPassword(password string, salt [4]byte, hash [32]byte) bool {
	encryptData := make([]byte, 0)
	encryptData = append(encryptData, []byte(password)...)
	encryptData = append(encryptData, salt[:]...)
	result := sha256.Sum256(encryptData)
	return result == hash
}
