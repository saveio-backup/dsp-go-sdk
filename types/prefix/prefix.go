package prefix

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

const (
	PREFIX_VERSION   = 1
	VERSION_LEN      = 1
	CRYPTO_LEN       = 1
	SALT_LEN         = 4
	HASH_LEN         = 32
	OWNER_LEN        = 20
	FILESIZE_LEN     = 8
	REVERSED_LEN     = 4
	CHECKSUM_LEN     = 4
	FILENAME_LEN     = 1
	PAYLOAD_SIZE_LEN = 4
	FILETYPE_LEN     = 1
	ENCRYPTTYPE_LEN  = 1
	MAX_PAYLOAD_SIZE = VERSION_LEN + CRYPTO_LEN + SALT_LEN + HASH_LEN + OWNER_LEN + FILESIZE_LEN +
		FILENAME_LEN + 2<<(FILENAME_LEN*8-1) + REVERSED_LEN + FILETYPE_LEN + CHECKSUM_LEN
)

const (
	FILETYPE_FILE = iota
	FILETYPE_DIR
)

const (
	ENCRYPTTYPE_NONE = iota
	ENCRYPTTYPE_AES
	ENCRYPTTYPE_ECIES
)

type FilePrefix struct {
	Version     uint8              // prefix version
	Encrypt     bool               // is file encrypt
	EncryptPwd  string             // file encrypt password
	EncryptSalt [4]byte            // random salt
	EncryptHash [HASH_LEN]byte     // encrypt hash = sha256(pwd+salt)
	Owner       common.Address     // file owner, 20 Bytes
	FileSize    uint64             // file size
	FileNameLen uint8              // file name length
	FileName    string             // file name string, max length is 2^8, real length = FileNameLen
	Reserved    [REVERSED_LEN]byte // reserved word field
	FileType    uint8              // file type
	EncryptType uint8              // encrypt type
}

func NewEncryptPrefix(password string, owner common.Address, fileSize uint64, isDir bool, eType uint8) *FilePrefix {
	fileType := FILETYPE_FILE
	if isDir {
		fileType = FILETYPE_DIR
	}
	p := &FilePrefix{
		Version:     PREFIX_VERSION,
		FileType:    uint8(fileType),
		Encrypt:     true,
		EncryptPwd:  password,
		EncryptType: eType,
		Owner:       owner,
		FileSize:    fileSize,
	}
	if err := p.MakeSalt(); err != nil {
		return nil
	}
	return p
}

// NewEncryptAPrefix make a encrypt asymmetrically prefix
func NewEncryptAPrefix(owner common.Address, fileSize uint64, isDir bool, eType uint8) *FilePrefix {
	return NewEncryptPrefix("", owner, fileSize, isDir, eType)
}

// MakeSalt. make a random encrypt salt for prefix
func (p *FilePrefix) MakeSalt() error {
	var salt [SALT_LEN]byte
	_, err := rand.Read(salt[:])
	if err != nil {
		return err
	}
	copy(p.EncryptSalt[:], salt[:])
	return nil
}

// totalSize + version + en/de + salt + hash + owner + fileSize + fileNameLem + fileName + reserved + checksum
// 4 byte + 1 byte  + 1 byte + 4 byte + 32 byte + 20 byte + 8 byte + 2 byte + ? byte + 4 byte + 4 byte = 80 + ?

func (p *FilePrefix) Serialize() []byte {
	var versionBuf [VERSION_LEN]byte
	versionBuf[0] = byte(p.Version)
	var cryptoBuf [CRYPTO_LEN]byte
	var salt [SALT_LEN]byte
	var hash [HASH_LEN]byte
	if p.Encrypt {
		cryptoBuf[0] = byte(1)
		copy(salt[:], p.EncryptSalt[:])
		encryptData := make([]byte, 0)
		encryptData = append(encryptData, []byte(p.EncryptPwd)...)
		encryptData = append(encryptData, salt[:]...)
		hash = sha256.Sum256(encryptData)
	}

	fileSizeBuf := make([]byte, FILESIZE_LEN)
	binary.BigEndian.PutUint64(fileSizeBuf, p.FileSize)

	var fileNameBuf [FILENAME_LEN]byte
	fileNameBuf[0] = byte(len(p.FileName))

	var typeBuffer [FILETYPE_LEN]byte
	typeBuffer[0] = p.FileType

	var eTypeBuffer [ENCRYPTTYPE_LEN]byte
	eTypeBuffer[0] = p.EncryptType

	payloadSize := uint32(VERSION_LEN + CRYPTO_LEN + SALT_LEN + HASH_LEN + len(p.Owner[:]) +
		FILESIZE_LEN + FILENAME_LEN + len(p.FileName) + REVERSED_LEN + FILETYPE_LEN + ENCRYPTTYPE_LEN + CHECKSUM_LEN)
	if payloadSize > MAX_PAYLOAD_SIZE {
		log.Warnf("payload size too big, %d, %d", payloadSize, MAX_PAYLOAD_SIZE)
		return nil
	}
	payloadSizeBuf := make([]byte, PAYLOAD_SIZE_LEN)
	binary.BigEndian.PutUint32(payloadSizeBuf, payloadSize)

	var result []byte
	result = append(result, versionBuf[0])
	result = append(result, cryptoBuf[0])
	result = append(result, salt[:]...)
	result = append(result, hash[:]...)
	result = append(result, p.Owner[:]...)
	result = append(result, fileSizeBuf...)
	result = append(result, fileNameBuf[0])
	result = append(result, p.FileName[:]...)
	result = append(result, p.Reserved[:]...)
	result = append(result, typeBuffer[0])
	result = append(result, eTypeBuffer[0])

	checkSum := crc32.ChecksumIEEE(result)
	checkSumBuf := make([]byte, CHECKSUM_LEN)
	binary.BigEndian.PutUint32(checkSumBuf, checkSum)

	result = append(result, checkSumBuf...)

	if len(result) > MAX_PAYLOAD_SIZE {
		log.Warnf("payload result size too big")
		return nil
	}

	base64Result := make([]byte, base64.StdEncoding.EncodedLen(len(result)))
	base64.StdEncoding.Encode(base64Result, result)

	totalSizeBase64 := make([]byte, base64.StdEncoding.EncodedLen(PAYLOAD_SIZE_LEN))
	base64.StdEncoding.Encode(totalSizeBase64, payloadSizeBuf)

	return append(totalSizeBase64, base64Result...)
}

func (p *FilePrefix) Deserialize(base64Buf []byte) error {
	encodeSizeLen := base64.StdEncoding.EncodedLen(PAYLOAD_SIZE_LEN)
	if len(base64Buf) < encodeSizeLen {
		return nil
	}
	payloadSizeBuf := make([]byte, encodeSizeLen)
	_, err := base64.StdEncoding.Decode(payloadSizeBuf, base64Buf[:encodeSizeLen])
	if err != nil {
		return err
	}
	payloadSize := GetPayloadLenFromBuf(payloadSizeBuf)
	buf := make([]byte, payloadSize)
	_, err = base64.StdEncoding.Decode(buf, base64Buf[encodeSizeLen:])
	if err != nil {
		return err
	}
	payload := buf[:payloadSize-CHECKSUM_LEN]
	checkSum := crc32.ChecksumIEEE(payload)
	check := binary.BigEndian.Uint32(buf[payloadSize-CHECKSUM_LEN:])
	if checkSum != check {
		return fmt.Errorf("check sum verify failed %x != %x", checkSum, check)
	}
	p.Version = buf[0]
	encryptEnd := VERSION_LEN + CRYPTO_LEN
	saltEnd := VERSION_LEN + CRYPTO_LEN + len(p.EncryptSalt)
	hashEnd := saltEnd + len(p.EncryptHash)
	if buf[VERSION_LEN] == 1 {
		p.Encrypt = true
	}
	copy(p.EncryptSalt[:], buf[encryptEnd:saltEnd])
	copy(p.EncryptHash[:], buf[saltEnd:hashEnd])

	addrEnd := hashEnd + len(common.ADDRESS_EMPTY[:])
	addr := buf[hashEnd:addrEnd]
	copy(p.Owner[:], addr[:])

	fileSizeEnd := addrEnd + FILESIZE_LEN
	p.FileSize = binary.BigEndian.Uint64(buf[addrEnd:fileSizeEnd])

	fileNameLenEnd := fileSizeEnd + FILENAME_LEN
	p.FileNameLen = buf[fileSizeEnd:fileNameLenEnd][0]

	fileNameBuf := make([]byte, p.FileNameLen)
	fileNameEnd := fileNameLenEnd + int(p.FileNameLen)
	copy(fileNameBuf[:], buf[fileNameLenEnd:fileNameEnd])
	p.FileName = string(fileNameBuf)

	reversedEnd := fileNameEnd + REVERSED_LEN
	copy(p.Reserved[:], buf[fileNameEnd:fileNameEnd+REVERSED_LEN])

	fileTypeEnd := reversedEnd + FILETYPE_LEN
	p.FileType = buf[reversedEnd:fileTypeEnd][0]

	eTypeEnd := fileTypeEnd + ENCRYPTTYPE_LEN
	p.EncryptType = buf[fileTypeEnd:eTypeEnd][0]

	return nil
}

func (p *FilePrefix) String() string {
	buf := p.Serialize()
	return string(buf)
}

func (p *FilePrefix) ParseFromString(base64Str string) {
	p.Deserialize([]byte(base64Str))
}

func (p *FilePrefix) Print() {
	log.Debugf("Version: %d", p.Version)
	log.Debugf("Encrypt: %t", p.Encrypt)
	log.Debugf("FileSize: %d", p.FileSize)
	log.Debugf("EncryptSalt: %v", p.EncryptSalt)
	log.Debugf("EncryptHash: %v", p.EncryptHash)
	log.Debugf("EncryptType: %v", p.EncryptType)
	log.Debugf("Owner: %s", p.Owner.ToBase58())
	log.Debugf("FileType: %d", p.FileType)
}

func VerifyEncryptPassword(password string, salt [4]byte, hash [32]byte) bool {
	encryptData := make([]byte, 0)
	encryptData = append(encryptData, []byte(password)...)
	encryptData = append(encryptData, salt[:]...)
	result := sha256.Sum256(encryptData)
	return result == hash
}

func GetPrefixEncrypted(prefix []byte) bool {
	filePrefix := &FilePrefix{}
	filePrefix.Deserialize(prefix)
	return filePrefix.Encrypt
}

func GetPrefixEncryptType(prefix []byte) byte {
	filePrefix := &FilePrefix{}
	filePrefix.Deserialize(prefix)
	return filePrefix.EncryptType
}

func GetPrefixFileType(prefix []byte) uint8 {
	filePrefix := &FilePrefix{}
	filePrefix.Deserialize(prefix)
	return filePrefix.FileType
}

func GetPayloadLenFromBuf(prefixLenBuf []byte) uint32 {
	payloadLen := binary.BigEndian.Uint32(prefixLenBuf)
	if payloadLen > MAX_PAYLOAD_SIZE {
		return MAX_PAYLOAD_SIZE
	}
	return payloadLen
}

// GetPrefixFromFile. read prefix of file
func GetPrefixFromFile(fullFilePath string) (*FilePrefix, []byte, error) {
	sourceFile, err := os.Open(fullFilePath)
	if err != nil {
		return nil, nil, err
	}
	defer sourceFile.Close()

	encodeSizeLen := base64.StdEncoding.EncodedLen(PAYLOAD_SIZE_LEN)
	payloadSizeEncodeBuf := make([]byte, encodeSizeLen)
	payloadSizeDecodeBuf := make([]byte, encodeSizeLen)

	if _, err := sourceFile.Read(payloadSizeEncodeBuf); err != nil {
		return nil, nil, err
	}
	if _, err := base64.StdEncoding.Decode(payloadSizeDecodeBuf, payloadSizeEncodeBuf); err != nil {
		return nil, nil, err
	}
	payloadSize := GetPayloadLenFromBuf(payloadSizeDecodeBuf)
	payloadEncodeBuf := make([]byte, base64.StdEncoding.EncodedLen(int(payloadSize)))
	if _, err := sourceFile.ReadAt(payloadEncodeBuf, int64(encodeSizeLen)); err != nil {
		return nil, nil, err
	}

	prefix := append(payloadSizeEncodeBuf, payloadEncodeBuf...)
	filePrefix := &FilePrefix{}
	filePrefix.Deserialize(prefix)
	return filePrefix, prefix, nil
}

func AddPrefixToFile(prefix *FilePrefix, path string) error {
	prefixStr := prefix.Serialize()
	input := path
	output := path + ".tmp"
	outputFile, err := os.OpenFile(output, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer func(outputFile *os.File) {
		err := outputFile.Close()
		if err != nil {
			log.Errorf("close file error: %v", err)
		}
	}(outputFile)
	_, err = outputFile.Write(prefixStr)
	if err != nil {
		return err
	}
	inputFile, err := os.Open(input)
	if err != nil {
		return err
	}
	_, err = outputFile.Seek(int64(len(prefixStr)), io.SeekStart)
	if err != nil {
		return err
	}
	if _, err := io.Copy(outputFile, inputFile); err != nil {
		return err
	}
	err = os.Rename(output, input)
	if err != nil {
		return err
	}
	return nil
}

func AddPrefixFileToDir(prefix *FilePrefix, dir string, fileName string) error {
	path := dir + string(filepath.Separator) + fileName
	err := os.WriteFile(path, prefix.Serialize(), 0666)
	if err != nil {
		return err
	}
	return nil
}

func RemovePrefixFileFromDir(dir string, fileName string) error {
	path := dir + string(filepath.Separator) + fileName
	err := os.Remove(path)
	if err != nil {
		return err
	}
	return nil
}
