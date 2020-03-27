package utils

import (
	"fmt"
	"hash/crc32"
	"os"

	"github.com/saveio/dsp-go-sdk/common"
)

// GetSimpleChecksumOfFile. Calculate a simple checksum of file.
// In this case, we use first CHUNK_SIZE/2 bytes and last CHUNK_SIZE/2 from file content as input.
// The checksum string is a hex string of checksum result.
func GetSimpleChecksumOfFile(filePath string) (string, error) {
	fi, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer fi.Close()
	fileStat, err := fi.Stat()
	if err != nil {
		return "", err
	}
	fileSize := fileStat.Size()

	// too small, use all content
	if fileSize <= common.CHUNK_SIZE {
		buf := make([]byte, fileSize)
		_, err := fi.Read(buf)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", crc32.ChecksumIEEE(buf)), nil
	}

	headBuf := make([]byte, common.CHUNK_SIZE/2)
	tailBuf := make([]byte, common.CHUNK_SIZE/2)
	_, err = fi.Read(headBuf)
	if err != nil {
		return "", err
	}

	_, err = fi.ReadAt(tailBuf, fileSize-common.CHUNK_SIZE/2)
	if err != nil {
		return "", err
	}
	midleBuf := make([]byte, common.CHUNK_SIZE/2)
	fi.ReadAt(midleBuf, common.CHUNK_SIZE/2)
	buf := make([]byte, common.CHUNK_SIZE)
	buf = append(buf, headBuf...)
	buf = append(buf, tailBuf...)
	return fmt.Sprintf("%x", crc32.ChecksumIEEE(buf)), nil
}

// GetChecksumOfFile. Calculate a simple checksum of file.
// In this case, we use first CHUNK_SIZE/2 bytes and last CHUNK_SIZE/2 from file content as input.
// The checksum string is a hex string of checksum result.
func GetChecksumOfFile(filePath string) (string, error) {
	fi, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer fi.Close()
	fileStat, err := fi.Stat()
	if err != nil {
		return "", err
	}
	fileSize := fileStat.Size()

	buf := make([]byte, fileSize)
	_, err = fi.Read(buf)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", crc32.ChecksumIEEE(buf)), nil
}
