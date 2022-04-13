package crypto

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/saveio/dsp-go-sdk/consts"
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
	if fileSize <= consts.CHUNK_SIZE {
		buf := make([]byte, fileSize)
		_, err := fi.Read(buf)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", crc32.ChecksumIEEE(buf)), nil
	}

	headBuf := make([]byte, consts.CHUNK_SIZE/2)
	tailBuf := make([]byte, consts.CHUNK_SIZE/2)
	_, err = fi.Read(headBuf)
	if err != nil {
		return "", err
	}

	_, err = fi.ReadAt(tailBuf, fileSize-consts.CHUNK_SIZE/2)
	if err != nil {
		return "", err
	}
	midleBuf := make([]byte, consts.CHUNK_SIZE/2)
	fi.ReadAt(midleBuf, consts.CHUNK_SIZE/2)
	buf := make([]byte, consts.CHUNK_SIZE)
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

// MD5All reads all the files in the file tree rooted at root and returns a map
// from file path to the MD5 sum of the file's contents.  If the directory walk
// fails or any read operation fails, MD5All returns an error.
func MD5All(root string) (map[string][md5.Size]byte, error) {
	m := make(map[string][md5.Size]byte)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		m[path] = md5.Sum(data)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

func GetSimpleChecksumOfDir(dirPath string) (string, error) {
	m, err := MD5All(dirPath)
	if err != nil {
		return "", err
	}
	// sort map by key alphabetically
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(m))
	for _, k := range keys {
		values = append(values, fmt.Sprintf("%x", m[k]))
	}
	// get sum of values
	var sum string
	sum = strings.Join(values, "")
	return fmt.Sprintf("%x", crc32.ChecksumIEEE([]byte(sum))), nil
}
