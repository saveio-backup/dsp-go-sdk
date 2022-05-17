package suffix

import (
	"crypto/rand"
	"github.com/saveio/themis/common/log"
	"io"
	"os"
)

const (
	SuffixLength   = 244 // 8 byte password => 112 byte cipher text => 244 hex string
	PasswordLength = 8
)

func AddSuffixToFile(suffix []byte, path string) error {
	outputFile, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func(outputFile *os.File) {
		err := outputFile.Close()
		if err != nil {
			log.Error("close output file failed, err:", err)
		}
	}(outputFile)
	_, err = outputFile.Write(suffix)
	if err != nil {
		return err
	}
	return nil
}

func AddCipherTextSuffixToFile(cipherText []byte, path string) error {
	var cipherKey [SuffixLength]byte
	copy(cipherKey[:], cipherText)
	return AddSuffixToFile(cipherKey[:], path)
}

func ReadCipherTextFromFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Error("close file failed, err:", err)
		}
	}(file)
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	start := stat.Size() - SuffixLength
	var cipherText [SuffixLength]byte
	_, err = file.ReadAt(cipherText[:], start)
	if err != nil {
		return nil, err
	}
	return cipherText[:], nil
}

func CutCipherTextFromFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Error("close file failed, err:", err)
		}
	}(file)
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	start := stat.Size() - SuffixLength
	_, err = file.Seek(start, io.SeekStart)
	if err != nil {
		return err
	}
	var cipherText [SuffixLength]byte
	_, err = file.ReadAt(cipherText[:], start)
	if err != nil {
		return err
	}
	err = os.Truncate(path, start)
	if err != nil {
		return err
	}
	return nil
}

func GenerateRandomPassword() ([]byte, error) {
	var password [PasswordLength]byte
	_, err := rand.Read(password[:])
	if err != nil {
		return nil, err
	}
	return password[:], nil
}
