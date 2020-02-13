package utils

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestGetDecryptedFilePath(t *testing.T) {
	filePath := "~/test.txt"
	fileName := "test.txt"
	fmt.Println(GetDecryptedFilePath(filePath, fileName))

	filePath = "~/test.txt"
	fileName = "test"
	fmt.Println(GetDecryptedFilePath(filePath, fileName))

	filePath = "~/test (1).txt"
	fileName = "test"
	fmt.Println(GetDecryptedFilePath(filePath, fileName))

	filePath = "~/test (1).txt"
	fileName = "test.txt"
	fmt.Println(GetDecryptedFilePath(filePath, fileName))

	filePath = "~/test.ept"
	fileName = "test.txt"
	fmt.Println(GetDecryptedFilePath(filePath, fileName))

	filePath = "~/test.ept"
	fileName = "test"
	fmt.Println(GetDecryptedFilePath(filePath, fileName))

	filePath = "~/test (1).ept"
	fileName = "test.txt"
	fmt.Println(GetDecryptedFilePath(filePath, fileName))

	filePath = "~/test (1).ept"
	fileName = "test"
	fmt.Println(GetDecryptedFilePath(filePath, fileName))

	fmt.Println("file base ", filepath.Base(filePath))
}
