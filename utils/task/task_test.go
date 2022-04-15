package task

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

	filePath = "Chain-1/Downloads/AYKnc5VDkvpb5f68XSjTyQzVHU4ZaojGxq/SaveQmTCeB2mf7XsdGeBRZo5S2eoVnEbbgY6hPmDeEHffKH6N5.ept/.DS_Store.ept"
	fileName = ".DS_Store.ept"
	fmt.Println(GetDecryptedFilePath(filePath, fileName))
}

func TestTotalJitterDelay(t *testing.T) {
	sum := uint64(0)
	for i := 1; i <= 50; i++ {
		sum += GetJitterDelay(i, 30)
	}
	fmt.Printf("total sec %d, hour %v\n", sum, sum/3600)
}
