package crypto

import (
	"fmt"
	"testing"
)

func TestComputeRootHash(t *testing.T) {
	hashes := make([]HashBytes, 0)
	for i := 0; i < 100; i++ {
		hashes = append(hashes, HashBytes("AUX8yWW8Q1vQmoyHT72cFRbn8fBmrpYkjP"))
	}
	root := ComputeMerkleRoot(hashes)
	fmt.Printf("root %x\n", root)
}

func BenchmarkTestComputeRootHash(b *testing.B) {
	hashes := make([]string, 0)
	for i := 0; i < b.N; i++ {
		hashes = append(hashes, "AUX8yWW8Q1vQmoyHT72cFRbn8fBmrpYkjP")
	}
	root := ComputeStringHashRoot(hashes)
	fmt.Printf("root %x\n", root)
}

func TestHardCodeComputeStringHashRoot(t *testing.T) {
	hashes := make([]string, 0)
	hashes = append(hashes, "zb2rhbgXCa2eUAu5G3k9hUiFg9dTBAneSjwkLX9TH5LKPAG4t")
	root := ComputeStringHashRoot(hashes)
	fmt.Printf("root %s\n", root)
}
