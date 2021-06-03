package crypto

import (
	"crypto/sha256"
	"encoding/hex"
)

type HashBytes []byte

// param hashes will be used as workspace
func ComputeStringHashRoot(hashes []string) string {
	list := make([]HashBytes, 0, len(hashes))
	for _, hash := range hashes {
		list = append(list, []byte(hash))
	}
	return hex.EncodeToString(ComputeMerkleRoot(list))
}

// param hashes will be used as workspace
func ComputeMerkleRoot(hashes []HashBytes) HashBytes {
	if len(hashes) == 0 {
		return HashBytes{}
	}
	sha := sha256.New()
	var temp HashBytes
	for len(hashes) != 1 {
		n := len(hashes) / 2
		for i := 0; i < n; i++ {
			sha.Reset()
			sha.Write(hashes[2*i][:])
			sha.Write(hashes[2*i+1][:])
			sha.Sum(temp[:0])
			sha.Reset()
			sha.Write(temp[:])
			sha.Sum(hashes[i][:0])
		}
		if len(hashes) == 2*n+1 {
			sha.Reset()
			sha.Write(hashes[2*n][:])
			sha.Write(hashes[2*n][:])

			sha.Sum(temp[:0])
			sha.Reset()
			sha.Write(temp[:])
			sha.Sum(hashes[n][:0])

			hashes = hashes[:n+1]
		} else {
			hashes = hashes[:n]
		}
	}

	return hashes[0]
}
