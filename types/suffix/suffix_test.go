package suffix

import (
	"testing"
)

func TestAddSuffixToFile(t *testing.T) {
	err := AddSuffixToFile([]byte("suffix"), "/Users/smallyu/work/test/file/aaa")
	if err != nil {
		t.Error(err)
	}
}

func TestAddCipherTextSuffixToFile(t *testing.T) {
	err := AddCipherTextSuffixToFile([]byte("ciphertext"), "/Users/smallyu/work/test/file/aaa")
	if err != nil {
		t.Error(err)
	}
}

func TestReadCipherTextFromFile(t *testing.T) {
	path := "/Users/smallyu/work/test/file/test1/aaa.epta"
	file, err := ReadCipherTextFromFile(path)
	if err != nil {
		t.Error(err)
	}
	t.Log(file)
	t.Log(string(file))
}

func TestGenerateRandomPassword(t *testing.T) {
	password, err := GenerateRandomPassword()
	if err != nil {
		t.Error(err)
	}
	t.Log(password)
}

func TestCutCipherTextFromFile(t *testing.T) {
	path := "/Users/smallyu/work/test/file/test1/aaa.epta"
	err := CutCipherTextFromFile(path)
	if err != nil {
		t.Error(err)
	}
}
