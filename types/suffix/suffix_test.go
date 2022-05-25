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
	path := "/Users/smallyu/work/gogs/edge-deploy/node1/Chain-1/Downloads/AYKnc5VDkvpb5f68XSjTyQzVHU4ZaojGxq/SaveQmWeAW8UgeSrMY8BnfpBxCS8EA5WHoooTvxpfjvFxK1shs.ept/aaa.ept"
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
	path := "/Users/smallyu/work/gogs/edge-deploy/node1/Chain-1/Downloads/AYKnc5VDkvpb5f68XSjTyQzVHU4ZaojGxq/SaveQmQjKfjVEgZdMhNMm2p9bNwqsd3P1zK8VQQrwmY6d9TaZf.ept/t2.ept"
	err := CutCipherTextFromFile(path)
	if err != nil {
		t.Error(err)
	}
}
