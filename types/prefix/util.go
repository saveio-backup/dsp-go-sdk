package prefix

import (
	"github.com/saveio/dsp-go-sdk/consts"
	"github.com/saveio/themis/crypto/keypair"
	"strings"
)

func GetEncryptType(password string, pubKey keypair.PublicKey) int {
	eType := ENCRYPTTYPE_NONE
	if password != "" {
		eType = ENCRYPTTYPE_AES
	}
	if pubKey != nil {
		eType = ENCRYPTTYPE_ECIES
	}
	return eType
}

func GetFileNameByEncryptType(fileName string, eType int) string {
	index := strings.LastIndex(fileName, ".")
	var oriName string
	if index == 0 {
		oriName = fileName
	} else {
		oriName = fileName[:index]
	}
	switch eType {
	case ENCRYPTTYPE_AES:
		fileName = oriName + consts.ENCRYPTED_FILE_EXTENSION
	case ENCRYPTTYPE_ECIES:
		fileName = oriName + consts.ENCRYPTED_A_FILE_EXTENSION
	}
	return fileName
}
