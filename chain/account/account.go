package account

import (
	"encoding/hex"
	"fmt"

	"github.com/oniio/oniChain/account"
	"github.com/oniio/oniChain/core/types"
	"github.com/oniio/oniChain/crypto/keypair"
	s "github.com/oniio/oniChain/crypto/signature"
)

/** AccountData - for wallet read and save, no crypto object included **/
type AccountData struct {
	keypair.ProtectedKey

	Label     string `json:"label"`
	PubKey    string `json:"publicKey"`
	SigSch    string `json:"signatureScheme"`
	IsDefault bool   `json:"isDefault"`
	Lock      bool   `json:"lock"`
	Scrypt    *keypair.ScryptParam
}

func NewAccountData(keyType keypair.KeyType, curveCode byte, sigScheme s.SignatureScheme, passwd []byte, scrypts ...*keypair.ScryptParam) (*AccountData, error) {
	if len(passwd) == 0 {
		return nil, fmt.Errorf("password cannot empty")
	}
	if !CheckKeyTypeCurve(keyType, curveCode) {
		return nil, fmt.Errorf("curve unmath key type")
	}
	if !CheckSigScheme(keyType, sigScheme) {
		return nil, fmt.Errorf("sigScheme:%s does not match with KeyType:%s", sigScheme.Name(), GetKeyTypeString(keyType))
	}
	var scrypt *keypair.ScryptParam
	if len(scrypts) > 0 {
		scrypt = scrypts[0]
	} else {
		scrypt = keypair.GetScryptParameters()
	}
	prvkey, pubkey, err := keypair.GenerateKeyPair(keyType, curveCode)
	if err != nil {
		return nil, fmt.Errorf("generateKeyPair error:%s", err)
	}
	address := types.AddressFromPubKey(pubkey)
	addressBase58 := address.ToBase58()
	prvSecret, err := keypair.EncryptWithCustomScrypt(prvkey, addressBase58, passwd, scrypt)
	if err != nil {
		return nil, fmt.Errorf("encryptPrivateKey error:%s", err)
	}
	accData := &AccountData{}
	accData.SetKeyPair(prvSecret)
	accData.SigSch = sigScheme.Name()
	accData.PubKey = hex.EncodeToString(keypair.SerializePublicKey(pubkey))
	accData.Scrypt = scrypt
	return accData, nil
}

func (this *AccountData) GetAccount(passwd []byte) (*account.Account, error) {
	privateKey, err := keypair.DecryptWithCustomScrypt(&this.ProtectedKey, passwd, this.Scrypt)
	if err != nil {
		return nil, fmt.Errorf("decrypt privateKey error:%s", err)
	}
	publicKey := privateKey.Public()
	addr := types.AddressFromPubKey(publicKey)
	scheme, err := s.GetScheme(this.SigSch)
	if err != nil {
		return nil, fmt.Errorf("signature scheme error:%s", err)
	}
	return &account.Account{
		PrivateKey: privateKey,
		PublicKey:  publicKey,
		Address:    addr,
		SigScheme:  scheme,
	}, nil
}

func (this *AccountData) GetScrypt() *keypair.ScryptParam {
	return this.Scrypt
}

func (this *AccountData) Clone() *AccountData {
	accData := &AccountData{
		Label:     this.Label,
		PubKey:    this.PubKey,
		SigSch:    this.SigSch,
		IsDefault: this.IsDefault,
		Lock:      this.Lock,
		Scrypt:    this.Scrypt,
	}
	accData.SetKeyPair(this.GetKeyPair())
	return accData
}

func (this *AccountData) SetKeyPair(keyinfo *keypair.ProtectedKey) {
	this.Address = keyinfo.Address
	this.EncAlg = keyinfo.EncAlg
	this.Alg = keyinfo.Alg
	this.Hash = keyinfo.Hash
	this.Key = make([]byte, len(keyinfo.Key))
	copy(this.Key, keyinfo.Key)
	this.Param = keyinfo.Param
	this.Salt = make([]byte, len(keyinfo.Salt))
	copy(this.Salt, keyinfo.Salt)
}

func (this *AccountData) GetKeyPair() *keypair.ProtectedKey {
	var keyinfo = new(keypair.ProtectedKey)
	keyinfo.Address = this.Address
	keyinfo.EncAlg = this.EncAlg
	keyinfo.Alg = this.Alg
	keyinfo.Hash = this.Hash
	keyinfo.Key = make([]byte, len(this.Key))
	copy(keyinfo.Key, this.Key)
	keyinfo.Param = this.Param
	keyinfo.Salt = make([]byte, len(this.Salt))
	copy(keyinfo.Salt, this.Salt)
	return keyinfo
}

func GetKeyTypeString(keyType keypair.KeyType) string {
	switch keyType {
	case keypair.PK_ECDSA:
		return "ECDSA"
	case keypair.PK_SM2:
		return "SM2"
	case keypair.PK_EDDSA:
		return "Ed25519"
	default:
		return "unknown key type"
	}
}

func CheckKeyTypeCurve(keyType keypair.KeyType, curveCode byte) bool {
	switch keyType {
	case keypair.PK_ECDSA:
		switch curveCode {
		case keypair.P224:
		case keypair.P256:
		case keypair.P384:
		case keypair.P521:
		default:
			return false
		}
	case keypair.PK_SM2:
		switch curveCode {
		case keypair.SM2P256V1:
		default:
			return false
		}
	case keypair.PK_EDDSA:
		switch curveCode {
		case keypair.ED25519:
		default:
			return false
		}
	}
	return true
}

func CheckSigScheme(keyType keypair.KeyType, sigScheme s.SignatureScheme) bool {
	switch keyType {
	case keypair.PK_ECDSA:
		switch sigScheme {
		case s.SHA224withECDSA:
		case s.SHA256withECDSA:
		case s.SHA384withECDSA:
		case s.SHA512withECDSA:
		case s.SHA3_224withECDSA:
		case s.SHA3_256withECDSA:
		case s.SHA3_384withECDSA:
		case s.SHA3_512withECDSA:
		case s.RIPEMD160withECDSA:
		default:
			return false
		}
	case keypair.PK_SM2:
		switch sigScheme {
		case s.SM3withSM2:
		default:
			return false
		}
	case keypair.PK_EDDSA:
		switch sigScheme {
		case s.SHA512withEDDSA:
		default:
			return false
		}
	default:
		return false
	}
	return true
}

func GetCurveName(pubKey []byte) string {
	if len(pubKey) < 2 {
		return ""
	}
	switch keypair.KeyType(pubKey[0]) {
	case keypair.PK_ECDSA, keypair.PK_SM2:
		c, err := keypair.GetCurve(pubKey[1])
		if err != nil {
			return ""
		}
		return c.Params().Name
	case keypair.PK_EDDSA:
		if pubKey[1] == keypair.ED25519 {
			return "ed25519"
		} else {
			return ""
		}
	default:
		return ""
	}
}
