package ontid

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/oniio/dsp-go-sdk/chain/account"
	"github.com/oniio/dsp-go-sdk/chain/client"
	sdkcom "github.com/oniio/dsp-go-sdk/chain/common"
	"github.com/oniio/dsp-go-sdk/chain/identity"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/common/serialization"
	"github.com/oniio/oniChain/crypto/keypair"
)

var (
	ONT_ID_CONTRACT_ADDRESS, _ = utils.AddressFromHexString("0300000000000000000000000000000000000000")
	ONT_ID_CONTRACT_VERSION    = byte(0)
)

type OntId struct {
	Client *client.ClientMgr
}

func (this *OntId) InvokeNativeContract(signer *account.Account, controller *identity.Controller, method string, params []interface{}) (common.Uint256, error) {
	if signer == nil {
		return common.UINT256_EMPTY, errors.New("signer is nil")
	}
	tx, err := utils.NewNativeInvokeTransaction(sdkcom.GAS_PRICE, sdkcom.GAS_LIMIT, ONT_ID_CONTRACT_VERSION, ONT_ID_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	if controller != nil {
		err = utils.SignToTransaction(tx, controller)
		if err != nil {
			return common.UINT256_EMPTY, err
		}
	}
	return this.Client.SendTransaction(tx)
}

func (this *OntId) PreExecInvokeNativeContract(
	method string,
	params []interface{},
) (*sdkcom.PreExecResult, error) {
	tx, err := utils.NewNativeInvokeTransaction(0, 0, ONT_ID_CONTRACT_VERSION, ONT_ID_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return nil, err
	}
	return this.Client.PreExecTransaction(tx)
}

func (this *OntId) RegIDWithPublicKey(gasPrice, gasLimit uint64, signer *account.Account, ontId string, controller *identity.Controller) (common.Uint256, error) {
	type regIDWithPublicKey struct {
		OntId  string
		PubKey []byte
	}
	return this.InvokeNativeContract(signer, controller, "regIDWithPublicKey", []interface{}{
		&regIDWithPublicKey{
			OntId:  ontId,
			PubKey: keypair.SerializePublicKey(controller.PublicKey),
		},
	})
}

func (this *OntId) RegIDWithAttributes(gasPrice, gasLimit uint64, signer *account.Account, ontId string, controller *identity.Controller, attributes []*identity.DDOAttribute) (common.Uint256, error) {
	type regIDWithAttribute struct {
		OntId      string
		PubKey     []byte
		Attributes []*identity.DDOAttribute
	}
	return this.InvokeNativeContract(signer, controller, "regIDWithAttributes", []interface{}{
		&regIDWithAttribute{
			OntId:      ontId,
			PubKey:     keypair.SerializePublicKey(controller.PublicKey),
			Attributes: attributes,
		},
	})
}

func (this *OntId) GetDDO(ontId string) (*identity.DDO, error) {
	result, err := this.PreExecInvokeNativeContract(
		"getDDO",
		[]interface{}{ontId},
	)
	if err != nil {
		return nil, err
	}
	data, err := result.Result.ToByteArray()
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	keyData, err := serialization.ReadVarBytes(buf)
	if err != nil {
		return nil, fmt.Errorf("key ReadVarBytes error:%s", err)
	}
	owners, err := this.getPublicKeys(ontId, keyData)
	if err != nil {
		return nil, fmt.Errorf("getPublicKeys error:%s", err)
	}
	attrData, err := serialization.ReadVarBytes(buf)
	attrs, err := this.getAttributes(ontId, attrData)
	if err != nil {
		return nil, fmt.Errorf("getAttributes error:%s", err)
	}
	recoveryData, err := serialization.ReadVarBytes(buf)
	if err != nil {
		return nil, fmt.Errorf("recovery ReadVarBytes error:%s", err)
	}
	var addr string
	if len(recoveryData) != 0 {
		address, err := common.AddressParseFromBytes(recoveryData)
		if err != nil {
			return nil, fmt.Errorf("AddressParseFromBytes error:%s", err)
		}
		addr = address.ToBase58()
	}

	ddo := &identity.DDO{
		OntId:      ontId,
		Owners:     owners,
		Attributes: attrs,
		Recovery:   addr,
	}
	return ddo, nil
}

func (this *OntId) AddKey(gasPrice, gasLimit uint64, ontId string, signer *account.Account, newPubKey keypair.PublicKey, controller *identity.Controller) (common.Uint256, error) {
	type addKey struct {
		OntId     string
		NewPubKey []byte
		PubKey    []byte
	}
	return this.InvokeNativeContract(signer, controller, "addKey", []interface{}{
		&addKey{
			OntId:     ontId,
			NewPubKey: keypair.SerializePublicKey(newPubKey),
			PubKey:    keypair.SerializePublicKey(controller.PublicKey),
		},
	})
}

func (this *OntId) RevokeKey(gasPrice, gasLimit uint64, ontId string, signer *account.Account, removedPubKey keypair.PublicKey, controller *identity.Controller) (common.Uint256, error) {
	type removeKey struct {
		OntId      string
		RemovedKey []byte
		PubKey     []byte
	}
	return this.InvokeNativeContract(signer, controller, "removeKey", []interface{}{
		&removeKey{
			OntId:      ontId,
			RemovedKey: keypair.SerializePublicKey(removedPubKey),
			PubKey:     keypair.SerializePublicKey(controller.PublicKey),
		},
	})
}

func (this *OntId) SetRecovery(gasPrice, gasLimit uint64, signer *account.Account, ontId string, recovery common.Address, controller *identity.Controller) (common.Uint256, error) {
	type addRecovery struct {
		OntId    string
		Recovery common.Address
		Pubkey   []byte
	}
	return this.InvokeNativeContract(signer, controller, "addRecovery", []interface{}{
		&addRecovery{
			OntId:    ontId,
			Recovery: recovery,
			Pubkey:   keypair.SerializePublicKey(controller.PublicKey),
		},
	})
}

func (this *OntId) ChangeRecovery(gasPrice, gasLimit uint64, signer *account.Account, ontId string, newRecovery, oldRecovery common.Address, controller *identity.Controller) (common.Uint256, error) {
	type changeRecovery struct {
		OntId       string
		NewRecovery common.Address
		OldRecovery common.Address
	}
	return this.InvokeNativeContract(signer, controller, "changeRecovery", []interface{}{
		&changeRecovery{
			OntId:       ontId,
			NewRecovery: newRecovery,
			OldRecovery: oldRecovery,
		},
	})

}

func (this *OntId) AddAttributes(gasPrice, gasLimit uint64, signer *account.Account, ontId string, attributes []*identity.DDOAttribute, controller *identity.Controller) (common.Uint256, error) {
	type addAttributes struct {
		OntId      string
		Attributes []*identity.DDOAttribute
		PubKey     []byte
	}
	return this.InvokeNativeContract(signer, controller, "addAttributes", []interface{}{
		&addAttributes{
			OntId:      ontId,
			Attributes: attributes,
			PubKey:     keypair.SerializePublicKey(controller.PublicKey),
		},
	})
}

func (this *OntId) RemoveAttribute(gasPrice, gasLimit uint64, signer *account.Account, ontId string, removeKey []byte, controller *identity.Controller) (common.Uint256, error) {
	type removeAttribute struct {
		OntId  string
		Key    []byte
		PubKey []byte
	}
	return this.InvokeNativeContract(signer, controller, "removeAttribute", []interface{}{
		&removeAttribute{
			OntId:  ontId,
			Key:    removeKey,
			PubKey: keypair.SerializePublicKey(controller.PublicKey),
		},
	})
}

func (this *OntId) GetAttributes(ontId string) ([]*identity.DDOAttribute, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		"getAttributes",
		[]interface{}{ontId})
	if err != nil {
		return nil, err
	}
	data, err := preResult.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("ToByteArray error:%s", err)
	}
	return this.getAttributes(ontId, data)
}

func (this *OntId) getAttributes(ontId string, data []byte) ([]*identity.DDOAttribute, error) {
	buf := bytes.NewBuffer(data)
	attributes := make([]*identity.DDOAttribute, 0)
	for {
		if buf.Len() == 0 {
			break
		}
		key, err := serialization.ReadVarBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("key ReadVarBytes error:%s", err)
		}
		valueType, err := serialization.ReadVarBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("value type ReadVarBytes error:%s", err)
		}
		value, err := serialization.ReadVarBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("value ReadVarBytes error:%s", err)
		}
		attributes = append(attributes, &identity.DDOAttribute{
			Key:       key,
			Value:     value,
			ValueType: valueType,
		})
	}
	//reverse
	for i, j := 0, len(attributes)-1; i < j; i, j = i+1, j-1 {
		attributes[i], attributes[j] = attributes[j], attributes[i]
	}
	return attributes, nil
}

func (this *OntId) VerifySignature(ontId string, keyIndex int, controller *identity.Controller) (bool, error) {
	tx, err := utils.NewNativeInvokeTransaction(
		0, 0,
		ONT_ID_CONTRACT_VERSION,
		ONT_ID_CONTRACT_ADDRESS,
		"verifySignature",
		[]interface{}{ontId, keyIndex})
	if err != nil {
		return false, err
	}
	err = utils.SignToTransaction(tx, controller)
	if err != nil {
		return false, err
	}
	preResult, err := this.Client.PreExecTransaction(tx)
	if err != nil {
		return false, err
	}
	return preResult.Result.ToBool()
}

func (this *OntId) GetPublicKeys(ontId string) ([]*identity.DDOOwner, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		"getPublicKeys",
		[]interface{}{
			ontId,
		})
	if err != nil {
		return nil, err
	}
	data, err := preResult.Result.ToByteArray()
	if err != nil {
		return nil, err
	}
	return this.getPublicKeys(ontId, data)
}

func (this *OntId) getPublicKeys(ontId string, data []byte) ([]*identity.DDOOwner, error) {
	buf := bytes.NewBuffer(data)
	owners := make([]*identity.DDOOwner, 0)
	for {
		if buf.Len() == 0 {
			break
		}
		index, err := serialization.ReadUint32(buf)
		if err != nil {
			return nil, fmt.Errorf("index ReadUint32 error:%s", err)
		}
		pubKeyId := fmt.Sprintf("%s#keys-%d", ontId, index)
		pkData, err := serialization.ReadVarBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("PubKey Idenx:%d ReadVarBytes error:%s", index, err)
		}
		pubKey, err := keypair.DeserializePublicKey(pkData)
		if err != nil {
			return nil, fmt.Errorf("DeserializePublicKey Index:%d error:%s", index, err)
		}
		keyType := keypair.GetKeyType(pubKey)
		owner := &identity.DDOOwner{
			PubKeyIndex: index,
			PubKeyId:    pubKeyId,
			Type:        account.GetKeyTypeString(keyType),
			Curve:       account.GetCurveName(pkData),
			Value:       hex.EncodeToString(pkData),
		}
		owners = append(owners, owner)
	}
	return owners, nil
}

func (this *OntId) GetKeyState(ontId string, keyIndex int) (string, error) {
	type keyState struct {
		OntId    string
		KeyIndex int
	}
	preResult, err := this.PreExecInvokeNativeContract(
		"getKeyState",
		[]interface{}{
			&keyState{
				OntId:    ontId,
				KeyIndex: keyIndex,
			},
		})
	if err != nil {
		return "", err
	}
	return preResult.Result.ToString()
}
