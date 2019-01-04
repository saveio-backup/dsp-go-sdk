package contract

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"

	chain "github.com/oniio/dsp-go-sdk/chain"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/common/log"
	"github.com/oniio/oniChain/smartcontract/service/native/micropayment"
	"github.com/oniio/oniChain/smartcontract/service/native/utils"
	"github.com/oniio/oniChain/vm/neovm/types"
)

// Micropaymenet contract const
const (
	MPAY_CONTRACT_VERSION = byte(0)
	DEFALUT_GAS_PRICE     = 0
	DEFAULT_GAS_LIMIT     = 30000
)

type MicroPayment struct {
	ChainSdk *chain.ChainSdk
	DefAcc   *chain.Account
	gasPrice uint64
	gasLimit uint64
	version  byte
}

func InitMicroPayment(acc *chain.Account, rpcSvrAddr string) *MicroPayment {
	sdk := chain.NewChainSdk()
	sdk.NewRpcClient().SetAddress(rpcSvrAddr)
	return &MicroPayment{
		ChainSdk: sdk,
		DefAcc:   acc,
		gasPrice: DEFALUT_GAS_PRICE,
		gasLimit: DEFAULT_GAS_LIMIT,
		version:  MPAY_CONTRACT_VERSION,
	}
}

func (this *MicroPayment) SetGasPrice(gasPrice uint64) {
	this.gasPrice = gasPrice
}

func (this *MicroPayment) SetGasLimit(gasLimit uint64) {
	this.gasLimit = gasLimit
}

func (this *MicroPayment) SetVersion(version byte) {
	this.version = version
}

func (this *MicroPayment) GetContractAddress() common.Address {
	return utils.MicroPayContractAddress
}

func (this *MicroPayment) GetVersion() byte {
	return this.version
}

func (this *MicroPayment) RegisterPaymentEndPoint(ip, port []byte, regAccount common.Address) ([]byte, error) {
	params := &micropayment.NodeInfo{
		WalletAddr: regAccount,
		IP:         ip,
		Port:       port,
	}
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_ENDPOINT_REGISTRY,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}
	return tx[:], nil
}

func (this *MicroPayment) OpenChannel(wallet1Addr, wallet2Addr common.Address, blockHeight uint64) ([]byte, error) {
	params := &micropayment.OpenChannelInfo{
		Participant1WalletAddr: wallet1Addr,
		Participant2WalletAddr: wallet2Addr,
		SettleBlockHeight:      blockHeight,
	}
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_OPEN_CHANNEL,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}
	return tx[:], nil
}

func (this *MicroPayment) SetTotalDeposit(channelId uint64, participantWalletAddr common.Address, partnerWalletAddr common.Address, deposit uint64) ([]byte, error) {
	params := &micropayment.SetTotalDepositInfo{
		ChannelID:             channelId,
		ParticipantWalletAddr: participantWalletAddr,
		PartnerWalletAddr:     partnerWalletAddr,
		SetTotalDeposit:       deposit,
	}
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_SET_TOTALDEPOSIT,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}
	return tx[:], nil
}
func (this *MicroPayment) SetTotalWithdraw(channelID uint64, participant, partner common.Address, totalWithdraw uint64, participantSig, participantPubKey, partnerSig, partnerPubKey []byte) ([]byte, error) {
	params := &micropayment.WithDraw{
		ChannelID:         channelID,
		Participant:       participant,
		Partner:           partner,
		TotalWithdraw:     totalWithdraw,
		ParticipantSig:    participantSig,
		ParticipantPubKey: participantPubKey,
		PartnerSig:        partnerSig,
		PartnerPubKey:     partnerPubKey,
	}
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_SET_TOTALWITHDRAW,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}
	return tx[:], nil
}
func (this *MicroPayment) CooperativeSettle(channelID uint64, participant1Address common.Address, participant1Balance uint64, participant2Address common.Address, participant2Balance uint64, participant1Signature, participant1PubKey, participant2Signature, participant2PubKey []byte) ([]byte, error) {
	params := &micropayment.CooperativeSettleInfo{
		ChannelID:             channelID,
		Participant1Address:   participant1Address,
		Participant1Balance:   participant1Balance,
		Participant2Address:   participant2Address,
		Participant2Balance:   participant2Balance,
		Participant1Signature: participant1Signature,
		Participant1PubKey:    participant1PubKey,
		Participant2Signature: participant2Signature,
		Participant2PubKey:    participant2PubKey,
	}
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_COOPERATIVESETTLE,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}
	return tx[:], nil
}
func (this *MicroPayment) CloseChannel(channelID uint64, participantAddress, partnerAddress common.Address, balanceHash []byte, nonce uint64, additionalHash, partnerSignature, partnerPubKey []byte) ([]byte, error) {
	params := &micropayment.CloseChannelInfo{
		ChannelID:          channelID,
		ParticipantAddress: participantAddress,
		PartnerAddress:     partnerAddress,
		BalanceHash:        balanceHash,
		Nonce:              nonce,
		AdditionalHash:     additionalHash,
		PartnerSignature:   partnerSignature,
		PartnerPubKey:      partnerPubKey,
	}
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_CLOSE_CHANNEL,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}
	return tx[:], nil
}
func (this *MicroPayment) RegisterSecret(secret []byte) ([]byte, error) {
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_SECRET_REG,
		[]interface{}{secret},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}

	return tx[:], nil
}
func (this *MicroPayment) RegisterSecretBatch(secrets []byte) ([]byte, error) {
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_SECRET_REG_BATCH,
		[]interface{}{secrets},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}
	return tx[:], nil
}

func (this *MicroPayment) GetSecretRevealBlockHeight(secretHash []byte) (uint64, error) {
	ret, err := this.ChainSdk.Native.PreExecInvokeNativeContract(
		this.GetContractAddress(),
		this.version,
		micropayment.MP_GET_SECRET_REVEAL_BLOCKHEIGHT,
		[]interface{}{secretHash},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return 0, err
	}
	buf, err := ret.Result.ToByteArray()
	if err != nil {
		return 0, err
	}
	height, err := strconv.ParseUint(hex.EncodeToString(common.ToArrayReverse(buf)), 16, 64)
	if err != nil {
		return 0, err
	}
	return height, nil
}
func (this *MicroPayment) UpdateNonClosingBalanceProof(chanID uint64, closeParticipant, nonCloseParticipant common.Address, balanceHash []byte, nonce uint64, additionalHash, closeSignature, nonCloseSignature, closePubKey, nonClosePubKey []byte) ([]byte, error) {
	params := &micropayment.UpdateNonCloseBalanceProof{
		ChanID:              chanID,
		CloseParticipant:    closeParticipant,
		NonCloseParticipant: nonCloseParticipant,
		BalanceHash:         balanceHash,
		Nonce:               nonce,
		AdditionalHash:      additionalHash,
		CloseSignature:      closeSignature,
		NonCloseSignature:   nonCloseSignature,
		ClosePubKey:         closePubKey,
		NonClosePubKey:      nonClosePubKey,
	}
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_UPDATE_NONCLOSING_BPF,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}

	return tx[:], nil
}

func (this *MicroPayment) SettleChannel(chanID uint64, participant1 common.Address, p1TransferredAmount, p1LockedAmount uint64, p1LocksRoot []byte, participant2 common.Address, p2TransferredAmount, p2LockedAmount uint64, p2LocksRoot []byte) ([]byte, error) {
	params := &micropayment.SettleChannelInfo{
		ChanID:              chanID,
		Participant1:        participant1,
		P1TransferredAmount: p1TransferredAmount,
		P1LockedAmount:      p1LockedAmount,
		P1LocksRoot:         p1LocksRoot,
		Participant2:        participant2,
		P2TransferredAmount: p2TransferredAmount,
		P2LockedAmount:      p2LockedAmount,
		P2LocksRoot:         p2LocksRoot,
	}
	tx, err := this.ChainSdk.Native.InvokeNativeContract(
		this.gasPrice,
		this.gasLimit,
		this.DefAcc,
		this.version,
		this.GetContractAddress(),
		micropayment.MP_SETTLE_CHANNEL,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}

	return tx[:], nil
}

func (this *MicroPayment) GetChannelInfo(channelID uint64, participant1, participant2 common.Address) (*micropayment.ChannelInfo, error) {
	params := &micropayment.GetChanInfo{
		ChannelID:    channelID,
		Participant1: participant1,
		Participant2: participant2,
	}

	ret, err := this.ChainSdk.Native.PreExecInvokeNativeContract(
		this.GetContractAddress(),
		this.version,
		micropayment.MP_GET_CHANNELINFO,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}

	buf, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, err
	}
	channelInfo := &micropayment.ChannelInfo{}
	source := common.NewZeroCopySource(buf)
	channelInfo.SettleBlockHeight, err = utils.DecodeVarUint(source)
	if err != nil {
		return nil, err
	}
	channelInfo.ChannelState, err = utils.DecodeVarUint(source)
	if err != nil {
		return nil, err
	}
	return channelInfo, nil
}

func (this *MicroPayment) GetChannelParticipantInfo(channelID uint64, participant1, participant2 common.Address) (*micropayment.Participant, error) {
	params := &micropayment.GetChanInfo{
		ChannelID:    channelID,
		Participant1: participant1,
		Participant2: participant2,
	}

	ret, err := this.ChainSdk.Native.PreExecInvokeNativeContract(
		this.GetContractAddress(),
		this.version,
		micropayment.MP_GET_CHANNEL_PARTICIPANTINFO,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}

	buf, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, err
	}
	source := common.NewZeroCopySource(buf)
	participant := &micropayment.Participant{}
	err = participant.Deserialization(source)
	if err != nil {
		return nil, err
	}
	return participant, nil
}

func (this *MicroPayment) GetOntBalance(address common.Address) (uint64, error) {
	return this.ChainSdk.Native.Ont.BalanceOf(address)
}

func (this *MicroPayment) GetOngBalance(address common.Address) (uint64, error) {
	return this.ChainSdk.Native.Ong.BalanceOf(address)
}

// GetEndpointByAddress get endpoint by user wallet address
func (this *MicroPayment) GetEndpointByAddress(nodeAddress common.Address) (*micropayment.NodeInfo, error) {
	ret, err := this.ChainSdk.Native.PreExecInvokeNativeContract(
		this.GetContractAddress(),
		this.version,
		micropayment.MP_FIND_ENDPOINT,
		[]interface{}{nodeAddress},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return nil, err
	}

	buf, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, err
	}
	nodeInfo := &micropayment.NodeInfo{}
	err = nodeInfo.Deserialize(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}

	return nodeInfo, nil
}

func (this *MicroPayment) GetChannelIdentifier(participant1WalletAddr, participant2WalletAddr common.Address) (uint64, error) {
	params := &micropayment.GetChannelId{
		Participant1WalletAddr: participant1WalletAddr,
		Participant2WalletAddr: participant2WalletAddr,
	}
	ret, err := this.ChainSdk.Native.PreExecInvokeNativeContract(
		this.GetContractAddress(),
		this.version,
		micropayment.MP_GET_CHANNELID,
		[]interface{}{params},
	)
	if err != nil {
		log.Errorf("Construct native invoke tx error, Msg:", err.Error())
		return 0, err
	}
	buf, err := ret.Result.ToByteArray()
	if err != nil {
		return 0, err
	}
	fmt.Printf("buf:%v\n", buf)
	valStr := fmt.Sprintf("%s", types.BigIntFromBytes(buf))
	return strconv.ParseUint(valStr, 10, 64)
}
