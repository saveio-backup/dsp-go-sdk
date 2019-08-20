package message

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	netcom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
)

func TestReadMsg(t *testing.T) {
	msg1 := NewFileFetchAsk("1", "132245457678789", []string{"1", "2"}, "wallet", "")
	fmt.Printf("msg1.header:%v, payload:%v\n", msg1.Header, msg1.Payload)
	msg1Proto := msg1.ToProtoMsg()
	msg2 := ReadMessage(msg1Proto)
	fmt.Printf("msg2.header:%v, payload:%v\n", msg2.Header, msg2.Payload)
	msg2payload := msg2.Payload.(*file.File)
	fmt.Printf("msg2.op:%d, hash:%s, payinfo:%v\n", msg2payload.Operation, msg2payload.Hash, msg2payload.PayInfo)
}

func TestFileMarshal(t *testing.T) {
	f := &file.File{
		Hash:        "1",
		BlockHashes: []string{"1", "2"},
		Operation:   1,
		PayInfo: &file.Payment{
			WalletAddress: "wallet",
		},
	}
	data, err := proto.Marshal(f)
	if err != nil {
		return
	}
	fmt.Printf("Data:%v\n", data)

	v := &file.File{}
	err = proto.Unmarshal(data, v)
	if err != nil {
		fmt.Printf("err is %v\n", err)
		return
	}
	fmt.Printf("f2:%v\n", v)

}
func TestReadBlockFlightsMsg(t *testing.T) {
	flights := make([]*block.Block, 0)
	b := &block.Block{
		SessionId: "12345678",
		Index:     0,
		FileHash:  "QmdadwjioeLALA",
		Hash:      "QmdadwjioeLALA",
		Operation: netcom.BLOCK_OP_GET,
		Payment: &payment.Payment{
			Sender: "AKuNEKrUcaLpmZNc2nEXCk3JSDHwKrQYJH",
			Asset:  1,
		},
	}
	flights = append(flights, b)
	msg1 := NewBlockFlightsReqMsg(flights, 213454364564)
	fmt.Printf("msg1.header:%v, payload:%v\n", msg1.Header, msg1.Payload)
	msg1Proto := msg1.ToProtoMsg()
	msg2 := ReadMessage(msg1Proto)
	fmt.Printf("msg2.header:%v, payload:%q\n", msg2.Header, msg2.Payload)
	msg2payload := msg2.Payload.(*block.BlockFlights)
	fmt.Printf("msg2.payload:%q\n", msg2payload)
}
