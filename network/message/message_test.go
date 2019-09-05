package message

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	netcom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
)

func TestReadMsg(t *testing.T) {
	msg1 := NewFileFetchAsk("1", "132245457678789", []string{"1", "2"}, "wallet", []byte{24, 123})
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

func TestBuildBlockFlightsMsg(t *testing.T) {
	blocks := make([]*block.Block, 0)
	for index := 0; index < 16; index++ {
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
			Data: make([]byte, 1024*256),
		}
		blocks = append(blocks, b)
	}
	flight := &block.BlockFlights{
		TimeStamp: time.Now().UnixNano(),
		Blocks:    blocks,
	}
	msg1 := NewBlockFlightsMsg(flight)

	buf1, _ := proto.Marshal(msg1.ToProtoMsg())
	fmt.Printf("after proto & before gzip,buf1 len:%v\n", len(buf1))
	buf2, _ := GzipEncode(buf1)
	fmt.Printf("after gzip,buf2 len:%v\n", len(buf2))
}

func GzipEncode(in []byte) ([]byte, error) {
	var (
		buffer bytes.Buffer
		out    []byte
		err    error
	)
	writer := gzip.NewWriter(&buffer)
	_, err = writer.Write(in)
	if err != nil {
		writer.Close()
		return out, err
	}
	err = writer.Close()
	if err != nil {
		return out, err
	}

	return buffer.Bytes(), nil
}
