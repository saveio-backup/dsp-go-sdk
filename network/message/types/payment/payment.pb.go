// Code generated by protoc-gen-go. DO NOT EDIT.
// source: payment.proto

package payment

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Payment struct {
	Sender               string   `protobuf:"bytes,1,opt,name=sender,proto3" json:"sender,omitempty"`
	Receiver             string   `protobuf:"bytes,2,opt,name=receiver,proto3" json:"receiver,omitempty"`
	PaymentId            uint64   `protobuf:"varint,3,opt,name=paymentId,proto3" json:"paymentId,omitempty"`
	Asset                int32    `protobuf:"varint,4,opt,name=asset,proto3" json:"asset,omitempty"`
	UnitPrice            uint64   `protobuf:"varint,5,opt,name=unitPrice,proto3" json:"unitPrice,omitempty"`
	Amount               uint64   `protobuf:"varint,6,opt,name=amount,proto3" json:"amount,omitempty"`
	FileHash             string   `protobuf:"bytes,7,opt,name=fileHash,proto3" json:"fileHash,omitempty"`
	BlockHash            string   `protobuf:"bytes,8,opt,name=blockHash,proto3" json:"blockHash,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Payment) Reset()         { *m = Payment{} }
func (m *Payment) String() string { return proto.CompactTextString(m) }
func (*Payment) ProtoMessage()    {}
func (*Payment) Descriptor() ([]byte, []int) {
	return fileDescriptor_payment_20cc69266455d5bf, []int{0}
}
func (m *Payment) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Payment.Unmarshal(m, b)
}
func (m *Payment) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Payment.Marshal(b, m, deterministic)
}
func (dst *Payment) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Payment.Merge(dst, src)
}
func (m *Payment) XXX_Size() int {
	return xxx_messageInfo_Payment.Size(m)
}
func (m *Payment) XXX_DiscardUnknown() {
	xxx_messageInfo_Payment.DiscardUnknown(m)
}

var xxx_messageInfo_Payment proto.InternalMessageInfo

func (m *Payment) GetSender() string {
	if m != nil {
		return m.Sender
	}
	return ""
}

func (m *Payment) GetReceiver() string {
	if m != nil {
		return m.Receiver
	}
	return ""
}

func (m *Payment) GetPaymentId() uint64 {
	if m != nil {
		return m.PaymentId
	}
	return 0
}

func (m *Payment) GetAsset() int32 {
	if m != nil {
		return m.Asset
	}
	return 0
}

func (m *Payment) GetUnitPrice() uint64 {
	if m != nil {
		return m.UnitPrice
	}
	return 0
}

func (m *Payment) GetAmount() uint64 {
	if m != nil {
		return m.Amount
	}
	return 0
}

func (m *Payment) GetFileHash() string {
	if m != nil {
		return m.FileHash
	}
	return ""
}

func (m *Payment) GetBlockHash() string {
	if m != nil {
		return m.BlockHash
	}
	return ""
}

func init() {
	proto.RegisterType((*Payment)(nil), "payment.Payment")
}

func init() { proto.RegisterFile("payment.proto", fileDescriptor_payment_20cc69266455d5bf) }

var fileDescriptor_payment_20cc69266455d5bf = []byte{
	// 187 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x44, 0x8f, 0x4d, 0xaa, 0xc2, 0x30,
	0x14, 0x85, 0xc9, 0x7b, 0xfd, 0x0d, 0xbc, 0x49, 0x78, 0xc8, 0x45, 0x1c, 0x14, 0x47, 0x1d, 0x39,
	0x71, 0x13, 0x3a, 0x2b, 0xdd, 0x41, 0xda, 0x5e, 0x31, 0xd8, 0x26, 0x25, 0x49, 0x05, 0x97, 0xeb,
	0x4e, 0x24, 0x3f, 0xb6, 0xb3, 0x7c, 0xe7, 0xe3, 0x1e, 0x4e, 0xe8, 0xdf, 0xcc, 0x5f, 0x13, 0x4a,
	0x7b, 0x9a, 0xb5, 0xb2, 0x8a, 0xe5, 0x11, 0x8f, 0x6f, 0x42, 0xf3, 0x26, 0xbc, 0xd9, 0x8e, 0x66,
	0x06, 0xe5, 0x80, 0x1a, 0x48, 0x45, 0xea, 0xb2, 0x8d, 0xc4, 0xf6, 0xb4, 0xd0, 0xd8, 0xa3, 0x78,
	0xa2, 0x86, 0x1f, 0x6f, 0x56, 0x66, 0x07, 0x5a, 0xc6, 0xaa, 0xeb, 0x00, 0xbf, 0x15, 0xa9, 0x93,
	0x76, 0x0b, 0xd8, 0x3f, 0x4d, 0xb9, 0x31, 0x68, 0x21, 0xa9, 0x48, 0x9d, 0xb6, 0x01, 0xdc, 0xcd,
	0x22, 0x85, 0x6d, 0xb4, 0xe8, 0x11, 0xd2, 0x70, 0xb3, 0x06, 0x6e, 0x05, 0x9f, 0xd4, 0x22, 0x2d,
	0x64, 0x5e, 0x45, 0x72, 0x2b, 0x6e, 0x62, 0xc4, 0x0b, 0x37, 0x77, 0xc8, 0xc3, 0x8a, 0x2f, 0xbb,
	0xc6, 0x6e, 0x54, 0xfd, 0xc3, 0xcb, 0xc2, 0xcb, 0x2d, 0xe8, 0x32, 0xff, 0xe7, 0xf3, 0x27, 0x00,
	0x00, 0xff, 0xff, 0x56, 0x82, 0xc5, 0xca, 0x04, 0x01, 0x00, 0x00,
}
