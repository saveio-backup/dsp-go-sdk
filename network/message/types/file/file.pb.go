// Code generated by protoc-gen-go. DO NOT EDIT.
// source: file.proto

package file

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
	Asset                int32    `protobuf:"varint,1,opt,name=asset,proto3" json:"asset,omitempty"`
	Price                uint64   `protobuf:"varint,2,opt,name=price,proto3" json:"price,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Payment) Reset()         { *m = Payment{} }
func (m *Payment) String() string { return proto.CompactTextString(m) }
func (*Payment) ProtoMessage()    {}
func (*Payment) Descriptor() ([]byte, []int) {
	return fileDescriptor_file_ff177ddf805cbba5, []int{0}
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

func (m *Payment) GetAsset() int32 {
	if m != nil {
		return m.Asset
	}
	return 0
}

func (m *Payment) GetPrice() uint64 {
	if m != nil {
		return m.Price
	}
	return 0
}

type File struct {
	Hash                 string   `protobuf:"bytes,1,opt,name=hash,proto3" json:"hash,omitempty"`
	BlockHashes          []string `protobuf:"bytes,2,rep,name=blockHashes,proto3" json:"blockHashes,omitempty"`
	Operation            int32    `protobuf:"varint,3,opt,name=operation,proto3" json:"operation,omitempty"`
	PayInfo              *Payment `protobuf:"bytes,4,opt,name=payInfo,proto3" json:"payInfo,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *File) Reset()         { *m = File{} }
func (m *File) String() string { return proto.CompactTextString(m) }
func (*File) ProtoMessage()    {}
func (*File) Descriptor() ([]byte, []int) {
	return fileDescriptor_file_ff177ddf805cbba5, []int{1}
}
func (m *File) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_File.Unmarshal(m, b)
}
func (m *File) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_File.Marshal(b, m, deterministic)
}
func (dst *File) XXX_Merge(src proto.Message) {
	xxx_messageInfo_File.Merge(dst, src)
}
func (m *File) XXX_Size() int {
	return xxx_messageInfo_File.Size(m)
}
func (m *File) XXX_DiscardUnknown() {
	xxx_messageInfo_File.DiscardUnknown(m)
}

var xxx_messageInfo_File proto.InternalMessageInfo

func (m *File) GetHash() string {
	if m != nil {
		return m.Hash
	}
	return ""
}

func (m *File) GetBlockHashes() []string {
	if m != nil {
		return m.BlockHashes
	}
	return nil
}

func (m *File) GetOperation() int32 {
	if m != nil {
		return m.Operation
	}
	return 0
}

func (m *File) GetPayInfo() *Payment {
	if m != nil {
		return m.PayInfo
	}
	return nil
}

func init() {
	proto.RegisterType((*Payment)(nil), "file.Payment")
	proto.RegisterType((*File)(nil), "file.File")
}

func init() { proto.RegisterFile("file.proto", fileDescriptor_file_ff177ddf805cbba5) }

var fileDescriptor_file_ff177ddf805cbba5 = []byte{
	// 181 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x4c, 0x8f, 0xb1, 0x0e, 0x82, 0x30,
	0x10, 0x86, 0x53, 0x28, 0x12, 0x8e, 0xb8, 0x34, 0x0e, 0x1d, 0x1c, 0x1a, 0x16, 0x3b, 0x31, 0x68,
	0x7c, 0x05, 0xa3, 0x9b, 0xe9, 0x1b, 0x14, 0x72, 0x84, 0x46, 0xa4, 0x0d, 0xed, 0xc2, 0xec, 0x8b,
	0x1b, 0x8a, 0x46, 0xb7, 0xff, 0xff, 0x2e, 0x97, 0xfb, 0x0e, 0xa0, 0x33, 0x03, 0xd6, 0x6e, 0xb2,
	0xc1, 0x32, 0xba, 0xe4, 0xea, 0x0c, 0xf9, 0x5d, 0xcf, 0x4f, 0x1c, 0x03, 0xdb, 0x41, 0xa6, 0xbd,
	0xc7, 0xc0, 0x89, 0x20, 0x32, 0x53, 0x6b, 0x59, 0xa8, 0x9b, 0x4c, 0x8b, 0x3c, 0x11, 0x44, 0x52,
	0xb5, 0x96, 0xea, 0x45, 0x80, 0x5e, 0xcc, 0x80, 0x8c, 0x01, 0xed, 0xb5, 0xef, 0xe3, 0x4e, 0xa1,
	0x62, 0x66, 0x02, 0xca, 0x66, 0xb0, 0xed, 0xe3, 0xaa, 0x7d, 0x8f, 0x9e, 0x27, 0x22, 0x95, 0x85,
	0xfa, 0x47, 0x6c, 0x0f, 0x85, 0x75, 0x38, 0xe9, 0x60, 0xec, 0xc8, 0xd3, 0x78, 0xee, 0x07, 0xd8,
	0x01, 0x72, 0xa7, 0xe7, 0xdb, 0xd8, 0x59, 0x4e, 0x05, 0x91, 0xe5, 0x71, 0x5b, 0x47, 0xef, 0x8f,
	0xa8, 0xfa, 0x4e, 0x9b, 0x4d, 0xfc, 0xe4, 0xf4, 0x0e, 0x00, 0x00, 0xff, 0xff, 0x64, 0x3f, 0x83,
	0x88, 0xd7, 0x00, 0x00, 0x00,
}
