// Code generated by protoc-gen-go. DO NOT EDIT.
// source: block.proto

package block

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

type Block struct {
	Index                int32    `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	FileHash             string   `protobuf:"bytes,2,opt,name=fileHash,proto3" json:"fileHash,omitempty"`
	Hash                 string   `protobuf:"bytes,3,opt,name=hash,proto3" json:"hash,omitempty"`
	Data                 []byte   `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
	Tag                  []byte   `protobuf:"bytes,5,opt,name=tag,proto3" json:"tag,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Block) Reset()         { *m = Block{} }
func (m *Block) String() string { return proto.CompactTextString(m) }
func (*Block) ProtoMessage()    {}
func (*Block) Descriptor() ([]byte, []int) {
	return fileDescriptor_block_7c1189fcc708ad0b, []int{0}
}
func (m *Block) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Block.Unmarshal(m, b)
}
func (m *Block) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Block.Marshal(b, m, deterministic)
}
func (dst *Block) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Block.Merge(dst, src)
}
func (m *Block) XXX_Size() int {
	return xxx_messageInfo_Block.Size(m)
}
func (m *Block) XXX_DiscardUnknown() {
	xxx_messageInfo_Block.DiscardUnknown(m)
}

var xxx_messageInfo_Block proto.InternalMessageInfo

func (m *Block) GetIndex() int32 {
	if m != nil {
		return m.Index
	}
	return 0
}

func (m *Block) GetFileHash() string {
	if m != nil {
		return m.FileHash
	}
	return ""
}

func (m *Block) GetHash() string {
	if m != nil {
		return m.Hash
	}
	return ""
}

func (m *Block) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Block) GetTag() []byte {
	if m != nil {
		return m.Tag
	}
	return nil
}

func init() {
	proto.RegisterType((*Block)(nil), "block.Block")
}

func init() { proto.RegisterFile("block.proto", fileDescriptor_block_7c1189fcc708ad0b) }

var fileDescriptor_block_7c1189fcc708ad0b = []byte{
	// 130 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x4e, 0xca, 0xc9, 0x4f,
	0xce, 0xd6, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x05, 0x73, 0x94, 0x8a, 0xb9, 0x58, 0x9d,
	0x40, 0x0c, 0x21, 0x11, 0x2e, 0xd6, 0xcc, 0xbc, 0x94, 0xd4, 0x0a, 0x09, 0x46, 0x05, 0x46, 0x0d,
	0xd6, 0x20, 0x08, 0x47, 0x48, 0x8a, 0x8b, 0x23, 0x2d, 0x33, 0x27, 0xd5, 0x23, 0xb1, 0x38, 0x43,
	0x82, 0x49, 0x81, 0x51, 0x83, 0x33, 0x08, 0xce, 0x17, 0x12, 0xe2, 0x62, 0xc9, 0x00, 0x89, 0x33,
	0x83, 0xc5, 0xc1, 0x6c, 0x90, 0x58, 0x4a, 0x62, 0x49, 0xa2, 0x04, 0x8b, 0x02, 0xa3, 0x06, 0x4f,
	0x10, 0x98, 0x2d, 0x24, 0xc0, 0xc5, 0x5c, 0x92, 0x98, 0x2e, 0xc1, 0x0a, 0x16, 0x02, 0x31, 0x93,
	0xd8, 0xc0, 0x4e, 0x30, 0x06, 0x04, 0x00, 0x00, 0xff, 0xff, 0xa5, 0xea, 0x69, 0x69, 0x91, 0x00,
	0x00, 0x00,
}
