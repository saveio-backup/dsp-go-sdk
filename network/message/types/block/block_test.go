package block

import (
	"fmt"
	"testing"
)

func TestMarshal(t *testing.T) {
	b1 := &Block{
		Data: []byte("123"),
	}
	b1b, err := b1.XXX_Marshal(nil, false)
	fmt.Printf("b1b:%v, err:%s", b1b, err)
}

func TestUnMarshal(t *testing.T) {
	b1 := &Block{
		Data: []byte("123"),
	}
	b1b, err := b1.XXX_Marshal(nil, false)
	fmt.Printf("b1b:%v, err:%s", b1b, err)
	if err != nil {
		return
	}
	b2 := &Block{}
	err = b2.XXX_Unmarshal(b1b)
	fmt.Printf("err:%v, data:%s\n", err, string(b2.Data))
}
