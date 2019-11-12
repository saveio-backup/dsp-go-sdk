package error

import (
	"fmt"
	"testing"
)

func TestNewError(t *testing.T) {
	a := New(1, "this is a format err %s", "hello")
	fmt.Printf("err :%s\n", a)
	var err2 error
	err2 = a
	fmt.Printf("err2: %s\n", err2)
	serr2 := err2.(*Error)
	fmt.Printf("code: %d\n", serr2.Code)
}

func TestErrIsNil(t *testing.T) {
	var err1 error
	var err2 *Error
	err3, ok1 := err1.(*Error)
	fmt.Printf("1 is nil %t, 2 is nil: %t, ok1: %t, err3: %t\n", err1 == nil, err2 == nil, ok1, err3 == nil)
	err1 = err2
	err3, ok2 := err1.(*Error)
	fmt.Printf("1 is nil %t, 2 is nil: %t, ok2: %t, err3: %t\n", err1 == nil, err2 == nil, ok2, err3 == nil)
}
