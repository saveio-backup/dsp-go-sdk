package async

import (
	"fmt"
	"time"
)

func ExampleRequestWithArgs() {
	args := make([][]interface{}, 0)
	args = append(args, []interface{}{1})
	args = append(args, []interface{}{2})
	args = append(args, []interface{}{2})
	args = append(args, []interface{}{3})
	delayFunc := func(a []interface{}, resp chan *RequestResponse) {
		time.Sleep(time.Duration(a[0].(int)) * time.Second)
		resp <- &RequestResponse{
			Result: a,
		}
	}
	resps := RequestWithArgs(delayFunc, args)
	for _, resp := range resps {
		fmt.Printf("result %v\n", resp.Result)
	}
	fmt.Printf("done in %d second", 3)
	// Output:
	// result [1]
	// result [2]
	// result [2]
	// result [3]
	// done in 3 second
}

func ExampleRequestOneWithArgs() {
	args := make([][]interface{}, 0)
	args = append(args, []interface{}{1})
	args = append(args, []interface{}{2})
	args = append(args, []interface{}{2})
	args = append(args, []interface{}{3})
	delayFunc := func(a []interface{}, resp chan *RequestResponse) bool {
		time.Sleep(time.Duration(a[0].(int)) * time.Second)
		resp <- &RequestResponse{
			Result: a,
		}
		return true
	}
	resps := RequestOneWithArgs(delayFunc, args)
	for _, resp := range resps {
		fmt.Printf("result %v\n", resp.Result)
	}
	fmt.Printf("done in %d second", 1)
	// Output:
	// result [1]
	// done in 1 second
}

func ExampleDoWithTimeout() {
	f := func() error {
		time.Sleep(time.Duration(2) * time.Second)
		return nil
	}
	err1 := DoWithTimeout(f, time.Duration(1)*time.Second)
	fmt.Printf("err1 %v\n", err1)
	f2 := func() error {
		return nil
	}
	err2 := DoWithTimeout(f2, time.Duration(1)*time.Second)
	fmt.Printf("err2 %v\n", err2)
	// Output:
	// err1 action timeout
	// err2 <nil>
}
