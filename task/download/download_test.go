package download

import (
	"fmt"
	"testing"
)

func Test_rankByValue(t *testing.T) {
	m := make(map[string]int64)
	m["a"] = 4
	m["b"] = 1
	m["c"] = 2
	m["d"] = 3
	value := rankByValue(m)
	fmt.Println(value)
}
