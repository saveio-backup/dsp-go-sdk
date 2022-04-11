package download

import (
	"fmt"
	"testing"
)

func TestSplitFileNameFromPath(t *testing.T) {
	arr := []string{
		"/a160",
		"/a166",
		"/aaa/a139",
		"/aaa/bbb/a160",
		"/aaa/",
		"/",
	}
	for _, v := range arr {
		path, name, file := SplitFileNameFromPath(v)
		fmt.Println(v, path, name, file)
	}
}
