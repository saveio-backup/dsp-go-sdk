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
		"/a/WechatIMG494.png",
	}
	for _, v := range arr {
		path, name, file := SplitFileNameFromPath(v)
		fmt.Println(v, path, name, file)
	}
}

func TestReplaceFileToDir(t *testing.T) {
	path := "/Users/smallyu/work/gogs/edge-deploy/node1/Chain-1/Downloads/AYKnc5VDkvpb5f68XSjTyQzVHU4ZaojGxq/SaveQma4vybM23GRVTAYdDseBAU6ynZ4CXJWGL4Q4YzPoorC1R.ept"
	err := ReplaceFileToDir(path)
	if err != nil {
		fmt.Println(err)
	}
}
