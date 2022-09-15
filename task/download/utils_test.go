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

func TestSplitFileNameFromPathWithOS(t *testing.T) {
	arr := []string{
		"/a160",
		"/a166",
		"/aaa/a139",
		"/aaa/bbb/a160",
		"/aaa/",
		"/",
		"/a/WechatIMG494.png",
		"C:\\a\\WechatIMG494.png",
		"C:\\a\\WechatIMG494.png\\",
		"C:\\a\\WechatIMG494.png\\a",
	}
	for _, v := range arr {
		path, name, file := SplitFileNameFromPathWithOS(v)
		fmt.Println(v, path, name, file)
	}
}

func TestRemoveSuffix(t *testing.T) {
	s := []string{
		"aaaa",
		"aaaa-",
		"aaaa--",
		"aaaa---",
		"aaaa----",
	}
	for _, v := range s {
		fmt.Println(RemoveSuffix(v))
	}
}

func TestSetMapWithSuffix(t *testing.T) {
	dict := make(map[string]int64)
	s := []string{
		"aaaa",
		"aaaa-",
		"aaaa--",
		"aaaa---",
		"aaaa----",
	}
	for _, v := range s {
		SetMapWithSuffix(dict, v, 1)
	}
	fmt.Println(dict)
}
