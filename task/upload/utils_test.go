package upload

import "testing"

func Test_getDirNameFromPath(t *testing.T) {
	paths := []string{
		"/Users/smallyu/work/test/file/aaa",
		"/Users/smallyu/work/test/file/aaa/bbb",
		"/Users/smallyu/work/test/file/aaa/bbb/ccc",
		"C:\\Users\\smallyu\\work\\test\\file\\aaa",
		"C:\\Users\\smallyu\\work\\test\\file\\aaa\\bbb",
		"C:\\Users\\smallyu\\work\\test\\file\\aaa\\bbb\\ccc",
	}
	for _, path := range paths {
		dirName := getDirNameFromPath(path)
		t.Log(dirName)
	}
}
