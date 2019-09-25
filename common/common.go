package common

import (
	"os"
	"strings"
)

const (
	DSP_SDK_VERSION = "1.0.0" // dsp go sdk version
)

// FileExisted checks whether filename exists in filesystem
func FileExisted(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func ConntextTimeoutErr(err error) bool {
	return strings.Contains(err.Error(), "context deadline exceeded")
}

func CreateDirIfNeed(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}
