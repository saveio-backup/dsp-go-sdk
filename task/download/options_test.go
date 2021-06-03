package download

import (
	"testing"

	"github.com/saveio/dsp-go-sdk/task/base"
)

func TestNewDownloadTaskWithOpts(t *testing.T) {
	dt := NewDownloadTask("123", 2, nil)
	dt.SetInfoWithOptions(
		base.Asset(1),
	)
}
