package chain

import (
	"errors"
	"strings"
)

func (t *Themis) FormatError(err error) error {
	if strings.Contains(err.Error(), "appCallTransfer") {
		return errors.New("insufficient balance")
	}
	return err
}
