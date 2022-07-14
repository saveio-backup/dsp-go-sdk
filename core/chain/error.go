package chain

import (
	"errors"
	"strings"
)

func (c *Chain) FormatError(err error) error {

	if strings.Contains(err.Error(), "appCallTransfer") {
		return errors.New("insufficient balance")
	}

	return err

}
