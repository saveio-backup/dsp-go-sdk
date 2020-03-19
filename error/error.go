package error

import (
	"errors"
	"fmt"
)

type Error struct {
	Code    uint32
	Message string
	Cause   error
}

func New(code uint32, format string, a ...interface{}) *Error {
	msg := fmt.Sprintf(format, a...)
	err := errors.New(msg)
	return &Error{
		Code:    code,
		Message: msg,
		Cause:   err,
	}
}

// NewWithError. new dsp error with non-nil error
func NewWithError(code uint32, err error) *Error {
	msg := ""
	if dErr, ok := err.(*Error); ok {
		msg = dErr.Message
	}
	if len(msg) == 0 {
		msg = "internal error"
	}
	return &Error{
		Code:    code,
		Message: msg,
		Cause:   err,
	}
}

func (serr *Error) Error() string {
	if serr == nil {
		return "error is nil"
	}
	return fmt.Sprintf("code: %d, message: %v", serr.Code, serr.Message)
}
