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
	if err != nil {
		if dErr, ok := err.(*Error); ok {
			msg = dErr.Message
		} else {
			msg = err.Error()
		}
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

// Error. implement error string
func (serr *Error) Error() string {
	if serr == nil {
		return "error is nil"
	}
	return fmt.Sprintf("code: %d, message: %v", serr.Code, serr.Message)
}

func (serr *Error) Unwrap() error {
	return serr.Cause
}

// HasErrorCode. check if a error has same error code
func (serr *Error) HasErrorCode(errCode uint32) bool {
	if serr == nil {
		return false
	}
	return serr.Code == errCode
}

// func ConntextTimeoutErr(err error) bool {
// 	return strings.Contains(err.Error(), "context deadline exceeded")
// }
