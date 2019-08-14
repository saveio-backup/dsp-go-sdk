package error

import "errors"

type SDKError struct {
	Code    uint32
	Message string
	Error   error
}

func NewDetailError(code uint32, msg string) *SDKError {
	err := errors.New(msg)
	return &SDKError{
		Code:    code,
		Message: msg,
		Error:   err,
	}
}
