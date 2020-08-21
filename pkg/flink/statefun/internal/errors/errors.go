package errors

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// stateFunError represents one or more details about an error. They are usually
// nested in the order that additional context was wrapped around the original
// error.
type stateFunError struct {
	code    int
	cause   error
	message string
}

func BadRequest(format string, args ...interface{}) error {
	return &stateFunError{
		code:  http.StatusBadRequest,
		cause: fmt.Errorf(format, args...),
	}
}

func Wrap(e error, format string, args ...interface{}) error {
	code := http.StatusInternalServerError
	if inner, ok := e.(*stateFunError); ok {
		code = inner.code
	}

	return &stateFunError{
		code:    code,
		cause:   e,
		message: fmt.Sprintf(format, args...),
	}
}

func ToCode(e error) int {
	switch unwrapped := e.(type) {
	case *stateFunError:
		return unwrapped.code
	default:
		return http.StatusInternalServerError
	}
}

// Error outputs a beamError as a string. The top-level error message is
// displayed first, followed by each error's context and error message in
// sequence. The original error is output last.
func (e *stateFunError) Error() string {
	var builder strings.Builder

	e.printRecursive(&builder)

	return builder.String()
}

// printRecursive is a helper function for outputting the contexts and messages
// of a sequence of beamErrors.
func (e *stateFunError) printRecursive(builder *strings.Builder) {
	wraps := e.cause != nil

	if e.message != "" {
		builder.WriteString(e.message)
		if wraps {
			builder.WriteString("\n\tcaused by:\n")
		}
	}

	if wraps {
		if be, ok := e.cause.(*stateFunError); ok {
			be.printRecursive(builder)
		} else {
			builder.WriteString(e.cause.Error())
		}
	}
}

// Format implements the fmt.Formatter interface
func (e *stateFunError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v', 's':
		_, _ = io.WriteString(s, e.Error())
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", e.Error())
	}
}
