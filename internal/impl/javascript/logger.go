package javascript

import (
	"github.com/benthosdev/benthos/v4/internal/log"
)

// Logger wraps the service.Logger so that we can define the below methods.
type VMLogger struct {
	l log.Modular
}

// Log will be used for "console.log()" in JS
func (l *VMLogger) Log(message string) {
	l.l.Infoln(message)
}

// Warn will be used for "console.warn()" in JS
func (l *VMLogger) Warn(message string) {
	l.l.Warnln(message)
}

// Error will be used for "console.error()" in JS
func (l *VMLogger) Error(message string) {
	l.l.Errorln(message)
}
