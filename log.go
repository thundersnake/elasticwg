package elasticwg

// Logger logging interface intended to be implemented for this library
type Logger interface {
	Info(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warning(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Error(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}
