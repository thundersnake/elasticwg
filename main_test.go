package elasticwg

import (
	"github.com/op/go-logging"
	"os"
	"testing"
)

var gTestLogger = logging.MustGetLogger("unittests")
var esURL = "http://elasticsearch:9200"

// TestMain unit tests ramp up
func TestMain(m *testing.M) {
	code := m.Run()

	// Deinit code
	os.Exit(code)
}
