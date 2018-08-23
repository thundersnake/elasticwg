package elastic_wg

import (
	"github.com/op/go-logging"
	"os"
	"testing"
)

var gTestLogger = logging.MustGetLogger("unittests")

// TestMain unit tests ramp up
func TestMain(m *testing.M) {
	code := m.Run()

	// Deinit code
	os.Exit(code)
}
