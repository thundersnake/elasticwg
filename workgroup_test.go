package elasticwg

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testProducer struct {
}

func (p *testProducer) Produce(pe *Producer) {

}

func TestNewWorkgroup(t *testing.T) {
	assert.Nil(t, NewWorkgroup(
		esURL,
		"test_index",
		"doc_type_test",
		&testProducer{},
		10,
		500,
		nil),
	)

	assert.Nil(t, NewWorkgroup(
		esURL,
		"test_index",
		"doc_type_test",
		nil,
		1,
		5000,
		gTestLogger),
	)

	assert.Nil(t, NewWorkgroup(
		esURL,
		"test_index",
		"doc_type_test",
		&testProducer{},
		0,
		5000,
		gTestLogger),
	)

	assert.Nil(t, NewWorkgroup(
		esURL,
		"test_index",
		"doc_type_test",
		&testProducer{},
		-10,
		5000,
		gTestLogger),
	)

	assert.Nil(t, NewWorkgroup(
		esURL,
		"test_index",
		"doc_type_test",
		&testProducer{},
		10,
		0,
		gTestLogger),
	)

	assert.Nil(t, NewWorkgroup(
		esURL,
		"test_index",
		"doc_type_test",
		&testProducer{},
		10,
		-88,
		gTestLogger),
	)

	assert.NotNil(t, NewWorkgroup(
		esURL,
		"test_index",
		"doc_type_test",
		&testProducer{},
		10,
		500,
		gTestLogger),
	)
}

func TestWorkgroupSetOnProduceCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, "test_index", "doc_type_test", &testProducer{}, 10, 500, gTestLogger)
	wg.SetOnProduceCallback(func(uint64) {
		gTestLogger.Info("test")
	})

	assert.NotNil(t, wg.p.onProduceCallback)
}
