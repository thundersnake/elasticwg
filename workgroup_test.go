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

func TestWorkgroup_SetOnProduceCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, "test_index", "doc_type_test", &testProducer{}, 10, 500, gTestLogger)
	wg.SetOnProduceCallback(func(a uint64) {
		gTestLogger.Info("test %d", a)
	})

	assert.NotNil(t, wg.p.onProduceCallback)
}

func TestWorkgroup_SetFailureCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, "test_index", "doc_type_test", &testProducer{}, 10, 500, gTestLogger)
	wg.SetFailureCallback(func() {
		gTestLogger.Info("test")
	})

	assert.NotNil(t, wg.onFailureCallback)
}

func TestWorkgroup_SetFinishCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, "test_index", "doc_type_test", &testProducer{}, 10, 500, gTestLogger)
	wg.SetFinishCallback(func() {
		gTestLogger.Info("test")
	})

	assert.NotNil(t, wg.onFinishCallback)
}

func TestWorkgroup_SetIndexMapping(t *testing.T) {
	wg := NewWorkgroup(esURL, "test_index", "doc_type_test", &testProducer{}, 10, 500, gTestLogger)
	indexMapping := make(map[string]interface{}, 0)
	wg.SetIndexMapping(indexMapping)

	assert.NotNil(t, wg.indexMapping)
}

func TestWorkgroup_SetOnProductionFinishedCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, "test_index", "doc_type_test", &testProducer{}, 10, 500, gTestLogger)
	wg.SetOnProductionFinishedCallback(func(a uint64) {
		gTestLogger.Infof("test: %d", a)
	})

	assert.NotNil(t, wg.p.onProductionFinishedCallback)
}

func TestWorkgroup_SetOnPushCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, "test_index", "doc_type_test", &testProducer{}, 10, 500, gTestLogger)
	wg.SetOnPushCallback(func(a int) {
		gTestLogger.Infof("test: %d", a)
	})

	assert.NotNil(t, wg.onPushCallback)
}

func TestWorkgroup_SetStartupCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, "test_index", "doc_type_test", &testProducer{}, 10, 500, gTestLogger)
	wg.SetStartupCallback(func() bool {
		gTestLogger.Info("test")
		return true
	})

	assert.NotNil(t, wg.onStartupCallback)
}
