package elasticwg

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testProducer struct {
}

func (p *testProducer) Produce(pe *Producer) {

}

var testCfg = WorkgroupConfig{
	IndexName:    "test_index",
	DocType:      "doc_type_test",
	NumConsumers: 10,
	BulkSize:     500,
}

func TestNewWorkgroup(t *testing.T) {
	cfg := WorkgroupConfig{
		IndexName:    "test_index",
		DocType:      "doc_type_test",
		NumConsumers: 10,
		BulkSize:     500,
	}

	assert.Nil(t, NewWorkgroup(
		esURL,
		cfg,
		&testProducer{},
		nil),
	)

	cfg.NumConsumers = 1
	cfg.BulkSize = 5000
	assert.Nil(t, NewWorkgroup(
		esURL,
		cfg,
		nil,
		gTestLogger),
	)

	cfg.NumConsumers = 0
	cfg.BulkSize = 5000
	assert.Nil(t, NewWorkgroup(
		esURL,
		cfg,
		&testProducer{},
		gTestLogger),
	)

	cfg.NumConsumers = -1
	cfg.BulkSize = 5000
	assert.Nil(t, NewWorkgroup(
		esURL,
		cfg,
		&testProducer{},
		gTestLogger),
	)

	cfg.NumConsumers = 10
	cfg.BulkSize = 0
	assert.Nil(t, NewWorkgroup(
		esURL,
		cfg,
		&testProducer{},
		gTestLogger),
	)

	cfg.NumConsumers = 10
	cfg.BulkSize = -88
	assert.Nil(t, NewWorkgroup(
		esURL,
		cfg,
		&testProducer{},
		gTestLogger),
	)

	cfg.NumConsumers = 10
	cfg.BulkSize = 500
	cfg.ChannelBufferSize = -1
	assert.Nil(t, NewWorkgroup(
		esURL,
		cfg,
		&testProducer{},
		gTestLogger),
	)

	cfg.NumConsumers = 10
	cfg.BulkSize = 500
	cfg.ChannelBufferSize = -1001545
	assert.Nil(t, NewWorkgroup(
		esURL,
		cfg,
		&testProducer{},
		gTestLogger),
	)

	cfg.NumConsumers = 10
	cfg.BulkSize = 500
	cfg.ChannelBufferSize = 15000
	assert.NotNil(t, NewWorkgroup(
		esURL,
		cfg,
		&testProducer{},
		gTestLogger),
	)

	cfg.NumConsumers = 10
	cfg.BulkSize = 500
	assert.NotNil(t, NewWorkgroup(
		esURL,
		cfg,
		&testProducer{},
		gTestLogger),
	)
}

func TestWorkgroup_SetOnProduceCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	wg.SetOnProduceCallback(func(a uint64) {
		gTestLogger.Info("test %d", a)
	})

	assert.NotNil(t, wg.p.onProduceCallback)
}

func TestWorkgroup_SetFailureCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	wg.SetFailureCallback(func() {
		gTestLogger.Info("test")
	})

	assert.NotNil(t, wg.onFailureCallback)
}

func TestWorkgroup_SetFinishCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	wg.SetFinishCallback(func(bool) {
		gTestLogger.Info("test")
	})

	assert.NotNil(t, wg.onFinishCallback)
}

func TestWorkgroup_SetIndexMapping(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	indexMapping := make(map[string]interface{}, 0)
	wg.SetIndexMapping(indexMapping)

	assert.NotNil(t, wg.indexMapping)
}

func TestWorkgroup_SetIndexMappingFromFileUnkFile(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	assert.False(t, wg.SetIndexMappingFromFile("ci/mapping_test_unkfile.json"))
	assert.Nil(t, wg.indexMapping)
}

func TestWorkgroup_SetIndexMappingFromFileBadJSON(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	assert.False(t, wg.SetIndexMappingFromFile("ci/mapping_test.badjson"))
	assert.Nil(t, wg.indexMapping)
}

func TestWorkgroup_SetIndexMappingFromFile(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	assert.True(t, wg.SetIndexMappingFromFile("ci/mapping_test.json"))
	assert.NotNil(t, wg.indexMapping)
}

func TestWorkgroup_SetOnProductionFinishedCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	wg.SetOnProductionFinishedCallback(func(a uint64) {
		gTestLogger.Infof("test: %d", a)
	})

	assert.NotNil(t, wg.p.onProductionFinishedCallback)
}

func TestWorkgroup_SetVerificationStopCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	wg.SetVerificationStopCallback(func() bool {
		return true
	})

	assert.NotNil(t, wg.onVerifyStopCallback)
}

func TestWorkgroup_RequestStop(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	assert.NotNil(t, wg.stopChan)
	assert.False(t, wg.RequestStop())
}

func TestWorkgroup_SetOnPushCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	wg.SetOnPushCallback(func(a int) {
		gTestLogger.Infof("test: %d", a)
	})

	assert.NotNil(t, wg.onPushCallback)
}

func TestWorkgroup_SetStartupCallback(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	wg.SetStartupCallback(func() bool {
		gTestLogger.Info("test")
		return true
	})

	assert.NotNil(t, wg.onStartupCallback)
}

func TestWorkgroup_GetIndexName(t *testing.T) {
	wg := NewWorkgroup(esURL, testCfg, &testProducer{}, gTestLogger)
	wg.cfg.IndexName = "unittest_fake_name_5dsfsdf"
	assert.Equal(t, "unittest_fake_name_5dsfsdf", wg.GetIndexName())
}
