package elasticwg

import (
	"context"
	"encoding/json"
	"github.com/tevino/abool"
	"gopkg.in/olivere/elastic.v5"
	"io/ioutil"
	"sync"
	"time"
)

// Workgroup the main object intended to be used to process data
type Workgroup struct {
	elasticURL           string
	cfg                  WorkgroupConfig
	p                    *Producer
	logger               Logger
	FailureOnDupIndex    bool
	indexMapping         map[string]interface{}
	onStartupCallback    func() bool
	onFailureCallback    func()
	onFinishCallback     func(bool)
	onPushCallback       func(int)
	onVerifyStopCallback func() bool
	shouldStopFlag       *abool.AtomicBool
}

// NewWorkgroup creates the workgroup and define the initialization parameters
func NewWorkgroup(esURL string, wcfg WorkgroupConfig, pi ProducerInterface, logger Logger) *Workgroup {
	if logger == nil {
		return nil
	}

	if pi == nil {
		logger.Error("ProducerInterface is nil!")
		return nil
	}

	if wcfg.NumConsumers <= 0 {
		logger.Error("consumerNumber must be > 0!")
		return nil
	}

	if wcfg.BulkSize <= 0 {
		logger.Error("bulkSize must be > 0!")
		return nil
	}

	if wcfg.ChannelBufferSize < 0 {
		logger.Error("channelBufferSize must be >= 0!")
		return nil
	}

	wg := &Workgroup{
		cfg:               wcfg,
		elasticURL:        esURL,
		logger:            logger,
		FailureOnDupIndex: true,
		shouldStopFlag:    abool.New(),
		p: &Producer{
			pi:     pi,
			logger: logger,
		},
	}

	// Share the shouldStopFlag with the producer
	wg.p.shouldStopFlag = wg.shouldStopFlag

	if len(wg.cfg.MappingFile) > 0 && !wg.SetIndexMappingFromFile(wg.cfg.MappingFile) {
		return nil
	}

	return wg
}

// SetOnProduceCallback define the callback to call when a document is produced
// callback parameter is the current document produced count
func (w *Workgroup) SetOnProduceCallback(cb func(uint64)) {
	w.p.onProduceCallback = cb
}

// SetOnProductionFinishedCallback define the callback to call when production is finished
// callback parameter is the document produced count
func (w *Workgroup) SetOnProductionFinishedCallback(cb func(uint64)) {
	w.p.onProductionFinishedCallback = cb
}

// SetStartupCallback define the callback to call when workgroup starts
func (w *Workgroup) SetStartupCallback(cb func() bool) {
	w.onStartupCallback = cb
}

// SetFailureCallback define the callback to call when the workgroup has a failure
func (w *Workgroup) SetFailureCallback(cb func()) {
	w.onFailureCallback = cb
}

// SetFinishCallback define the callback to call when the workgroup has successfully finished
// Boolean parameter define if workgroup was cancelled
func (w *Workgroup) SetFinishCallback(cb func(bool)) {
	w.onFinishCallback = cb
}

// SetOnPushCallback define the callback to call when a bulk request has been pushed to Elasticsearch
func (w *Workgroup) SetOnPushCallback(cb func(int)) {
	w.onPushCallback = cb
}

// SetIndexMapping define index mapping to apply just after index creation
func (w *Workgroup) SetIndexMapping(mapping map[string]interface{}) {
	w.indexMapping = mapping
}

// SetVerificationStopCallback set a function to call periodically to verify if we should stop the workgroup
func (w *Workgroup) SetVerificationStopCallback(cb func() bool) {
	w.onVerifyStopCallback = cb
}

// RequestStop launch a stopping order to all consumers & producers.
// Producer stop depend on the implementation. ShouldStop must be called regularly.
func (w *Workgroup) RequestStop() {
	w.shouldStopFlag.Set()
}

// ShouldStop returns true if we should stop the workgroup
func (w *Workgroup) ShouldStop() bool {
	return w.shouldStopFlag.IsSet()
}

// SetIndexMappingFromFile read file at path and load indexMapping to apply
// just after index creation
func (w *Workgroup) SetIndexMappingFromFile(path string) bool {
	bJSON, err := ioutil.ReadFile(path)
	if err != nil {
		w.logger.Errorf("Unable to read index mapping from file '%s': %v", path, err)
		return false
	}

	if err := json.Unmarshal(bJSON, &w.indexMapping); err != nil {
		w.logger.Errorf("Unable to unmarshal index mapping from file '%s': %v", path, err)
		return false
	}

	return true
}

// GetIndexName returns the configured index name
func (w *Workgroup) GetIndexName() string {
	return w.cfg.IndexName
}

func (w *Workgroup) spawnConsumers(wg *sync.WaitGroup, ch chan *Document) {
	for i := 0; i < w.cfg.NumConsumers; i++ {
		wg.Add(1)
		c := newConsumer(
			w.elasticURL,
			w.cfg.IndexName,
			w.cfg.DocType,
			w.cfg.BulkSize,
			w.logger,
			w.shouldStopFlag)

		// Set the consumer callback function if defined on the workgroup
		if w.onPushCallback != nil {
			c.onPushCallback = w.onPushCallback
		}

		go c.Consume(ch, wg)
	}
}

// Run make the workgroup run
func (w *Workgroup) Run() bool {
	if w.onStartupCallback != nil {
		// If startup callback has failed, stop immediately
		if !w.onStartupCallback() {
			if w.onFailureCallback != nil {
				w.onFailureCallback()
			}
			return false
		}
	}

	if w.onVerifyStopCallback != nil {
		go func() {
			for !w.ShouldStop() {
				if w.onVerifyStopCallback() {
					w.RequestStop()
				}

				time.Sleep(5 * time.Second)
			}
		}()
	}

	// If we leave the Run function we should notify everybody that they should stop too.
	// This is essentially for the onVerifyStopCallback loop
	defer w.RequestStop()

	if w.ShouldStop() {
		return false
	}

	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(w.elasticURL),
	)
	if err != nil {
		w.logger.Errorf("%v\n", err)
		if w.onFailureCallback != nil {
			w.onFailureCallback()
		}
		return false
	}

	if w.ShouldStop() {
		return false
	}

	tStart := time.Now()
	n := 0

	esConfig := IndexConfig{}
	esConfig.Index.NumberOfReplicas = 0
	esConfig.Index.RefreshInterval = "-1"
	if _, err := client.CreateIndex(w.cfg.IndexName).Do(context.Background()); err != nil && w.FailureOnDupIndex {
		w.logger.Errorf("Unable to create elasticsearch index '%s': %v", w.cfg.IndexName, err)
		if w.onFailureCallback != nil {
			w.onFailureCallback()
		}
		return false
	}

	if w.ShouldStop() {
		return false
	}

	if w.indexMapping != nil {
		if _, err := client.PutMapping().Index(w.cfg.IndexName).Type(w.cfg.DocType).BodyJson(w.indexMapping).
			Do(context.Background()); err != nil {
			w.logger.Errorf("Unable to put elasticsearch index mapping on '%s': %v", w.cfg.IndexName, err)
			if w.onFailureCallback != nil {
				w.onFailureCallback()
			}
			return false
		}
	}

	if w.ShouldStop() {
		return false
	}

	if _, err := client.IndexPutSettings(w.cfg.IndexName).BodyJson(esConfig).Do(context.Background()); err != nil {
		w.logger.Errorf("Unable to put elasticsearch index settings on '%s': %v", w.cfg.IndexName, err)
		if w.onFailureCallback != nil {
			w.onFailureCallback()
		}
		return false
	}

	if w.ShouldStop() {
		return false
	}

	// Create the production wait group
	wgProduce := &sync.WaitGroup{}
	cDoc := make(chan *Document, w.cfg.ChannelBufferSize)

	// Configure & start the producer
	wgProduce.Add(1)
	w.p.setChannelAndWaitGroup(cDoc, wgProduce)
	go w.p.produce()

	if w.ShouldStop() {
		return false
	}

	// Create the consuming wait group & start consuming
	wgConsume := &sync.WaitGroup{}
	w.spawnConsumers(wgConsume, cDoc)

	wgProduce.Wait()
	// Production finished, closing the channel
	close(cDoc)

	// Now finishing to consume
	wgConsume.Wait()

	// If it's not manually stopped, set the ES index config properly
	if !w.ShouldStop() {
		// Re-set ES index standard configs
		esConfig.Index.NumberOfReplicas = 1
		esConfig.Index.RefreshInterval = "10s"
		if _, err := client.IndexPutSettings(w.cfg.IndexName).BodyJson(esConfig).Do(context.Background()); err != nil {
			w.logger.Errorf("Unable to put elasticsearch index settings.")
			if w.onFailureCallback != nil {
				w.onFailureCallback()
			}
			return false
		}
	}

	// Send the finish callback with the manual stop flag
	if w.onFinishCallback != nil {
		w.onFinishCallback(w.ShouldStop())
	}

	tEnd := time.Now()
	elapsed := tEnd.Sub(tStart)
	w.logger.Infof("%d documents indexed in %s (bulksize: %d).", n, elapsed.String(), w.cfg.BulkSize)
	return true
}
