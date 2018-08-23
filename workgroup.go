package elastic_wg

import (
	"context"
	"gopkg.in/olivere/elastic.v5"
	"sync"
	"time"
)

type Workgroup struct {
	consumerNumber    int
	elasticURL        string
	index             string
	docType           string
	bulkSize          int
	p                 *Producer
	logger            Logger
	FailureOnDupIndex bool
	onStartupCallback func() bool
	onFailureCallback func()
	onFinishCallback  func()
	onPushCallback    func(int)
}

func NewWorkgroup(esURL string, index string, docType string, pi ProducerInterface, consumerNumber int,
	bulkSize int, logger Logger) *Workgroup {
	if logger == nil {
		return nil
	}

	if pi == nil {
		logger.Error("ProducerInterface is nil!")
		return nil
	}

	if consumerNumber <= 0 {
		logger.Error("consumerNumber must be > 0!")
		return nil
	}

	if bulkSize <= 0 {
		logger.Error("bulkSize must be > 0!")
		return nil
	}

	return &Workgroup{
		consumerNumber:    consumerNumber,
		elasticURL:        esURL,
		bulkSize:          bulkSize,
		index:             index,
		docType:           docType,
		logger:            logger,
		FailureOnDupIndex: true,
		p: &Producer{
			pi: pi,
		},
	}
}

func (w *Workgroup) SetOnProduceCallback(cb func(uint64)) {
	w.p.onProduceCallback = cb
}

func (w *Workgroup) SetOnProductionFinishedCallback(cb func(uint64)) {
	w.p.onProductionFinishedCallback = cb
}

func (w *Workgroup) SetStartupCallback(cb func() bool) {
	w.onStartupCallback = cb
}

func (w *Workgroup) SetFailureCallback(cb func()) {
	w.onFailureCallback = cb
}

func (w *Workgroup) SetFinishCallback(cb func()) {
	w.onFinishCallback = cb
}

func (w *Workgroup) SetOnPushCallback(cb func(int)) {
	w.onPushCallback = cb
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

	tStart := time.Now()
	n := 0

	esConfig := IndexConfig{}
	esConfig.Index.NumberOfReplicas = 0
	esConfig.Index.RefreshInterval = "-1"
	if _, err := client.CreateIndex(w.index).Do(context.Background()); err != nil && w.FailureOnDupIndex {
		w.logger.Errorf("Unable to create elasticsearch index '%s': %v", w.index, err)
		if w.onFailureCallback != nil {
			w.onFailureCallback()
		}
		return false
	}

	if _, err := client.IndexPutSettings(w.index).BodyJson(esConfig).Do(context.Background()); err != nil {
		w.logger.Errorf("Unable to put elasticsearch index settings on '%s': %v", w.index, err)
		if w.onFailureCallback != nil {
			w.onFailureCallback()
		}
		return false
	}

	// Create the production wait group
	wgProduce := &sync.WaitGroup{}
	cDoc := make(chan *Document)

	// Configure & start the producer
	wgProduce.Add(1)
	w.p.setChannelAndWaitGroup(cDoc, wgProduce)
	go w.p.produce()

	// Create the consuming wait group & start consuming
	wgConsume := &sync.WaitGroup{}
	for i := 0; i < w.consumerNumber; i++ {
		wgConsume.Add(1)
		c := Consumer{
			BulkSize:   w.bulkSize,
			ElasticURL: w.elasticURL,
			DocType:    w.docType,
			Index:      w.index,
			logger:     w.logger,
		}

		// Set the consumer callback function if defined on the workgroup
		if w.onPushCallback != nil {
			c.onPushCallback = w.onPushCallback
		}

		go c.Consume(cDoc, wgConsume)
	}

	wgProduce.Wait()
	// Production finished, closing the channel
	close(cDoc)

	// Now finishing to consume
	wgConsume.Wait()

	// Re-set ES index standard configs
	esConfig.Index.NumberOfReplicas = 1
	esConfig.Index.RefreshInterval = "10s"
	if _, err := client.IndexPutSettings(w.index).BodyJson(esConfig).Do(context.Background()); err != nil {
		w.logger.Errorf("Unable to put elasticsearch index settings.")
		if w.onFailureCallback != nil {
			w.onFailureCallback()
		}
		return false
	}

	if w.onFinishCallback != nil {
		w.onFinishCallback()
	}

	tEnd := time.Now()
	elapsed := tEnd.Sub(tStart)
	w.logger.Infof("%d documents indexed in %s (bulksize: %d).", n, elapsed.String(), w.bulkSize)
	return true
}
