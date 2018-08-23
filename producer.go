package elasticwg

import "sync"

// ProducerInterface a generic interface which provides documents to be pushed to the consumers
type ProducerInterface interface {
	Produce(*Producer)
}

// Producer ows the ProducerInterface and publish to the consumer channel
type Producer struct {
	c                            chan *Document
	wg                           *sync.WaitGroup
	pi                           ProducerInterface
	counter                      uint64
	onProduceCallback            func(uint64)
	onProductionFinishedCallback func(uint64)
}

func (p *Producer) setChannelAndWaitGroup(ch chan *Document, w *sync.WaitGroup) {
	p.c = ch
	p.wg = w
}

// Push push Elasticsearch document to the consuming channel & run the onProduceCallback if provided
func (p *Producer) Push(doc *Document) {
	p.c <- doc
	p.counter++
	if p.onProduceCallback != nil {
		p.onProduceCallback(p.counter)
	}
}

func (p *Producer) produce() {
	defer p.wg.Done()
	p.pi.Produce(p)

	// Exec the produce callback a last time at the end
	if p.onProductionFinishedCallback != nil {
		p.onProductionFinishedCallback(p.counter)
	}
}
