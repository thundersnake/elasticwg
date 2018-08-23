package elastic_wg

import "sync"

type ProducerInterface interface {
	Produce(*Producer)
}

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
