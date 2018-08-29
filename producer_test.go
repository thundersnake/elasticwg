package elasticwg

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tevino/abool"
	"strconv"
	"sync"
	"testing"
)

func TestProducer_setChannelAndWaitGroup(t *testing.T) {
	p := Producer{}
	c := make(chan *Document)
	w := &sync.WaitGroup{}
	p.setChannelAndWaitGroup(c, w)

	assert.NotNil(t, p.c)
	assert.NotNil(t, p.wg)
}

func TestProducer_Push(t *testing.T) {
	p := Producer{}
	c := make(chan *Document, 1)
	w := &sync.WaitGroup{}
	w.Add(1)
	p.setChannelAndWaitGroup(c, w)

	p.Push(&Document{
		ID:      "3",
		Content: []string{"test1", "test2"},
	})

	assert.Equal(t, uint64(1), p.counter)

	resultDoc := <-c
	close(c)
	assert.NotNil(t, resultDoc)
	assert.Equal(t, "3", resultDoc.ID)
	assert.Equal(t, []string{"test1", "test2"}, resultDoc.Content)
}

func TestProducer_PushWithCallback(t *testing.T) {
	var finalCount uint64
	expectedCount := 150

	p := Producer{
		onProduceCallback: func(a uint64) {
			finalCount = a
		},
	}
	c := make(chan *Document, expectedCount)
	w := &sync.WaitGroup{}
	w.Add(1)
	p.setChannelAndWaitGroup(c, w)

	for i := 0; i < expectedCount; i++ {
		p.Push(&Document{
			ID:      "3",
			Content: []string{"test1", "test2"},
		})
	}

	assert.Equal(t, uint64(expectedCount), finalCount)
}

type testProducerInterface struct {
}

func (tpi *testProducerInterface) Produce(p *Producer) {
	for i := 0; i < 250; i++ {
		p.Push(&Document{
			ID:      strconv.Itoa(i),
			Content: fmt.Sprintf("test_%d", i),
		})
	}
}

func TestProducer_Produce(t *testing.T) {
	var finalCount uint64
	expectedCount := 250

	p := Producer{
		pi: &testProducerInterface{},
		onProduceCallback: func(a uint64) {
			finalCount = a
		},
	}

	c := make(chan *Document, expectedCount)
	w := &sync.WaitGroup{}
	w.Add(1)
	p.setChannelAndWaitGroup(c, w)

	p.produce()

	w.Wait()
	assert.Equal(t, uint64(expectedCount), finalCount)
}

func TestProducer_ProduceWithFinal(t *testing.T) {
	var finalCount uint64
	expectedCount := 250

	p := Producer{
		pi: &testProducerInterface{},
		onProductionFinishedCallback: func(u uint64) {
			finalCount = u
		},
	}

	c := make(chan *Document, expectedCount)
	w := &sync.WaitGroup{}
	w.Add(1)
	p.setChannelAndWaitGroup(c, w)

	p.produce()

	w.Wait()
	assert.Equal(t, uint64(expectedCount), finalCount)
}

func TestProducer_ShouldStop(t *testing.T) {
	p := Producer{
		pi:             &testProducerInterface{},
		logger:         gTestLogger,
		shouldStopFlag: abool.New(),
	}

	assert.False(t, p.ShouldStop())
	p.shouldStopFlag.Set()
	assert.True(t, p.ShouldStop())
}
