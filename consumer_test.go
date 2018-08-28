package elasticwg

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v5"
	"strconv"
	"sync"
	"testing"
)

func TestConsumer_ConsumeWrongURL(t *testing.T) {
	c := make(chan *Document)
	w := &sync.WaitGroup{}
	w.Add(1)

	consumer := Consumer{
		logger: gTestLogger,
	}

	assert.False(t, consumer.Consume(c, w))
}

func TestConsumer_pushBulkFailure(t *testing.T) {
	consumer := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
	}

	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(esURL),
	)

	assert.Nil(t, err)
	if err != nil {
		println(err)
		return
	}

	assert.False(t, consumer.pushBulk(client.Bulk()))
}

func TestConsumer_pushBulkIndexNameMissing(t *testing.T) {
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		DocType:    "pushBulk",
	}

	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(esURL),
	)

	assert.Nil(t, err)
	if err != nil {
		println(err)
		return
	}

	doc := Document{
		ID:      "TestConsumer_pushBulk",
		Content: "coucou",
	}

	bulk := client.Bulk()
	req := elastic.NewBulkIndexRequest().
		Index(c.Index).
		Type(c.DocType).
		Id(doc.ID).
		Doc(doc.Content)
	bulk = bulk.Add(req)

	assert.False(t, c.pushBulk(bulk))
}

func TestConsumer_pushBulkDocTypeMissing(t *testing.T) {
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		Index:      "Doctypemissing",
	}

	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(esURL),
	)

	assert.Nil(t, err)
	if err != nil {
		println(err)
		return
	}

	doc := Document{
		ID:      "TestConsumer_pushBulk",
		Content: "coucou",
	}

	bulk := client.Bulk()
	req := elastic.NewBulkIndexRequest().
		Index(c.Index).
		Type(c.DocType).
		Id(doc.ID).
		Doc(doc.Content)
	bulk = bulk.Add(req)

	assert.False(t, c.pushBulk(bulk))
}

func TestConsumer_pushBulk(t *testing.T) {
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		Index:      "test3",
		DocType:    "pushBulk",
	}

	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(esURL),
	)

	assert.Nil(t, err)
	if err != nil {
		println(err)
		return
	}

	doc := Document{
		ID:      "TestConsumer_pushBulk",
		Content: "coucou",
	}

	bulk := client.Bulk()
	req := elastic.NewBulkIndexRequest().
		Index(c.Index).
		Type(c.DocType).
		Id(doc.ID).
		Doc(doc.Content)
	bulk = bulk.Add(req)

	assert.True(t, c.pushBulk(bulk))
}

func TestConsumer_pushBulkCallback(t *testing.T) {
	pushedNumber := 0
	expectedNumber := 40
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		Index:      "test3",
		DocType:    "pushBulk",
		onPushCallback: func(i int) {
			pushedNumber = i
		},
	}

	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(esURL),
	)

	assert.Nil(t, err)
	if err != nil {
		println(err)
		return
	}

	doc := Document{
		ID:      "TestConsumer_pushBulk",
		Content: "coucou",
	}

	bulk := client.Bulk()
	req := elastic.NewBulkIndexRequest().
		Index(c.Index).
		Type(c.DocType).
		Id(doc.ID).
		Doc(doc.Content)
	for i := 0; i < expectedNumber; i++ {
		bulk = bulk.Add(req)
	}

	assert.True(t, c.pushBulk(bulk))
	assert.Equal(t, expectedNumber, pushedNumber)
}

func TestConsumer_ConsumeEmpty(t *testing.T) {
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		Index:      "test7",
		DocType:    "Consumer_Consume",
		BulkSize:   100,
	}

	ch := make(chan *Document)
	w := &sync.WaitGroup{}
	w.Add(1)

	go func() {
		close(ch)
	}()
	assert.True(t, c.Consume(ch, w))
}

func TestConsumer_ConsumeInvalidURL(t *testing.T) {
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: "http://fakeurl.fake.fake.fake.polp",
		Index:      "test7",
		DocType:    "Consumer_Consume",
		BulkSize:   100,
	}

	ch := make(chan *Document)
	close(ch)
	w := &sync.WaitGroup{}
	w.Add(1)
	assert.False(t, c.Consume(ch, w))
}

func TestConsumer_ConsumeInvalidBulkSize(t *testing.T) {
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		Index:      "test7",
		DocType:    "Consumer_Consume",
	}

	ch := make(chan *Document)
	close(ch)
	w := &sync.WaitGroup{}
	w.Add(1)
	assert.False(t, c.Consume(ch, w))

	w.Add(1)
	c.BulkSize = 50
	assert.False(t, c.Consume(ch, w))

	w.Add(1)
	c.BulkSize = 100
	assert.True(t, c.Consume(ch, w))
}

func TestConsumer_Consume(t *testing.T) {
	expectedConsume := 50
	consumeNumber := 0
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		Index:      "test7",
		DocType:    "Consumer_Consume",
		BulkSize:   100,
		onPushCallback: func(i int) {
			consumeNumber = i
		},
	}

	ch := make(chan *Document)
	w := &sync.WaitGroup{}
	w.Add(1)

	go func() {
		for i := 0; i < expectedConsume; i++ {
			ch <- &Document{
				ID:      strconv.Itoa(i),
				Content: fmt.Sprintf("toto-%d", i),
			}
		}
		close(ch)
	}()

	assert.True(t, c.Consume(ch, w))
	assert.Equal(t, expectedConsume, consumeNumber)
}

func TestConsumer_ConsumeMultiBulk(t *testing.T) {
	expectedConsume := 250
	consumeNumber := 0
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		Index:      "test8",
		DocType:    "Consumer_ConsumeMultiBulk",
		BulkSize:   100,
		onPushCallback: func(i int) {
			consumeNumber += i
		},
	}

	ch := make(chan *Document)
	w := &sync.WaitGroup{}
	w.Add(1)

	go func() {
		for i := 0; i < expectedConsume; i++ {
			ch <- &Document{
				ID:      strconv.Itoa(i),
				Content: fmt.Sprintf("toto-%d", i),
			}
		}
		close(ch)
	}()

	assert.True(t, c.Consume(ch, w))
	assert.Equal(t, expectedConsume, consumeNumber)
}

func TestConsumer_ConsumeMultiBulkLogBlock(t *testing.T) {
	expectedConsume := 10000
	consumeNumber := 0
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		Index:      "test8",
		DocType:    "Consumer_ConsumeMultiBulk",
		BulkSize:   100,
		onPushCallback: func(i int) {
			consumeNumber += i
		},
	}

	ch := make(chan *Document)
	w := &sync.WaitGroup{}
	w.Add(1)

	go func() {
		for i := 0; i < expectedConsume; i++ {
			ch <- &Document{
				ID:      strconv.Itoa(i),
				Content: fmt.Sprintf("toto-%d", i),
			}
		}
		close(ch)
	}()

	assert.True(t, c.Consume(ch, w))
	assert.Equal(t, expectedConsume, consumeNumber)
}

func TestConsumer_ShouldStop(t *testing.T) {
	c := Consumer{
		logger:     gTestLogger,
		ElasticURL: esURL,
		Index:      "test8",
		DocType:    "Consumer_ConsumeMultiBulk",
		BulkSize:   100,
		stopChan:   make(chan struct{}),
	}

	assert.False(t, c.shouldStop())

	close(c.stopChan)
	assert.True(t, c.shouldStop())
}
