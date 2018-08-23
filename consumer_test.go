package elasticwg

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v5"
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
