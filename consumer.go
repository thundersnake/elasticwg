package elasticwg

import (
	"context"
	"gopkg.in/olivere/elastic.v5"
	"sync"
)

// Consumer consumes produces data from the workgroup channel
type Consumer struct {
	Index          string
	DocType        string
	BulkSize       int
	ElasticURL     string
	onPushCallback func(int)
	logger         Logger
}

func (c *Consumer) pushBulk(bulkRequest *elastic.BulkService) bool {
	bulkRequestActions := bulkRequest.NumberOfActions()
	retryCounter := 0
performBulk:
	_, err := bulkRequest.Do(context.Background())
	if err != nil {
		c.logger.Warningf("Failed to perform a bulk query: %v", err)
		retryCounter++
		// try to push 5 times
		if retryCounter < 5 {
			goto performBulk
		} else {
			c.logger.Error("Unable to push bulk query after 5 tentatives, aborting consuming.")
			return false
		}
	}

	// If push callback is defined, call it
	if c.onPushCallback != nil {
		c.onPushCallback(bulkRequestActions)
	}

	return true
}

// Consume consume documents inside a bulk request and send it to Elasticsearch
func (c *Consumer) Consume(cDoc chan *Document, wg *sync.WaitGroup) bool {
	defer wg.Done()
	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(c.ElasticURL),
	)
	if err != nil {
		c.logger.Errorf("%v", err)
		return false
	}

	n := 0
	bulkRequest := client.Bulk()
	for doc := range cDoc {
		n++

		req := elastic.NewBulkIndexRequest().
			Index(c.Index).
			Type(c.DocType).
			Id(doc.ID).
			Doc(doc.Content)
		bulkRequest = bulkRequest.Add(req)

		if n%c.BulkSize == 0 {
			if !c.pushBulk(bulkRequest) {
				return false
			}

			if n%5000 == 0 {
				c.logger.Infof("Pushed %d docs to elasticsearch", n)
			}
		}
	}

	// Flush remaining docs
	if bulkRequest.NumberOfActions() > 0 {
		if !c.pushBulk(bulkRequest) {
			return false
		}

		c.logger.Infof("Pushed %d docs to elasticsearch", n)
	}

	c.logger.Infof("Consuming finished. Pushed %d docs to elasticsearch", n)
	return true
}
