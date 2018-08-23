package elasticwg

// Document An Elasticsearch simple document
// ID is the Elasticsearch document ID
// Content is the document itself
type Document struct {
	ID      string
	Content interface{}
}
