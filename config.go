package elasticwg

// IndexConfig The elasticsearch index configuration object
type IndexConfig struct {
	Index struct {
		NumberOfReplicas int    `json:"number_of_replicas,omit"`
		RefreshInterval  string `json:"refresh_interval"`
	} `json:"index"`
}

// WorkgroupConfig workgroup configuration object
type WorkgroupConfig struct {
	IndexName    string `yaml:"name"`
	DocType      string `yaml:"docType"`
	NumConsumers int    `yaml:"numWorkers"`
	BulkSize     int    `yaml:"bulkSize"`
	MappingFile  string `yaml:"mapping-file"`
}
