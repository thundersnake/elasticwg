package elasticwg

// IndexConfig The elasticsearch index configuration object
type IndexConfig struct {
	Index struct {
		NumberOfReplicas int    `json:"number_of_replicas,omit"`
		RefreshInterval  string `json:"refresh_interval"`
	} `json:"index"`
}
