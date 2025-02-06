package cloudsql

import (
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/internal/cloudsqlutil"
	"github.com/tmc/langchaingo/vectorstores"
)

const (
	defaultIndexNameSuffix = "langchainvectorindex"
)

type VectorStore struct {
	engine             cloudsqlutil.PostgresEngine
	embedder           embeddings.Embedder
	tableName          string
	schemaName         string
	idColumn           string
	metadataJsonColumn string
	contentColumn      string
	embeddingColumn    string
	metadataColumns    []string
	k                  int
	distanceStrategy   distanceStrategy
}

type BaseIndex struct {
	name             string
	indexType        string
	options          Index
	distanceStrategy distanceStrategy
	partialIndexes   []string
}

type SearchDocument struct {
	Content            string
	Langchain_metadata string
	Distance           float32
}

var _ vectorstores.VectorStore = &VectorStore{}
