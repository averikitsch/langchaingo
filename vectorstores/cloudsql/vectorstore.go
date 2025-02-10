package cloudsql

import (
	"context"
	"fmt"
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

// NewVectorStore creates a new VectorStore with options.
func NewVectorStore(ctx context.Context, engine cloudsqlutil.PostgresEngine, embedder embeddings.Embedder, tableName string, opts ...CloudSQLVectoreStoresOption) (VectorStore, error) {
	vs, err := applyCloudSQLVectorStoreOptions(engine, embedder, tableName, opts...)
	if err != nil {
		return VectorStore{}, err
	}
	return vs, nil
}

// ApplyVectorIndex creates an index in the table of the embeddings
func (vs *VectorStore) ApplyVectorIndex(ctx context.Context, index BaseIndex, name string) error {
	if index.indexType == "exactnearestneighbor" {
		return vs.DropVectorIndex(ctx, name)
	}

	filter := ""
	if len(index.partialIndexes) > 0 {
		filter = fmt.Sprintf("WHERE %s", index.partialIndexes)
	}
	optsString, err := index.indexOptions()
	if err != nil {
		return fmt.Errorf("indexOptions error: %w", err)
	}
	params := fmt.Sprintf("WITH %s", optsString)

	if name == "" {
		if index.name == "" {
			index.name = vs.tableName + defaultIndexNameSuffix
		}
		name = index.name
	}

	function := index.distanceStrategy.searchFunction()
	stmt := fmt.Sprintf("CREATE INDEX %s ON %s.%s USING %s (%s %s) %s %s",
		name, vs.schemaName, vs.tableName, index.indexType, vs.embeddingColumn, function, params, filter)

	_, err = vs.engine.Pool.Exec(ctx, stmt)
	if err != nil {
		return fmt.Errorf("failed to execute creation of index: %w", err)
	}

	return nil
}

// DropVectorIndex drops the vector index from the VectorStore.
func (vs *VectorStore) DropVectorIndex(ctx context.Context, indexName string) error {
	if indexName == "" {
		indexName = vs.tableName + defaultIndexNameSuffix
	}
	query := fmt.Sprintf("DROP INDEX IF EXISTS %s;", indexName)
	_, err := vs.engine.Pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop vector index: %w", err)
	}

	return nil
}
