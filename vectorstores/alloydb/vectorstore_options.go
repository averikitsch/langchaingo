package alloydb

import (
	"errors"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/internal/alloydbutil"
	"github.com/tmc/langchaingo/vectorstores"
)

const (
	defaultSchemaName         = "public"
	defaultIDColumn           = "langchain_id"
	defaultContentColumn      = "content"
	defaultEmbeddingColumn    = "embedding"
	defaultMetadataJSONColumn = "langchain_metadata"
	defaultK                  = 4
)

// VectoreStoresOption is a function for creating new vector store
// with other than the default values.
type VectoreStoresOption func(vs *VectorStore)

// WithSchemaName sets the VectorStore's schemaName field.
func WithSchemaName(schemaName string) VectoreStoresOption {
	return func(v *VectorStore) {
		v.schemaName = schemaName
	}
}

// WithContentColumn sets VectorStore's the idColumn field.
func WithIDColumn(idColumn string) VectoreStoresOption {
	return func(v *VectorStore) {
		v.idColumn = idColumn
	}
}

// WithMetadataJSONColumn sets VectorStore's the metadataJSONColumn field.
func WithMetadataJSONColumn(metadataJSONColumn string) VectoreStoresOption {
	return func(v *VectorStore) {
		v.metadataJSONColumn = metadataJSONColumn
	}
}

// WithContentColumn sets the VectorStore's ContentColumn field.
func WithContentColumn(contentColumn string) VectoreStoresOption {
	return func(v *VectorStore) {
		v.contentColumn = contentColumn
	}
}

// WithEmbeddingColumn sets the EmbeddingColumn field.
func WithEmbeddingColumn(embeddingColumn string) VectoreStoresOption {
	return func(v *VectorStore) {
		v.embeddingColumn = embeddingColumn
	}
}

// WithMetadataColumns sets the VectorStore's MetadataColumns field.
func WithMetadataColumns(metadataColumns []string) VectoreStoresOption {
	return func(v *VectorStore) {
		v.metadataColumns = metadataColumns
	}
}

// WithK sets the number of Documents to return from the VectorStore.
func WithK(k int) VectoreStoresOption {
	return func(v *VectorStore) {
		v.k = k
	}
}

// WithDistanceStrategy sets the distance strategy used by the VectorStore.
func WithDistanceStrategy(distanceStrategy distanceStrategy) VectoreStoresOption {
	return func(v *VectorStore) {
		v.distanceStrategy = distanceStrategy
	}
}

// applyAlloyDBVectorStoreOptions applies the given VectorStore options to the
// VectorStore with an alloydb Engine.
func applyAlloyDBVectorStoreOptions(engine alloydbutil.PostgresEngine,
	embedder embeddings.Embedder,
	tableName string,
	opts ...VectoreStoresOption,
) (VectorStore, error) {
	// Check for required values.
	if engine.Pool == nil {
		return VectorStore{}, errors.New("missing vector store engine")
	}
	if embedder == nil {
		return VectorStore{}, errors.New("missing vector store embeder")
	}
	if tableName == "" {
		return VectorStore{}, errors.New("missing vector store table name")
	}
	defaultDistanceStrategy := CosineDistance{}

	vs := &VectorStore{
		engine:             engine,
		embedder:           embedder,
		tableName:          tableName,
		schemaName:         defaultSchemaName,
		idColumn:           defaultIDColumn,
		contentColumn:      defaultContentColumn,
		embeddingColumn:    defaultEmbeddingColumn,
		metadataJSONColumn: defaultMetadataJSONColumn,
		k:                  defaultK,
		distanceStrategy:   defaultDistanceStrategy,
		metadataColumns:    []string{},
	}
	for _, opt := range opts {
		opt(vs)
	}

	return *vs, nil
}

func applyOpts(options ...vectorstores.Option) vectorstores.Options {
	opts := vectorstores.Options{}
	for _, opt := range options {
		opt(&opts)
	}
	return opts
}
