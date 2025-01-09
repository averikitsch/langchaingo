package alloydb

import (
	"errors"

	"github.com/tmc/langchaingo/embeddings"
)

const (
	defaultCollectionName           = "langchain"
	defaultPreDeleteCollection      = false
	defaultEmbeddingStoreTableName  = "langchain_pg_embedding"
	defaultCollectionStoreTableName = "langchain_pg_collection"
)

// VectoreStoresOption is a function for creating new vector store
// with other than the default values.
type VectoreStoresOption func(vs *vectorStore)

// WithEngine sets the Engine field for the vectorStore.
func WithEngine(engine PostgresEngine) VectoreStoresOption {
	return func(vs *vectorStore) {
		vs.engine = engine
	}
}

// WithEmbedder sets the Embedder field for the vectorStore.
func WithEmbedder(embedder embeddings.Embedder) VectoreStoresOption {
	return func(vs *vectorStore) {
		vs.embedder = embedder
	}
}

// WithEmbeddingTableName sets the embeddingTableName field for the vectorStore.
func WithEmbeddingTableName(embeddingTableName string) VectoreStoresOption {
	return func(vs *vectorStore) {
		vs.embeddingTableName = embeddingTableName
	}
}

// WithCollectionTableName sets the collectionTableName field for the vectorStore.
func WithCollectionTableName(collectionTableName string) VectoreStoresOption {
	return func(vs *vectorStore) {
		vs.collectionTableName = collectionTableName
	}
}

// WithCollectionUUID sets the collectionUUID field for the vectorStore.
func WithCollectionUUID(collectionUUID string) VectoreStoresOption {
	return func(vs *vectorStore) {
		vs.collectionUUID = collectionUUID
	}
}

// WithCollectionName sets the collectionName field for the vectorStore.
func WithCollectionName(collectionName string) VectoreStoresOption {
	return func(vs *vectorStore) {
		vs.collectionName = collectionName
	}
}

// ApplyVectorStoreOptions applies the given vectorStore options to the vectorStore.
func ApplyVectorStoreOptions(opts ...VectoreStoresOption) (vectorStore, error) {
	vs := &vectorStore{
		collectionName:      defaultCollectionName,
		embeddingTableName:  defaultEmbeddingStoreTableName,
		collectionTableName: defaultCollectionStoreTableName,
	}
	for _, opt := range opts {
		opt(vs)
	}
	if vs.engine.pool == nil {
		return vectorStore{}, errors.New("missing vector store engine")
	}
	if vs.embedder == nil {
		return vectorStore{}, errors.New("missing vector store embeder")
	}
	return *vs, nil
}
