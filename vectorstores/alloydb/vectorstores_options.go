package alloydb

import (
	"errors"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/internal/alloydbutil"
)

const (
	defaultSchemaName         = "public"
	defaultIDColumn           = "langchain_id"
	defaultContentColumn      = "content"
	defaultEmbeddingColumn    = "embedding"
	defaultMetadataJsonColumn = "langchain_metadata"
)

type QueryOptions interface {
	ToString() string
}

// AlloyDBVectoreStoresOption is a function for creating new vector store
// with other than the default values.
type AlloyDBVectoreStoresOption func(vs *VectorStore)

// WithSchemaName sets the VectorStore's schemaName field.
func WithSchemaName(schemaName string) AlloyDBVectoreStoresOption {
	return func(v *VectorStore) {
		v.schemaName = schemaName
	}
}

// WithContentColumn sets VectorStore's the idColumn field.
func WithIDColumn(idColumn string) AlloyDBVectoreStoresOption {
	return func(v *VectorStore) {
		v.idColumn = idColumn
	}
}

// WithMetadataJsonColumn sets VectorStore's the metadataJsonColumn field.
func WithMetadataJsonColumn(metadataJsonColumn string) AlloyDBVectoreStoresOption {
	return func(v *VectorStore) {
		v.metadataJsonColumn = metadataJsonColumn
	}
}

// WithContentColumn sets the VectorStore's ContentColumn field.
func WithContentColumn(contentColumn string) AlloyDBVectoreStoresOption {
	return func(v *VectorStore) {
		v.contentColumn = contentColumn
	}
}

// WithEmbeddingColumn sets the EmbeddingColumn field.
func WithEmbeddingColumn(embeddingColumn string) AlloyDBVectoreStoresOption {
	return func(v *VectorStore) {
		v.embeddingColumn = embeddingColumn
	}
}

// WithMetadataColumns sets the VectorStore's MetadataColumns field.
func WithMetadataColumns(metadataColumns []string) AlloyDBVectoreStoresOption {
	return func(v *VectorStore) {
		v.metadataColumns = metadataColumns
	}
}

// WithOverwrite is an option for VectorStore to
// allow dangerous operations.
func WithOverwrite() AlloyDBVectoreStoresOption {
	return func(v *VectorStore) {
		v.overwrite = true
	}
}

// applyAlloyDBVectorStoreOptions applies the given VectorStore options to the
// VectorStore with an alloydb Engine.
func applyAlloyDBVectorStoreOptions(engine alloydbutil.PostgresEngine, embedder embeddings.Embedder, tableName string, opts ...AlloyDBVectoreStoresOption) (VectorStore, error) {
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
	vs := &VectorStore{
		engine:             engine,
		embedder:           embedder,
		tableName:          tableName,
		schemaName:         defaultSchemaName,
		idColumn:           defaultIDColumn,
		contentColumn:      defaultContentColumn,
		embeddingColumn:    defaultEmbeddingColumn,
		metadataJsonColumn: defaultMetadataJsonColumn,
		metadataColumns:    []string{},
	}
	for _, opt := range opts {
		opt(vs)
	}

	return *vs, nil
}
