package alloydb

import (
	"errors"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/internal/alloydbutil"
	"github.com/tmc/langchaingo/vectorstores"
)

const (
	defaultSchemaName         = "public"
	defaultIDColumn           = "langchain_id" // TODO :: Confirm this
	defaultContentColumn      = "content"
	defaultEmbeddingColumn    = "embedding"
	defaultMetadataJsonColumn = "langchain_metadata" // TODO :: Confirm this
)

// MyQueryOptions options that can be converted to strings.
type MyQueryOptions struct {
	optionString string
	optionInt    int
}

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

// WithOverwriteExisting is an option for VectorStore to
// allow dangerous operations.
func WithOverwriteExisting() AlloyDBVectoreStoresOption {
	return func(v *VectorStore) {
		v.overwriteExisting = true
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
		engine:          engine,
		embedder:        embedder,
		tableName:       tableName,
		idColumn:        defaultIDColumn,
		contentColumn:   defaultContentColumn,
		embeddingColumn: defaultEmbeddingColumn,
		metadataColumns: []string{}, // TODO :: confirm this initialization is needed.

	}
	for _, opt := range opts {
		opt(vs)
	}

	return *vs, nil
}

func applyOpts(options ...vectorstores.Option) (vectorstores.Options, error) {
	opts := vectorstores.Options{}
	for _, opt := range options {
		opt(&opts)
	}

	// TODO :: add default threshold value and check vaules for it
	/* TODO :: Add correct checks
	 	if opts.NameSpace != "" {
		vs.collectionName = opts.NameSpace
	}
			if opts.ScoreThreshold != 0 || opts.Filters != nil || opts.NameSpace != "" {
		return nil, errors.New("vector store unsupported options")
	}
	*/
	return opts, nil
}