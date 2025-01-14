package alloydb

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/internal/alloydbutil"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
)

const (
	defaultIndexNameSuffix = "langchainvectorindex"
	similaritySearchQuery  = `WITH filtered_embedding_dims AS MATERIALIZED (
    SELECT
        *
    FROM
        %s
    WHERE
        vector_dims (
                embedding
        ) = $1
)
SELECT
    data.document,
    data.cmetadata,
    (1 - data.distance) AS score
FROM (
    SELECT
        filtered_embedding_dims.*,
        embedding <=> $2 AS distance
    FROM
        filtered_embedding_dims
    JOIN %s ON filtered_embedding_dims.collection_id=%s.uuid
    WHERE %s.name='%s') AS data
WHERE %s
ORDER BY
    data.distance
LIMIT $3`
)

type VectorStore struct {
	engine            alloydbutil.PostgresEngine
	embedder          embeddings.Embedder
	tableName         string
	schemaName        string //TODO:: Confirm if is this needed
	idColumn          string // TODO :: Confirm if is this needed
	contentColumn     string
	embeddingColumn   string
	metadataColumns   []string
	overwriteExisting bool
	indexQueryOptions []QueryOptions
}

var _ vectorstores.VectorStore = &VectorStore{}

// NewVectorStore creates a new VectorStore with options.
func NewVectorStore(ctx context.Context, engine alloydbutil.PostgresEngine, embedder embeddings.Embedder, tableName string, opts ...AlloyDBVectoreStoresOption) (VectorStore, error) {
	vs, err := applyAlloyDBVectorStoreOptions(engine, embedder, tableName, opts...)
	if err != nil {
		return VectorStore{}, err
	}
	return vs, nil
}

// AddDocuments adds documents to the Postgres collection,
// and returns the ids of the added documents.
func (vs *VectorStore) AddDocuments(ctx context.Context, docs []schema.Document, options ...vectorstores.Option) ([]string, error) {
	docContents := []string{}
	docMetadatas := []map[string]any{}
	for _, docs := range docs {
		docContents = append(docContents, docs.PageContent)
		docMetadatas = append(docMetadatas, docs.Metadata)
	}
	embeddings, err := vs.embedder.EmbedDocuments(ctx, docContents)
	if err != nil {
		return nil, fmt.Errorf("failed embed documents: %w", err)
	}

	ids := make([]string, len(docContents))
	for i := range docContents {
		ids[i] = uuid.New().String()
	}

	// If metadatas are not provided, initialize with empty maps
	if len(docMetadatas) == 0 {
		docMetadatas = make([]map[string]interface{}, len(docContents))
		for i := range docContents {
			docMetadatas[i] = make(map[string]interface{})
		}
	}

	b := &pgx.Batch{}

	for i := range docContents {
		id := ids[i]
		content := docContents[i]
		embedding := embeddings[i]
		metadata := docMetadatas[i]

		// Construct metadata column names if present // TODO :: Check this, what is it doing?
		metadataColNames := ""
		if len(vs.metadataColumns) > 0 {
			metadataColNames = ", " + strings.Join(vs.metadataColumns, ", ")
		}

		insertStmt := fmt.Sprintf(`INSERT INTO "%s" (%s, %s, %s%s)`, // TODO :: Isnt schema name needed?
			vs.tableName, vs.idColumn, vs.contentColumn, vs.embeddingColumn, metadataColNames)
		valuesStmt := "VALUES ($1, $2, $3"
		values := []interface{}{id, content, embedding}

		for _, metadataColumn := range vs.metadataColumns {
			if val, ok := metadata[metadataColumn]; ok {
				valuesStmt += fmt.Sprintf(", $%d", len(values)+1)
				values = append(values, val)
				delete(metadata, metadataColumn)
			} else {
				valuesStmt += ", NULL"
			}
		}
		// TODO :: is adding JSON column and/or close statement needed?
		valuesStmt += ")"
		query := insertStmt + valuesStmt
		b.Queue(query, values...)
	}
	return ids, vs.engine.Pool.SendBatch(ctx, b).Close()
}

// SimilaritySearch performs a similarity search on the database using the
// query vector.
func (vs *VectorStore) SimilaritySearch(ctx context.Context, query string, numDocuments int, options ...vectorstores.Option) ([]schema.Document, error) {
	/*
		Develop this
	}*/
	return nil, nil
}

// Develop ApplyVectorIndex

// ReIndex re-indexes the VectorStore.
func (vs *VectorStore) ReIndex(ctx context.Context, indexName string) error {
	if indexName == "" {
		indexName = vs.tableName + defaultIndexNameSuffix
	}
	query := `REINDEX INDEX $1;`
	_, err := vs.engine.Pool.Exec(ctx, query, indexName)
	if err != nil {
		return fmt.Errorf("failed to reindex: %w", err)
	}

	return nil
}

// DropVectorIndex drops the vector index from the VectorStore.
func (vs *VectorStore) DropVectorIndex(ctx context.Context, indexName string) error {
	if indexName == "" {
		indexName = vs.tableName + defaultIndexNameSuffix
	}
	query := `DROP INDEX IF EXISTS $1;`
	_, err := vs.engine.Pool.Exec(ctx, query, indexName)
	if err != nil {
		return fmt.Errorf("failed to drop vector index: %w", err)
	}

	return nil
}

// IsValidIndex checks if index exists in the VectorStore.
func (vs *VectorStore) IsValidIndex(ctx context.Context, indexName string) (bool, error) {
	if indexName == "" {
		indexName = vs.tableName + defaultIndexNameSuffix
	}
	query := `SELECT tablename, indexname 
			  FROM pg_indexes 
			  WHERE tablename = $1 AND schemaname = $2 AND indexname = $3;`

	var tablename, indexnameFromDb string
	err := vs.engine.Pool.QueryRow(ctx, query, vs.tableName, vs.schemaName, indexName).Scan(&tablename, &indexnameFromDb)
	if err != nil {
		return false, fmt.Errorf("failed to check if index exists: %w", err)
	}

	return indexnameFromDb == indexName, nil
}
