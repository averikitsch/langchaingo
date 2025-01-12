package alloydb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx"
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
	idColumn          string // TODO :: Confirm this
	contentColumn     string
	embeddingColumn   string
	metadataColumns   []string
	overwriteExisting bool
	indexQueryOptions []QueryOptions
}

var _ vectorstores.VectorStore = &VectorStore{}

func ToString([]QueryOptions) ([]string, error) {
	// Fihish this
	return []string{}, nil
}

// NewVectorStore creates a new VectorStore with options.
func NewVectorStore(ctx context.Context, engine alloydbutil.PostgresEngine, embedder embeddings.Embedder, tableName string, opts ...AlloyDBVectoreStoresOption) (VectorStore, error) {
	vs, err := ApplyAlloyDBVectorStoreOptions(engine, embedder, tableName, opts...)
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
	opts, err := applyOpts(options...) // TODO :: Is this needed?
	if err != nil {
		return nil, err
	}
	embedding, err := vs.embedder.EmbedQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed add documents: %w", err)
	}

	thresholdQuery := "SELECT * FROM documents WHERE similarity_function($1, embedding) > 0.5" // Placeholder query
	rows, err := vs.engine.Pool.Query(ctx, query, embedding)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []schema.Document
	for rows.Next() {
		var doc schema.Document
		var score float32
		err := rows.Scan(&doc.PageContent, &score)
		if err != nil {
			return nil, err
		}

		metadata := map[string]any{}
		for _, col := range vs.metadataColumns {
			// TODO :: What should it be added here
			metadata[col] = "sample metadata"
		}

		doc.Metadata = metadata
		result = append(result, DocumentScore{
			Document: doc,
			Score:    score,
		})
	}

	return result, nil
	return docs, rows.Err()
}

func validateScoreThreshold(scoreThreshold float32) (float32, error) {
	if scoreThreshold < 0 || scoreThreshold > 1 {
		return 0, errors.New("score threshold must be between 0 and 1")
	}
	return scoreThreshold, nil
}

// getFilters return metadata filters, now only support map[key]value pattern
// TODO: should support more types like {"key1": {"key2":"values2"}} or {"key": ["value1", "values2"]}.
func getFilters(opts vectorstores.Options) (map[string]any, error) {
	if opts.Filters != nil {
		if filters, ok := opts.Filters.(map[string]any); ok {
			return filters, nil
		}
		return nil, errors.New("invalid filters")
	}
	return map[string]any{}, nil
}

// TODO :: Modify queries!
func (vs *VectorStore) ApplyVectorIndex(ctx context.Context, index BaseIndex, name string) error {
	query := fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS <changeTableName> ON <changeTableName>;`)

	_, err := vs.engine.Pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to apply vector index: %w", err)
	}

	return nil
}

func (vs *VectorStore) ReIndex(ctx context.Context) error {
	query := fmt.Sprintf(`
        REINDEX INDEX <changeTableName>;`)

	_, err := vs.engine.Pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to reindex: %w", err)
	}

	return nil
}

func (vs *VectorStore) DropVectorIndex(ctx context.Context) error {
	query := fmt.Sprintf(`
        DROP INDEX IF EXISTS <changeTableName>;`)

	_, err := vs.engine.Pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop vector index: %w", err)
	}

	return nil
}

func (vs *VectorStore) IsValidIndex(ctx context.Context) (bool, error) {
	query := fmt.Sprintf(` SELECT COUNT(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $2; `)

	var count int
	err := vs.engine.Pool.QueryRow(ctx, query, "<changeTableName>", "<changeIndexName>`").Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check validate index: %w", err)
	}

	return count > 0, nil
}
