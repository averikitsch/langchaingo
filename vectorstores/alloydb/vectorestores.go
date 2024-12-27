package alloydb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx"
	"github.com/pgvector/pgvector-go"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
)

// VectorStore is the interface for saving and querying documents in the
// form of vector embeddings.
type VectorStore interface {
	AddDocuments(ctx context.Context, docs []schema.Document, options ...Option) ([]string, error)
	SimilaritySearch(ctx context.Context, query string, numDocuments int, options ...Option) ([]schema.Document, error) //nolint:lll
}

// AddDocuments adds documents to the Postgres collection,
// and returns the ids of the added documents.
func (p *PostgresEngine) AddDocuments(ctx context.Context, docs []schema.Document, options ...Option) ([]string, error) {
	opts := vectorstores.Options{}
	for _, opt := range options { // should I add the required fields to the engineOptions??
		opt(&opts)
	}

	docs = deduplicate(ctx, opts, docs)

	texts := make([]string, 0, len(docs))
	for _, doc := range docs {
		texts = append(texts, doc.PageContent)
	}

	// TODO :: where else should I implement this embedder?
	var embedder embeddings.Embedder
	if opts.Embedder != nil {
		embedder = opts.Embedder
	}

	vectors, err := embedder.EmbedDocuments(ctx, texts)
	if err != nil {
		return nil, err
	}

	if len(vectors) != len(docs) {
		return nil, errors.New("number of vectors from embedder does not match number of documents")
	}

	b := &pgx.Batch{}
	sql := fmt.Sprintf(`INSERT INTO %s (uuid, document, embedding, cmetadata, collection_id)
		VALUES($1, $2, $3, $4, $5)`, s.embeddingTableName) // should I add embeddingTableName to the engine??

	ids := make([]string, len(docs))
	for docIdx, doc := range docs {
		id := uuid.New().String()
		ids[docIdx] = id
		// should I add collectionUUID to the engine??
		b.Queue(sql, id, doc.PageContent, pgvector.NewVector(vectors[docIdx]), doc.Metadata, s.collectionUUID)
	}
	return ids, p.pool.SendBatch(ctx, b).Close()
}

func deduplicate(
	ctx context.Context,
	opts vectorstores.Options,
	docs []schema.Document,
) []schema.Document {
	if opts.Deduplicater == nil {
		return docs
	}

	filtered := make([]schema.Document, 0, len(docs))
	for _, doc := range docs {
		if !opts.Deduplicater(ctx, doc) {
			filtered = append(filtered, doc)
		}
	}

	return filtered
}

// SimilaritySearch performs a similarity search on the database using the
// query vector.
func (p *PostgresEngine) SimilaritySearch(ctx context.Context, query string, numDocuments int, options ...Option) ([]schema.Document, error) {
	opts := vectorstores.Options{}
	for _, opt := range options {
		opt(&opts)
	}
	collectionName := s.getNameSpace(opts)
	scoreThreshold, err := s.getScoreThreshold(opts)
	if err != nil {
		return nil, err
	}
	filter, err := s.getFilters(opts)
	if err != nil {
		return nil, err
	}
	embedder := s.embedder
	if opts.Embedder != nil {
		embedder = opts.Embedder
	}
	embedderData, err := embedder.EmbedQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	whereQuerys := make([]string, 0)
	if scoreThreshold != 0 {
		whereQuerys = append(whereQuerys, fmt.Sprintf("data.distance < %f", 1-scoreThreshold))
	}
	for k, v := range filter {
		whereQuerys = append(whereQuerys, fmt.Sprintf("(data.cmetadata ->> '%s') = '%s'", k, v))
	}
	whereQuery := strings.Join(whereQuerys, " AND ")
	if len(whereQuery) == 0 {
		whereQuery = "TRUE"
	}
	dims := len(embedderData)
	sql := fmt.Sprintf(`WITH filtered_embedding_dims AS MATERIALIZED (
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
		JOIN %s ON filtered_embedding_dims.collection_id=%s.uuid WHERE %s.name='%s') AS data
WHERE %s
ORDER BY
	data.distance
LIMIT $3`, s.embeddingTableName,
		s.collectionTableName, s.collectionTableName, s.collectionTableName, collectionName,
		whereQuery)
	rows, err := p.pool.Query(ctx, sql, dims, pgvector.NewVector(embedderData), numDocuments)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	docs := make([]schema.Document, 0)
	for rows.Next() {
		doc := schema.Document{}
		if err := rows.Scan(&doc.PageContent, &doc.Metadata, &doc.Score); err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, rows.Err()
}

// TODO :: Check this! should have options instead
func (p *PostgresEngine) applyVectorIndex(ctx context.Context, config vectorStoresConfig) error {
	query := fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s (%s);
    `, config.tableName, config.columnName, config.tableName, config.columnName)

	_, err := p.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to apply vector index: %w", err)
	}

	return nil
}

func (p *PostgresEngine) reIndex(ctx context.Context, config vectorStoresConfig) error {
	query := fmt.Sprintf(`
        REINDEX INDEX idx_%s_%s;
    `, config.tableName, config.columnName)

	_, err := p.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to reindex: %w", err)
	}

	return nil
}

func (p *PostgresEngine) dropVectorIndex(ctx context.Context, config vectorStoresConfig) error {
	query := fmt.Sprintf(`
        DROP INDEX IF EXISTS idx_%s_%s;
    `, config.tableName, config.columnName)

	_, err := p.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop vector index: %w", err)
	}

	return nil
}

func (p *PostgresEngine) isValidIndex(ctx context.Context, config vectorStoresConfig) (bool, error) {
	query := fmt.Sprintf(`
        SELECT COUNT(*)
        FROM pg_indexes
        WHERE tablename = $1 AND indexname = $2;
    `)

	var count int
	err := p.pool.QueryRow(ctx, query, config.tableName, "idx_"+config.tableName+"_"+config.columnName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check validate index: %w", err)
	}

	return count > 0, nil
}
