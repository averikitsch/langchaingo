package alloydb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pgvector/pgvector-go"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
)

type vectorStore struct {
	engine              PostgresEngine
	embedder            embeddings.Embedder
	collectionName      string
	embeddingTableName  string
	collectionTableName string
	collectionUUID      string
}

var _ vectorstores.VectorStore = &vectorStore{}

// NewVectorStore creates a new Store with options.
func NewVectorStore(ctx context.Context, opts ...VectoreStoresOption) (vectorStore, error) {
	vs, err := ApplyVectorStoreOptions(opts...)
	if err != nil {
		return vectorStore{}, err
	}
	return vs, nil
}

// AddDocuments adds documents to the Postgres collection,
// and returns the ids of the added documents.
func (vs *vectorStore) AddDocuments(ctx context.Context, docs []schema.Document, options ...vectorstores.Option) ([]string, error) {
	opts := vectorstores.Options{}
	if opts.ScoreThreshold != 0 || opts.Filters != nil || opts.NameSpace != "" {
		return nil, errors.New("vector store unsupported options")
	}

	docs = deduplicate(ctx, opts, docs)

	texts := make([]string, 0, len(docs))
	for _, doc := range docs {
		texts = append(texts, doc.PageContent)
	}

	embedder := vs.embedder
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
	VALUES($1, $2, $3, $4, $5)`, vs.embeddingTableName)

	ids := make([]string, len(docs))
	for docIdx, doc := range docs {
		id := uuid.New().String()
		ids[docIdx] = id
		b.Queue(sql, id, doc.PageContent, pgvector.NewVector(vectors[docIdx]), doc.Metadata, vs.collectionUUID)
	}
	return ids, vs.engine.pool.SendBatch(ctx, b).Close()
}

// SimilaritySearch performs a similarity search on the database using the
// query vector.
func (vs *vectorStore) SimilaritySearch(ctx context.Context, query string, numDocuments int, options ...vectorstores.Option) ([]schema.Document, error) {
	opts := getOptions(options...)
	if opts.NameSpace != "" {
		vs.collectionName = opts.NameSpace
	}
	scoreThreshold, err := validateScoreThreshold(opts.ScoreThreshold)
	if err != nil {
		return nil, err
	}
	filter, err := getFilters(opts)
	if err != nil {
		return nil, err
	}
	embedder := vs.embedder
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
LIMIT $3`, vs.embeddingTableName,
		vs.collectionTableName, vs.collectionTableName, vs.collectionTableName, vs.collectionName,
		whereQuery)

	rows, err := vs.engine.pool.Query(ctx, sql, dims, pgvector.NewVector(embedderData), numDocuments)
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

func getOptions(options ...vectorstores.Option) vectorstores.Options {
	opts := vectorstores.Options{}
	for _, opt := range options {
		opt(&opts)
	}
	return opts
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

func deduplicate(ctx context.Context, opts vectorstores.Options, docs []schema.Document) []schema.Document {
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

// TODO :: Modify queries!
func (vs *vectorStore) ApplyVectorIndex(ctx context.Context) error {
	query := fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS <changeTableName> ON <changeTableName>;`)

	_, err := vs.engine.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to apply vector index: %w", err)
	}

	return nil
}

func (vs *vectorStore) ReIndex(ctx context.Context) error {
	query := fmt.Sprintf(`
        REINDEX INDEX <changeTableName>;`)

	_, err := vs.engine.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to reindex: %w", err)
	}

	return nil
}

func (vs *vectorStore) DropVectorIndex(ctx context.Context) error {
	query := fmt.Sprintf(`
        DROP INDEX IF EXISTS <changeTableName>;`)

	_, err := vs.engine.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop vector index: %w", err)
	}

	return nil
}

func (vs *vectorStore) IsValidIndex(ctx context.Context) (bool, error) {
	query := fmt.Sprintf(` SELECT COUNT(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $2; `)

	var count int
	err := vs.engine.pool.QueryRow(ctx, query, "<changeTableName>", "<changeIndexName>`").Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check validate index: %w", err)
	}

	return count > 0, nil
}
