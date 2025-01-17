package alloydb

import (
	"context"
	"encoding/json"
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
	engine             alloydbutil.PostgresEngine
	embedder           embeddings.Embedder
	tableName          string
	schemaName         string
	idColumn           string
	metadataJsonColumn string
	contentColumn      string
	embeddingColumn    string
	metadataColumns    []string
	overwriteExisting  bool
	k                  int
	distanceStrategy   distanceStrategy
	indexQueryOptions  []QueryOptions
}

type BaseIndex struct {
	name             string
	indexType        string
	distanceStrategy distanceStrategy
	partialIndexes   []string
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

// AddDocuments adds documents to the Postgres collection, and returns the ids
// of the added documents.
func (vs *VectorStore) AddDocuments(ctx context.Context, docs []schema.Document, options ...vectorstores.Option) ([]string, error) {
	var texts []string
	for _, doc := range docs {
		texts = append(texts, doc.PageContent)
	}
	embeddings, err := vs.embedder.EmbedDocuments(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("failed embed documents: %w", err)
	}
	// If no ids provided, generate them.
	ids := make([]string, len(texts))
	for i, doc := range docs {
		if val, ok := doc.Metadata["id"].(string); ok {
			ids[i] = val
		} else {
			ids[i] = uuid.New().String()
		}
	}
	// If no metadata provided, initialize with empty maps
	metadatas := make([]map[string]interface{}, len(texts))
	for i := range docs {
		if docs[i].Metadata == nil {
			metadatas[i] = make(map[string]interface{})
		} else {
			metadatas[i] = docs[i].Metadata
		}
	}
	b := &pgx.Batch{}

	for i := range texts {
		id := ids[i]
		content := texts[i]
		embedding := embeddings[i]
		metadata := metadatas[i]

		// Construct metadata column names if present
		metadataColNames := ""
		if len(vs.metadataColumns) > 0 {
			metadataColNames = ", " + strings.Join(vs.metadataColumns, ", ")
		}

		insertStmt := fmt.Sprintf(`INSERT INTO "%s"."%s" (%s, %s, %s%s)`,
			vs.schemaName, vs.tableName, vs.idColumn, vs.contentColumn, vs.embeddingColumn, metadataColNames)
		valuesStmt := "VALUES ($1, $2, $3"
		values := []interface{}{id, content, embedding}
		// Add metadata
		for _, metadataColumn := range vs.metadataColumns {
			if val, ok := metadata[metadataColumn]; ok {
				valuesStmt += fmt.Sprintf(", $%d", len(values)+1)
				values = append(values, val)
				delete(metadata, metadataColumn)
			} else {
				valuesStmt += ", NULL"
			}
		}
		// Add JSON column and/or close statement
		if vs.metadataJsonColumn != "" {
			valuesStmt += fmt.Sprintf(", $%d", len(values)+1)
			metadataJson, err := json.Marshal(metadata)
			if err != nil {
				return nil, fmt.Errorf("failed to transform metadata to json: %w", err)
			}
			values = append(values, metadataJson)
		}
		valuesStmt += ")"
		query := insertStmt + valuesStmt
		b.Queue(query, values...)
	}

	batchResults := vs.engine.Pool.SendBatch(ctx, b)
	if err := batchResults.Close(); err != nil {
		return nil, fmt.Errorf("failed to execute batch: %w", err)
	}

	return ids, nil
}

// SimilaritySearch performs a similarity search on the database using the
// query vector.
func (vs *VectorStore) SimilaritySearch(ctx context.Context, query string, numDocuments int, options ...vectorstores.Option) ([]schema.Document, error) {
	opts, err := applyOpts(options...)
	if err != nil {
		return nil, fmt.Errorf("failed to apply vector store options: %w", err)
	}
	var documents []schema.Document
	embedding, err := vs.embedder.EmbedQuery(ctx, query)
	operator := vs.distanceStrategy.operator()
	searchFunction := vs.distanceStrategy.searchFunction()

	columns := append(vs.metadataColumns, vs.idColumn, vs.contentColumn, vs.embeddingColumn) // TODO :: double check this.
	if vs.metadataJsonColumn != "" {
		columns = append(columns, vs.metadataJsonColumn)
	}
	columnNames := `" ` + strings.Join(columns, `", "`) + `"`
	whereClause := ""
	if opts.Filters != "" { // TODO :: Check for filters examples
		whereClause = fmt.Sprintf("WHERE %s", opts.Filters)
	}
	stmt := fmt.Sprintf(`
        SELECT %s, %s(%s, $1) AS distance 
        FROM "%s"."%s" %s 
        ORDER BY %s %s $1 LIMIT $2;
    `, columnNames, searchFunction, vs.embeddingColumn, vs.schemaName, vs.tableName, whereClause, vs.embeddingColumn, operator)

	rows, err := vs.engine.Pool.Query(ctx, stmt, embedding, vs.k)
	if err != nil {
		return nil, fmt.Errorf("failed to execute similar search query: %w", err)
	}
	defer rows.Close()

	var results []map[string]any // TODO :: check maps
	for rows.Next() {
		resultMap := make(map[string]any)
		err := rows.Scan(&resultMap)
		if err != nil {
			return nil, fmt.Errorf("failed to scan result: %w", err)
		}
		results = append(results, resultMap)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	var documentsWithScores []struct {
		Document schema.Document
		Score    float64
	}

	for _, row := range results {
		metadata := make(map[string]interface{})
		if vs.metadataJsonColumn != "" && row[vs.metadataJsonColumn] != nil {
			if err := json.Unmarshal(row[vs.metadataJsonColumn].([]byte), &metadata); err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata JSON: %w", err)
			}
		}
		for _, col := range vs.metadataColumns {
			if val, ok := row[col]; ok {
				metadata[col] = val
			}
		}
		document := schema.Document{
			PageContent: row[vs.contentColumn].(string),
			Metadata:    metadata,
		}
		distance := row["distance"].(float64)
		documentsWithScores = append(documentsWithScores, struct {
			Document schema.Document
			Score    float64
		}{Document: document, Score: distance})
	}
	for _, docAndScore := range documentsWithScores {
		documents = append(documents, docAndScore.Document)
	}
	return documents, nil
}

// ApplyVectorIndex creates an index in the table of the embeddings
func (vs *VectorStore) ApplyVectorIndex(ctx context.Context, index BaseIndex, name, scannIndexFunction string, concurrently bool) error {
	if index.indexType == "exactnearestneighbor" {
		return vs.DropVectorIndex(ctx, name)
	}
	function := index.distanceStrategy // TODO :: modify this to type DistanceStrategy
	if index.indexType == "ScaNN" {
		_, err := vs.engine.Pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS alloydb_scann")
		if err != nil {
			return fmt.Errorf("failed to create alloydb scann extension: %w", err)
		}
		function = scannIndexFunction // TODO :: modify this to type DistanceStrategy
	}
	filter := ""
	if len(index.partialIndexes) > 0 {
		filter = fmt.Sprintf("WHERE %s", index.partialIndexes)
	}
	params := index.indexOptions() // TODO :: check this to type DistanceStrategy

	if name == "" {
		if index.name == "" {
			index.name = vs.tableName + defaultIndexNameSuffix
		}
		name = index.name
	}

	concurrentlyStr := ""
	if concurrently {
		concurrentlyStr = "CONCURRENTLY"
	}

	stmt := fmt.Sprintf("CREATE INDEX %s %s ON %s.%s USING %s (%s %s) %s %s",
		concurrentlyStr, name, vs.schemaName, vs.tableName, index.indexType, vs.embeddingColumn, function, params, filter)

	_, err := vs.engine.Pool.Exec(ctx, stmt)
	if err != nil {
		return fmt.Errorf("failed to execute creation of index: %w", err)
	}
	_, err = vs.engine.Pool.Exec(ctx, "COMMIT")
	if err != nil {
		return fmt.Errorf("failed to commit index: %w", err)
	}
	return nil
}

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
