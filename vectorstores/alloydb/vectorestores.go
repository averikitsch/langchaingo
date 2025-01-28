package alloydb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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
	k                  int
	distanceStrategy   distanceStrategy
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
func (vs *VectorStore) AddDocuments(ctx context.Context, docs []schema.Document, _ ...vectorstores.Option) ([]string, error) {
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
	metadatas := make([]map[string]any, len(texts))
	for i := range docs {
		if docs[i].Metadata == nil {
			metadatas[i] = make(map[string]any)
		} else {
			metadatas[i] = docs[i].Metadata
		}
	}
	b := &pgx.Batch{}

	for i := range texts {
		id := ids[i]
		content := texts[i]
		embedding := vectorToString(embeddings[i])
		metadata := metadatas[i]

		// Construct metadata column names if present
		metadataColNames := ""
		if len(vs.metadataColumns) > 0 {
			metadataColNames = ", " + strings.Join(vs.metadataColumns, ", ")
		}

		if vs.metadataJsonColumn != "" {
			metadataColNames += ", " + vs.metadataJsonColumn
		}

		insertStmt := fmt.Sprintf(`INSERT INTO "%s"."%s" (%s, %s, %s%s)`,
			vs.schemaName, vs.tableName, vs.idColumn, vs.contentColumn, vs.embeddingColumn, metadataColNames)
		valuesStmt := "VALUES ($1, $2, $3"
		values := []any{id, content, embedding}

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
func (vs *VectorStore) SimilaritySearch(ctx context.Context, query string, _ int, options ...vectorstores.Option) ([]schema.Document, error) {
	opts, err := applyOpts(options...)
	if err != nil {
		return nil, fmt.Errorf("failed to apply vector store options: %w", err)
	}
	var documents []schema.Document
	embedding, err := vs.embedder.EmbedQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed embed query: %w", err)
	}
	operator := vs.distanceStrategy.operator()
	searchFunction := vs.distanceStrategy.searchFunction()

	columns := append(vs.metadataColumns, vs.idColumn, vs.contentColumn, vs.embeddingColumn)
	if vs.metadataJsonColumn != "" {
		columns = append(columns, vs.metadataJsonColumn)
	}
	columnNames := `" ` + strings.Join(columns, `", "`) + `"`
	whereClause := ""
	if opts.Filters != "" {
		whereClause = fmt.Sprintf("WHERE %s", opts.Filters)
	}
	stmt := fmt.Sprintf(`
        SELECT %s, %s(%s, $1) AS distance FROM "%s"."%s" %s ORDER BY %s %s $1 LIMIT $2;`,
		columnNames, searchFunction, vs.embeddingColumn, vs.schemaName, vs.tableName, whereClause, vs.embeddingColumn, operator)

	results, err := vs.executeSQLQuery(ctx, stmt, embedding)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	documents, err = vs.processResultsToDocuments(results)
	if err != nil {
		return nil, fmt.Errorf("failed to process Results to Documents with Scores: %w", err)
	}
	return documents, nil
}

func (vs *VectorStore) executeSQLQuery(ctx context.Context, stmt string, embedding []float32) ([]map[string]any, error) {
	rows, err := vs.engine.Pool.Query(ctx, stmt, embedding, vs.k)
	if err != nil {
		return nil, fmt.Errorf("failed to execute similar search query: %w", err)
	}
	defer rows.Close()

	var results []map[string]any
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
	return results, nil
}

func (vs *VectorStore) processResultsToDocuments(results []map[string]any) ([]schema.Document, error) {
	var documents []schema.Document
	for _, row := range results {
		metadata := make(map[string]any)
		if vs.metadataJsonColumn != "" && row[vs.metadataJsonColumn] != nil {
			if jsonBytes, ok := row[vs.metadataJsonColumn].([]byte); ok {
				if err := json.Unmarshal(jsonBytes, &metadata); err != nil {
					return nil, fmt.Errorf("failed to unmarshal metadata JSON: %w", err)
				}
			} else {
				return nil, fmt.Errorf("expected byte slice for metadata JSON, but got %T", row[vs.metadataJsonColumn])
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
		distance, ok := row["distance"].(float64)
		if !ok {
			return nil, fmt.Errorf("expected distance to be a floating value, but got %T", row["distance"])
		}
		document.Score = float32(distance)
		documents = append(documents, document)
	}
	return documents, nil
}

// ApplyVectorIndex creates an index in the table of the embeddings
func (vs *VectorStore) ApplyVectorIndex(ctx context.Context, index BaseIndex, name string, concurrently, overwrite bool, indexOpts ...int) error {
	if index.indexType == "exactnearestneighbor" {
		return vs.DropVectorIndex(ctx, name, overwrite)
	}
	function := index.distanceStrategy.searchFunction()
	if index.indexType == "ScaNN" {
		_, err := vs.engine.Pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS alloydb_scann")
		if err != nil {
			return fmt.Errorf("failed to create alloydb scann extension: %w", err)
		}
	}
	filter := ""
	if len(index.partialIndexes) > 0 {
		filter = fmt.Sprintf("WHERE %s", index.partialIndexes)
	}
	params := fmt.Sprintf("WITH %s", index.indexOptions(indexOpts))

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
	query := fmt.Sprintf("REINDEX INDEX %s;", indexName)
	_, err := vs.engine.Pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to reindex: %w", err)
	}

	return nil
}

// DropVectorIndex drops the vector index from the VectorStore.
func (vs *VectorStore) DropVectorIndex(ctx context.Context, indexName string, overwrite bool) error {
	// Overwrite allows dangerous operations like a Drop query.
	if !overwrite {
		return nil
	}
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

// IsValidIndex checks if index exists in the VectorStore.
func (vs *VectorStore) IsValidIndex(ctx context.Context, indexName string) (bool, error) {
	if indexName == "" {
		indexName = vs.tableName + defaultIndexNameSuffix
	}
	query := fmt.Sprintf("SELECT tablename, indexname  FROM pg_indexes WHERE tablename = '%s' AND schemaname = '%s' AND indexname = '%s';", vs.tableName, vs.schemaName, indexName)
	var tablename, indexnameFromDb string
	err := vs.engine.Pool.QueryRow(ctx, query).Scan(&tablename, &indexnameFromDb)
	if err != nil {
		return false, fmt.Errorf("failed to check if index exists: %w", err)
	}

	return indexnameFromDb == indexName, nil
}

func (vs *VectorStore) NewBaseIndex(indexName, indexType string, strategy distanceStrategy, partialIndexes []string) BaseIndex {
	return BaseIndex{
		name:             indexName,
		indexType:        indexType,
		distanceStrategy: strategy,
		partialIndexes:   partialIndexes,
	}
}

func vectorToString(vec []float32) string {
	var buf strings.Builder
	buf.WriteString("[")

	for i := 0; i < len(vec); i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatFloat(float64(vec[i]), 'f', -1, 32))
	}

	buf.WriteString("]")
	return buf.String()
}
