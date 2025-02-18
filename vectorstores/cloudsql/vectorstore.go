package cloudsql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/internal/cloudsqlutil"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
	"strconv"
	"strings"
)

const (
	defaultIndexNameSuffix = "langchainvectorindex"
)

type VectorStore struct {
	engine             cloudsqlutil.PostgresEngine
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
	options          Index
	distanceStrategy distanceStrategy
	partialIndexes   []string
}

type SearchDocument struct {
	Content            string
	Langchain_metadata string
	Distance           float32
}

var _ vectorstores.VectorStore = &VectorStore{}

// NewVectorStore creates a new VectorStore with options.
func NewVectorStore(ctx context.Context, engine cloudsqlutil.PostgresEngine, embedder embeddings.Embedder, tableName string, opts ...CloudSQLVectoreStoresOption) (VectorStore, error) {
	vs, err := applyCloudSQLVectorStoreOptions(engine, embedder, tableName, opts...)
	if err != nil {
		return VectorStore{}, err
	}
	return vs, nil
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
	searchFunction := vs.distanceStrategy.similaritySearchFunction()

	columns := append(vs.metadataColumns, vs.contentColumn)
	if vs.metadataJsonColumn != "" {
		columns = append(columns, vs.metadataJsonColumn)
	}
	columnNames := strings.Join(columns, `, `)
	whereClause := ""
	if opts.Filters != nil {
		whereClause = fmt.Sprintf("WHERE %s", opts.Filters)
	}
	stmt := fmt.Sprintf(`
        SELECT %s, %s(%s, '%s') AS distance FROM "%s"."%s" %s ORDER BY %s %s '%s' LIMIT $1::int;`,
		columnNames, searchFunction, vs.embeddingColumn, vectorToString(embedding), vs.schemaName, vs.tableName, whereClause, vs.embeddingColumn, operator, vectorToString(embedding))

	results, err := vs.executeSQLQuery(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	documents, err = vs.processResultsToDocuments(results)
	if err != nil {
		return nil, fmt.Errorf("failed to process Results to Documents with Scores: %w", err)
	}
	return documents, nil
}

func (vs *VectorStore) executeSQLQuery(ctx context.Context, stmt string) ([]SearchDocument, error) {
	rows, err := vs.engine.Pool.Query(ctx, stmt, vs.k)
	if err != nil {
		return nil, fmt.Errorf("failed to execute similar search query: %w", err)
	}
	defer rows.Close()

	var results []SearchDocument
	for rows.Next() {
		doc := SearchDocument{}

		err = rows.Scan(&doc.Content, &doc.Langchain_metadata, &doc.Distance)
		if err != nil {
			return nil, fmt.Errorf("failed to scan result: %w", err)
		}
		results = append(results, doc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return results, nil
}

func (vs *VectorStore) processResultsToDocuments(results []SearchDocument) ([]schema.Document, error) {
	var documents []schema.Document
	for _, result := range results {

		mapMetadata := map[string]any{}
		err := json.Unmarshal([]byte(result.Langchain_metadata), &mapMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal langchain metadata: %w", err)
		}
		doc := schema.Document{
			PageContent: result.Content,
			Metadata:    mapMetadata,
			Score:       result.Distance,
		}
		documents = append(documents, doc)
	}
	return documents, nil
}

// ApplyVectorIndex creates an index in the table of the embeddings
func (vs *VectorStore) ApplyVectorIndex(ctx context.Context, index BaseIndex, name string, concurrently bool) error {
	if index.indexType == "exactnearestneighbor" {
		return vs.DropVectorIndex(ctx, name)
	}

	filter := ""
	if len(index.partialIndexes) > 0 {
		filter = fmt.Sprintf("WHERE %s", index.partialIndexes)
	}
	optsString, err := index.indexOptions()
	if err != nil {
		return fmt.Errorf("indexOptions error: %w", err)
	}
	params := fmt.Sprintf("WITH %s", optsString)

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

	function := index.distanceStrategy.searchFunction()
	stmt := fmt.Sprintf("CREATE INDEX %s %s ON %s.%s USING %s (%s %s) %s %s",
		concurrentlyStr, name, vs.schemaName, vs.tableName, index.indexType, vs.embeddingColumn, function, params, filter)

	_, err = vs.engine.Pool.Exec(ctx, stmt)
	if err != nil {
		return fmt.Errorf("failed to execute creation of index: %w", err)
	}

	return nil
}

// DropVectorIndex drops the vector index from the VectorStore.
func (vs *VectorStore) DropVectorIndex(ctx context.Context, indexName string) error {
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

// ReIndex recreates the index on the VectorStore.
func (vs *VectorStore) ReIndex(ctx context.Context) error {
	indexName := vs.tableName + defaultIndexNameSuffix
	return vs.ReIndexWithName(ctx, indexName)
}

// ReIndex recreates the index on the VectorStore by name.
func (vs *VectorStore) ReIndexWithName(ctx context.Context, indexName string) error {
	query := fmt.Sprintf("REINDEX INDEX %s;", indexName)
	_, err := vs.engine.Pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to reindex: %w", err)
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
