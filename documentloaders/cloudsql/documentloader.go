package cloudsql

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/textsplitter"
	"github.com/tmc/langchaingo/util/cloudsqlutil"
)

const (
	defaultMetadataJSONColumn = "langchain_metadata"
	defaultSchemaName         = "public"
)

// DocumentLoader is responsible for loading documents from a Postgres database.
type DocumentLoader struct {
	engine             cloudsqlutil.PostgresEngine
	query              string
	tableName          string
	schemaName         string
	contentColumns     []string
	metadataColumns    []string
	metadataJSONColumn string
	format             string
	formatter          func(map[string]any, []string) string
}

// NewDocumentLoader creates a new DocumentLoader instance.
func NewDocumentLoader(ctx context.Context, engine cloudsqlutil.PostgresEngine, options ...DocumentLoaderOption) (*DocumentLoader, error) {
	documentLoader := &DocumentLoader{
		engine:     engine,
		schemaName: defaultSchemaName,
	}

	if err := applyCloudSQLDocumentLoaderOptions(documentLoader, options); err != nil {
		return nil, err
	}

	if err := validateQuery(documentLoader.query); err != nil {
		return nil, err
	}

	fieldDescriptions, err := documentLoader.getFieldDescriptions(ctx)
	if err != nil {
		return nil, err
	}

	if err := documentLoader.configureColumns(fieldDescriptions); err != nil {
		return nil, err
	}

	if err := documentLoader.validateColumns(fieldDescriptions); err != nil {
		return nil, err
	}

	return documentLoader, nil
}

// textFormatter formats row data into a text string.
func textFormatter(row map[string]any, contentColumns []string) string {
	var sb strings.Builder
	for _, column := range contentColumns {
		if val, ok := row[column]; ok {
			sb.WriteString(fmt.Sprintf("%v ", val))
		}
	}
	return strings.TrimSpace(sb.String())
}

// csvFormatter formats row data into a CSV string.
func csvFormatter(row map[string]any, contentColumns []string) string {
	var sb strings.Builder
	writer := csv.NewWriter(&sb)
	record := make([]string, 0, len(contentColumns))
	for _, column := range contentColumns {
		if val, ok := row[column]; ok {
			record = append(record, fmt.Sprintf("%v", val))
		}
	}
	if err := writer.Write(record); err != nil {
		// Should not happen in normal cases as values are usually simple types
		return ""
	}
	writer.Flush()
	return strings.TrimSuffix(sb.String(), "\n") // Remove trailing newline
}

// yamlFormatter formats row data into a YAML string.
func yamlFormatter(row map[string]any, contentColumns []string) string {
	var sb strings.Builder
	for _, column := range contentColumns {
		if val, ok := row[column]; ok {
			sb.WriteString(fmt.Sprintf("%s: %v\n", column, val))
		}
	}
	return strings.TrimSpace(sb.String())
}

// jsonFormatter formats row data into a JSON string.
func jsonFormatter(row map[string]any, contentColumns []string) string {
	data := make(map[string]any)
	for _, column := range contentColumns {
		if val, ok := row[column]; ok {
			data[column] = val
		}
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		// Should not happen in normal cases as values are usually simple types
		return ""
	}
	return string(jsonData)
}

// parseDocFromRow parses a Document from a row of data.
func (l *DocumentLoader) parseDocFromRow(row map[string]any) (schema.Document, error) {
	pageContent := l.formatter(row, l.contentColumns)
	metadata := make(map[string]any)

	if l.metadataJSONColumn != "" {
		value, ok := row[l.metadataJSONColumn]
		if ok {
			mapValues := value.(map[string]any)
			for k, v := range mapValues {
				metadata[k] = v
			}
		}
	}

	for _, column := range l.metadataColumns {
		if column != l.metadataJSONColumn {
			metadata[column] = row[column]
		}
	}

	return schema.Document{
		PageContent: pageContent,
		Metadata:    metadata,
	}, nil
}

// Load executes the configured SQL query and returns a list of Document objects.
func (l *DocumentLoader) Load(ctx context.Context) ([]schema.Document, error) {
	rows, err := l.engine.Pool.Query(ctx, l.query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	var documents []schema.Document

	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("unable to parse row: %w", err)
		}
		mapColumnNameValue := make(map[string]any)
		for i, f := range fieldDescriptions {
			mapColumnNameValue[f.Name] = v[i]
		}
		doc, err := l.parseDocFromRow(mapColumnNameValue)
		if err != nil {
			return nil, err
		}
		documents = append(documents, doc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during rows iteration: %w", err)
	}

	return documents, nil
}

func (l *DocumentLoader) LoadAndSplit(ctx context.Context, splitter textsplitter.TextSplitter) ([]schema.Document, error) {
	if splitter == nil {
		splitter = textsplitter.NewRecursiveCharacter()
	}

	docs, err := l.Load(ctx)
	if err != nil {
		return nil, err
	}
	return textsplitter.SplitDocuments(splitter, docs)
}

func (l *DocumentLoader) validateColumns(fieldDescriptions []pgconn.FieldDescription) error {
	allNames := make(map[string]struct{})
	for _, name := range l.contentColumns {
		allNames[name] = struct{}{}
	}
	for _, name := range l.metadataColumns {
		allNames[name] = struct{}{}
	}

	for name := range allNames {
		found := false
		for _, col := range fieldDescriptions {
			if col.Name == name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column '%s' not found in query result", name)
		}
	}
	return nil
}
