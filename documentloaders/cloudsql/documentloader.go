package cloudsql

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/averikitsch/langchaingo/schema"
	"github.com/averikitsch/langchaingo/textsplitter"
	"github.com/averikitsch/langchaingo/util/cloudsqlutil"
	"github.com/jackc/pgtype"
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
	formatter          func(map[string]interface{}, []string) string
}

// NewDocumentLoader creates a new DocumentLoader instance.
func NewDocumentLoader(ctx context.Context, options []DocumentLoaderOption) (*DocumentLoader, error) {

	documentLoader, err := applyCloudSQLDocumentLoaderOptions(options)
	if err != nil {
		return nil, err
	}

	// Validate columns against the query result
	re := regexp.MustCompile(`(?i)^\s*SELECT\s+.+\s+FROM\s+([a-zA-Z0-9_]+\.)?([a-zA-Z0-9_]+)\b`)
	query := re.FindString(documentLoader.query)
	if query == "" {
		return nil, errors.New("query is not valid")
	}
	query = fmt.Sprintf("%s LIMIT 1", query)

	rows, err := documentLoader.engine.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	fieldDescription := rows.FieldDescriptions()

	if len(documentLoader.contentColumns) == 0 {
		documentLoader.contentColumns = []string{fieldDescription[0].Name}
	}

	if len(documentLoader.metadataColumns) == 0 {
		for _, col := range fieldDescription {
			if !slices.Contains(documentLoader.contentColumns, col.Name) {
				documentLoader.metadataColumns = append(documentLoader.metadataColumns, col.Name)
			}
		}
	}

	if documentLoader.metadataJSONColumn == "" {
		documentLoader.metadataJSONColumn = defaultMetadataJSONColumn
	}
	found := false
	for _, col := range fieldDescription {
		if col.Name == documentLoader.metadataJSONColumn {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("metadata JSON column '%s' not found in query result", documentLoader.metadataJSONColumn)
	}

	allNames := make(map[string]struct{})
	for _, name := range documentLoader.contentColumns {
		allNames[name] = struct{}{}
	}
	for _, name := range documentLoader.metadataColumns {
		allNames[name] = struct{}{}
	}

	for name := range allNames {
		found := false
		for _, col := range fieldDescription {
			if col.Name == name {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column '%s' not found in query result", name)
		}
	}

	return documentLoader, nil
}

// textFormatter formats row data into a text string.
func textFormatter(row map[string]interface{}, contentColumns []string) string {
	var sb strings.Builder
	for _, column := range contentColumns {
		if val, ok := row[column]; ok {
			sb.WriteString(fmt.Sprintf("%v ", val))
		}
	}
	return strings.TrimSpace(sb.String())
}

// csvFormatter formats row data into a CSV string.
func csvFormatter(row map[string]interface{}, contentColumns []string) string {
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
func yamlFormatter(row map[string]interface{}, contentColumns []string) string {
	var sb strings.Builder
	for _, column := range contentColumns {
		if val, ok := row[column]; ok {
			sb.WriteString(fmt.Sprintf("%s: %v\n", column, val))
		}
	}
	return strings.TrimSpace(sb.String())
}

// jsonFormatter formats row data into a JSON string.
func jsonFormatter(row map[string]interface{}, contentColumns []string) string {
	data := make(map[string]interface{})
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
func (l *DocumentLoader) parseDocFromRow(row map[string]interface{}) (schema.Document, error) {
	pageContent := l.formatter(row, l.contentColumns)
	metadata := make(map[string]interface{})

	populateJSONMetadata := func(data []byte) error {
		var jsonMetadata map[string]interface{}
		err := json.Unmarshal(data, &jsonMetadata)
		if err != nil {
			return fmt.Errorf("failed to parse JSON from column '%s': %w", l.metadataJSONColumn, err)
		}
		for k, v := range jsonMetadata {
			metadata[k] = v
		}
		return nil
	}

	if l.metadataJSONColumn != "" {
		value := row[l.metadataJSONColumn]
		switch v := value.(type) {
		case []byte:
			if err := populateJSONMetadata(v); err != nil {
				return schema.Document{}, err
			}
		case string:
			if err := populateJSONMetadata([]byte(v)); err != nil {
				return schema.Document{}, err
			}
		default:
			return schema.Document{}, fmt.Errorf("failed to parse JSON from column '%s': invalid column type", l.metadataJSONColumn)
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
	documents := make([]schema.Document, 0)
	rows, err := l.engine.Pool.Query(ctx, l.query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	valuesPrt := make([]interface{}, len(columnNames))

	for i, fd := range fieldDescriptions {
		columnNames[i] = fd.Name
		switch fd.DataTypeOID {
		case pgtype.TimeOID, pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.DateOID:
			valuesPrt[i] = new(sql.NullTime)
		case pgtype.VarcharOID, pgtype.TextOID, pgtype.JSONOID:
			valuesPrt[i] = new(sql.NullString)
		case pgtype.BoolOID:
			valuesPrt[i] = new(sql.NullBool)
		case pgtype.Float4OID, pgtype.Float8OID:
			valuesPrt[i] = new(sql.NullFloat64)
		case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
			valuesPrt[i] = new(sql.NullInt64)
		default:
			valuesPrt[i] = new(sql.RawBytes)
		}
	}

	for rows.Next() {
		columnValues := make(map[string]any, len(columnNames))
		if err := rows.Scan(valuesPrt...); err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}

		if err = rows.Scan(valuesPrt...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		for i, name := range columnNames {
			switch v := valuesPrt[i].(type) {
			case *sql.NullTime:
				if v.Valid {
					columnValues[name] = v.Time
				}
			case *sql.NullString:
				if v.Valid {
					columnValues[name] = v.String
				}
			case *sql.NullBool:
				if v.Valid {
					columnValues[name] = v.Bool
				}
			case *sql.NullInt64:
				if v.Valid {
					columnValues[name] = v.Int64
				}
			case *sql.NullFloat64:
				if v.Valid {
					columnValues[name] = v.Float64
				}
			case *sql.RawBytes:
				columnValues[name] = *v
			default:
				columnValues[name] = valuesPrt[i]
			}
		}

		doc, err := l.parseDocFromRow(columnValues)
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
	splitteddocs := make([]schema.Document, 0)
	if splitter == nil {
		splitter = textsplitter.NewRecursiveCharacter()
	}

	docs, err := l.Load(ctx)
	if err != nil {
		return nil, err
	}

	for _, doc := range docs {
		contents, err := splitter.SplitText(doc.PageContent)
		if err != nil {
			return nil, fmt.Errorf("failed to split page content: %w", err)
		}
		for _, content := range contents {
			newDoc := schema.Document{
				PageContent: content,
				Metadata:    doc.Metadata,
				Score:       doc.Score,
			}
			splitteddocs = append(splitteddocs, newDoc)
		}
	}

	return splitteddocs, nil
}

func applyCloudSQLDocumentLoaderOptions(options []DocumentLoaderOption) (*DocumentLoader, error) {
	dl := &DocumentLoader{
		schemaName: defaultSchemaName,
	}

	for _, opt := range options {
		opt(dl)
	}

	if dl.engine.Pool == nil {
		return nil, fmt.Errorf("engine.Pool must be specified")
	}

	if dl.query == "" && dl.tableName == "" {
		return nil, fmt.Errorf("either query or tableName must be specified")
	}
	if dl.format != "" && dl.formatter != nil {
		return nil, fmt.Errorf("only one of 'format' or 'formatter' must be specified")
	}

	if dl.query == "" {
		dl.query = fmt.Sprintf(`SELECT * FROM %s.%s`, dl.schemaName, dl.tableName)
	}

	if dl.formatter == nil {
		switch strings.ToLower(dl.format) {
		case "": // default formatter
			dl.formatter = textFormatter
		case "csv":
			dl.formatter = csvFormatter
		case "text":
			dl.formatter = textFormatter
		case "json":
			dl.formatter = jsonFormatter
		case "yaml":
			dl.formatter = yamlFormatter
		default:
			return nil, fmt.Errorf("format must be type: 'csv', 'text', 'json', 'yaml'")
		}
	}

	return dl, nil
}
