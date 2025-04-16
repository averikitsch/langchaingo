package loader

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/averikitsch/langchaingo/schema"
	"github.com/averikitsch/langchaingo/textsplitter"
	"github.com/averikitsch/langchaingo/util/alloydbutil"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	defaultMetadataJSONColumn = "langchain_metadata"
	defaultSchemaName         = "public"
)

// Document represents a loaded document with content and metadata.
type Document struct {
	Content  string
	Metadata map[string]interface{}
}

// Config holds the configuration for the DocumentLoader.
type Config struct {
	engine             alloydbutil.PostgresEngine
	query              string
	tableName          string
	schemaName         string
	contentColumns     []string
	metadataColumns    []string
	metadataJSONColumn string
	format             string
	formatter          func(map[string]interface{}, []string) string
}

// DocumentLoader is responsible for loading documents from a Postgres database.
type DocumentLoader struct {
	config *Config
}

// NewDocumentLoader creates a new DocumentLoader instance.
func NewDocumentLoader(config *Config) (*DocumentLoader, error) {

	ctx := context.Background()

	// Validate columns against the query result
	re := regexp.MustCompile(`(?i)^\s*SELECT\s+.+\s+FROM\s+([a-zA-Z0-9_]+\.)?([a-zA-Z0-9_]+)\b`)
	query := re.FindString(config.query)
	if query == "" {
		return nil, errors.New("query is not valid")
	}
	query = fmt.Sprintf("%s LIMIT 1", query)

	rows, err := config.engine.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	fieldDescription := rows.FieldDescriptions()

	if len(config.contentColumns) == 0 {
		config.contentColumns = []string{fieldDescription[0].Name}
	}

	if len(config.metadataColumns) == 0 {
		metadataCols := make([]string, 0)
		for _, col := range fieldDescription {
			if !slices.Contains(config.contentColumns, col.Name) {
				metadataCols = append(metadataCols, col.Name)
			}
		}
		config.metadataColumns = metadataCols
	}

	if config.metadataJSONColumn == "" {
		config.metadataJSONColumn = defaultMetadataJSONColumn
	}
	found := false
	for _, col := range fieldDescription {
		if col.Name == config.metadataJSONColumn {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("metadata JSON column '%s' not found in query result", config.metadataJSONColumn)
	}

	allNames := make(map[string]bool)
	for _, name := range config.contentColumns {
		allNames[name] = true
	}
	for _, name := range config.metadataColumns {
		allNames[name] = true
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

	return &DocumentLoader{config: config}, nil
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
	pageContent := l.config.formatter(row, l.config.contentColumns)
	metadata := make(map[string]interface{})

	populateJSONMetadata := func(data []byte) error {
		var jsonMetadata map[string]interface{}
		err := json.Unmarshal(data, &jsonMetadata)
		if err != nil {
			return fmt.Errorf("failed to parse JSON from column '%s': %w", l.config.metadataJSONColumn, err)
		}
		for k, v := range jsonMetadata {
			metadata[k] = v
		}
		return nil
	}

	if l.config.metadataJSONColumn != "" {
		value := row[l.config.metadataJSONColumn]
		switch value.(type) {
		case []byte:
			if err := populateJSONMetadata(value.([]byte)); err != nil {
				return schema.Document{}, err
			}
		case string:
			if err := populateJSONMetadata([]byte(value.(string))); err != nil {
				return schema.Document{}, err
			}
		default:
			return schema.Document{}, fmt.Errorf("failed to parse JSON from column '%s': invalid column type", l.config.metadataJSONColumn)
		}
	}

	for _, column := range l.config.metadataColumns {
		if column != l.config.metadataJSONColumn {
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
	rows, err := l.config.engine.Pool.Query(ctx, l.config.query)
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
			return nil, fmt.Errorf("scan row failed: %v", err)
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

// NewConfig creates a new Config.
func NewConfig(engine alloydbutil.PostgresEngine, options ...Option) (*Config, error) {
	config := &Config{
		engine:     engine,
		schemaName: defaultSchemaName,
	}

	for _, opt := range options {
		opt(config)
	}

	if config.engine.Pool == nil {
		return nil, fmt.Errorf("engine.Pool must be specified")
	}

	if config.query == "" && config.tableName == "" {
		return nil, fmt.Errorf("either query or tableName must be specified")
	}
	if config.format != "" && config.formatter != nil {
		return nil, fmt.Errorf("only one of 'format' or 'formatter' must be specified")
	}

	if config.query == "" {
		config.query = fmt.Sprintf(`SELECT * FROM %s.%s`, config.schemaName, config.tableName)
	}

	if config.format != "" {
		switch strings.ToLower(config.format) {
		case "csv":
			config.formatter = csvFormatter
		case "text":
			config.formatter = textFormatter
		case "json":
			config.formatter = jsonFormatter
		case "yaml":
			config.formatter = yamlFormatter
		default:
			return nil, fmt.Errorf("format must be type: 'csv', 'text', 'json', 'yaml'")
		}
	} else if config.formatter == nil {
		config.formatter = textFormatter
	}

	return config, nil
}

// Option is a functional option for configuring the DocumentLoader.
type Option func(*Config)

// WithSchemaName sets the schema name for the table. Defaults to "public".
func WithSchemaName(schemaName string) Option {
	return func(config *Config) {
		config.schemaName = schemaName
	}
}

// WithQuery sets the SQL query to execute. If not provided, a default query is generated from the table name.
func WithQuery(query string) Option {
	return func(config *Config) {
		config.query = query
	}
}

// WithTableName sets the table name to load data from. If not provided, a custom query must be specified.
func WithTableName(tableName string) Option {
	return func(config *Config) {
		config.tableName = tableName

	}
}

// WithFormatter sets a custom formatter to convert row data into document content.
func WithFormatter(formatter func(map[string]interface{}, []string) string) Option {
	return func(config *Config) {
		config.formatter = formatter
	}
}

// WithFormat sets the format for the document content. Predefined formats are "csv", "text", "json", and "yaml".
// Only one of WithFormat or WithFormatter should be specified.
func WithFormat(format string) Option {
	return func(config *Config) {
		config.format = format
	}
}

// WithContentColumns sets the list of columns to use for the document content.
func WithContentColumns(contentColumns []string) Option {
	return func(config *Config) {
		config.contentColumns = contentColumns
	}
}

// WithMetadataColumns sets the list of columns to use for the document metadata.
func WithMetadataColumns(metadataColumns []string) Option {
	return func(config *Config) {
		config.metadataColumns = metadataColumns
	}
}

// WithMetadataJSONColumn sets the column name containing JSON metadata.
func WithMetadataJSONColumn(metadataJsonColumn string) Option {
	return func(config *Config) {
		config.metadataJSONColumn = metadataJsonColumn
	}
}
