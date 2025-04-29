package cloudsql

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/textsplitter"
	"github.com/tmc/langchaingo/util/cloudsqlutil"
)

const (
	defaultMetadataJSONColumn = "langchain_metadata"
	defaultSchemaName         = "public"
)

type valueProcessor struct {
	extractors map[reflect.Type]func(any) (any, bool)
}

func newValueProcessor() *valueProcessor {
	return &valueProcessor{
		extractors: map[reflect.Type]func(any) (any, bool){
			reflect.TypeOf(&sql.NullTime{}): func(v any) (any, bool) {
				if nv, ok := v.(*sql.NullTime); ok && nv.Valid {
					return nv.Time, true
				}
				return nil, false
			},
			reflect.TypeOf(&sql.NullString{}): func(v any) (any, bool) {
				if nv, ok := v.(*sql.NullString); ok && nv.Valid {
					return nv.String, true
				}
				return nil, false
			},
			reflect.TypeOf(&sql.NullBool{}): func(v any) (any, bool) {
				if nv, ok := v.(*sql.NullBool); ok && nv.Valid {
					return nv.Bool, true
				}
				return nil, false
			},
			reflect.TypeOf(&sql.NullInt64{}): func(v any) (any, bool) {
				if nv, ok := v.(*sql.NullInt64); ok && nv.Valid {
					return nv.Int64, true
				}
				return nil, false
			},
			reflect.TypeOf(&sql.NullFloat64{}): func(v any) (any, bool) {
				if nv, ok := v.(*sql.NullFloat64); ok && nv.Valid {
					return nv.Float64, true
				}
				return nil, false
			},
			reflect.TypeOf(&sql.RawBytes{}): func(v any) (any, bool) {
				if rv, ok := v.(*sql.RawBytes); ok {
					return *rv, true
				}
				return nil, false
			},
		},
	}
}

func (vp *valueProcessor) process(valuesPrt []any, columnNames []string) map[string]any {
	columnValues := make(map[string]any)
	for i, name := range columnNames {
		if handler, ok := vp.extractors[reflect.TypeOf(valuesPrt[i])]; ok {
			if val, valid := handler(valuesPrt[i]); valid {
				columnValues[name] = val
				continue
			}
		}
		columnValues[name] = valuesPrt[i]
	}
	return columnValues
}

type oidProcessor struct {
	extractors map[pgtype.OID]any
}

func newOidProcessor() *oidProcessor {
	return &oidProcessor{
		extractors: map[pgtype.OID]any{
			pgtype.TimeOID:        new(sql.NullTime),
			pgtype.TimestampOID:   new(sql.NullTime),
			pgtype.TimestamptzOID: new(sql.NullTime),
			pgtype.DateOID:        new(sql.NullTime),
			pgtype.VarcharOID:     new(sql.NullString),
			pgtype.TextOID:        new(sql.NullString),
			pgtype.JSONOID:        new(sql.NullString),
			pgtype.BoolOID:        new(sql.NullBool),
			pgtype.Float4OID:      new(sql.NullFloat64),
			pgtype.Float8OID:      new(sql.NullFloat64),
			pgtype.Int2OID:        new(sql.NullInt64),
			pgtype.Int4OID:        new(sql.NullInt64),
			pgtype.Int8OID:        new(sql.NullInt64),
		},
	}
}

func (vp *oidProcessor) process(oid pgtype.OID) (any, bool) {
	v, ok := vp.extractors[oid]
	return v, ok
}

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

	oidProcessor   *oidProcessor
	valueProcessor *valueProcessor
}

// NewDocumentLoader creates a new DocumentLoader instance.
func NewDocumentLoader(ctx context.Context, engine cloudsqlutil.PostgresEngine, options []DocumentLoaderOption) (*DocumentLoader, error) {
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

	documentLoader.oidProcessor = newOidProcessor()
	documentLoader.valueProcessor = newValueProcessor()

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

	populateJSONMetadata := func(data []byte) error {
		var jsonMetadata map[string]any
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
	rows, err := l.engine.Pool.Query(ctx, l.query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	valuesPrt := make([]any, len(columnNames))

	for i, fd := range fieldDescriptions {
		columnNames[i] = fd.Name
		t, ok := l.oidProcessor.process(pgtype.OID(fd.DataTypeOID))
		if !ok {
			valuesPrt[i] = new(sql.RawBytes)
		}
		valuesPrt[i] = t
	}

	var documents []schema.Document
	for rows.Next() {
		if err := rows.Scan(valuesPrt...); err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}

		if err = rows.Scan(valuesPrt...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		columnValues := l.valueProcessor.process(valuesPrt, columnNames)

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

	if splitter == nil {
		splitter = textsplitter.NewRecursiveCharacter()
	}

	docs, err := l.Load(ctx)
	if err != nil {
		return nil, err
	}

	var splitDocs []schema.Document
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
			splitDocs = append(splitDocs, newDoc)
		}
	}

	return splitDocs, nil
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
