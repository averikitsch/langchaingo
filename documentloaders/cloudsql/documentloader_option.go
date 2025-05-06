package cloudsql

import (
	"fmt"
	"regexp"
	"strings"
)

const sqlregularexpresion = `(?i)^\s*SELECT\s+.+\s+FROM\s+((")?([a-zA-Z0-9_]+)(")?\.)?(")?([a-zA-Z0-9_]+)(")?\b`

// DocumentLoaderOption is a functional option for configuring the DocumentLoader.
type DocumentLoaderOption func(*DocumentLoader)

// WithSchemaName sets the schema name for the table. Defaults to "public".
func WithSchemaName(schemaName string) DocumentLoaderOption {
	return func(documentLoader *DocumentLoader) {
		documentLoader.schemaName = schemaName
	}
}

// WithQuery sets the SQL query to execute. If not provided, a default query is generated from the table name.
func WithQuery(query string) DocumentLoaderOption {
	return func(documentLoader *DocumentLoader) {
		documentLoader.query = query
	}
}

// WithTableName sets the table name to load data from. If not provided, a custom query must be specified.
func WithTableName(tableName string) DocumentLoaderOption {
	return func(documentLoader *DocumentLoader) {
		documentLoader.tableName = tableName
	}
}

// WithFormatter sets a custom formatter to convert row data into document content.
func WithFormatter(formatter func(map[string]interface{}, []string) string) DocumentLoaderOption {
	return func(documentLoader *DocumentLoader) {
		documentLoader.formatter = formatter
	}
}

// WithFormat sets the format for the document content. Predefined formats are "csv", "text", "json", and "yaml".
// Only one of WithFormat or WithFormatter should be specified.
func WithFormat(format string) DocumentLoaderOption {
	return func(documentLoader *DocumentLoader) {
		documentLoader.format = format
	}
}

// WithContentColumns sets the list of columns to use for the document content.
func WithContentColumns(contentColumns []string) DocumentLoaderOption {
	return func(documentLoader *DocumentLoader) {
		documentLoader.contentColumns = contentColumns
	}
}

// WithMetadataColumns sets the list of columns to use for the document metadata.
func WithMetadataColumns(metadataColumns []string) DocumentLoaderOption {
	return func(documentLoader *DocumentLoader) {
		documentLoader.metadataColumns = metadataColumns
	}
}

// WithMetadataJSONColumn sets the column name containing JSON metadata.
func WithMetadataJSONColumn(metadataJSONColumn string) DocumentLoaderOption {
	return func(documentLoader *DocumentLoader) {
		documentLoader.metadataJSONColumn = metadataJSONColumn
	}
}

func applyCloudSQLDocumentLoaderOptions(dl *DocumentLoader, options []DocumentLoaderOption) error {
	for _, opt := range options {
		opt(dl)
	}

	return validateDocumentLoader(dl)
}

func validateDocumentLoader(dl *DocumentLoader) error {
	formatters := map[string]func(_ map[string]any, _ []string) string{
		"csv":  csvFormatter,
		"":     textFormatter,
		"text": textFormatter,
		"json": jsonFormatter,
		"yaml": yamlFormatter,
	}

	if dl.engine.Pool == nil {
		return fmt.Errorf("engine.Pool must be specified")
	}

	if dl.query == "" && dl.tableName == "" {
		return fmt.Errorf("either query or tableName must be specified")
	}

	if dl.query != "" && dl.tableName != "" {
		return fmt.Errorf("only one of 'table_name' or 'query' should be specified")
	}

	if dl.format != "" && dl.formatter != nil {
		return fmt.Errorf("only one of 'format' or 'formatter' must be specified")
	}

	if dl.query == "" {
		dl.query = fmt.Sprintf(`SELECT * FROM "%s"."%s"`, dl.schemaName, dl.tableName)
	}

	if dl.formatter == nil {
		f, ok := formatters[strings.ToLower(dl.format)]
		if !ok {
			return fmt.Errorf("format must be type: 'csv', 'text', 'json', 'yaml'")
		}
		dl.formatter = f
	}
	return nil
}

func validateQuery(query string) error {
	re := regexp.MustCompile(sqlregularexpresion)
	if !re.MatchString(query) {
		return fmt.Errorf("query is not valid for the following regular expression: %s", sqlregularexpresion)
	}
	return nil
}
