package cloudsql

import "github.com/averikitsch/langchaingo/util/cloudsqlutil"

// DocumentLoaderOption is a functional option for configuring the DocumentLoader.
type DocumentLoaderOption func(*DocumentLoader)

// WithEngine sets the engine for the document loader.
func WithEngine(engine cloudsqlutil.PostgresEngine) DocumentLoaderOption {
	return func(documentLoader *DocumentLoader) {
		documentLoader.engine = engine
	}
}

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
