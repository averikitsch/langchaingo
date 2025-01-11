package cloudsqlloader

import (
	"fmt"
	"github.com/tmc/langchaingo/internal/cloudsqlutil"
)

type Option func(loader *DocumentLoader) error

var formats []string = []string{"csv", "text", "JSON", "YAML"}

func WithDocumentLoaderInstance(engine *cloudsqlutil.PostgresEngine, tableName string) Option {
	return func(loader *DocumentLoader) error {
		loader.pool = engine
		loader.tableName = tableName
		return nil
	}
}

func WithQuery(query string) Option {
	return func(loader *DocumentLoader) error {
		loader.query = query
		return nil
	}
}

func WithSchemaName(schemaName string) Option {
	return func(loader *DocumentLoader) error {
		loader.schemaName = schemaName
		return nil
	}
}

func WithContentColumns(contentColumns []string) Option {
	return func(loader *DocumentLoader) error {
		loader.contentColumns = contentColumns
		return nil
	}
}

func WithMetadataColumns(metadataColumns []string) Option {
	return func(loader *DocumentLoader) error {
		loader.metadataColumns = metadataColumns
		return nil
	}
}

func WithMetadataJSONColumn(metadataJSONColumn []string) Option {
	return func(loader *DocumentLoader) error {
		loader.metadataJSONColumn = metadataJSONColumn
		return nil
	}
}

func WithFormatter(formatter Formatter) Option {
	return func(loader *DocumentLoader) error {
		loader.formatter = formatter
		return nil
	}
}

func WithFormat(format string) Option {
	return func(loader *DocumentLoader) error {
		loader.format = format
		return nil
	}
}

func isFormatSupported(f string) bool {
	for _, format := range formats {
		if f == format {
			return true
		}
	}
	return false
}

func applyLoaderOptions(l *DocumentLoader, options ...Option) error {

	for _, option := range options {
		if err := option(l); err != nil {
			return err
		}
	}
	if l.tableName != "" && l.query != "" {
		return fmt.Errorf("Only one of 'tableName' or 'query' should be specified.")
	}

	if l.tableName == "" && l.query == "" {
		return fmt.Errorf("At least one of 'tableName' or 'query' should be specified.")
	}

	if l.format != "" && !isFormatSupported(l.format) {
		return fmt.Errorf("format must be type: 'csv', 'text', 'JSON', 'YAML'")
	}

	if l.format != "" && l.formatter != nil {
		return fmt.Errorf("Only one of 'format' or 'formatter' should be specified.")
	}

	if l.schemaName == "" {
		l.schemaName = "public"
	}

	if l.metadataJSONColumn == nil {
		l.metadataJSONColumn = []string{"langchain_metadata"}
	}

	if l.format == "" {
		l.format = "text"
	}

	if l.query == "" {
		l.query = fmt.Sprintf("SELECT * FROM %s.%s", l.schemaName, l.tableName)
	}

	return nil
}
