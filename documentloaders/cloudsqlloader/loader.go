package cloudsqlloader

import (
	"context"
	"github.com/tmc/langchaingo/internal/cloudsqlutil"
)

type DocumentLoader struct {
	engine             *cloudsqlutil.PostgresEngine // cloudsql engine with pool connection to the postgres database
	query              string                       // SQL query. Defaults to None.
	tableName          string                       // Name of table to query. Defaults to None.
	schemaName         string                       //  Database schema name of the table. Defaults to "public".
	contentColumns     []string                     // Column that represent a Document's page_content. Defaults to the first column.
	metadataColumns    []string                     // Column(s) that represent a Document's metadata. Defaults to None.
	metadataJSONColumn []string                     // Column to store metadata as JSON. Defaults to "langchain_metadata".
	formatter          Formatter                    // A function to format page content (OneOf: format, formatter). Defaults to None.
	format             string                       // Format of page content (OneOf: text, csv, YAML, JSON). Defaults to 'text'.
}

type Formatter func(string) (string, error)

func NewDocumentLoader(ctx context.Context, opts ...Option) (*DocumentLoader, error) {
	documentLoader := new(DocumentLoader)
	if err := applyLoaderOptions(documentLoader, opts...); err != nil {
		return &DocumentLoader{}, err
	}

	return documentLoader, nil
}
