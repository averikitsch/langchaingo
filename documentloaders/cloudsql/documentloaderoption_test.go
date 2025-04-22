package cloudsql

import (
	"testing"

	"github.com/averikitsch/langchaingo/util/cloudsqlutil"
	"github.com/stretchr/testify/assert"
)

func TestDocumentLoaderOption(t *testing.T) {

	tests := []struct {
		name         string
		args         []DocumentLoaderOption
		wantErr      bool
		validateFunc func(t *testing.T, c *DocumentLoader, err error)
	}{
		{
			name: "pgxpool is nil",
			args: []DocumentLoaderOption{
				WithEngine(cloudsqlutil.PostgresEngine{}),
			},
			wantErr: true,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				assert.EqualError(t, err, "engine.Pool must be specified")
			},
		},
		{
			name: "table name and query are empty",
			args: []DocumentLoaderOption{
				WithEngine(engine),
			},
			wantErr: true,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				assert.EqualError(t, err, "either query or tableName must be specified")
			},
		},
		{
			name: "format and formatter are not empty",
			args: []DocumentLoaderOption{
				WithEngine(engine),
				WithTableName("testtable"),
				WithFormat("json"),
				WithFormatter(func(m map[string]interface{}, s []string) string {
					return "formatter"
				}),
			},
			wantErr: true,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				assert.EqualError(t, err, "only one of 'format' or 'formatter' must be specified")
			},
		},
		{
			name: "invalid format are not empty",
			args: []DocumentLoaderOption{
				WithEngine(engine),
				WithTableName("testtable"),
				WithFormat("invalidformata"),
			},

			wantErr: true,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				assert.EqualError(t, err, "format must be type: 'csv', 'text', 'json', 'yaml'")
			},
		},
		{
			name: "success",
			args: []DocumentLoaderOption{
				WithEngine(engine),
				WithTableName("testtable"),
				WithFormat("json"),
			},

			wantErr: false,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				assert.Equal(t, c.query, "SELECT * FROM public.testtable")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := applyCloudSQLDocumentLoaderOptions(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("applyCloudSQLDocumentLoaderOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.validateFunc(t, got, err)
		})
	}
}
