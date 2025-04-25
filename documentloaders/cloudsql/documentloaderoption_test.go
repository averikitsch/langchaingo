package cloudsql

import (
	"testing"

	"github.com/averikitsch/langchaingo/util/cloudsqlutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocumentLoaderOption(t *testing.T) {
	t.Parallel()

	testEngine, teardown, err := setup()
	require.NoError(t, err)
	t.Cleanup(teardown)

	tests := []struct {
		name         string
		args         []DocumentLoaderOption
		wantErr      bool
		validateFunc func(t *testing.T, c *DocumentLoader, err error)
	}{
		{
			name:    "pgxpool is nil",
			args:    []DocumentLoaderOption{WithEngine(cloudsqlutil.PostgresEngine{})},
			wantErr: true,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, c)
				assert.EqualError(t, err, "engine.Pool must be specified")
			},
		},
		{
			name:    "table name and query are empty",
			args:    []DocumentLoaderOption{WithEngine(testEngine)},
			wantErr: true,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, c)
				assert.EqualError(t, err, "either query or tableName must be specified")
			},
		},
		{
			name: "format and formatter are not empty",
			args: []DocumentLoaderOption{WithEngine(testEngine), WithTableName("testtable"), WithFormat("json"), WithFormatter(func(_ map[string]interface{}, _ []string) string {
				return "formatter"
			})},
			wantErr: true,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, c)
				assert.EqualError(t, err, "only one of 'format' or 'formatter' must be specified")
			},
		},
		{
			name:    "invalid format are not empty",
			args:    []DocumentLoaderOption{WithEngine(testEngine), WithTableName("testtable"), WithFormat("invalidformata")},
			wantErr: true,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, c)
				assert.EqualError(t, err, "format must be type: 'csv', 'text', 'json', 'yaml'")
			},
		},
		{
			name:    "success",
			args:    []DocumentLoaderOption{WithEngine(testEngine), WithTableName("testtable"), WithFormat("json")},
			wantErr: false,
			validateFunc: func(t *testing.T, c *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, err)
				require.NotNil(t, c)
				assert.Equal(t, c.query, "SELECT * FROM public.testtable")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := applyCloudSQLDocumentLoaderOptions(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("applyCloudSQLDocumentLoaderOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.validateFunc(t, got, err)
		})
	}
}
