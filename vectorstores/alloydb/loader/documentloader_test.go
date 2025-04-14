package loader

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/averikitsch/langchaingo/schema"
	"github.com/averikitsch/langchaingo/util/alloydbutil"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var engine alloydbutil.PostgresEngine

type pgvectorContainer struct {
	testcontainers.Container
	URI string
}

func setupPgvector(ctx context.Context) (*pgvectorContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "pgvector/pgvector:pg16", // Or your preferred version
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpassword",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithStartupTimeout(5 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start pgvector container: %w", err)
	}

	pgvC := &pgvectorContainer{Container: container}

	ip, err := container.Host(ctx)
	if err != nil {
		return pgvC, fmt.Errorf("failed to get container host: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return pgvC, fmt.Errorf("failed to get mapped port: %w", err)
	}

	uri := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("testuser", "testpassword"),
		Host:     net.JoinHostPort(ip, mappedPort.Port()),
		Path:     "/testdb",
		RawQuery: "sslmode=disable",
	}

	pgvC.URI = uri.String()

	// You might want to initialize the pgvector extension here
	// by connecting to the database and running "CREATE EXTENSION vector;"
	// This can be done in a separate function after setupPgvector.

	return pgvC, nil

}

func setupEngine() (alloydbutil.PostgresEngine, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := setupPgvector(ctx)
	if err != nil {
		return alloydbutil.PostgresEngine{}, func() {}, err
	}

	pool, err := pgxpool.New(ctx, container.URI)
	if err != nil {
		return alloydbutil.PostgresEngine{}, func() {}, fmt.Errorf("failed to instantiate pgx pool: %w", err)
	}

	// verify if we have connectivity with database. timeout is 3 seconds.
	var attemptErr error
	for attempts := 0; attempts < 3; attempts++ {
		if attemptErr = pool.Ping(ctx); attemptErr != nil {
			time.Sleep(1 * time.Second)
		}
	}
	if attemptErr != nil {
		return alloydbutil.PostgresEngine{}, func() {}, fmt.Errorf("failed to connect to postgres container: %w", attemptErr)
	}

	eng, err := alloydbutil.NewPostgresEngine(context.Background(),
		alloydbutil.WithPool(pool),
	)
	if err != nil {
		return alloydbutil.PostgresEngine{}, func() {}, fmt.Errorf("failed to instantiate pgx pool: %w", err)
	}

	return eng, func() {
		eng.Close()
		container.Terminate(ctx)

	}, nil

}

func TestMain(m *testing.M) {
	eng, teardown, err := setupEngine()
	engine = eng
	if err != nil {
		os.Exit(1)
		return
	}

	m.Run()
	teardown()

}

func TestNewConfig(t *testing.T) {
	type args struct {
		engine  alloydbutil.PostgresEngine
		options []Option
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		validateFunc func(t *testing.T, c *Config, err error)
	}{
		{
			name: "pgxpool is nil",
			args: args{
				engine: alloydbutil.PostgresEngine{},
			},
			wantErr: true,
			validateFunc: func(t *testing.T, c *Config, err error) {
				assert.EqualError(t, err, "engine.Pool must be specified")
			},
		},
		{
			name: "table name and query are empty",
			args: args{
				engine: engine,
			},
			wantErr: true,
			validateFunc: func(t *testing.T, c *Config, err error) {
				assert.EqualError(t, err, "either query or tableName must be specified")
			},
		},
		{
			name: "format and formatter are not empty",
			args: args{
				engine: engine,
				options: []Option{
					WithTableName("testtable"),
					WithFormat("json"),
					WithFormatter(func(m map[string]interface{}, s []string) string {
						return "formatter"
					}),
				},
			},
			wantErr: true,
			validateFunc: func(t *testing.T, c *Config, err error) {
				assert.EqualError(t, err, "only one of 'format' or 'formatter' must be specified")
			},
		},
		{
			name: "invalid format are not empty",
			args: args{
				engine: engine,
				options: []Option{
					WithTableName("testtable"),
					WithFormat("invalidformata"),
				},
			},
			wantErr: true,
			validateFunc: func(t *testing.T, c *Config, err error) {
				assert.EqualError(t, err, "format must be type: 'csv', 'text', 'json', 'yaml'")
			},
		},
		{
			name: "success",
			args: args{
				engine: engine,
				options: []Option{
					WithTableName("testtable"),
					WithFormat("json"),
				},
			},
			wantErr: false,
			validateFunc: func(t *testing.T, c *Config, err error) {
				assert.Equal(t, c.query, "SELECT * FROM public.testtable")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewConfig(tt.args.engine, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tt.validateFunc(t, got, err)
		})
	}
}

func TestNewDocumentLoader(t *testing.T) {
	createTable(t)

	tests := []struct {
		name         string
		setConfig    func() *Config
		want         *DocumentLoader
		validateFunc func(t *testing.T, d *DocumentLoader, error error)
	}{
		{
			name: "invalid query",
			setConfig: func() *Config {
				config, err := NewConfig(engine, WithQuery("SELECT FROM table"))
				require.NoError(t, err)
				return config
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				assert.Nil(t, d)
				assert.EqualError(t, err, "query is not valid")
			},
		},
		{
			name: "table does not exist",
			setConfig: func() *Config {
				config, err := NewConfig(engine, WithTableName("invalidtable"))
				require.NoError(t, err)
				return config
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				assert.Nil(t, d)
				assert.ErrorContains(t, err, `failed to execute query: ERROR: relation "public.invalidtable" does not exist`)
			},
		},
		{
			name: "invalid  metadata JSON column (using default)",
			setConfig: func() *Config {
				config, err := NewConfig(engine, WithTableName("testtable"))
				require.NoError(t, err)
				return config
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				assert.Nil(t, d)
				assert.ErrorContains(t, err, "metadata JSON column 'langchain_metadata' not found in query result")
			},
		},
		{
			name: "invalid column name for content",
			setConfig: func() *Config {
				config, err := NewConfig(engine, WithTableName("testtable"),
					WithMetadataJSONColumn("c_json_metadata"),
					WithContentColumns([]string{"c_invalid"}))
				require.NoError(t, err)
				return config
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				assert.Nil(t, d)
				assert.ErrorContains(t, err, "column 'c_invalid' not found in query result")
			},
		},
		{
			name: "invalid column name for metadata",
			setConfig: func() *Config {
				config, err := NewConfig(engine, WithTableName("testtable"),
					WithMetadataJSONColumn("c_json_metadata"),
					WithMetadataColumns([]string{"c_invalid"}))
				require.NoError(t, err)
				return config
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				assert.Nil(t, d)
				assert.ErrorContains(t, err, "column 'c_invalid' not found in query result")
			},
		},
		{
			name: "success without content column",
			setConfig: func() *Config {
				config, err := NewConfig(engine, WithTableName("testtable"),
					WithMetadataJSONColumn("c_json_metadata"))
				require.NoError(t, err)
				return config
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				require.NoError(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, d.config.engine, engine)
				assert.Equal(t, d.config.query, "SELECT * FROM public.testtable")
				assert.Equal(t, d.config.tableName, "testtable")
				assert.Equal(t, d.config.schemaName, "public")
				assert.Equal(t, d.config.contentColumns, []string{"c_id"})
				assert.Equal(t, d.config.metadataColumns, []string{
					"c_content",
					"c_embedding",
					"c_session",
					"c_user",
					"c_date",
					"c_active",
					"c_json_metadata"})
				assert.Equal(t, d.config.metadataJSONColumn, "c_json_metadata")

			},
		},
		{
			name: "success with content column",
			setConfig: func() *Config {
				config, err := NewConfig(engine, WithTableName("testtable"),
					WithMetadataJSONColumn("c_json_metadata"),
					WithContentColumns([]string{"c_content"}))
				require.NoError(t, err)
				return config
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				require.NoError(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, d.config.engine, engine)
				assert.Equal(t, d.config.query, "SELECT * FROM public.testtable")
				assert.Equal(t, d.config.tableName, "testtable")
				assert.Equal(t, d.config.schemaName, "public")
				assert.Equal(t, d.config.contentColumns, []string{"c_content"})
				assert.Equal(t, d.config.metadataColumns, []string{
					"c_id",
					"c_embedding",
					"c_session",
					"c_user",
					"c_date",
					"c_active",
					"c_json_metadata"})
				assert.Equal(t, d.config.metadataJSONColumn, "c_json_metadata")

			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDocumentLoader(tt.setConfig())
			tt.validateFunc(t, got, err)
		})
	}
}

func TestDocumentLoader_Load(t *testing.T) {
	createTable(t)
	insertRows(t)

	ctx := context.Background()

	tests := []struct {
		name         string
		config       *Config
		validateFunc func(t *testing.T, d []schema.Document, err error)
	}{
		{
			name: "fail to execute the query",
			config: &Config{
				engine:     engine,
				tableName:  "testtable",
				schemaName: "public",
				format:     "json",
				query:      "SELECT invalid_column FROM public.testtable",
			},
			validateFunc: func(t *testing.T, d []schema.Document, err error) {
				require.Error(t, err)
				assert.ErrorContains(t, err, "failed to execute query")
			},
		},
		{
			name: "success",
			config: &Config{
				engine:          engine,
				tableName:       "testtable",
				schemaName:      "public",
				metadataColumns: []string{"c_id", "c_date", "c_user", "c_session"},
				formatter:       jsonFormatter,
				query:           "SELECT * FROM public.testtable WHERE c_session = 100",
			},
			validateFunc: func(t *testing.T, d []schema.Document, err error) {
				require.NoError(t, err)
				require.Len(t, d, 1)
				require.Len(t, d[0].Metadata, 4)
				assert.Equal(t, "user1", d[0].Metadata["c_user"])
				assert.Equal(t, int64(100), d[0].Metadata["c_session"])
				fmt.Println(d[0].Metadata)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &DocumentLoader{
				config: tt.config,
			}
			got, err := l.Load(ctx)
			tt.validateFunc(t, got, err)
		})
	}
}

func TestDocumentLoader_LoadAndSplit(t *testing.T) {
	createTable(t)
	insertRows(t)

	ctx := context.Background()

	tests := []struct {
		name         string
		config       *Config
		validateFunc func(t *testing.T, d []schema.Document, err error)
	}{
		{
			name: "fail to execute the query",
			config: &Config{
				engine:     engine,
				tableName:  "testtable",
				schemaName: "public",
				format:     "json",
				query:      "SELECT invalid_column FROM public.testtable",
			},
			validateFunc: func(t *testing.T, d []schema.Document, err error) {
				require.Error(t, err)
				assert.ErrorContains(t, err, "failed to execute query")
			},
		},
		{
			name: "success",
			config: &Config{
				engine:          engine,
				tableName:       "testtable",
				schemaName:      "public",
				metadataColumns: []string{"c_id", "c_date", "c_user", "c_session"},
				formatter:       jsonFormatter,
				query:           "SELECT * FROM public.testtable WHERE c_session = 100",
			},
			validateFunc: func(t *testing.T, d []schema.Document, err error) {
				require.NoError(t, err)
				require.Len(t, d, 1)
				require.Len(t, d[0].Metadata, 4)
				assert.Equal(t, "user1", d[0].Metadata["c_user"])
				assert.Equal(t, int64(100), d[0].Metadata["c_session"])
				fmt.Println(d[0].Metadata)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &DocumentLoader{
				config: tt.config,
			}
			got, err := l.LoadAndSplit(ctx, nil)
			tt.validateFunc(t, got, err)
		})
	}
}

func createTable(t *testing.T) {
	err := engine.InitVectorstoreTable(context.Background(), alloydbutil.VectorstoreTableOptions{
		TableName:          "testtable",
		VectorSize:         3,
		SchemaName:         "public",
		ContentColumnName:  "c_content",
		EmbeddingColumn:    "c_embedding",
		MetadataJSONColumn: "c_json_metadata",
		IDColumn: alloydbutil.Column{
			Name:     "c_id",
			Nullable: false,
		},
		MetadataColumns: []alloydbutil.Column{
			{
				Name:     "c_session",
				DataType: "integer",
				Nullable: false,
			},
			{
				Name:     "c_user",
				DataType: "varchar(30)",
				Nullable: false,
			},
			{
				Name:     "c_date",
				DataType: "timestamp",
				Nullable: true,
			},
			{
				Name:     "c_active",
				DataType: "bool",
				Nullable: true,
			},
			{
				Name:     "c_json_metadata",
				DataType: "JSON",
				Nullable: true,
			},
		},
		OverwriteExisting: true,
		StoreMetadata:     false,
	})
	require.NoError(t, err)

}

func insertRows(t *testing.T) {
	_, err := engine.Pool.Exec(context.Background(),
		`INSERT INTO public.testtable(c_id,c_embedding,c_session,c_user,c_date,c_content, c_json_metadata) 
			 VALUES ('ef0f712a-472a-4477-825d-6f3738659f31','[3.0,1.4,2.9]', 100, 'user1', '2025-02-12', 'somecontent', '{"somefield": "somevalue"}' ), 
			        ('352c5ae2-feb3-47ad-a32c-306963e5bfaf','[2.7,0.4,1.8]', 200, 'user2', '2024-02-12', 'someothercontent','{"somefield": "anothervalue"}')`)
	require.NoError(t, err)
}
