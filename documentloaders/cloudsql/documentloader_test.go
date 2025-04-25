package cloudsql

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/averikitsch/langchaingo/util/cloudsqlutil"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

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
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(5 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
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

	return pgvC, nil
}

func setUpEngine() (cloudsqlutil.PostgresEngine, func(), error) {
	username := os.Getenv("POSTGRES_USERNAME")
	password := os.Getenv("POSTGRES_PASSWORD")
	database := os.Getenv("POSTGRES_DATABASE")
	projectID := os.Getenv("POSTGRES_PROJECT_ID")
	region := os.Getenv("POSTGRES_REGION")
	instance := os.Getenv("POSTGRES_INSTANCE")

	// if not all the environments are define for connect to cloud sql, use the test container
	if username == "" || password == "" || database == "" || projectID == "" || region == "" || instance == "" {
		log.Println("one or more environment variables are empty (POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_DATABASE, " +
			"POSTGRES_PROJECT_ID, POSTGRES_REGION, POSTGRES_INSTANCE). Using test container")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		container, err := setupPgvector(ctx)
		if err != nil {
			return cloudsqlutil.PostgresEngine{}, nil, err
		}

		pool, err := pgxpool.New(ctx, container.URI)
		if err != nil {
			return cloudsqlutil.PostgresEngine{}, nil, fmt.Errorf("failed to instantiate pgx pool: %w", err)
		}

		eng, err := cloudsqlutil.NewPostgresEngine(context.Background(),
			cloudsqlutil.WithPool(pool),
		)

		return eng, func() {
			_ = container.Terminate(ctx)
		}, err
	}

	eng, err := cloudsqlutil.NewPostgresEngine(context.Background(),
		cloudsqlutil.WithUser(username),
		cloudsqlutil.WithPassword(password),
		cloudsqlutil.WithDatabase(database),
		cloudsqlutil.WithCloudSQLInstance(projectID, region, instance),
	)
	return eng, nil, err
}

func setup() (cloudsqlutil.PostgresEngine, func(), error) {
	eng, cleanUp, err := setUpEngine()
	if err != nil {
		if cleanUp != nil {
			cleanUp()
		}
		return cloudsqlutil.PostgresEngine{}, func() {}, fmt.Errorf("failed to instantiate pgx pool: %w", err)
	}

	return eng, func() {
		eng.Close()
		if cleanUp != nil {
			cleanUp()
		}
	}, nil
}

func TestNewDocumentLoader_Fail(t *testing.T) {
	t.Parallel()
	testEngine, teardown, err := setup()
	require.NoError(t, err)
	t.Cleanup(teardown)

	createTable(t, testEngine)

	tests := []struct {
		name              string
		setDocumentLoader func() (*DocumentLoader, error)
		want              *DocumentLoader
		validateFunc      func(t *testing.T, d *DocumentLoader, err error)
	}{
		{
			name: "invalid engine",
			setDocumentLoader: func() (*DocumentLoader, error) {
				return NewDocumentLoader(context.Background(), []DocumentLoaderOption{})
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, d)
				assert.EqualError(t, err, "engine.Pool must be specified")
			},
		},
		{
			name: "invalid query",
			setDocumentLoader: func() (*DocumentLoader, error) {
				return NewDocumentLoader(context.Background(), []DocumentLoaderOption{WithEngine(testEngine), WithQuery("SELECT FROM table")})
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, d)
				assert.EqualError(t, err, "query is not valid")
			},
		},
		{
			name: "table does not exist",
			setDocumentLoader: func() (*DocumentLoader, error) {
				return NewDocumentLoader(context.Background(), []DocumentLoaderOption{WithEngine(testEngine), WithTableName("invalidtable")})
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, d)
				assert.ErrorContains(t, err, `failed to execute query: ERROR: relation "public.invalidtable" does not exist`)
			},
		},
		{
			name: "invalid  metadata JSON column (using default)",
			setDocumentLoader: func() (*DocumentLoader, error) {
				return NewDocumentLoader(context.Background(), []DocumentLoaderOption{WithEngine(testEngine), WithTableName("testtable")})
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, d)
				assert.ErrorContains(t, err, "metadata JSON column 'langchain_metadata' not found in query result")
			},
		},
		{
			name: "invalid column name for content",
			setDocumentLoader: func() (*DocumentLoader, error) {
				return NewDocumentLoader(context.Background(), []DocumentLoaderOption{WithEngine(testEngine), WithTableName("testtable"), WithMetadataJSONColumn("c_json_metadata"), WithContentColumns([]string{"c_invalid"})})
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, d)
				assert.ErrorContains(t, err, "column 'c_invalid' not found in query result")
			},
		},
		{
			name: "invalid column name for metadata",
			setDocumentLoader: func() (*DocumentLoader, error) {
				return NewDocumentLoader(context.Background(), []DocumentLoaderOption{WithEngine(testEngine), WithTableName("testtable"), WithMetadataJSONColumn("c_json_metadata"), WithMetadataColumns([]string{"c_invalid"})})
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				t.Helper()
				assert.Nil(t, d)
				assert.ErrorContains(t, err, "column 'c_invalid' not found in query result")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.setDocumentLoader()
			tt.validateFunc(t, got, err)
		})
	}
}

func TestNewDocumentLoader_Success(t *testing.T) {
	t.Parallel()
	testEngine, teardown, err := setup()
	require.NoError(t, err)
	t.Cleanup(teardown)

	createTable(t, testEngine)

	tests := []struct {
		name              string
		setDocumentLoader func() (*DocumentLoader, error)
		want              *DocumentLoader
		validateFunc      func(t *testing.T, d *DocumentLoader, err error)
	}{
		{
			name: "success without content column",
			setDocumentLoader: func() (*DocumentLoader, error) {
				return NewDocumentLoader(context.Background(), []DocumentLoaderOption{WithEngine(testEngine), WithTableName("testtable"),
					WithMetadataJSONColumn("c_json_metadata")})
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				t.Helper()
				require.NoError(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, d.engine, testEngine)
				assert.Equal(t, d.query, "SELECT * FROM public.testtable")
				assert.Equal(t, d.tableName, "testtable")
				assert.Equal(t, d.schemaName, "public")
				assert.Equal(t, d.contentColumns, []string{"c_id"})
				assert.Equal(t, d.metadataColumns, []string{"c_content", "c_embedding", "c_session", "c_user", "c_date", "c_active", "c_json_metadata"})
				assert.Equal(t, d.metadataJSONColumn, "c_json_metadata")
			},
		},
		{
			name: "success with content column",
			setDocumentLoader: func() (*DocumentLoader, error) {
				return NewDocumentLoader(context.Background(), []DocumentLoaderOption{WithEngine(testEngine), WithTableName("testtable"),
					WithMetadataJSONColumn("c_json_metadata"),
					WithContentColumns([]string{"c_content"})})
			},
			validateFunc: func(t *testing.T, d *DocumentLoader, err error) {
				t.Helper()
				require.NoError(t, err)
				assert.NotNil(t, d)
				assert.Equal(t, d.engine, testEngine)
				assert.Equal(t, d.query, "SELECT * FROM public.testtable")
				assert.Equal(t, d.tableName, "testtable")
				assert.Equal(t, d.schemaName, "public")
				assert.Equal(t, d.contentColumns, []string{"c_content"})
				assert.Equal(t, d.metadataColumns, []string{"c_id", "c_embedding", "c_session", "c_user", "c_date", "c_active", "c_json_metadata"})
				assert.Equal(t, d.metadataJSONColumn, "c_json_metadata")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.setDocumentLoader()
			tt.validateFunc(t, got, err)
		})
	}
}

func TestDocumentLoader_Load(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	testEngine, teardown, err := setup()
	require.NoError(t, err)
	t.Cleanup(teardown)

	createTable(t, testEngine)
	insertRows(t, testEngine)

	options := []DocumentLoaderOption{
		WithEngine(testEngine),
		WithTableName("testtable"),
		WithSchemaName("public"),
		WithMetadataColumns([]string{"c_id", "c_date", "c_user", "c_session"}),
		WithMetadataJSONColumn("c_json_metadata"),
		WithFormatter(jsonFormatter),
		WithQuery("SELECT * FROM public.testtable WHERE c_session = 100"),
	}

	l, err := NewDocumentLoader(ctx, options)
	require.NoError(t, err)
	d, err := l.Load(ctx)
	require.NoError(t, err)
	require.Len(t, d, 1)
	require.Len(t, d[0].Metadata, 5)
	assert.Equal(t, "user1", d[0].Metadata["c_user"])
	assert.Equal(t, int64(100), d[0].Metadata["c_session"])
}

func TestDocumentLoader_LoadAndSplit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	testEngine, teardown, err := setup()
	require.NoError(t, err)
	t.Cleanup(teardown)

	createTable(t, testEngine)
	insertRows(t, testEngine)

	options := []DocumentLoaderOption{
		WithEngine(testEngine),
		WithTableName("testtable"),
		WithSchemaName("public"),
		WithMetadataColumns([]string{"c_id", "c_date", "c_user", "c_session"}),
		WithMetadataJSONColumn("c_json_metadata"),
		WithFormatter(jsonFormatter),
		WithQuery("SELECT * FROM public.testtable WHERE c_session = 100"),
	}

	l, err := NewDocumentLoader(ctx, options)
	require.NoError(t, err)
	d, err := l.LoadAndSplit(ctx, nil)
	require.NoError(t, err)
	require.Len(t, d, 1)
	require.Len(t, d[0].Metadata, 5)
	assert.Equal(t, "user1", d[0].Metadata["c_user"])
	assert.Equal(t, int64(100), d[0].Metadata["c_session"])
}

func createTable(t *testing.T, testEngine cloudsqlutil.PostgresEngine) {
	t.Helper()

	err := testEngine.InitVectorstoreTable(context.Background(), cloudsqlutil.VectorstoreTableOptions{
		TableName:          "testtable",
		VectorSize:         3,
		SchemaName:         "public",
		ContentColumnName:  "c_content",
		EmbeddingColumn:    "c_embedding",
		MetadataJSONColumn: "c_json_metadata",
		IDColumn: cloudsqlutil.Column{
			Name:     "c_id",
			Nullable: false,
		},
		MetadataColumns: []cloudsqlutil.Column{
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

func insertRows(t *testing.T, testEngine cloudsqlutil.PostgresEngine) {
	t.Helper()
	_, err := testEngine.Pool.Exec(context.Background(),
		`INSERT INTO public.testtable(c_id,c_embedding,c_session,c_user,c_date,c_content, c_json_metadata)
			 VALUES ('ef0f712a-472a-4477-825d-6f3738659f31','[3.0,1.4,2.9]', 100, 'user1', '2025-02-12', 'somecontent', '{"somefield": "somevalue"}' ),
			        ('352c5ae2-feb3-47ad-a32c-306963e5bfaf','[2.7,0.4,1.8]', 200, 'user2', '2024-02-12', 'someothercontent','{"somefield": "anothervalue"}')`)
	require.NoError(t, err)
}
