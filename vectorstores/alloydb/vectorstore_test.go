package alloydb_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/ollama"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/util/alloydbutil"
	"github.com/tmc/langchaingo/vectorstores/alloydb"
)

func getEnvVariables(t *testing.T) (string, string, string, string, string, string, string, string) {
	t.Helper()

	username := os.Getenv("ALLOYDB_USERNAME")
	if username == "" {
		t.Skip("ALLOYDB_USERNAME environment variable not set")
	}
	password := os.Getenv("ALLOYDB_PASSWORD")
	if password == "" {
		t.Skip("ALLOYDB_PASSWORD environment variable not set")
	}
	database := os.Getenv("ALLOYDB_DATABASE")
	if database == "" {
		t.Skip("ALLOYDB_DATABASE environment variable not set")
	}
	table := os.Getenv("ALLOYDB_TABLE")
	if table == "" {
		t.Skip("ALLOYDB_TABLE environment variable not set")
	}
	projectID := os.Getenv("ALLOYDB_PROJECT_ID")
	if projectID == "" {
		t.Skip("ALLOYDB_PROJECT_ID environment variable not set")
	}
	region := os.Getenv("ALLOYDB_REGION")
	if region == "" {
		t.Skip("ALLOYDB_REGION environment variable not set")
	}
	instance := os.Getenv("ALLOYDB_INSTANCE")
	if instance == "" {
		t.Skip("ALLOYDB_INSTANCE environment variable not set")
	}
	cluster := os.Getenv("ALLOYDB_CLUSTER")
	if cluster == "" {
		t.Skip("ALLOYDB_CLUSTER environment variable not set")
	}

	return username, password, database, projectID, region, instance, cluster, table
}

func setEngine(t *testing.T) (alloydbutil.PostgresEngine, error) {
	t.Helper()
	username, password, database, projectID, region, instance, cluster, _ := getEnvVariables(t)
	ctx := context.Background()
	pgEngine, err := alloydbutil.NewPostgresEngine(ctx,
		alloydbutil.WithUser(username),
		alloydbutil.WithPassword(password),
		alloydbutil.WithDatabase(database),
		alloydbutil.WithAlloyDBInstance(projectID, region, cluster, instance),
	)
	if err != nil {
		t.Fatal("Could not set Engine: ", err)
	}

	return *pgEngine
}

func setVectorStore(t *testing.T) (alloydb.VectorStore, func() error, error) {
	t.Helper()
	_, _, _, _, _, _, _, table := getEnvVariables(t)
	pgEngine, err := setEngine(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	vectorstoreTableoptions := alloydbutil.VectorstoreTableOptions{
		TableName:     table,
		VectorSize:    768,
		StoreMetadata: true,
	}
	err = pgEngine.InitVectorstoreTable(ctx, vectorstoreTableoptions)
	if err != nil {
		t.Fatal(err)
	}
	llmm, err := ollama.New(
		ollama.WithModel("llama3"),
	)
	if err != nil {
		t.Fatal(err)
	}
	e, err := embeddings.NewEmbedder(llmm)
	if err != nil {
		t.Fatal(err)
	}
	vs, err := alloydb.NewVectorStore(ctx, pgEngine, e, table)
	if err != nil {
		t.Fatal(err)
	}

	cleanUpTableFn := func() error {
		_, err := pgEngine.Pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
		return err
	}
	return vs, cleanUpTableFn, nil
}

func TestPingToDB(t *testing.T) {
	t.Parallel()
	engine := setEngine(t)

	defer engine.Close()

	if err := engine.Pool.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestApplyVectorIndexAndDropIndex(t *testing.T) {
	t.Parallel()
	vs, cleanUpTableFn, err := setVectorStore(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	idx := vs.NewBaseIndex("testindex", "hnsw", alloydb.CosineDistance{}, []string{}, alloydb.HNSWOptions{})
	err := vs.ApplyVectorIndex(ctx, idx, "testindex", false, false)
	if err != nil {
		t.Fatal(err)
	}
	err = vs.DropVectorIndex(ctx, "testindex", true)
	if err != nil {
		t.Fatal(err)
	}
	err = cleanUpTableFn()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIsValidIndex(t *testing.T) {
	t.Parallel()
	vs, cleanUpTableFn, err := setVectorStore(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	idx := vs.NewBaseIndex("testindex", "hnsw", alloydb.CosineDistance{}, []string{}, alloydb.HNSWOptions{})
	err := vs.ApplyVectorIndex(ctx, idx, "testindex", false, false)
	if err != nil {
		t.Fatal(err)
	}
	isValid, err := vs.IsValidIndex(ctx, "testindex")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(isValid)
	err = vs.DropVectorIndex(ctx, "testindex", true)
	if err != nil {
		t.Fatal(err)
	}
	err = cleanUpTableFn()
	if err != nil {
		t.Fatal(err)
	}
}

func TestAddDocuments(t *testing.T) {
	t.Parallel()
	vs, cleanUpTableFn, err := setVectorStore(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err := vs.AddDocuments(ctx, []schema.Document{
		{
			PageContent: "Tokyo",
			Metadata: map[string]any{
				"population": 38,
				"area":       2190,
			},
		},
		{
			PageContent: "Paris",
			Metadata: map[string]any{
				"population": 11,
				"area":       105,
			},
		},
		{
			PageContent: "London",
			Metadata: map[string]any{
				"population": 9.5,
				"area":       1572,
			},
		},
		{
			PageContent: "Santiago",
			Metadata: map[string]any{
				"population": 6.9,
				"area":       641,
			},
		},
		{
			PageContent: "Buenos Aires",
			Metadata: map[string]any{
				"population": 15.5,
				"area":       203,
			},
		},
		{
			PageContent: "Rio de Janeiro",
			Metadata: map[string]any{
				"population": 13.7,
				"area":       1200,
			},
		},
		{
			PageContent: "Sao Paulo",
			Metadata: map[string]any{
				"population": 22.6,
				"area":       1523,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = cleanUpTableFn()
	if err != nil {
		t.Fatal(err)
	}
}
