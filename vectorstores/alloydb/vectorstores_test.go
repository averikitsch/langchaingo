package alloydb_test

import (
	"context"
	"fmt"
	"github.com/tmc/langchaingo/internal/alloydbutil"
	"github.com/tmc/langchaingo/vectorstores/alloydb"
	"os"
	"testing"
)

type TestEmbedder struct {
}

func (de TestEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([][]float32, error) {
	return [][]float32{}, nil
}
func (de TestEmbedder) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	return []float32{}, nil
}

func getEnvVariables(t *testing.T) (string, string, string, string, string, string, string) {
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

	return username, password, database, projectID, region, instance, cluster
}

func setEngine(t *testing.T) (alloydbutil.PostgresEngine, error) {
	os.Setenv("ALLOYDB_USERNAME", "postgres")
	os.Setenv("ALLOYDB_PASSWORD", "alloydbtest")
	os.Setenv("ALLOYDB_DATABASE", "postgres")
	os.Setenv("ALLOYDB_PROJECT_ID", "devshop-mosaic-11010494")
	os.Setenv("ALLOYDB_REGION", "us-central1")
	os.Setenv("ALLOYDB_INSTANCE", "senseai-alloydb-cluster-primary")
	os.Setenv("ALLOYDB_CLUSTER", "senseai-alloydb-cluster")
	username, password, database, projectID, region, instance, cluster := getEnvVariables(t)
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

	return *pgEngine, nil
}

func setVectoreStore(t *testing.T) (alloydb.VectorStore, error) {
	pgEngine, err := setEngine(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	vs, err := alloydb.NewVectorStore(ctx, pgEngine, TestEmbedder{}, "items")
	if err != nil {
		t.Fatal(err)
	}
	return vs, nil
}

func TestPingToDB(t *testing.T) {
	engine, err := setEngine(t)

	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	if err = engine.Pool.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestApplyVectorIndexAndDropIndex(t *testing.T) {
	vs, err := setVectoreStore(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	idx := vs.NewBaseIndex("testindex", "hnsw", 1, []string{})
	err = vs.ApplyVectorIndex(ctx, idx, "testindex", false)
	if err != nil {
		t.Fatal(err)
	}
	err = vs.DropVectorIndex(ctx, "testindex")
	if err != nil {
		t.Fatal(err)
	}
}

func TestIsValidIndex(t *testing.T) {
	vs, err := setVectoreStore(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	idx := vs.NewBaseIndex("testindex", "hnsw", 1, []string{})
	err = vs.ApplyVectorIndex(ctx, idx, "testindex", false)
	if err != nil {
		t.Fatal(err)
	}
	isValid, err := vs.IsValidIndex(ctx, "testindex")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(isValid)
	err = vs.DropVectorIndex(ctx, "testindex")
	if err != nil {
		t.Fatal(err)
	}
}
