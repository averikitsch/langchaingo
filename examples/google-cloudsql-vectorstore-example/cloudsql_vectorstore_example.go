package main

import (
	"context"
	"fmt"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/googleai"
	"github.com/tmc/langchaingo/llms/googleai/vertex"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/util/cloudsqlutil"
	"github.com/tmc/langchaingo/vectorstores"
	"github.com/tmc/langchaingo/vectorstores/cloudsql"
	"log"
	"os"
)

func getEnvVariables() (string, string, string, string, string, string, string, string) {
	// Requires environment variable POSTGRES_USERNAME to be set.
	username := os.Getenv("POSTGRES_USERNAME")
	if username == "" {
		log.Fatal("env variable POSTGRES_USERNAME is empty")
	}
	// Requires environment variable POSTGRES_PASSWORD to be set.
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		log.Fatal("env variable POSTGRES_PASSWORD is empty")
	}
	// Requires environment variable POSTGRES_DATABASE to be set.
	database := os.Getenv("POSTGRES_DATABASE")
	if database == "" {
		log.Fatal("env variable POSTGRES_DATABASE is empty")
	}
	// Requires environment variable PROJECT_ID to be set.
	projectID := os.Getenv("PROJECT_ID")
	if projectID == "" {
		log.Fatal("env variable PROJECT_ID is empty")
	}
	// Requires environment variable POSTGRES_REGION to be set.
	region := os.Getenv("POSTGRES_REGION")
	if region == "" {
		log.Fatal("env variable POSTGRES_REGION is empty")
	}
	// Requires environment variable POSTGRES_INSTANCE to be set.
	instance := os.Getenv("POSTGRES_INSTANCE")
	if instance == "" {
		log.Fatal("env variable POSTGRES_INSTANCE is empty")
	}
	// Requires environment variable POSTGRES_TABLE to be set.
	table := os.Getenv("POSTGRES_TABLE")
	if table == "" {
		log.Fatal("env variable POSTGRES_TABLE is empty")
	}

	// Requires environment variable GOOGLE_CLOUD_LOCATION to be set.
	location := os.Getenv("GOOGLE_CLOUD_LOCATION")
	if location == "" {
		log.Fatal("env variable GOOGLE_CLOUD_LOCATION is empty")
	}

	return username, password, database, projectID, region, instance, table, location
}

func initializeTable(ctx context.Context, pgEngine cloudsqlutil.PostgresEngine, table string) error {
	// Initialize table for the Vectorstore to use. You only need to do this the first time you use this table.
	vectorstoreTableoptions := cloudsqlutil.VectorstoreTableOptions{
		TableName:         table,
		VectorSize:        768,
		StoreMetadata:     true,
		OverwriteExisting: true,
		MetadataColumns: []cloudsqlutil.Column{
			{
				Name:     "area",
				DataType: "int",
			},
			{
				Name:     "population",
				DataType: "int",
			},
		},
	}

	return pgEngine.InitVectorstoreTable(ctx, vectorstoreTableoptions)
}

func initializeEmbeddings(ctx context.Context, projectID, cloudLocation string) (*embeddings.EmbedderImpl, error) {
	// Initialize VertexAI LLM
	llm, err := vertex.New(ctx, googleai.WithCloudProject(projectID), googleai.WithCloudLocation(cloudLocation), googleai.WithDefaultModel("text-embedding-005"))
	if err != nil {
		log.Panic(err)
	}

	return embeddings.NewEmbedder(llm)
}

func main() {
	// Requires the Environment variables to be set as indicated in the getEnvVariables function.
	username, password, database, projectID, region, instance, table, cloudLocation := getEnvVariables()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgEngine, err := cloudsqlutil.NewPostgresEngine(ctx,
		cloudsqlutil.WithUser(username),
		cloudsqlutil.WithPassword(password),
		cloudsqlutil.WithDatabase(database),
		cloudsqlutil.WithCloudSQLInstance(projectID, region, instance),
		cloudsqlutil.WithIPType("PUBLIC"),
	)
	if err != nil {
		log.Panic(err)
	}

	err = initializeTable(ctx, pgEngine, cloudLocation)
	if err != nil {
		log.Panic(err)
	}
	e, err := initializeEmbeddings(ctx, projectID, cloudLocation)
	if err != nil {
		log.Panic(err)
	}

	// Create a new Vectorstore
	vs, err := cloudsql.NewVectorStore(pgEngine, e, table, cloudsql.WithMetadataColumns([]string{"area", "population"}))
	if err != nil {
		log.Panic(err)
	}

	_, err = vs.AddDocuments(ctx, []schema.Document{
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
		log.Panic(err)
	}
	similaritySearchesCalls(ctx, vs)
}

func similaritySearchesCalls(ctx context.Context, vs cloudsql.VectorStore) {
	docs, err := vs.SimilaritySearch(ctx, "Japan", 0)
	if err != nil {
		log.Panic(err)
	}

	fmt.Println("Docs:", docs)
	filter := "\"area\" > 1500"
	filteredDocs, err := vs.SimilaritySearch(ctx, "Japan", 0, vectorstores.WithFilters(filter))
	if err != nil {
		log.Panic(err)
	}
	fmt.Println("FilteredDocs:", filteredDocs)
}
