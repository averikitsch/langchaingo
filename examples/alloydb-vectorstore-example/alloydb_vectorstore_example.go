package main

import (
	"fmt"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/internal/alloydbutil"
	"github.com/tmc/langchaingo/llms/openai"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores/alloydb"
	"log"
	"os"
)

func getEnvVariables() (string, string, string, string, string, string, string, string) {
	// Requires environment variable ALLOYDB_USERNAME to be set.
	username := os.Getenv("ALLOYDB_USERNAME")
	// Requires environment variable ALLOYDB_PASSWORD to be set.
	password := os.Getenv("ALLOYDB_PASSWORD")
	// Requires environment variable ALLOYDB_DATABASE to be set.
	database := os.Getenv("ALLOYDB_DATABASE")
	// Requires environment variable ALLOYDB_PROJECT_ID to be set.
	projectID := os.Getenv("ALLOYDB_PROJECT_ID")
	// Requires environment variable ALLOYDB_REGION to be set.
	region := os.Getenv("ALLOYDB_REGION")
	// Requires environment variable ALLOYDB_INSTANCE to be set.
	instance := os.Getenv("ALLOYDB_INSTANCE")
	// Requires environment variable ALLOYDB_CLUSTER to be set.
	cluster := os.Getenv("ALLOYDB_CLUSTER")
	// Requires environment variable ALLOYDB_TABLE to be set.
	table := os.Getenv("ALLOYDB_TABLE")

	return username, password, database, projectID, region, instance, cluster, table
}

func main() {
	// Requires the Environment variables to be set as indicated in the getEnvVariables function.
	username, password, database, projectID, region, instance, cluster, table := getEnvVariables()

	pgEngine, err := alloydbutil.NewPostgresEngine(ctx,
		alloydbutil.WithUser(username),
		alloydbutil.WithPassword(password),
		alloydbutil.WithDatabase(database),
		alloydbutil.WithAlloyDBInstance(projectID, region, cluster, instance),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create an embeddings client using the OpenAI API. Requires environment variable OPENAI_API_KEY to be set.
	llm, err := openai.New()
	if err != nil {
		log.Fatal(err)
	}

	e, err := embeddings.NewEmbedder(llm)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new alloydb vectorstore .
	ctx := context.Background()
	vs, err := alloydb.NewVectorStore(ctx, pgEngine, e, table)

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
		log.Fatal(err)
	}

	docs, err := vs.SimilaritySearch(ctx, "Japan")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Docs:", docs)
}
