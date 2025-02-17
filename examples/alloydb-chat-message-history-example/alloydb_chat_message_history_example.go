package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/tmc/langchaingo/internal/alloydbutil"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/memory/alloydb"
)

func getEnvVariables() (string, string, string, string, string, string, string, string, string, string) {
	// Requires environment variable ALLOYDB_USERNAME to be set.
	username := os.Getenv("ALLOYDB_USERNAME")
	if username == "" {
		log.Fatal("env variable ALLOYDB_USERNAME is empty")
	}
	// Requires environment variable ALLOYDB_PASSWORD to be set.
	password := os.Getenv("ALLOYDB_PASSWORD")
	if password == "" {
		log.Fatal("env variable ALLOYDB_PASSWORD is empty")
	}
	// Requires environment variable ALLOYDB_DATABASE to be set.
	database := os.Getenv("ALLOYDB_DATABASE")
	if database == "" {
		log.Fatal("env variable ALLOYDB_DATABASE is empty")
	}
	// Requires environment variable ALLOYDB_PROJECT_ID to be set.
	projectID := os.Getenv("ALLOYDB_PROJECT_ID")
	if projectID == "" {
		log.Fatal("env variable ALLOYDB_PROJECT_ID is empty")
	}
	// Requires environment variable ALLOYDB_REGION to be set.
	region := os.Getenv("ALLOYDB_REGION")
	if region == "" {
		log.Fatal("env variable ALLOYDB_REGION is empty")
	}
	// Requires environment variable ALLOYDB_INSTANCE to be set.
	instance := os.Getenv("ALLOYDB_INSTANCE")
	if instance == "" {
		log.Fatal("env variable ALLOYDB_INSTANCE is empty")
	}
	// Requires environment variable ALLOYDB_CLUSTER to be set.
	cluster := os.Getenv("ALLOYDB_CLUSTER")
	if cluster == "" {
		log.Fatal("env variable ALLOYDB_CLUSTER is empty")
	}
	// Requires environment variable ALLOYDB_TABLE to be set.
	tableName := os.Getenv("ALLOYDB_TABLE")
	if tableName == "" {
		log.Fatal("env variable ALLOYDB_TABLE is empty")
	}
	// Requires environment variable ALLOYDB_SESSION_ID to be set.
	sessionID := os.Getenv("ALLOYDB_SESSION_ID")
	if sessionID == "" {
		log.Fatal("env variable ALLOYDB_SESSION_ID is empty")
	}
	// Requires environment variable ALLOYDB_SCHEMA to be set.
	schemaName := os.Getenv("ALLOYDB_SCHEMA")
	if schemaName == "" {
		log.Fatal("env variable ALLOYDB_SCHEMA is empty")
	}

	return username, password, database, projectID, region, instance, cluster, tableName, sessionID, schemaName
}

func printMessages(ctx context.Context, cmh alloydb.ChatMessageHistory) {
	msgs, err := cmh.Messages(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, msg := range msgs {
		fmt.Println("Message:", msg)
	}
}

func main() {
	// Requires the Environment variables to be set as indicated in the getEnvVariables function.
	username, password, database, projectID, region, instance, cluster, tableName, sessionID, schemaName := getEnvVariables()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pgEngine, err := alloydbutil.NewPostgresEngine(ctx,
		alloydbutil.WithUser(username),
		alloydbutil.WithPassword(password),
		alloydbutil.WithDatabase(database),
		alloydbutil.WithAlloyDBInstance(projectID, region, cluster, instance),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize ChatHistoryTable table using InitChatHistoryTable method
	err = pgEngine.InitChatHistoryTable(ctx, tableName, schemaName)
	if err != nil {
		log.Fatal(err)
	}

	cmh, err := alloydb.NewChatMessageHistory(ctx, *pgEngine, tableName, sessionID, alloydb.WithSchemaName(schemaName), alloydb.WithOverwrite())
	if err != nil {
		log.Fatal(err)
	}

	aiMessage := llms.AIChatMessage{Content: "test AI message"}
	humanMessage := llms.HumanChatMessage{Content: "test HUMAN message"}

	err = cmh.AddUserMessage(ctx, string(aiMessage.GetContent()))
	if err != nil {
		log.Fatal(err)
	}

	err = cmh.AddUserMessage(ctx, string(humanMessage.GetContent()))
	if err != nil {
		log.Fatal(err)
	}

	printMessages(ctx, cmh)

	multipleMessages := []llms.ChatMessage{
		llms.AIChatMessage{Content: "first AI test message from AddMessages"},
		llms.AIChatMessage{Content: "second AI test message from AddMessages"},
		llms.HumanChatMessage{Content: "first HUMAN test message from AddMessages"},
	}

	err = cmh.AddMessages(ctx, multipleMessages)
	if err != nil {
		log.Fatal(err)
	}

	printMessages(ctx, cmh)

	overWrittingMessages := []llms.ChatMessage{
		llms.AIChatMessage{Content: "overwrited AI test message"},
		llms.HumanChatMessage{Content: "overwrited HUMAN test message"},
	}

	err = cmh.SetMessages(ctx, overWrittingMessages)
	if err != nil {
		log.Fatal(err)
	}

	printMessages(ctx, cmh)

	err = cmh.Clear(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
