package alloydb_test

import (
	"context"
	"github.com/tmc/langchaingo/internal/alloydbutil"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/memory/alloydb"
	"os"
	"testing"
)

type chatMsg struct{}

func (c chatMsg) GetType() llms.ChatMessageType {
	return llms.ChatMessageTypeHuman
}

func (c chatMsg) GetContent() string {
	return "test content"
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

func TestNewChatMessageHistory(t *testing.T) {
	engine, err := setEngine(t)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	ctx := context.Background()
	_, err = alloydb.NewChatMessageHistory(ctx, engine, "items", "session")
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateTable(t *testing.T) {
	engine, err := setEngine(t)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	ctx := context.Background()
	chatMsgHistory, err := alloydb.NewChatMessageHistory(ctx, engine, "items", "session")
	if err != nil {
		t.Fatal(err)
	}

	err = chatMsgHistory.AddMessage(ctx, chatMsg{})
	if err != nil {
		t.Fatal(err)
	}
	err = chatMsgHistory.AddAIMessage(ctx, "AI message")
	if err != nil {
		t.Fatal(err)
	}
	err = chatMsgHistory.AddUserMessage(ctx, "user message")
	if err != nil {
		t.Fatal(err)
	}
	err = chatMsgHistory.Clear(ctx)
	if err != nil {
		t.Fatal(err)
	}
}
