package cloudsql

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/util/cloudsqlutil"
)

type chatMsg struct{}

func (chatMsg) GetType() llms.ChatMessageType {
	return llms.ChatMessageTypeHuman
}

func (chatMsg) GetContent() string {
	return "test content"
}

func getEnvVariables(t *testing.T) (string, string, string, string, string, string) {
	t.Helper()

	username := os.Getenv("POSTGRES_USERNAME")
	if username == "" {
		t.Skip("POSTGRES_USERNAME environment variable not set")
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		t.Skip("POSTGRES_PASSWORD environment variable not set")
	}
	database := os.Getenv("POSTGRES_DATABASE")
	if database == "" {
		t.Skip("POSTGRES_DATABASE environment variable not set")
	}
	projectID := os.Getenv("POSTGRES_PROJECT_ID")
	if projectID == "" {
		t.Skip("POSTGRES_PROJECT_ID environment variable not set")
	}
	region := os.Getenv("POSTGRES_REGION")
	if region == "" {
		t.Skip("POSTGRES_REGION environment variable not set")
	}
	instance := os.Getenv("POSTGRES_INSTANCE")
	if instance == "" {
		t.Skip("POSTGRES_INSTANCE environment variable not set")
	}

	return username, password, database, projectID, region, instance
}

func setEngine(ctx context.Context, t *testing.T) (cloudsqlutil.PostgresEngine, error) {
	t.Helper()
	username, password, database, projectID, region, instance := getEnvVariables(t)

	pgEngine, err := cloudsqlutil.NewPostgresEngine(ctx,
		cloudsqlutil.WithUser(username),
		cloudsqlutil.WithPassword(password),
		cloudsqlutil.WithDatabase(database),
		cloudsqlutil.WithCloudSQLInstance(projectID, region, instance),
	)

	return pgEngine, err
}

func initChatHistoryTable(ctx context.Context, t *testing.T, engine cloudsqlutil.PostgresEngine, tableName string) {
	t.Helper()
	if tableName == "" {
		t.Fatalf("table name must be provided")
	}
	err := engine.InitChatHistoryTable(ctx, tableName)
	if err != nil {
		t.Fatalf("error initializing table: %v", err)
	}
}

func assertError(t *testing.T, err error, expectedError string) {
	t.Helper()
	if (err == nil && expectedError != "") || (err != nil && !strings.Contains(err.Error(), expectedError)) {
		t.Fatalf("unexpected error: got %v, want %v", err, expectedError)
	}
}

func TestValidateTable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	engine, err := setEngine(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		engine.Close()
	})
	tcs := []struct {
		desc      string
		tableName string
		sessionID string
		err       string
	}{
		{
			desc:      "Successful creation of Chat Message History",
			tableName: "chatItems",
			sessionID: "cloudSQLSession",
			err:       "",
		},
		{
			desc:      "Creation of Chat Message History with missing table",
			tableName: "",
			sessionID: "cloudSQLSession",
			err:       "table name must be provided",
		},
		{
			desc:      "Creation of Chat Message History with missing session ID",
			tableName: "chatCloudSQLItems",
			sessionID: "",
			err:       "session ID must be provided",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			err = engine.InitChatHistoryTable(ctx, tc.tableName)
			if err != nil {
				t.Fatal("Failed to create chat msg table", err)
			}
			initChatHistoryTable(ctx, t, engine, tc.tableName)
			chatMsgHistory, err := NewChatMessageHistory(ctx, engine, tc.tableName, tc.sessionID)
			assertError(t, err, tc.err)

			// if the chat message history was created successfully, continue with the other methods tests
			if err := chatMsgHistory.AddMessage(ctx, chatMsg{}); err != nil {
				t.Fatal(err)
			}
			if err := chatMsgHistory.AddAIMessage(ctx, "AI message"); err != nil {
				t.Fatal(err)
			}
			if err := chatMsgHistory.AddUserMessage(ctx, "user message"); err != nil {
				t.Fatal(err)
			}
			if err := chatMsgHistory.Clear(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
