package cloudsql

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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

func preCheckEnvSetting(t *testing.T) string {
	t.Helper()

	if openaiKey := os.Getenv("OPENAI_API_KEY"); openaiKey == "" {
		t.Skip("OPENAI_API_KEY not set")
	}

	pgvectorURL := os.Getenv("PGVECTOR_CONNECTION_STRING")
	if pgvectorURL == "" {
		pgVectorContainer, err := tcpostgres.RunContainer(
			context.Background(),
			testcontainers.WithImage("docker.io/pgvector/pgvector:pg16"),
			tcpostgres.WithDatabase("db_test"),
			tcpostgres.WithUsername("user"),
			tcpostgres.WithPassword("passw0rd!"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(30*time.Second)),
		)
		if err != nil && strings.Contains(err.Error(), "Cannot connect to the Docker daemon") {
			t.Skip("Docker not available")
		}
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, pgVectorContainer.Terminate(context.Background()))
		})

		str, err := pgVectorContainer.ConnectionString(context.Background(), "sslmode=disable")
		require.NoError(t, err)

		pgvectorURL = str
	}

	return pgvectorURL
}

func setEngineWithImage(ctx context.Context, t *testing.T) (cloudsqlutil.PostgresEngine, error) {
	t.Helper()
	pgvectorURL := preCheckEnvSetting(t)
	myPool, err := pgxpool.New(ctx, pgvectorURL)
	if err != nil {
		t.Fatal("Could not set Engine: ", err)
	}
	// Call NewPostgresEngine to initialize the database connection
	pgEngine, err := cloudsqlutil.NewPostgresEngine(ctx,
		cloudsqlutil.WithPool(myPool),
	)
	if err != nil {
		t.Fatal("Could not set Engine: ", err)
	}

	return pgEngine, err
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
	engine, err := setEngineWithImage(ctx, t)
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
