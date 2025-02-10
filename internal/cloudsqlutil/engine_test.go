package cloudsqlutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
)

type TestEngine struct {
	eg    PostgresEngine
	error error
}

func getEnvVariables(t *testing.T) (string, string, string, string, string, string) {
	t.Helper()

	username := os.Getenv("CLOUDSQL_USERNAME")
	if username == "" {
		t.Skip("CLOUDSQL_USERNAME environment variable not set")
	}
	password := os.Getenv("CLOUDSQL_PASSWORD")
	if password == "" {
		t.Skip("CLOUDSQL_PASSWORD environment variable not set")
	}
	database := os.Getenv("CLOUDSQL_DATABASE")
	if database == "" {
		t.Skip("CLOUDSQL_DATABASE environment variable not set")
	}
	projectID := os.Getenv("CLOUDSQL_PROJECT_ID")
	if projectID == "" {
		t.Skip("CLOUSQL_PROJECT_ID environment variable not set")
	}
	region := os.Getenv("CLOUDSQL_REGION")
	if region == "" {
		t.Skip("CLOUDSQL_REGION environment variable not set")
	}
	instance := os.Getenv("CLOUDSQL_INSTANCE")
	if instance == "" {
		t.Skip("CLOUDSQL_INSTANCE environment variable not set")
	}

	return username, password, database, projectID, region, instance
}

func setEngine(t *testing.T, ctx context.Context) (PostgresEngine, error) {
	username, password, database, projectID, region, instance := getEnvVariables(t)
	pgEngine, err := NewPostgresEngine(ctx,
		WithUser(username),
		WithPassword(password),
		WithDatabase(database),
		WithCloudSQLInstance(projectID, region, instance),
	)

	return *pgEngine, err
}

func TestNewPostgresEngine(t *testing.T) {
	t.Parallel()
	username, password, database, projectID, region, instance := getEnvVariables(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testEngines := []TestEngine{
		newTestEngine(ctx, WithUser(username),
			WithPassword(password),
			WithDatabase(database),
			WithCloudSQLInstance(projectID, region, instance),
		),
		newTestEngine(ctx,
			WithUser(""),
			WithPassword(""),
			WithDatabase(database),
			WithCloudSQLInstance(projectID, region, instance),
		),
		newTestEngine(ctx,
			WithUser(username),
			WithPassword(password),
			WithDatabase(database),
			WithCloudSQLInstance(projectID, region, ""),
		),
		newTestEngine(ctx,
			WithUser(username),
			WithPassword(""),
			WithDatabase(database),
			WithCloudSQLInstance(projectID, "", instance),
		),
	}

	for index, engine := range testEngines {
		t.Run(fmt.Sprintf("Engine No. %d", index+1), func(t *testing.T) {
			defer engine.eg.Close()
			t.Parallel()
			if index == 0 && engine.error == nil {
				if err := engine.eg.Pool.Ping(ctx); err != nil {
					t.Fatal("Engine was created but failed to ping the DB: ", err)
				}
			} else {
				if engine.error == nil {
					t.Errorf("Engine %s should have failed but didn't", strconv.Itoa(index))
				}
			}
		})
	}
}

func newTestEngine(ctx context.Context, opts ...Option) TestEngine {
	pgEngine, err := NewPostgresEngine(ctx, opts...)
	return TestEngine{*pgEngine, err}
}

func TestGetUser(t *testing.T) {
	t.Parallel()

	testServiceAccount := "test-service-account-email@test.com"
	// Mock EmailRetriever function for testing
	var mockEmailRetrevier = func(ctx context.Context) (string, error) {
		return testServiceAccount, nil
	}

	// A failing mock function for testing
	var mockFailingEmailRetrevier = func(ctx context.Context) (string, error) {
		return "", errors.New("missing or invalid credentials")
	}

	tests := []struct {
		name             string
		engineConfig     engineConfig
		expectedErr      string
		expectedUserName string
		expectedIamAuth  bool
	}{
		{
			name:             "User and Password provided",
			engineConfig:     engineConfig{user: "testUser", password: "testPass"},
			expectedUserName: "testUser",
			expectedIamAuth:  false,
		},
		{
			name:             "Neither User nor Password, but service account email retrieved",
			engineConfig:     engineConfig{emailRetreiver: mockEmailRetrevier},
			expectedUserName: testServiceAccount,
			expectedIamAuth:  true,
		},
		{
			name:         "Error - User provided but Password missing",
			engineConfig: engineConfig{user: "testUser", password: ""},
			expectedErr:  "unable to retrieve a valid username",
		},
		{
			name:         "Error - Password provided but User missing",
			engineConfig: engineConfig{user: "", password: "testPassword"},
			expectedErr:  "unable to retrieve a valid username",
		},
		{
			name:         "Error - Failure retrieving service account email",
			engineConfig: engineConfig{emailRetreiver: mockFailingEmailRetrevier},
			expectedErr:  "unable to retrieve service account email: missing or invalid credentials",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			username, usingIAMAuth, err := getUser(context.Background(), tc.engineConfig)

			// Check if the error matches the expected error
			if err != nil && err.Error() != tc.expectedErr {
				t.Errorf("expected error %v, got %v", tc.expectedErr, err)
			}
			// If error was expected and matched, go to next test
			if tc.expectedErr != "" {
				return
			}
			// Validate if the username matches the expected username
			if username != tc.expectedUserName {
				t.Errorf("expected user %s, got %s", tc.expectedUserName, tc.engineConfig.user)
			}
			// Validate if IamAuth was expected
			if usingIAMAuth != tc.expectedIamAuth {
				t.Errorf("expected user %s, got %s", tc.expectedUserName, tc.engineConfig.user)
			}
		})
	}
}
