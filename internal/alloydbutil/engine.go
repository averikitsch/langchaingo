package alloydbutil

import (
	"context"
	"errors"
	"fmt"
	"net"

	"cloud.google.com/go/alloydbconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/oauth2/v2"
	"google.golang.org/api/option"
)

type EmailRetriever func(context.Context) (string, error)

type PostgresEngine struct {
	Pool *pgxpool.Pool
}

// NewPostgresEngine creates a new PostgresEngine.
func NewPostgresEngine(ctx context.Context, opts ...Option) (*PostgresEngine, error) {
	pgEngine := new(PostgresEngine)
	cfg, err := applyClientOptions(opts...)
	if err != nil {
		return nil, err
	}
	user, usingIAMAuth, err := getUser(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error assigning user. Err: %w", err)
	}
	if usingIAMAuth {
		cfg.user = user
	}
	if cfg.connPool == nil {
		cfg.connPool, err = createPool(ctx, cfg, usingIAMAuth)

		if err != nil {
			return &PostgresEngine{}, err
		}
	}
	pgEngine.Pool = cfg.connPool
	return pgEngine, nil
}

// createPool creates a connection pool to the PostgreSQL database.
func createPool(ctx context.Context, cfg engineConfig, usingIAMAuth bool) (*pgxpool.Pool, error) {
	dialeropts := []alloydbconn.Option{}
	dsn := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", cfg.user, cfg.password, cfg.database)
	if usingIAMAuth {
		dialeropts = append(dialeropts, alloydbconn.WithIAMAuthN())
		dsn = fmt.Sprintf("user=%s dbname=%s sslmode=disable", cfg.user, cfg.database)
	}
	d, err := alloydbconn.NewDialer(ctx, dialeropts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize connection: %w", err)
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %w", err)
	}
	instanceURI := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/instances/%s", cfg.projectID, cfg.region, cfg.cluster, cfg.instance)
	config.ConnConfig.DialFunc = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		if cfg.ipType == "PRIVATE" {
			return d.Dial(ctx, instanceURI, alloydbconn.WithPrivateIP())
		}
		return d.Dial(ctx, instanceURI, alloydbconn.WithPublicIP())
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}
	return pool, nil
}

// Close closes the connection.
func (p *PostgresEngine) Close() {
	if p.Pool != nil {
		// Close the connection pool.
		p.Pool.Close()
	}
}

// getUser retrieves the username, a flag indicating if IAM authentication
// will be used and an error.
func getUser(ctx context.Context, config engineConfig) (string, bool, error) {
	if config.user != "" && config.password != "" {
		// If both username and password are provided use provided username.
		return config.user, false, nil
	} else if config.iamAccountEmail != "" {
		// If iamAccountEmail is provided use it as user.
		return config.iamAccountEmail, true, nil
	} else if config.user == "" && config.password == "" && config.iamAccountEmail == "" {
		// If neither user and password nor iamAccountEmail are provided,
		// retrieve IAM email from the environment.
		serviceAccountEmail, err := config.emailRetreiver(ctx)
		if err != nil {
			return "", false, fmt.Errorf("unable to retrieve service account email: %w", err)
		}
		return serviceAccountEmail, true, nil
	}

	// If no user can be determined, return an error.
	return "", false, errors.New("unable to retrieve a valid username")
}

// getServiceAccountEmail retrieves the IAM principal email with users account.
func getServiceAccountEmail(ctx context.Context) (string, error) {
	scopes := []string{"https://www.googleapis.com/auth/userinfo.email"}
	// Get credentials using email scope
	credentials, err := google.FindDefaultCredentials(ctx, scopes...)
	if err != nil {
		return "", fmt.Errorf("unable to get default credentials: %w", err)
	}

	// Verify valid TokenSource.
	if credentials.TokenSource == nil {
		return "", fmt.Errorf("missing or invalid credentials")
	}

	oauth2Service, err := oauth2.NewService(ctx, option.WithTokenSource(credentials.TokenSource))
	if err != nil {
		return "", fmt.Errorf("failed to create new service: %w", err)
	}

	// Fetch IAM principal email.
	userInfo, err := oauth2Service.Userinfo.Get().Do()
	if err != nil {
		return "", fmt.Errorf("failed to get user info: %w", err)
	}
	return userInfo.Email, nil
}
