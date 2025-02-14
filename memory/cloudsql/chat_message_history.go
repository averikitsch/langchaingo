package cloudsql

import (
	"context"
	"errors"
	"fmt"

	"github.com/tmc/langchaingo/internal/cloudsqlutil"
)

type ChatMessageHistory struct {
	engine     cloudsqlutil.PostgresEngine
	sessionID  string
	tableName  string
	schemaName string
	overwrite  bool
}

// var _ schema.ChatMessageHistory = &ChatMessageHistory{}

// NewChatMessageHistory creates a new NewChatMessageHistory with options.
func NewChatMessageHistory(ctx context.Context, engine cloudsqlutil.PostgresEngine, tableName string, sessionID string, opts ...ChatMessageHistoryStoresOption) (ChatMessageHistory, error) {
	var err error
	// Ensure required fields are set
	if engine.Pool == nil {
		return ChatMessageHistory{}, errors.New("cloudSQL engine must be provided")
	}
	if tableName == "" {
		return ChatMessageHistory{}, errors.New("table name must be provided")
	}
	if sessionID == "" {
		return ChatMessageHistory{}, errors.New("session ID must be provided")
	}
	cmh := ChatMessageHistory{
		engine:    engine,
		tableName: tableName,
		sessionID: sessionID,
	}
	cmh, err = applyChatMessageHistoryOptions(cmh, opts...)
	if err != nil {
		return ChatMessageHistory{}, fmt.Errorf("unable to apply provided options for chat message history: %w", err)
	}
	err = cmh.validateTable(ctx)
	if err != nil {
		return ChatMessageHistory{}, fmt.Errorf("error validating table '%s' in schema '%s': %w", tableName, cmh.schemaName, err)
	}
	return cmh, nil
}

// validateTable validates if a table with a specific schema exist and it
// contains the required columns.
func (c *ChatMessageHistory) validateTable(ctx context.Context) error {
	tableExistsQuery := fmt.Sprintf(`SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = '%s' AND table_name = '%s');`,
		c.schemaName, c.tableName)
	var exists bool
	err := c.engine.Pool.QueryRow(ctx, tableExistsQuery).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error validating the existance of table '%s' in schema '%s': %w", c.tableName, c.schemaName, err)
	}
	if !exists {
		return fmt.Errorf("table '%s' does not exist in schema '%s'", c.tableName, c.schemaName)
	}

	// Required columns with their types
	requiredColumns := map[string]string{
		"id":         "integer",
		"session_id": "text",
		"data":       "json",
		"type":       "text",
	}

	var columns = make(map[string]string)

	// Get the columns from the table
	columnsQuery := fmt.Sprintf(`
    	SELECT column_name, data_type
    	FROM information_schema.columns
   	 	WHERE table_schema = '%s' AND table_name = '%s';`, c.schemaName, c.tableName)

	rows, err := c.engine.Pool.Query(ctx, columnsQuery)
	if err != nil {
		return fmt.Errorf("error fetching columns from table '%s' in schema '%s': %w", c.tableName, c.schemaName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return fmt.Errorf("error scanning column names from table '%s' in schema '%s': %w", c.tableName, c.schemaName, err)
		}
		columns[columnName] = dataType
	}

	// Validate column names and types
	for reqColumn, expectedType := range requiredColumns {
		actualType, found := columns[reqColumn]
		if !found {
			return fmt.Errorf("error, column '%s' is missing in table '%s'. Expected columns: %v", reqColumn, c.tableName, requiredColumns)
		}
		if actualType != expectedType {
			return fmt.Errorf("error, column '%s' in table '%s' has type '%s', but expected type '%s'", reqColumn, c.tableName, actualType, expectedType)
		}
	}
	return nil
}
