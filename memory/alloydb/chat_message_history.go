package alloydb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tmc/langchaingo/internal/alloydbutil"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/schema"
)

type ChatMessageHistory struct {
	engine     alloydbutil.PostgresEngine
	sessionID  string
	tableName  string
	schemaName string
	overwrite  bool
}

var _ schema.ChatMessageHistory = &ChatMessageHistory{}

// NewChatMessageHistory creates a new NewChatMessageHistory with options.
func NewChatMessageHistory(ctx context.Context, engine alloydbutil.PostgresEngine, tableName string, sessionID string, opts ...ChatMessageHistoryStoresOption) (ChatMessageHistory, error) {
	var err error
	// Ensure required fields are set
	if engine.Pool == nil {
		return ChatMessageHistory{}, errors.New("alloyDB engine must be provided")
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
		return ChatMessageHistory{}, fmt.Errorf("applyChatMessageHistoryOptions(): %w", err)
	}
	err = cmh.validateTable(ctx)
	if err != nil {
		return ChatMessageHistory{}, fmt.Errorf("validateTable(): %w", err)
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
		return fmt.Errorf("error validating table %s: %w", c.tableName, err)
	}
	if !exists {
		return fmt.Errorf("table '%s' does not exist in schema '%s'", c.tableName, c.schemaName)
	}

	requiredColumns := []string{"id", "session_id", "data", "type"}
	for _, reqColumn := range requiredColumns {
		columnExistsQuery := fmt.Sprintf(`SELECT EXISTS (
			SELECT 1 FROM information_schema.columns 
			WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'
		);`, c.schemaName, c.tableName, reqColumn)
		var columnExists bool
		err := c.engine.Pool.QueryRow(ctx, columnExistsQuery).Scan(&columnExists)
		if err != nil {
			return fmt.Errorf("error scanning columns from table %s: %w", c.tableName, err)
		}
		if !columnExists {
			return fmt.Errorf("column '%s' is missing in table '%s'. Expected columns: %v", reqColumn, c.tableName, requiredColumns)
		}
	}
	return nil
}

// addMessage adds a new message into the ChatMessageHistory for a given
// session.
func (c *ChatMessageHistory) addMessage(ctx context.Context, content string, messageType llms.ChatMessageType) error {
	data, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to serialize content to JSON: %w", err)
	}
	query := fmt.Sprintf(`INSERT INTO "%s"."%s" (session_id, data, type) VALUES ($1, $2, $3)`,
		c.schemaName, c.tableName)

	_, err = c.engine.Pool.Exec(ctx, query, c.sessionID, data, messageType)
	if err != nil {
		return fmt.Errorf("failed to add message to database: %w", err)
	}
	return nil
}

// AddMessage adds a message to the ChatMessageHistory.
func (c *ChatMessageHistory) AddMessage(ctx context.Context, message llms.ChatMessage) error {
	return c.addMessage(ctx, message.GetContent(), message.GetType())
}

// AddAIMessage adds an AI-generated message to the ChatMessageHistory.
func (c *ChatMessageHistory) AddAIMessage(ctx context.Context, content string) error {
	return c.addMessage(ctx, content, llms.ChatMessageTypeAI)
}

// AddUserMessage adds a user-generated message to the ChatMessageHistory.
func (c *ChatMessageHistory) AddUserMessage(ctx context.Context, content string) error {
	return c.addMessage(ctx, content, llms.ChatMessageTypeHuman)
}

// Clear removes all messages associated with a session from the
// ChatMessageHistory.
func (c *ChatMessageHistory) Clear(ctx context.Context) error {
	if !c.overwrite {
		return nil
	}
	query := fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE session_id = $1`,
		c.schemaName, c.tableName)

	_, err := c.engine.Pool.Exec(ctx, query, c.sessionID)
	if err != nil {
		return fmt.Errorf("failed to clear session %s: %w", c.sessionID, err)
	}
	return err
}

// AddMessages adds multiple messages to the ChatMessageHistory for a given
// session.
func (c *ChatMessageHistory) AddMessages(ctx context.Context, messages []llms.ChatMessage) error {
	b := &pgx.Batch{}
	query := fmt.Sprintf(`INSERT INTO "%s"."%s" (session_id, data, type) VALUES ($1, $2, $3)`,
		c.schemaName, c.tableName)

	for _, message := range messages {
		b.Queue(query, c.sessionID, message.GetContent(), message.GetType())
	}
	return c.engine.Pool.SendBatch(ctx, b).Close()
}

// Messages retrieves all messages associated with a session from the
// ChatMessageHistory.
func (c *ChatMessageHistory) Messages(ctx context.Context) ([]llms.ChatMessage, error) {
	query := fmt.Sprintf(
		`SELECT id, session_id, data, type, timestamp FROM "%s"."%s" WHERE session_id = $1 ORDER BY id`,
		c.schemaName,
		c.tableName,
	)

	rows, err := c.engine.Pool.Query(ctx, query, c.sessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve messages: %w", err)
	}
	defer rows.Close()

	var messages []llms.ChatMessage
	for rows.Next() {
		var id int
		var sessionID, data, messageType string
		var timestamp time.Time
		if err := rows.Scan(&id, &sessionID, &data, &messageType, &timestamp); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Variable to hold the deserialized content
		var content string

		// Unmarshal the JSON data into the content variable
		err := json.Unmarshal([]byte(data), &content)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal data: %w", err)
		}
		switch messageType {
		case string(llms.ChatMessageTypeAI):
			messages = append(messages, llms.AIChatMessage{Content: content})
		case string(llms.ChatMessageTypeHuman):
			messages = append(messages, llms.HumanChatMessage{Content: content})
		case string(llms.ChatMessageTypeSystem):
			messages = append(messages, llms.SystemChatMessage{Content: content})
		default:
			return nil, fmt.Errorf("unsupported message type: %s", messageType)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over rows: %w", err)
	}

	return messages, nil
}

// SetMessages clears the current messages from the ChatMessageHistory for a
// given session and then adds new messages to it.
func (c *ChatMessageHistory) SetMessages(ctx context.Context, messages []llms.ChatMessage) error {
	if !c.overwrite {
		return nil
	}
	err := c.Clear(ctx)
	if err != nil {
		return err
	}

	b := &pgx.Batch{}
	query := fmt.Sprintf(`INSERT INTO "%s"."%s" (session_id, data, type) VALUES ($1, $2, $3)`,
		c.schemaName, c.tableName)

	for _, message := range messages {
		data, err := json.Marshal(message.GetContent())
		if err != nil {
			return fmt.Errorf("failed to serialize content to JSON: %w", err)
		}
		b.Queue(query, c.sessionID, data, message.GetType())
	}
	return c.engine.Pool.SendBatch(ctx, b).Close()
}
