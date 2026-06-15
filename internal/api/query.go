package api

import (
	"encoding/json"
	"fmt"
)

// QueryResultRequest payload enviado ao POST /agent/{key}/query-result.
type QueryResultRequest struct {
	CommandID       int                      `json:"command_id"`
	Columns         []string                 `json:"columns,omitempty"`
	Rows            []map[string]interface{} `json:"rows,omitempty"`
	RowCount        int                      `json:"row_count"`
	ExecutionTimeMs float64                  `json:"execution_time_ms"`
	Truncated       bool                     `json:"truncated"`
	MaxRows         int                      `json:"max_rows"`
	Error           string                   `json:"error,omitempty"`
	Cancelled       bool                     `json:"cancelled,omitempty"`
}

// PendingQueriesResponse payload recebido de GET /agent/{key}/pending-queries.
type PendingQueriesResponse struct {
	PendingQuery  *PendingQuery `json:"pending_query"`
	Cancellations []int         `json:"cancellations,omitempty"`
}

// GetPendingQueries busca queries pendentes na API (polling rapido).
// Retorna a query pendente (se houver) e os IDs de commands a cancelar.
func (c *Client) GetPendingQueries() (*PendingQuery, []int, error) {
	path := fmt.Sprintf("/agent/%s/pending-queries", c.agentKey)
	resp, err := c.doSigned("GET", path, nil)
	if err != nil {
		return nil, nil, err
	}

	var result PendingQueriesResponse
	if err := parseResponse(resp, &result); err != nil {
		return nil, nil, err
	}

	return result.PendingQuery, result.Cancellations, nil
}

// PushQueryResult envia o resultado de uma query executada localmente para a API.
func (c *Client) PushQueryResult(req *QueryResultRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("erro ao serializar query result: %w", err)
	}

	path := fmt.Sprintf("/agent/%s/query-result", c.agentKey)
	// Retry: o report de conclusao do comando precisa chegar, senao o comando
	// fica "running" ate expirar (10min). Tolera 429 transitorio durante um sync.
	resp, err := c.doSignedRetry("POST", path, body, 6)
	if err != nil {
		return err
	}

	return parseResponse(resp, nil)
}
