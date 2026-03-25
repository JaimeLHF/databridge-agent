package api

import (
	"encoding/json"
	"fmt"

	"github.com/prim-ideias/databridge-agent/internal/db"
)

// PushSchemaRequest payload enviado ao POST /agent/{key}/push-schema.
type PushSchemaRequest struct {
	Schema []db.SchemaTable `json:"schema"`
	Batch  int              `json:"batch"`
	Total  int              `json:"total_batches"`
}

// PushSchemaResponse payload recebido da API.
type PushSchemaResponse struct {
	Status      string `json:"status"`
	TablesCount int    `json:"tables_count"`
}

const schemaBatchSize = 50

// PushSchema envia o schema do banco local para a API em batches.
func (c *Client) PushSchema(schema []db.SchemaTable) (*PushSchemaResponse, error) {
	totalBatches := (len(schema) + schemaBatchSize - 1) / schemaBatchSize
	var lastResult *PushSchemaResponse

	for i := 0; i < len(schema); i += schemaBatchSize {
		end := i + schemaBatchSize
		if end > len(schema) {
			end = len(schema)
		}

		batchNum := (i / schemaBatchSize) + 1
		reqBody := PushSchemaRequest{
			Schema: schema[i:end],
			Batch:  batchNum,
			Total:  totalBatches,
		}

		body, err := json.Marshal(reqBody)
		if err != nil {
			return nil, fmt.Errorf("erro ao serializar schema batch %d: %w", batchNum, err)
		}

		path := fmt.Sprintf("/agent/%s/push-schema", c.agentKey)
		resp, err := c.doSigned("POST", path, body)
		if err != nil {
			return nil, fmt.Errorf("erro no batch %d/%d: %w", batchNum, totalBatches, err)
		}

		var result PushSchemaResponse
		if err := parseResponse(resp, &result); err != nil {
			return nil, fmt.Errorf("erro ao parsear resposta batch %d: %w", batchNum, err)
		}

		lastResult = &result
		fmt.Printf("  Batch %d/%d enviado (%d tabelas)\n", batchNum, totalBatches, end-i)
	}

	return lastResult, nil
}
