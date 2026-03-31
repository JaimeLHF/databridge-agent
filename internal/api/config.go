package api

import (
	"encoding/json"
	"fmt"
)

// ConfigResponse payload recebido do GET /agent/{key}/config.
type ConfigResponse struct {
	SyncInterval int             `json:"sync_interval"`
	SyncEnabled  bool            `json:"sync_enabled"`
	SchemaConfig json.RawMessage `json:"schema_config"`
}

// ParseSchemaConfig tenta decodificar schema_config como map.
// Retorna nil se vazio, array, ou invalido (PHP retorna [] para config vazia).
func (r *ConfigResponse) ParseSchemaConfig() map[string]interface{} {
	if len(r.SchemaConfig) == 0 {
		return nil
	}
	trimmed := string(r.SchemaConfig)
	if trimmed == "[]" || trimmed == "null" || trimmed == "{}" {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(r.SchemaConfig, &m); err != nil {
		return nil
	}
	return m
}

// GetConfig busca a configuracao atualizada da API.
func (c *Client) GetConfig() (*ConfigResponse, error) {
	path := fmt.Sprintf("/agent/%s/config", c.agentKey)
	resp, err := c.doSigned("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var result ConfigResponse
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
