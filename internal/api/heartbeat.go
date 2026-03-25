package api

import (
	"encoding/json"
	"fmt"
)

// HeartbeatRequest payload enviado ao POST /agent/{key}/heartbeat.
type HeartbeatRequest struct {
	AgentVersion string `json:"agent_version,omitempty"`
}

// HeartbeatResponse payload recebido da API.
type HeartbeatResponse struct {
	Status string          `json:"status"`
	Config *HeartbeatConfig `json:"config,omitempty"`
}

type HeartbeatConfig struct {
	SyncInterval int `json:"sync_interval"`
}

// Heartbeat envia um heartbeat para a API.
func (c *Client) Heartbeat(version string) (*HeartbeatResponse, error) {
	reqBody := HeartbeatRequest{
		AgentVersion: version,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("erro ao serializar heartbeat: %w", err)
	}

	path := fmt.Sprintf("/agent/%s/heartbeat", c.agentKey)
	resp, err := c.doSigned("POST", path, body)
	if err != nil {
		return nil, err
	}

	var result HeartbeatResponse
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
