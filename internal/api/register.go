package api

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
)

// RegisterRequest payload enviado ao POST /agent/register.
type RegisterRequest struct {
	ActivationToken string `json:"activation_token"`
	Hostname        string `json:"hostname"`
	OsInfo          string `json:"os_info"`
	DbDriver        string `json:"db_driver,omitempty"`
	AgentVersion    string `json:"agent_version,omitempty"`
}

// RegisterResponse payload recebido da API apos registro.
type RegisterResponse struct {
	AgentKey    string       `json:"agent_key"`
	AgentSecret string       `json:"agent_secret"`
	Config      RegisterConfig `json:"config"`
}

type RegisterConfig struct {
	SyncInterval int             `json:"sync_interval"`
	DbDriver     string          `json:"db_driver"`
	SchemaConfig json.RawMessage `json:"schema_config"`
}

// Register envia o activation_token para a API e recebe agent_key + secret.
func (c *Client) Register(token, dbDriver, version string) (*RegisterResponse, error) {
	hostname, _ := os.Hostname()

	reqBody := RegisterRequest{
		ActivationToken: token,
		Hostname:        hostname,
		OsInfo:          fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		DbDriver:        dbDriver,
		AgentVersion:    version,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("erro ao serializar request: %w", err)
	}

	resp, err := c.doPublic("POST", "/agent/register", body)
	if err != nil {
		return nil, err
	}

	var result RegisterResponse
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
