package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prim-ideias/databridge-agent/internal/auth"
	"github.com/prim-ideias/databridge-agent/internal/config"
)

// Client encapsula chamadas HTTP para a API DataBridge.
type Client struct {
	baseURL    string
	agentKey   string
	agentSecret string
	httpClient *http.Client
}

// NewClient cria um client a partir da config.
func NewClient(cfg *config.APIConfig) *Client {
	return &Client{
		baseURL:    cfg.URL,
		agentKey:   cfg.AgentKey,
		agentSecret: cfg.AgentSecret,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// doSigned executa uma request assinada com HMAC.
func (c *Client) doSigned(method, path string, body []byte) (*http.Response, error) {
	url := c.baseURL + path

	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("erro ao criar request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Agent-Key", c.agentKey)
	req.Header.Set("X-Agent-Signature", auth.Sign(body, c.agentSecret))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("erro na request %s %s: %w", method, path, err)
	}

	return resp, nil
}

// doPublic executa uma request sem autenticacao HMAC (usado no register).
func (c *Client) doPublic(method, path string, body []byte) (*http.Response, error) {
	url := c.baseURL + path

	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("erro ao criar request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("erro na request %s %s: %w", method, path, err)
	}

	return resp, nil
}

// parseResponse le e decodifica a resposta JSON.
func parseResponse(resp *http.Response, target interface{}) error {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("erro ao ler resposta: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("API retornou %d: %s", resp.StatusCode, string(body))
	}

	if target != nil {
		if err := json.Unmarshal(body, target); err != nil {
			return fmt.Errorf("erro ao decodificar resposta: %w", err)
		}
	}

	return nil
}
