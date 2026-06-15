package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

// retryableStatus reporta se um status HTTP justifica retry (rate limit / indisponibilidade transitoria).
func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || // 429
		code == http.StatusServiceUnavailable || // 503
		code == http.StatusBadGateway || // 502
		code == http.StatusGatewayTimeout // 504
}

// parseRetryAfter le o header Retry-After (segundos). Retorna 0 se ausente/invalido.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// doSignedRetry executa doSigned com retry/backoff em 429/502/503/504.
// Honra Retry-After quando presente; senao usa backoff exponencial (cap 30s).
// Erros de transporte (rede) tambem sao retentados. Em status nao-retentavel
// (2xx ou 4xx como 422) retorna a resposta para o caller tratar.
func (c *Client) doSignedRetry(method, path string, body []byte, maxAttempts int) (*http.Response, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var lastErr error
	backoff := 500 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, err := c.doSigned(method, path, body)
		if err != nil {
			lastErr = err
		} else if !retryableStatus(resp.StatusCode) {
			return resp, nil
		} else {
			wait := parseRetryAfter(resp.Header.Get("Retry-After"))
			if wait <= 0 {
				wait = backoff
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("API retornou %d (tentativa %d/%d)", resp.StatusCode, attempt, maxAttempts)
			err = nil
			if attempt < maxAttempts {
				time.Sleep(wait)
			}
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		// Erro de transporte — backoff e tenta de novo.
		if attempt < maxAttempts {
			time.Sleep(backoff)
		}
		backoff = minDuration(backoff*2, 30*time.Second)
	}

	return nil, lastErr
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
