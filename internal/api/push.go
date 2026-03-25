package api

import (
	"encoding/json"
	"fmt"
)

// PushRequest payload enviado ao POST /agent/{key}/push.
type PushRequest struct {
	Invoices []map[string]interface{} `json:"invoices"`
}

// PushResponse payload recebido da API apos push.
type PushResponse struct {
	Accepted   int `json:"accepted"`
	Duplicates int `json:"duplicates"`
}

// Push envia um lote de invoices para a API.
func (c *Client) Push(invoices []map[string]interface{}) (*PushResponse, error) {
	reqBody := PushRequest{
		Invoices: invoices,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("erro ao serializar push: %w", err)
	}

	path := fmt.Sprintf("/agent/%s/push", c.agentKey)
	resp, err := c.doSigned("POST", path, body)
	if err != nil {
		return nil, err
	}

	var result PushResponse
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
