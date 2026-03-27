package api

import (
	"encoding/json"
	"fmt"
)

// PushRequest payload enviado ao POST /agent/{key}/push.
type PushRequest struct {
	Invoices []map[string]interface{} `json:"invoices"`
	Mode     string                   `json:"mode,omitempty"` // "data" (padrao) ou "xml"
}

// PushResponse payload recebido da API apos push.
type PushResponse struct {
	Accepted   int `json:"accepted"`
	Duplicates int `json:"duplicates"`
}

// Push envia um lote de invoices para a API (modo data).
func (c *Client) Push(invoices []map[string]interface{}) (*PushResponse, error) {
	return c.doPush(invoices, "")
}

// PushXml envia um lote de XMLs de NF-e para a API (modo xml).
func (c *Client) PushXml(invoices []map[string]interface{}) (*PushResponse, error) {
	return c.doPush(invoices, "xml")
}

func (c *Client) doPush(invoices []map[string]interface{}, mode string) (*PushResponse, error) {
	reqBody := PushRequest{
		Invoices: invoices,
		Mode:     mode,
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
