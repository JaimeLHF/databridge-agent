package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// QueryResult armazena o resultado de uma query executada com limites de seguranca.
type QueryResult struct {
	Columns         []string                 `json:"columns"`
	Rows            []map[string]interface{} `json:"rows"`
	RowCount        int                      `json:"row_count"`
	ExecutionTimeMs float64                  `json:"execution_time_ms"`
	Truncated       bool                     `json:"truncated"`
	MaxRows         int                      `json:"max_rows"`
}

// ExecuteQuery executa uma query SELECT com timeout e limite de rows.
func ExecuteQuery(conn *sql.DB, query string, maxRows int, timeout time.Duration) (*QueryResult, error) {
	return ExecuteQueryCtx(context.Background(), conn, query, maxRows, timeout)
}

// ExecuteQueryCtx executa uma query SELECT permitindo cancelamento externo
// via parentCtx. Quando parentCtx for cancelado, o driver MySQL/Postgres
// aborta a query no banco (KILL / pg_cancel_backend).
func ExecuteQueryCtx(parentCtx context.Context, conn *sql.DB, query string, maxRows int, timeout time.Duration) (*QueryResult, error) {
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	start := time.Now()

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("erro ao executar query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("erro ao ler colunas: %w", err)
	}

	var results []map[string]interface{}
	truncated := false

	for rows.Next() {
		if len(results) >= maxRows {
			truncated = true
			break
		}

		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("erro ao escanear row: %w", err)
		}

		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("erro ao iterar rows: %w", err)
	}

	elapsed := time.Since(start)

	return &QueryResult{
		Columns:         columns,
		Rows:            results,
		RowCount:        len(results),
		ExecutionTimeMs: float64(elapsed.Milliseconds()),
		Truncated:       truncated,
		MaxRows:         maxRows,
	}, nil
}
