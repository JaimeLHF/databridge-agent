package db

import (
	"database/sql"
	"fmt"

	"github.com/prim-ideias/databridge-agent/internal/config"
)

// Open abre conexao com o banco de acordo com o driver configurado.
func Open(cfg *config.DatabaseConfig) (*sql.DB, error) {
	switch cfg.Driver {
	case "mysql":
		return OpenMySQL(cfg)
	case "pgsql":
		return OpenPgsql(cfg)
	default:
		return nil, fmt.Errorf("driver desconhecido: %s (use mysql ou pgsql)", cfg.Driver)
	}
}

// ScanRows executa uma query e retorna os resultados como slice de maps.
// Cada map representa uma row com colunas como chaves.
func ScanRows(db *sql.DB, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("erro ao executar query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("erro ao ler colunas: %w", err)
	}

	var results []map[string]interface{}

	for rows.Next() {
		// Cria slice de pointers para scan
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
			// Converte []byte para string (MySQL retorna bytes)
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

	return results, nil
}
