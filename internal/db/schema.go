package db

import (
	"database/sql"
	"fmt"
)

// SchemaTable representa uma tabela com suas colunas.
type SchemaTable struct {
	Table   string         `json:"table"`
	Columns []SchemaColumn `json:"columns"`
}

// SchemaColumn representa uma coluna de tabela.
type SchemaColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// DiscoverSchema le o information_schema e retorna todas as tabelas com colunas.
func DiscoverSchema(conn *sql.DB, driver string) ([]SchemaTable, error) {
	var query string

	switch driver {
	case "mysql":
		query = `SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY TABLE_NAME, ORDINAL_POSITION`

	case "pgsql":
		query = `SELECT c.table_name, c.column_name, c.data_type
		FROM information_schema.columns c
		WHERE c.table_schema = 'public'
			AND c.table_name IN (
				SELECT table_name FROM information_schema.tables
				WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
			)
		ORDER BY c.table_name, c.ordinal_position`

	default:
		return nil, fmt.Errorf("driver desconhecido para discover: %s", driver)
	}

	rows, err := conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao descobrir schema: %w", err)
	}
	defer rows.Close()

	// Agrupar por tabela
	tableMap := make(map[string][]SchemaColumn)
	var tableOrder []string

	for rows.Next() {
		var tableName, colName, colType string
		if err := rows.Scan(&tableName, &colName, &colType); err != nil {
			return nil, fmt.Errorf("erro ao ler coluna: %w", err)
		}

		if _, exists := tableMap[tableName]; !exists {
			tableOrder = append(tableOrder, tableName)
		}

		tableMap[tableName] = append(tableMap[tableName], SchemaColumn{
			Name: colName,
			Type: colType,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("erro ao iterar schema: %w", err)
	}

	// Montar resultado mantendo ordem
	result := make([]SchemaTable, 0, len(tableOrder))
	for _, name := range tableOrder {
		result = append(result, SchemaTable{
			Table:   name,
			Columns: tableMap[name],
		})
	}

	return result, nil
}
