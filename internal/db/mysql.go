package db

import (
	"database/sql"
	"fmt"

	"github.com/prim-ideias/databridge-agent/internal/config"

	_ "github.com/go-sql-driver/mysql"
)

// OpenMySQL abre conexao com banco MySQL.
func OpenMySQL(cfg *config.DatabaseConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Name,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir conexao MySQL: %w", err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao conectar MySQL (%s:%d/%s): %w", cfg.Host, cfg.Port, cfg.Name, err)
	}

	return db, nil
}
