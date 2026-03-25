package db

import (
	"database/sql"
	"fmt"

	"github.com/prim-ideias/databridge-agent/internal/config"

	_ "github.com/lib/pq"
)

// OpenPgsql abre conexao com banco PostgreSQL.
func OpenPgsql(cfg *config.DatabaseConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Name,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir conexao PostgreSQL: %w", err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao conectar PostgreSQL (%s:%d/%s): %w", cfg.Host, cfg.Port, cfg.Name, err)
	}

	return db, nil
}
