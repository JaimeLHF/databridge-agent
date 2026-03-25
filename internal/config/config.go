package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config representa a configuracao completa do agent.
type Config struct {
	API       APIConfig       `yaml:"api"`
	Database  DatabaseConfig  `yaml:"database"`
	Sync      SyncConfig      `yaml:"sync"`
	Heartbeat HeartbeatConfig `yaml:"heartbeat"`
}

type APIConfig struct {
	URL         string `yaml:"url"`
	AgentKey    string `yaml:"agent_key"`
	AgentSecret string `yaml:"agent_secret"`
}

type DatabaseConfig struct {
	Driver   string `yaml:"driver"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Name     string `yaml:"name"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type SyncConfig struct {
	Interval  int `yaml:"interval"`   // segundos
	BatchSize int `yaml:"batch_size"`
	SinceDays int `yaml:"since_days"`
}

type HeartbeatConfig struct {
	Interval int `yaml:"interval"` // segundos
}

const defaultConfigFile = "config.yaml"

// DefaultConfig retorna configuracao com valores padrao.
func DefaultConfig() *Config {
	return &Config{
		Sync: SyncConfig{
			Interval:  3600,
			BatchSize: 100,
			SinceDays: 90,
		},
		Heartbeat: HeartbeatConfig{
			Interval: 60,
		},
	}
}

// ConfigPath retorna o caminho do config.yaml ao lado do executavel.
func ConfigPath() string {
	exe, err := os.Executable()
	if err != nil {
		return defaultConfigFile
	}
	return filepath.Join(filepath.Dir(exe), defaultConfigFile)
}

// Load le o config.yaml do disco.
func Load() (*Config, error) {
	return LoadFrom(ConfigPath())
}

// LoadFrom le o config.yaml de um caminho especifico.
func LoadFrom(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("erro ao ler config %s: %w", path, err)
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("erro ao parsear config: %w", err)
	}

	return cfg, nil
}

// Save persiste o config.yaml no disco.
func Save(cfg *Config) error {
	return SaveTo(cfg, ConfigPath())
}

// SaveTo persiste o config.yaml em um caminho especifico.
func SaveTo(cfg *Config, path string) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("erro ao serializar config: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("erro ao salvar config %s: %w", path, err)
	}

	return nil
}

// Validate verifica se os campos obrigatorios estao preenchidos.
func (c *Config) Validate() error {
	if c.API.URL == "" {
		return fmt.Errorf("api.url e obrigatorio")
	}
	if c.API.AgentKey == "" || c.API.AgentSecret == "" {
		return fmt.Errorf("api.agent_key e api.agent_secret sao obrigatorios (execute 'install' primeiro)")
	}
	if c.Database.Driver == "" {
		return fmt.Errorf("database.driver e obrigatorio (mysql ou pgsql)")
	}
	if c.Database.Host == "" {
		return fmt.Errorf("database.host e obrigatorio")
	}
	if c.Database.Name == "" {
		return fmt.Errorf("database.name e obrigatorio")
	}
	return nil
}

// ValidateDatabase verifica apenas a config de banco (para test-db).
func (c *Config) ValidateDatabase() error {
	if c.Database.Driver == "" {
		return fmt.Errorf("database.driver e obrigatorio")
	}
	if c.Database.Host == "" {
		return fmt.Errorf("database.host e obrigatorio")
	}
	if c.Database.Name == "" {
		return fmt.Errorf("database.name e obrigatorio")
	}
	return nil
}
