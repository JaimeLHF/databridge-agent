package svcmgr

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/kardianos/service"
	"github.com/prim-ideias/databridge-agent/internal/config"
	"github.com/prim-ideias/databridge-agent/internal/sync"
)

const serviceName = "DataBridgeAgent"
const serviceDisplayName = "DataBridge Agent"
const serviceDescription = "Agente de sincronizacao DataBridge - envia dados do banco local para a plataforma."

// program implementa a interface service.Interface.
type program struct {
	cancel  context.CancelFunc
	version string
}

func (p *program) Start(s service.Service) error {
	go p.run()
	return nil
}

func (p *program) run() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[service] Erro ao carregar config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("[service] Config invalida: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	syncer := sync.New(cfg, p.version)
	if err := syncer.Run(ctx); err != nil {
		log.Printf("[service] Erro no sync: %v", err)
	}
}

func (p *program) Stop(s service.Service) error {
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}

// newService cria a instancia de service.Service.
func newService(version string) (service.Service, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter caminho do executavel: %w", err)
	}

	workDir := filepath.Dir(exe)

	svcConfig := &service.Config{
		Name:             serviceName,
		DisplayName:      serviceDisplayName,
		Description:      serviceDescription,
		Arguments:        []string{"start"},
		WorkingDirectory: workDir,
	}

	prg := &program{version: version}
	return service.New(prg, svcConfig)
}

// Install registra o servico no sistema operacional.
func Install(version string) error {
	s, err := newService(version)
	if err != nil {
		return err
	}

	if err := s.Install(); err != nil {
		return fmt.Errorf("erro ao instalar servico: %w", err)
	}

	return nil
}

// Uninstall remove o servico do sistema operacional.
func Uninstall(version string) error {
	s, err := newService(version)
	if err != nil {
		return err
	}

	if err := s.Uninstall(); err != nil {
		return fmt.Errorf("erro ao remover servico: %w", err)
	}

	return nil
}

// Start inicia o servico.
func Start(version string) error {
	s, err := newService(version)
	if err != nil {
		return err
	}

	if err := s.Start(); err != nil {
		return fmt.Errorf("erro ao iniciar servico: %w", err)
	}

	return nil
}

// Stop para o servico.
func Stop(version string) error {
	s, err := newService(version)
	if err != nil {
		return err
	}

	if err := s.Stop(); err != nil {
		return fmt.Errorf("erro ao parar servico: %w", err)
	}

	return nil
}

// Status retorna o status do servico.
func Status(version string) (string, error) {
	s, err := newService(version)
	if err != nil {
		return "", err
	}

	status, err := s.Status()
	if err != nil {
		return "desconhecido", err
	}

	switch status {
	case service.StatusRunning:
		return "rodando", nil
	case service.StatusStopped:
		return "parado", nil
	default:
		return "desconhecido", nil
	}
}

// RunAsService verifica se esta sendo executado pelo gerenciador de servicos.
// Se sim, roda como servico e retorna true. Se nao, retorna false.
func RunAsService(version string) bool {
	if service.Interactive() {
		return false
	}

	s, err := newService(version)
	if err != nil {
		log.Printf("[service] Erro ao criar servico: %v", err)
		return false
	}

	if err := s.Run(); err != nil {
		log.Printf("[service] Erro ao rodar servico: %v", err)
	}
	return true
}
