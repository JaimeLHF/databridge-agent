package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/prim-ideias/databridge-agent/internal/api"
	"github.com/prim-ideias/databridge-agent/internal/config"
	"github.com/prim-ideias/databridge-agent/internal/db"
	"github.com/prim-ideias/databridge-agent/internal/svcmgr"
	"github.com/prim-ideias/databridge-agent/internal/sync"
	"github.com/prim-ideias/databridge-agent/internal/wizard"
)

// Version e injetada via ldflags no build.
var Version = "dev"

func main() {
	// Se executado pelo gerenciador de servicos (Windows SCM / systemd),
	// roda como servico e sai. Nao entra no CLI interativo.
	if svcmgr.RunAsService(Version) {
		return
	}

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "install":
		cmdInstall()
	case "start":
		cmdStart()
	case "service":
		cmdService()
	case "status":
		cmdStatus()
	case "test-db":
		cmdTestDb()
	case "discover":
		cmdDiscover()
	case "version":
		fmt.Printf("databridge-agent %s\n", Version)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Comando desconhecido: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`DataBridge Agent - Conector push para bancos de clientes

Uso:
  databridge-agent <comando> [opcoes]

Comandos:
  install     Instala e configura o agent interativamente
  start       Inicia o sync loop (foreground)
  service     Gerencia o servico do sistema (install/start/stop/status)
  discover    Descobre o schema do banco e envia para a API
  status      Mostra status da config e conexao
  test-db     Testa conexao com o banco local
  version     Mostra a versao do agent
  help        Mostra esta ajuda

Instalacao rapida:
  databridge-agent install --api-url=https://api.com/api/v1 --token=TOKEN

Instalacao interativa (recomendado):
  databridge-agent install

Servico:
  databridge-agent service install   Instala como servico do sistema
  databridge-agent service start     Inicia o servico
  databridge-agent service stop      Para o servico
  databridge-agent service uninstall Remove o servico
  databridge-agent service status    Mostra status do servico`)
}

// ── install ──────────────────────────────────────────────────────────────────

func cmdInstall() {
	var apiURL, token string

	// Parse flags como atalhos (opcional — wizard pergunta se faltar)
	for _, arg := range os.Args[2:] {
		if len(arg) > 10 && arg[:10] == "--api-url=" {
			apiURL = arg[10:]
		}
		if len(arg) > 8 && arg[:8] == "--token=" {
			token = arg[8:]
		}
	}

	wizard.Section("DataBridge Agent - Instalacao")
	fmt.Printf("Versao: %s\n\n", Version)

	// Verificar se ja existe config com credenciais (install anterior interrompido)
	var cfg *config.Config
	existingCfg, loadErr := config.Load()
	if loadErr == nil && existingCfg.API.AgentKey != "" && existingCfg.API.AgentSecret != "" {
		fmt.Println("Configuracao existente detectada:")
		fmt.Printf("  API: %s\n", existingCfg.API.URL)
		fmt.Printf("  Agent Key: %s...%s\n", existingCfg.API.AgentKey[:8], existingCfg.API.AgentKey[len(existingCfg.API.AgentKey)-4:])
		if existingCfg.Database.Host != "" {
			fmt.Printf("  Banco: %s (%s:%d/%s)\n",
				existingCfg.Database.Driver, existingCfg.Database.Host,
				existingCfg.Database.Port, existingCfg.Database.Name)
		}
		fmt.Println()

		choice := wizard.PromptSelect("O que deseja fazer?", []wizard.SelectOption{
			{Label: "Reconfigurar banco de dados", Value: "reconfig"},
			{Label: "Reinstalar do zero (novo token)", Value: "fresh"},
		}, 0)

		if choice == "reconfig" {
			cfg = existingCfg
			goto configDb
		}
		// fresh: continua com o fluxo normal de registro
	}

	// ── Step 1: API URL e Token ──
	if apiURL == "" {
		apiURL = wizard.Prompt("URL da API DataBridge", "https://laravel.blueviolet-beaver-250951.hostingersite.com/api/v1")
	} else {
		fmt.Printf("URL da API: %s\n", apiURL)
	}

	if token == "" {
		token = wizard.Prompt("Token de ativacao", "")
	}

	if token == "" {
		wizard.Error("Token de ativacao e obrigatorio. Copie do painel web.")
		os.Exit(1)
	}

	// ── Step 2: Registrar na API ──
	{
		fmt.Printf("\nRegistrando agent em %s...\n", apiURL)

		tempCfg := &config.APIConfig{URL: apiURL}
		client := api.NewClient(tempCfg)

		result, err := client.Register(token, "", Version)
		if err != nil {
			wizard.Error(fmt.Sprintf("Falha ao registrar: %v", err))
			fmt.Println("\nVerifique:")
			fmt.Println("  - A URL da API esta correta?")
			fmt.Println("  - O token foi copiado corretamente?")
			fmt.Println("  - O servidor esta acessivel deste computador?")
			os.Exit(1)
		}

		wizard.Success("Agent registrado!")
		fmt.Printf("  Agent Key: %s...%s\n", result.AgentKey[:8], result.AgentKey[len(result.AgentKey)-4:])

		// Criar config base
		cfg = config.DefaultConfig()
		cfg.API.URL = apiURL
		cfg.API.AgentKey = result.AgentKey
		cfg.API.AgentSecret = result.AgentSecret

		if result.Config.SyncInterval > 0 {
			cfg.Sync.Interval = result.Config.SyncInterval
		}

		// Pre-preencher driver se veio da API
		if result.Config.DbDriver != "" {
			cfg.Database.Driver = result.Config.DbDriver
			if result.Config.DbDriver == "pgsql" {
				cfg.Database.Port = 5432
			} else {
				cfg.Database.Port = 3306
			}
		}
		// Salvar config imediatamente apos registro (para nao perder credenciais se Ctrl+C)
		if err := config.Save(cfg); err != nil {
			log.Printf("Aviso: nao foi possivel salvar config parcial: %v", err)
		}
	}

configDb:

	// ── Step 3: Configurar Banco de Dados ──
	wizard.Section("Configuracao do Banco de Dados")

	if cfg.Database.Driver == "" {
		cfg.Database.Driver = wizard.PromptSelect("Tipo do banco", []wizard.SelectOption{
			{Label: "MySQL", Value: "mysql"},
			{Label: "PostgreSQL", Value: "pgsql"},
		}, 0)
	} else {
		driverLabel := "MySQL"
		if cfg.Database.Driver == "pgsql" {
			driverLabel = "PostgreSQL"
		}
		fmt.Printf("Tipo do banco: %s (configurado no painel)\n", driverLabel)
	}

	defaultPort := 3306
	if cfg.Database.Driver == "pgsql" {
		defaultPort = 5432
	}

	cfg.Database.Host = wizard.Prompt("Host", "localhost")
	cfg.Database.Port = wizard.PromptInt("Porta", defaultPort)
	cfg.Database.Name = wizard.Prompt("Nome do banco", "")
	cfg.Database.Username = wizard.Prompt("Usuario", cfg.Database.Driver) // default "mysql" ou "pgsql"
	cfg.Database.Password = wizard.PromptPassword("Senha")

	// ── Step 4: Testar conexao ──
	for {
		fmt.Printf("\nTestando conexao %s (%s:%d/%s)...\n",
			cfg.Database.Driver, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)

		conn, err := db.Open(&cfg.Database)
		if err != nil {
			wizard.Error(fmt.Sprintf("Falha na conexao: %v", err))
			if !wizard.Confirm("Tentar novamente com outros dados?", true) {
				// Salvar config sem DB para o usuario editar depois
				if err := config.Save(cfg); err != nil {
					log.Fatalf("Erro ao salvar config: %v", err)
				}
				fmt.Printf("\nConfig salvo em: %s\n", config.ConfigPath())
				fmt.Println("Edite a secao 'database' e rode: databridge-agent test-db")
				os.Exit(0)
			}
			// Deixar o usuario corrigir
			cfg.Database.Host = wizard.Prompt("Host", cfg.Database.Host)
			cfg.Database.Port = wizard.PromptInt("Porta", cfg.Database.Port)
			cfg.Database.Name = wizard.Prompt("Nome do banco", cfg.Database.Name)
			cfg.Database.Username = wizard.Prompt("Usuario", cfg.Database.Username)
			cfg.Database.Password = wizard.PromptPassword("Senha")
			continue
		}
		conn.Close()
		wizard.Success(fmt.Sprintf("Conectado ao %s!", cfg.Database.Driver))
		break
	}

	// Salvar config (antes do discover, para garantir que nao perde progresso)
	if err := config.Save(cfg); err != nil {
		log.Fatalf("Erro ao salvar config: %v", err)
	}

	// ── Step 5: Descobrir schema ──
	wizard.Section("Descoberta de Schema")

	if wizard.Confirm("Descobrir tabelas e colunas agora?", true) {
		fmt.Println("Descobrindo schema...")

		conn, err := db.Open(&cfg.Database)
		if err != nil {
			wizard.Error(fmt.Sprintf("Falha ao conectar: %v", err))
		} else {
			schema, err := db.DiscoverSchema(conn, cfg.Database.Driver)
			conn.Close()

			if err != nil {
				wizard.Error(fmt.Sprintf("Falha ao descobrir schema: %v", err))
			} else {
				totalCols := 0
				for _, t := range schema {
					totalCols += len(t.Columns)
				}
				fmt.Printf("Encontradas %d tabelas com %d colunas.\n", len(schema), totalCols)

				fmt.Println("Enviando para a API...")
				authClient := api.NewClient(&cfg.API)
				pushResult, err := authClient.PushSchema(schema)
				if err != nil {
					wizard.Error(fmt.Sprintf("Falha ao enviar: %v", err))
				} else {
					wizard.Success(fmt.Sprintf("%d tabelas enviadas!", pushResult.TablesCount))
				}
			}
		}
	} else {
		fmt.Println("Voce pode rodar depois: databridge-agent discover")
	}

	// ── Step 6: Instalar como servico ──
	wizard.Section("Servico do Sistema")

	if wizard.Confirm("Instalar como servico do sistema? (inicia automaticamente)", true) {
		fmt.Println("Instalando servico DataBridgeAgent...")
		if err := svcmgr.Install(Version); err != nil {
			wizard.Error(fmt.Sprintf("Falha ao instalar servico: %v", err))
			fmt.Println("Voce pode instalar depois: databridge-agent service install")
		} else {
			fmt.Println("Iniciando servico...")
			if err := svcmgr.Start(Version); err != nil {
				wizard.Warn(fmt.Sprintf("Servico instalado mas nao iniciou: %v", err))
				fmt.Println("Inicie manualmente: databridge-agent service start")
			} else {
				wizard.Success("Servico DataBridgeAgent instalado e rodando!")
			}
		}
	} else {
		fmt.Println("Voce pode instalar depois: databridge-agent service install")
		fmt.Println("Ou rodar em foreground:    databridge-agent start")
	}

	// ── Concluido ──
	wizard.Section("Concluido!")

	configPath := config.ConfigPath()
	fmt.Printf("Config salvo em: %s\n\n", configPath)
	fmt.Println("O agent esta configurado e pronto!")
	fmt.Println("Proximo passo: configure o mapeamento de colunas no painel web.")
}

// ── start ────────────────────────────────────────────────────────────────────

func cmdStart() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Erro ao carregar config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Config invalida: %v", err)
	}

	fmt.Printf("DataBridge Agent %s iniciando...\n", Version)
	fmt.Printf("API: %s\n", cfg.API.URL)
	fmt.Printf("Banco: %s (%s:%d/%s)\n", cfg.Database.Driver, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)

	// Contexto com graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		fmt.Printf("\nRecebido sinal %v, encerrando...\n", sig)
		cancel()
	}()

	syncer := sync.New(cfg, Version)
	if err := syncer.Run(ctx); err != nil {
		log.Fatalf("Erro no sync: %v", err)
	}
}

// ── status ───────────────────────────────────────────────────────────────────

func cmdStatus() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Config: NAO ENCONTRADO (%v)\n", err)
		fmt.Println("Execute 'databridge-agent install' primeiro.")
		return
	}

	fmt.Println("=== DataBridge Agent Status ===")
	fmt.Printf("Versao:     %s\n", Version)
	fmt.Printf("API URL:    %s\n", cfg.API.URL)

	if cfg.API.AgentKey != "" {
		fmt.Printf("Agent Key:  %s...%s\n", cfg.API.AgentKey[:8], cfg.API.AgentKey[len(cfg.API.AgentKey)-4:])
	} else {
		fmt.Println("Agent Key:  (nao configurado)")
	}

	fmt.Printf("Banco:      %s (%s:%d/%s)\n", cfg.Database.Driver, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)
	fmt.Printf("Sync:       a cada %ds, batch %d, janela %d dias\n", cfg.Sync.Interval, cfg.Sync.BatchSize, cfg.Sync.SinceDays)
	fmt.Printf("Heartbeat:  a cada %ds\n", cfg.Heartbeat.Interval)
}

// ── discover ─────────────────────────────────────────────────────────────────

func cmdDiscover() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Erro ao carregar config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Config invalida: %v", err)
	}

	fmt.Printf("Descobrindo schema do banco %s (%s:%d/%s)...\n",
		cfg.Database.Driver, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)

	conn, err := db.Open(&cfg.Database)
	if err != nil {
		log.Fatalf("Erro ao conectar: %v", err)
	}
	defer conn.Close()

	schema, err := db.DiscoverSchema(conn, cfg.Database.Driver)
	if err != nil {
		log.Fatalf("Erro ao descobrir schema: %v", err)
	}

	totalCols := 0
	for _, t := range schema {
		totalCols += len(t.Columns)
	}

	fmt.Printf("Encontradas %d tabelas com %d colunas.\n", len(schema), totalCols)

	// Enviar para a API
	fmt.Println("Enviando schema para a API...")
	client := api.NewClient(&cfg.API)
	pushResult, err := client.PushSchema(schema)
	if err != nil {
		log.Fatalf("Erro ao enviar schema: %v", err)
	}

	fmt.Printf("OK! %d tabelas enviadas.\n", pushResult.TablesCount)
	fmt.Println("\nProximo passo: configure o mapeamento de colunas no frontend.")
}

// ── service ──────────────────────────────────────────────────────────────────

func cmdService() {
	if len(os.Args) < 3 {
		fmt.Println(`Uso: databridge-agent service <acao>

Acoes:
  install     Instala como servico do sistema
  uninstall   Remove o servico
  start       Inicia o servico
  stop        Para o servico
  status      Mostra status do servico`)
		os.Exit(1)
	}

	action := os.Args[2]

	switch action {
	case "install":
		fmt.Println("Instalando servico DataBridgeAgent...")
		if err := svcmgr.Install(Version); err != nil {
			log.Fatalf("Erro: %v", err)
		}
		fmt.Println("Servico instalado! Iniciando...")
		if err := svcmgr.Start(Version); err != nil {
			fmt.Printf("Aviso: servico instalado mas nao iniciou: %v\n", err)
			fmt.Println("Inicie manualmente: databridge-agent service start")
			return
		}
		fmt.Println("Servico DataBridgeAgent instalado e rodando!")

	case "uninstall":
		fmt.Println("Parando servico...")
		_ = svcmgr.Stop(Version) // ignora erro se ja parado
		fmt.Println("Removendo servico...")
		if err := svcmgr.Uninstall(Version); err != nil {
			log.Fatalf("Erro: %v", err)
		}
		fmt.Println("Servico removido.")

	case "start":
		if err := svcmgr.Start(Version); err != nil {
			log.Fatalf("Erro: %v", err)
		}
		fmt.Println("Servico iniciado.")

	case "stop":
		if err := svcmgr.Stop(Version); err != nil {
			log.Fatalf("Erro: %v", err)
		}
		fmt.Println("Servico parado.")

	case "status":
		status, err := svcmgr.Status(Version)
		if err != nil {
			fmt.Printf("Status do servico: %s (erro: %v)\n", status, err)
			return
		}
		fmt.Printf("Status do servico: %s\n", status)

	default:
		fmt.Fprintf(os.Stderr, "Acao desconhecida: %s\n", action)
		fmt.Println("Acoes validas: install, uninstall, start, stop, status")
		os.Exit(1)
	}
}

// ── test-db ──────────────────────────────────────────────────────────────────

func cmdTestDb() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Erro ao carregar config: %v", err)
	}

	if err := cfg.ValidateDatabase(); err != nil {
		log.Fatalf("Config de banco invalida: %v", err)
	}

	fmt.Printf("Testando conexao %s (%s:%d/%s)...\n",
		cfg.Database.Driver, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)

	conn, err := db.Open(&cfg.Database)
	if err != nil {
		log.Fatalf("FALHA: %v", err)
	}
	defer conn.Close()

	fmt.Println("OK! Conexao estabelecida com sucesso.")
}
