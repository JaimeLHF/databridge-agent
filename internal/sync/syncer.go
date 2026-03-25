package sync

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/prim-ideias/databridge-agent/internal/api"
	"github.com/prim-ideias/databridge-agent/internal/config"
	"github.com/prim-ideias/databridge-agent/internal/db"
)

// Syncer gerencia o loop de sync + heartbeat.
type Syncer struct {
	cfg     *config.Config
	client  *api.Client
	conn    *sql.DB
	version string

	// Schema config obtido da API (tabelas e colunas mapeadas)
	schemaConfig *schemaMapping

	// Cursor: ultima data sincronizada (persistido em arquivo local)
	lastSyncedAt time.Time
}

// schemaMapping armazena o mapeamento de tabelas/colunas do schema_config.
type schemaMapping struct {
	Tables        map[string]string            `json:"tables"`
	Columns       map[string]map[string]string `json:"columns"`
	ExtractionSql string
}

const cursorFile = ".databridge-cursor"

// New cria um novo Syncer.
func New(cfg *config.Config, version string) *Syncer {
	return &Syncer{
		cfg:     cfg,
		client:  api.NewClient(&cfg.API),
		version: version,
	}
}

// Run inicia o sync loop e heartbeat em goroutines separadas.
// Bloqueia ate o contexto ser cancelado (SIGINT/SIGTERM).
func (s *Syncer) Run(ctx context.Context) error {
	// Abrir conexao com banco local
	conn, err := db.Open(&s.cfg.Database)
	if err != nil {
		return fmt.Errorf("falha ao conectar no banco local: %w", err)
	}
	s.conn = conn
	defer conn.Close()

	// Carregar cursor
	s.loadCursor()

	// Carregar schema_config da API
	s.loadSchemaConfig()

	log.Printf("[sync] Conectado ao banco %s (%s:%d/%s)",
		s.cfg.Database.Driver, s.cfg.Database.Host, s.cfg.Database.Port, s.cfg.Database.Name)
	log.Printf("[sync] Cursor: %s", s.lastSyncedAt.Format("2006-01-02"))
	log.Printf("[sync] Intervalo: %ds | Batch: %d | Heartbeat: %ds",
		s.cfg.Sync.Interval, s.cfg.Sync.BatchSize, s.cfg.Heartbeat.Interval)

	if s.schemaConfig != nil {
		invTable := s.schemaConfig.Tables["invoices"]
		log.Printf("[sync] Tabela de notas: %s", invTable)
	} else {
		log.Println("[sync] Schema nao configurado. Executando auto-discover...")
		s.autoDiscover()
	}

	// Heartbeat goroutine
	go s.heartbeatLoop(ctx)

	// Command poll goroutine (polling rapido para queries do frontend)
	go s.commandPollLoop(ctx)

	// Sync imediato + loop
	s.doSync()
	s.syncLoop(ctx)

	log.Println("[sync] Encerrado.")
	return nil
}

// loadSchemaConfig busca a config da API e extrai o mapeamento de tabelas/colunas.
func (s *Syncer) loadSchemaConfig() {
	cfgResp, err := s.client.GetConfig()
	if err != nil {
		log.Printf("[sync] Aviso: nao foi possivel carregar config da API: %v", err)
		return
	}

	sc := cfgResp.ParseSchemaConfig()
	if sc == nil {
		return
	}

	mapping := &schemaMapping{
		Tables:  make(map[string]string),
		Columns: make(map[string]map[string]string),
	}

	// Extrair tables
	if tables, ok := sc["tables"].(map[string]interface{}); ok {
		for k, v := range tables {
			if s, ok := v.(string); ok {
				mapping.Tables[k] = s
			}
		}
	}

	// Extrair columns
	if columns, ok := sc["columns"].(map[string]interface{}); ok {
		for section, cols := range columns {
			if colMap, ok := cols.(map[string]interface{}); ok {
				mapping.Columns[section] = make(map[string]string)
				for k, v := range colMap {
					if s, ok := v.(string); ok {
						mapping.Columns[section][k] = s
					}
				}
			}
		}
	}

	// Extrair extraction_sql (query customizada de extracao)
	if esql, ok := sc["extraction_sql"].(string); ok {
		mapping.ExtractionSql = esql
	}

	// Aceitar config se tem tabela de invoices OU extraction_sql customizado
	if mapping.Tables["invoices"] != "" || mapping.ExtractionSql != "" {
		s.schemaConfig = mapping
	}
}

// heartbeatLoop envia heartbeats periodicamente.
func (s *Syncer) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.cfg.Heartbeat.Interval) * time.Second)
	defer ticker.Stop()

	// Heartbeat imediato
	s.sendHeartbeat()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sendHeartbeat()
		}
	}
}

func (s *Syncer) sendHeartbeat() {
	_, err := s.client.Heartbeat(s.version)
	if err != nil {
		log.Printf("[heartbeat] Erro: %v", err)
	}
}

// executeRemoteQuery executa uma query SQL recebida via heartbeat e envia o resultado para a API.
func (s *Syncer) executeRemoteQuery(pq *api.PendingQuery) {
	log.Printf("[query] Executando query remota #%d: %.80s...", pq.ID, pq.SQL)

	result, err := db.ExecuteQuery(s.conn, pq.SQL, 15, 15*time.Second)

	req := &api.QueryResultRequest{
		CommandID: pq.ID,
		MaxRows:   15,
	}

	if err != nil {
		log.Printf("[query] Erro na query #%d: %v", pq.ID, err)
		req.Error = err.Error()
	} else {
		log.Printf("[query] Query #%d concluida: %d rows em %.1fms", pq.ID, result.RowCount, result.ExecutionTimeMs)
		req.Columns = result.Columns
		req.Rows = result.Rows
		req.RowCount = result.RowCount
		req.ExecutionTimeMs = result.ExecutionTimeMs
		req.Truncated = result.Truncated
		req.MaxRows = result.MaxRows
	}

	if pushErr := s.client.PushQueryResult(req); pushErr != nil {
		log.Printf("[query] Erro ao enviar resultado da query #%d: %v", pq.ID, pushErr)
	}
}

// commandPollLoop faz polling rapido (3s) para queries pendentes do frontend.
func (s *Syncer) commandPollLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pq, err := s.client.GetPendingQueries()
			if err != nil {
				// Silencioso — erros de rede nao devem poluir os logs
				continue
			}
			if pq != nil {
				s.executeRemoteQuery(pq)
			}
		}
	}
}

// syncLoop executa sincronizacoes no intervalo configurado.
func (s *Syncer) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.cfg.Sync.Interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.doSync()
		}
	}
}

// doSync executa uma rodada de sincronizacao.
func (s *Syncer) doSync() {
	if s.schemaConfig == nil {
		log.Println("[sync] Schema nao configurado. Pulando sync. Execute 'discover' e configure no frontend.")
		return
	}

	if s.schemaConfig.ExtractionSql != "" {
		log.Println("[sync] Modo: SQL customizado")
	} else {
		log.Printf("[sync] Modo: mapeamento (tabela: %s)", s.schemaConfig.Tables["invoices"])
	}
	log.Printf("[sync] Iniciando sync desde %s...", s.lastSyncedAt.Format("2006-01-02 15:04:05"))

	rows, err := s.fetchRows()
	if err != nil {
		log.Printf("[sync] Erro ao buscar dados: %v", err)
		return
	}

	if len(rows) == 0 {
		log.Println("[sync] Nenhum dado novo encontrado.")
		return
	}

	log.Printf("[sync] %d rows encontradas. Enviando em batches de %d...", len(rows), s.cfg.Sync.BatchSize)

	totalAccepted := 0
	totalDuplicates := 0

	// Dividir em batches e enviar
	for i := 0; i < len(rows); i += s.cfg.Sync.BatchSize {
		end := i + s.cfg.Sync.BatchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[i:end]

		invoices := make([]map[string]interface{}, len(batch))
		for j, row := range batch {
			invoices[j] = map[string]interface{}{
				"invoice": row,
				"items":   []interface{}{},
			}
		}

		result, err := s.client.Push(invoices)
		if err != nil {
			log.Printf("[sync] Erro ao enviar batch %d-%d: %v", i, end, err)
			continue
		}

		totalAccepted += result.Accepted
		totalDuplicates += result.Duplicates
	}

	// Atualizar cursor
	s.lastSyncedAt = time.Now()
	s.saveCursor()

	log.Printf("[sync] Concluido. Aceitas: %d, Duplicatas: %d", totalAccepted, totalDuplicates)
}

// fetchRows busca rows do banco local usando o schema_config.
func (s *Syncer) fetchRows() ([]map[string]interface{}, error) {
	// Modo SQL customizado: usar a query definida pelo usuario no frontend
	if s.schemaConfig.ExtractionSql != "" {
		log.Printf("[sync] Usando query de extracao customizada")
		return db.ScanRows(s.conn, s.schemaConfig.ExtractionSql)
	}

	// Modo mapeamento: montar query automaticamente a partir de tabelas/colunas
	invoiceTable := s.schemaConfig.Tables["invoices"]
	dateColumn := "issue_date"

	// Usar coluna de data do mapeamento se disponivel
	if invCols, ok := s.schemaConfig.Columns["invoices"]; ok {
		if dc, ok := invCols["issue_date"]; ok && dc != "" {
			dateColumn = dc
		}
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s >= ? ORDER BY %s LIMIT 10000",
		invoiceTable, dateColumn, dateColumn)

	// PostgreSQL usa $1 ao inves de ?
	if s.cfg.Database.Driver == "pgsql" {
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1 ORDER BY %s LIMIT 10000",
			invoiceTable, dateColumn, dateColumn)
	}

	return db.ScanRows(s.conn, query, s.lastSyncedAt)
}

// loadCursor carrega a ultima data sincronizada do arquivo local.
func (s *Syncer) loadCursor() {
	data, err := os.ReadFile(cursorFile)
	if err != nil {
		// Primeira sync: usa since_days da config
		s.lastSyncedAt = time.Now().AddDate(0, 0, -s.cfg.Sync.SinceDays)
		return
	}

	t, err := time.Parse(time.RFC3339, string(data))
	if err != nil {
		s.lastSyncedAt = time.Now().AddDate(0, 0, -s.cfg.Sync.SinceDays)
		return
	}

	s.lastSyncedAt = t
}

// saveCursor persiste a ultima data sincronizada.
func (s *Syncer) saveCursor() {
	data := []byte(s.lastSyncedAt.Format(time.RFC3339))
	if err := os.WriteFile(cursorFile, data, 0600); err != nil {
		log.Printf("[sync] Aviso: nao foi possivel salvar cursor: %v", err)
	}
}

// autoDiscover descobre o schema do banco e envia para a API automaticamente.
// Seguro pois so le information_schema (read-only) e PushSchema e idempotente.
func (s *Syncer) autoDiscover() {
	schema, err := db.DiscoverSchema(s.conn, s.cfg.Database.Driver)
	if err != nil {
		log.Printf("[auto-discover] Erro ao descobrir schema: %v", err)
		return
	}

	totalCols := 0
	for _, t := range schema {
		totalCols += len(t.Columns)
	}
	log.Printf("[auto-discover] Encontradas %d tabelas com %d colunas.", len(schema), totalCols)

	result, err := s.client.PushSchema(schema)
	if err != nil {
		log.Printf("[auto-discover] Erro ao enviar schema: %v", err)
		return
	}

	log.Printf("[auto-discover] Schema enviado! %d tabelas registradas.", result.TablesCount)
}
