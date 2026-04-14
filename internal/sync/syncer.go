package sync

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
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

	// sync_enabled: controlado pelo usuario via frontend
	syncEnabled bool

	// Cursor: ultima data sincronizada (persistido em arquivo local)
	lastSyncedAt time.Time
}

// schemaMapping armazena o mapeamento de tabelas/colunas do schema_config.
type schemaMapping struct {
	Tables         map[string]string            `json:"tables"`
	Columns        map[string]map[string]string `json:"columns"`
	ExtractionSql  string
	ExtractionMode string // "data" (padrao), "xml"
	XmlTable       string // tabela que contem os XMLs
	XmlColumn      string // coluna com o XML da NF-e
	IdColumn       string // coluna de ID (para dedup/cursor)
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
		if s.schemaConfig.ExtractionMode == "xml" {
			log.Printf("[sync] Modo: XML extraction (tabela: %s, coluna: %s)", s.schemaConfig.XmlTable, s.schemaConfig.XmlColumn)
		} else if s.schemaConfig.ExtractionSql != "" {
			log.Println("[sync] Modo: SQL customizado")
		} else {
			invTable := s.schemaConfig.Tables["invoices"]
			log.Printf("[sync] Tabela de notas: %s", invTable)
		}
	} else {
		log.Println("[sync] Schema nao configurado. Executando auto-discover...")
		s.autoDiscover()
	}

	// Heartbeat goroutine
	go s.heartbeatLoop(ctx)

	// Command poll goroutine (polling rapido para queries do frontend)
	go s.commandPollLoop(ctx)

	// Sync imediato (somente se habilitado) + loop
	if s.syncEnabled {
		s.doSync()
	} else {
		log.Println("[sync] Sync desabilitado. Aguardando ativacao pelo usuario.")
	}
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

	// Extrair extraction_mode e config XML
	if mode, ok := sc["extraction_mode"].(string); ok {
		mapping.ExtractionMode = mode
	}
	if xt, ok := sc["xml_table"].(string); ok {
		mapping.XmlTable = xt
	}
	if xc, ok := sc["xml_column"].(string); ok {
		mapping.XmlColumn = xc
	}
	if ic, ok := sc["id_column"].(string); ok {
		mapping.IdColumn = ic
	}

	// Extrair sync_enabled (controlado pelo usuario via frontend)
	if se, ok := sc["sync_enabled"].(bool); ok {
		s.syncEnabled = se
	} else {
		s.syncEnabled = false
	}

	// Aceitar config se tem tabela de invoices, extraction_sql, OU modo XML configurado
	if mapping.Tables["invoices"] != "" || mapping.ExtractionSql != "" || (mapping.ExtractionMode == "xml" && mapping.XmlTable != "" && mapping.XmlColumn != "") {
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
	resp, err := s.client.Heartbeat(s.version)
	if err != nil {
		log.Printf("[heartbeat] Erro: %v", err)
		return
	}

	// Atualizar sync interval se a API informar
	if resp.Config != nil && resp.Config.SyncInterval > 0 && resp.Config.SyncInterval != s.cfg.Sync.Interval {
		log.Printf("[heartbeat] Sync interval atualizado: %ds -> %ds", s.cfg.Sync.Interval, resp.Config.SyncInterval)
		s.cfg.Sync.Interval = resp.Config.SyncInterval
	}

	// Atualizar sync_enabled a partir do heartbeat para reacao rapida ao toggle no frontend
	if resp.Config != nil {
		prev := s.syncEnabled
		s.syncEnabled = resp.Config.SyncEnabled
		if prev != s.syncEnabled {
			if s.syncEnabled {
				log.Println("[heartbeat] Sync habilitado pelo usuario.")
			} else {
				log.Println("[heartbeat] Sync desabilitado pelo usuario.")
			}
		}
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

// commandPollLoop faz polling rapido (3s) para comandos pendentes do frontend.
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
				switch pq.Type {
				case "discover":
					s.handleDiscoverCommand(pq)
				case "sync":
					s.handleSyncCommand(pq)
				case "test_extract":
					s.handleTestExtractCommand(pq)
				default:
					s.executeRemoteQuery(pq)
				}
			}
		}
	}
}

// handleDiscoverCommand executa re-discovery de schema e envia resultado para a API.
func (s *Syncer) handleDiscoverCommand(pq *api.PendingQuery) {
	log.Printf("[command] Re-discovery de schema solicitado (command #%d)", pq.ID)

	schema, err := db.DiscoverSchema(s.conn, s.cfg.Database.Driver)
	if err != nil {
		log.Printf("[command] Erro ao descobrir schema: %v", err)
		// Reportar erro para a API
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     fmt.Sprintf("Erro ao descobrir schema: %v", err),
		})
		return
	}

	totalCols := 0
	for _, t := range schema {
		totalCols += len(t.Columns)
	}
	log.Printf("[command] Encontradas %d tabelas com %d colunas.", len(schema), totalCols)

	result, err := s.client.PushSchema(schema)
	if err != nil {
		log.Printf("[command] Erro ao enviar schema: %v", err)
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     fmt.Sprintf("Erro ao enviar schema: %v", err),
		})
		return
	}

	log.Printf("[command] Schema re-descoberto e enviado! %d tabelas.", result.TablesCount)

	// Reportar sucesso
	s.client.PushQueryResult(&api.QueryResultRequest{
		CommandID:       pq.ID,
		Columns:         []string{"tables_count"},
		Rows:            []map[string]interface{}{{"tables_count": result.TablesCount}},
		RowCount:        1,
		ExecutionTimeMs: 0,
		Truncated:       false,
		MaxRows:         1,
	})
}

// handleSyncCommand executa sync manual solicitado pelo frontend.
func (s *Syncer) handleSyncCommand(pq *api.PendingQuery) {
	log.Printf("[command] Sync manual solicitado (command #%d)", pq.ID)

	start := time.Now()
	s.doSync()
	elapsed := time.Since(start)

	log.Printf("[command] Sync manual concluido em %.1fs (command #%d)", elapsed.Seconds(), pq.ID)

	s.client.PushQueryResult(&api.QueryResultRequest{
		CommandID:       pq.ID,
		Columns:         []string{"status", "duration_ms"},
		Rows:            []map[string]interface{}{{"status": "completed", "duration_ms": elapsed.Milliseconds()}},
		RowCount:        1,
		ExecutionTimeMs: float64(elapsed.Milliseconds()),
		Truncated:       false,
		MaxRows:         1,
	})
}

// syncLoop executa sincronizacoes no intervalo configurado.
// Se o schema nao estiver configurado, faz retry rapido (60s) ate encontrar.
func (s *Syncer) syncLoop(ctx context.Context) {
	interval := time.Duration(s.cfg.Sync.Interval) * time.Second
	if s.schemaConfig == nil {
		interval = 60 * time.Second
		log.Printf("[sync] Schema nao configurado. Retry a cada 60s ate configurar.")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hadConfig := s.schemaConfig != nil

			// Recarregar config para obter sync_enabled atualizado
			s.loadSchemaConfig()

			if !s.syncEnabled {
				if s.schemaConfig != nil {
					log.Println("[sync] Sync desabilitado pelo usuario. Pulando sync periodico.")
				}
				continue
			}

			s.doSync()
			hasConfig := s.schemaConfig != nil

			// Transicao: sem config → com config → mudar para intervalo normal
			if !hadConfig && hasConfig {
				ticker.Reset(time.Duration(s.cfg.Sync.Interval) * time.Second)
				log.Printf("[sync] Config detectado! Proximo sync em %ds.", s.cfg.Sync.Interval)
			}
			// Transicao: com config → sem config (removido) → retry rapido
			if hadConfig && !hasConfig {
				ticker.Reset(60 * time.Second)
				log.Printf("[sync] Config removido. Retry a cada 60s.")
			}
		}
	}
}

// doSync executa uma rodada de sincronizacao.
func (s *Syncer) doSync() {
	// Sempre recarregar config da API antes de cada sync
	// para pegar extraction_sql/mapeamento configurado pelo frontend.
	s.loadSchemaConfig()

	if s.schemaConfig == nil {
		log.Println("[sync] Schema nao configurado. Pulando sync. Execute 'discover' e configure no frontend.")
		return
	}

	// Modo XML: extrai XMLs de NF-e do banco e envia para parsing
	if s.schemaConfig.ExtractionMode == "xml" {
		s.doSyncXml()
		return
	}

	// Modo data (SQL customizado ou mapeamento)
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

// doSyncXml extrai XMLs de NF-e do banco local e envia para parsing pelo backend.
func (s *Syncer) doSyncXml() {
	xmlTable := s.schemaConfig.XmlTable
	xmlCol := s.schemaConfig.XmlColumn
	idCol := s.schemaConfig.IdColumn

	log.Println("[sync] Modo: XML extraction")
	log.Printf("[sync] Tabela: %s | Coluna XML: %s | ID: %s", xmlTable, xmlCol, idCol)

	// Escapar identificadores para prevenir SQL injection
	qTable, err := db.QuoteIdentifier(xmlTable, s.cfg.Database.Driver)
	if err != nil {
		log.Printf("[sync] Nome de tabela XML invalido %q: %v", xmlTable, err)
		return
	}
	qXmlCol, err := db.QuoteIdentifier(xmlCol, s.cfg.Database.Driver)
	if err != nil {
		log.Printf("[sync] Nome de coluna XML invalido %q: %v", xmlCol, err)
		return
	}
	qIdCol, err := db.QuoteIdentifier(idCol, s.cfg.Database.Driver)
	if err != nil {
		log.Printf("[sync] Nome de coluna ID invalido %q: %v", idCol, err)
		return
	}

	// Query para extrair XMLs nao-vazios
	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s IS NOT NULL AND LENGTH(%s) > 100 ORDER BY %s LIMIT 500",
		qIdCol, qXmlCol, qTable, qXmlCol, qXmlCol, qIdCol,
	)

	rows, err := db.ScanRows(s.conn, query)
	if err != nil {
		log.Printf("[sync] Erro ao buscar XMLs: %v", err)
		return
	}

	if len(rows) == 0 {
		log.Println("[sync] Nenhum XML encontrado.")
		return
	}

	log.Printf("[sync] %d XMLs encontrados. Enviando em batches de %d...", len(rows), s.cfg.Sync.BatchSize)

	totalAccepted := 0
	totalDuplicates := 0

	for i := 0; i < len(rows); i += s.cfg.Sync.BatchSize {
		end := i + s.cfg.Sync.BatchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[i:end]
		invoices := make([]map[string]interface{}, len(batch))

		invoices = invoices[:0] // reset slice, keep capacity
		skipped := 0
		for _, row := range batch {
			xmlContent := ""
			if v, ok := row[xmlCol]; ok {
				xmlContent = fmt.Sprintf("%v", v)
			}

			rowId := ""
			if v, ok := row[idCol]; ok {
				rowId = fmt.Sprintf("%v", v)
			}

			// Skip non-NF-e documents (CT-e, MDF-e, etc.)
			if !looksLikeNfeXml(xmlContent) {
				skipped++
				if skipped <= 3 {
					preview := xmlContent
					if len(preview) > 80 {
						preview = preview[:80] + "..."
					}
					log.Printf("[sync] XML filtrado (row %s): nao parece NF-e. Preview: %s", rowId, preview)
				}
				continue
			}

			invoices = append(invoices, map[string]interface{}{
				"xml":    xmlContent,
				"row_id": rowId,
			})
		}

		if len(invoices) == 0 {
			if skipped > 0 {
				log.Printf("[sync] Batch %d-%d: todos os %d XMLs filtrados (nenhum NF-e detectado)", i, end, skipped)
			}
			continue
		}

		result, err := s.client.PushXml(invoices)
		if err != nil {
			log.Printf("[sync] Erro ao enviar batch XML %d-%d: %v", i, end, err)
			continue
		}

		totalAccepted += result.Accepted
		totalDuplicates += result.Duplicates
	}

	s.lastSyncedAt = time.Now()
	s.saveCursor()

	log.Printf("[sync] XML sync concluido. Aceitas: %d, Duplicatas: %d", totalAccepted, totalDuplicates)
}

// fetchRows busca rows do banco local usando o schema_config.
func (s *Syncer) fetchRows() ([]map[string]interface{}, error) {
	// Modo SQL customizado: usar a query definida pelo usuario no frontend
	if s.schemaConfig.ExtractionSql != "" {
		log.Printf("[sync] Usando query de extracao customizada")

		// Validar que a query e um SELECT seguro (sem multi-statement, sem keywords perigosas)
		if err := db.ValidateSelectQuery(s.schemaConfig.ExtractionSql); err != nil {
			return nil, fmt.Errorf("query customizada rejeitada: %w", err)
		}

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

	// Escapar nomes de tabela e coluna para prevenir SQL injection
	quotedTable, err := db.QuoteIdentifier(invoiceTable, s.cfg.Database.Driver)
	if err != nil {
		return nil, fmt.Errorf("nome de tabela invalido %q: %w", invoiceTable, err)
	}
	quotedDateCol, err := db.QuoteIdentifier(dateColumn, s.cfg.Database.Driver)
	if err != nil {
		return nil, fmt.Errorf("nome de coluna invalido %q: %w", dateColumn, err)
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s >= ? ORDER BY %s LIMIT 10000",
		quotedTable, quotedDateCol, quotedDateCol)

	// PostgreSQL usa $1 ao inves de ?
	if s.cfg.Database.Driver == "pgsql" {
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1 ORDER BY %s LIMIT 10000",
			quotedTable, quotedDateCol, quotedDateCol)
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

// handleTestExtractCommand extrai uma amostra (3 registros) e envia para
// dry-run de normalizacao via /test-push. O resultado e reportado como command result.
func (s *Syncer) handleTestExtractCommand(pq *api.PendingQuery) {
	log.Printf("[command] Test extract solicitado (command #%d)", pq.ID)

	// Recarregar config para ter mapeamento mais recente
	s.loadSchemaConfig()

	if s.schemaConfig == nil {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     "Schema nao configurado. Configure o mapeamento no frontend antes de testar.",
		})
		return
	}

	start := time.Now()

	if s.schemaConfig.ExtractionMode == "xml" {
		s.doTestExtractXml(pq)
	} else {
		s.doTestExtractData(pq)
	}

	elapsed := time.Since(start)
	log.Printf("[command] Test extract concluido em %.1fs (command #%d)", elapsed.Seconds(), pq.ID)
}

// doTestExtractData extrai 3 rows em modo data e envia para test-push.
func (s *Syncer) doTestExtractData(pq *api.PendingQuery) {
	var rows []map[string]interface{}
	var err error

	if s.schemaConfig.ExtractionSql != "" {
		// Validar que a query e um SELECT seguro
		if valErr := db.ValidateSelectQuery(s.schemaConfig.ExtractionSql); valErr != nil {
			s.client.PushQueryResult(&api.QueryResultRequest{
				CommandID: pq.ID,
				Error:     fmt.Sprintf("Query customizada rejeitada: %v", valErr),
			})
			return
		}
		// Executar SQL customizado com LIMIT 3
		rows, err = db.ScanRows(s.conn, s.schemaConfig.ExtractionSql+" LIMIT 3")
	} else {
		invoiceTable := s.schemaConfig.Tables["invoices"]
		if invoiceTable == "" {
			s.client.PushQueryResult(&api.QueryResultRequest{
				CommandID: pq.ID,
				Error:     "Tabela de invoices nao configurada no mapeamento.",
			})
			return
		}
		// Escapar nome da tabela
		quotedTable, quotErr := db.QuoteIdentifier(invoiceTable, s.cfg.Database.Driver)
		if quotErr != nil {
			s.client.PushQueryResult(&api.QueryResultRequest{
				CommandID: pq.ID,
				Error:     fmt.Sprintf("Nome de tabela invalido %q: %v", invoiceTable, quotErr),
			})
			return
		}
		query := fmt.Sprintf("SELECT * FROM %s LIMIT 3", quotedTable)
		rows, err = db.ScanRows(s.conn, query)
	}

	if err != nil {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     fmt.Sprintf("Erro ao extrair amostra: %v", err),
		})
		return
	}

	if len(rows) == 0 {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     "Nenhum registro encontrado na tabela configurada.",
		})
		return
	}

	invoices := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		invoices[i] = map[string]interface{}{
			"invoice": row,
			"items":   []interface{}{},
		}
	}

	result, err := s.client.TestPush(invoices, "")
	if err != nil {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     fmt.Sprintf("Erro ao enviar test-push: %v", err),
		})
		return
	}

	// Converter resultado do test-push para command result
	s.client.PushQueryResult(&api.QueryResultRequest{
		CommandID: pq.ID,
		Columns:   []string{"tested", "valid", "errors"},
		Rows: []map[string]interface{}{{
			"tested": result.Tested,
			"valid":  result.Valid,
			"errors": result.Errors,
		}},
		RowCount: 1,
	})
}

// doTestExtractXml extrai 3 XMLs e envia para test-push em modo xml.
func (s *Syncer) doTestExtractXml(pq *api.PendingQuery) {
	xmlTable := s.schemaConfig.XmlTable
	xmlCol := s.schemaConfig.XmlColumn
	idCol := s.schemaConfig.IdColumn

	if xmlTable == "" || xmlCol == "" {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     "Tabela ou coluna XML nao configurada.",
		})
		return
	}

	// Escapar identificadores
	qTable, quotErr := db.QuoteIdentifier(xmlTable, s.cfg.Database.Driver)
	if quotErr != nil {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     fmt.Sprintf("Nome de tabela invalido %q: %v", xmlTable, quotErr),
		})
		return
	}
	qXmlCol, quotErr := db.QuoteIdentifier(xmlCol, s.cfg.Database.Driver)
	if quotErr != nil {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     fmt.Sprintf("Nome de coluna invalido %q: %v", xmlCol, quotErr),
		})
		return
	}
	qIdCol := qTable // fallback
	if idCol != "" {
		qIdCol, quotErr = db.QuoteIdentifier(idCol, s.cfg.Database.Driver)
		if quotErr != nil {
			s.client.PushQueryResult(&api.QueryResultRequest{
				CommandID: pq.ID,
				Error:     fmt.Sprintf("Nome de coluna ID invalido %q: %v", idCol, quotErr),
			})
			return
		}
	}

	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s IS NOT NULL AND LENGTH(%s) > 100 LIMIT 3",
		qIdCol, qXmlCol, qTable, qXmlCol, qXmlCol,
	)

	rows, err := db.ScanRows(s.conn, query)
	if err != nil {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     fmt.Sprintf("Erro ao extrair XMLs: %v", err),
		})
		return
	}

	if len(rows) == 0 {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     "Nenhum XML encontrado na tabela/coluna configurada.",
		})
		return
	}

	invoices := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		xmlContent := ""
		if v, ok := row[xmlCol]; ok {
			xmlContent = fmt.Sprintf("%v", v)
		}
		rowId := ""
		if v, ok := row[idCol]; ok {
			rowId = fmt.Sprintf("%v", v)
		}
		invoices[i] = map[string]interface{}{
			"xml":    xmlContent,
			"row_id": rowId,
		}
	}

	result, err := s.client.TestPush(invoices, "xml")
	if err != nil {
		s.client.PushQueryResult(&api.QueryResultRequest{
			CommandID: pq.ID,
			Error:     fmt.Sprintf("Erro ao enviar test-push XML: %v", err),
		})
		return
	}

	s.client.PushQueryResult(&api.QueryResultRequest{
		CommandID: pq.ID,
		Columns:   []string{"tested", "valid", "errors"},
		Rows: []map[string]interface{}{{
			"tested": result.Tested,
			"valid":  result.Valid,
			"errors": result.Errors,
		}},
		RowCount: 1,
	})
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

// containsNfeMarkers checks for NF-e XML tags in plain text.
func containsNfeMarkers(s string) bool {
	return strings.Contains(s, "<nfeProc") ||
		strings.Contains(s, "<NFe") ||
		strings.Contains(s, "<infNFe") ||
		strings.Contains(s, "nfe:infNFe")
}

// looksLikeNfeXml checks if the XML content looks like an NF-e document.
// Handles plain XML, base64-encoded XML, and gzip+base64-encoded XML.
// Returns false for CT-e, MDF-e, NFC-e and other non-NF-e fiscal documents.
func looksLikeNfeXml(content string) bool {
	trimmed := strings.TrimSpace(content)

	// Plain XML — check directly
	if strings.HasPrefix(trimmed, "<") {
		return containsNfeMarkers(content)
	}

	// Try base64 decode
	decoded, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(trimmed)
	}
	if err != nil || len(decoded) == 0 {
		return false
	}

	// Check if decoded is gzip (magic bytes: 0x1f 0x8b)
	if len(decoded) >= 2 && decoded[0] == 0x1f && decoded[1] == 0x8b {
		gr, err := gzip.NewReader(bytes.NewReader(decoded))
		if err != nil {
			return false
		}
		defer gr.Close()
		decompressed, err := io.ReadAll(gr)
		if err != nil {
			return false
		}
		return containsNfeMarkers(string(decompressed))
	}

	// Base64 without gzip
	return containsNfeMarkers(string(decoded))
}
