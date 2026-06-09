package db

import (
	"fmt"
	"regexp"
	"strings"
)

// dangerousKeywords are SQL keywords that should never appear in user-supplied queries.
// The agent should only execute read-only operations on the client's database.
var dangerousKeywords = []string{
	"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
	"CREATE", "REPLACE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
	"CALL", "MERGE", "UPSERT", "RENAME", "LOAD", "INTO OUTFILE",
	"INTO DUMPFILE",
}

// identifierRegex validates that a SQL identifier (table/column name) contains
// only safe characters: letters, digits, underscores, dots (for schema.table).
var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

// ValidateSelectQuery checks that a SQL string is a safe, single-statement SELECT query.
// Returns an error if the query contains dangerous keywords or multiple statements.
func ValidateSelectQuery(sql string) error {
	trimmed := strings.TrimSpace(sql)

	if trimmed == "" {
		return fmt.Errorf("query vazia")
	}

	// Must start with a read-only verb (case-insensitive): SELECT, SHOW, EXPLAIN, DESCRIBE, DESC
	upper := strings.ToUpper(trimmed)
	allowedPrefixes := []string{"SELECT", "SHOW", "EXPLAIN", "DESCRIBE", "DESC ", "WITH"}
	prefixOK := false
	for _, p := range allowedPrefixes {
		if strings.HasPrefix(upper, p) {
			prefixOK = true
			break
		}
	}
	if !prefixOK {
		return fmt.Errorf("apenas queries SELECT/SHOW/EXPLAIN/DESCRIBE/WITH são permitidas")
	}

	// Strip string literals to avoid false positives on keywords inside strings
	// Replace 'anything' and "anything" with empty string for keyword scanning
	cleaned := stripStringLiterals(upper)

	// Strip trailing semicolons (harmless, common in saved queries)
	cleaned = strings.TrimRight(cleaned, "; \t\n\r")

	// Reject multi-statement queries (semicolons not inside strings)
	if strings.Contains(cleaned, ";") {
		return fmt.Errorf("queries com múltiplos statements (;) não são permitidas")
	}

	// Check for dangerous keywords
	for _, keyword := range dangerousKeywords {
		// Use word boundary matching to avoid false positives
		// e.g., "UPDATED_AT" should not match "UPDATE"
		pattern := fmt.Sprintf(`\b%s\b`, keyword)
		matched, _ := regexp.MatchString(pattern, cleaned)
		if matched {
			return fmt.Errorf("keyword proibida detectada: %s — apenas SELECT é permitido", keyword)
		}
	}

	return nil
}

// QuoteIdentifier safely quotes a SQL identifier (table or column name).
// Uses backticks for MySQL and double quotes for PostgreSQL.
// Returns an error if the identifier contains invalid characters.
func QuoteIdentifier(identifier string, driver string) (string, error) {
	if identifier == "" {
		return "", fmt.Errorf("identificador vazio")
	}

	// Validate that identifier only contains safe characters
	if !identifierRegex.MatchString(identifier) {
		return "", fmt.Errorf("identificador contém caracteres inválidos: %q", identifier)
	}

	// Handle schema.table notation (e.g., "dbo.invoices")
	parts := strings.Split(identifier, ".")
	quoted := make([]string, len(parts))

	for i, part := range parts {
		if !identifierRegex.MatchString(part) {
			return "", fmt.Errorf("parte do identificador contém caracteres inválidos: %q", part)
		}

		switch driver {
		case "pgsql":
			// PostgreSQL uses double quotes
			quoted[i] = `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
		default:
			// MySQL uses backticks
			quoted[i] = "`" + strings.ReplaceAll(part, "`", "``") + "`"
		}
	}

	return strings.Join(quoted, "."), nil
}

// stripStringLiterals removes string literals from SQL to avoid false positive
// keyword detection inside quoted strings.
func stripStringLiterals(sql string) string {
	result := make([]byte, 0, len(sql))
	inSingle := false
	inDouble := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}

		if !inSingle && !inDouble {
			result = append(result, ch)
		}
	}

	return string(result)
}
