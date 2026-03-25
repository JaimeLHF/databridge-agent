package wizard

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/term"
)

var reader = bufio.NewReader(os.Stdin)

// Prompt le uma linha do usuario com valor default opcional.
// Exibe: "label [default]: " e retorna o valor digitado ou o default.
func Prompt(label, defaultVal string) string {
	if defaultVal != "" {
		fmt.Printf("%s [%s]: ", label, defaultVal)
	} else {
		fmt.Printf("%s: ", label)
	}

	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)

	if line == "" {
		return defaultVal
	}
	return line
}

// PromptInt le um inteiro do usuario com valor default.
func PromptInt(label string, defaultVal int) int {
	result := Prompt(label, strconv.Itoa(defaultVal))
	val, err := strconv.Atoi(result)
	if err != nil {
		return defaultVal
	}
	return val
}

// PromptPassword le uma senha sem exibir no terminal.
// Fallback para leitura normal se terminal nao suportar.
func PromptPassword(label string) string {
	fmt.Printf("%s: ", label)

	// Tentar ler sem echo via x/term
	if fd := int(os.Stdin.Fd()); term.IsTerminal(fd) {
		pass, err := term.ReadPassword(fd)
		fmt.Println() // nova linha apos digitar
		if err == nil {
			return strings.TrimSpace(string(pass))
		}
	}

	// Fallback: leitura normal
	line, _ := reader.ReadString('\n')
	return strings.TrimSpace(line)
}

// PromptSelect mostra opcoes numeradas e retorna o valor escolhido.
// options e um slice de pares [label, valor].
func PromptSelect(label string, options []SelectOption, defaultIdx int) string {
	fmt.Printf("%s:\n", label)
	for i, opt := range options {
		fmt.Printf("  %d) %s\n", i+1, opt.Label)
	}

	defaultStr := strconv.Itoa(defaultIdx + 1)
	choice := Prompt("Escolha", defaultStr)

	idx, err := strconv.Atoi(choice)
	if err != nil || idx < 1 || idx > len(options) {
		return options[defaultIdx].Value
	}
	return options[idx-1].Value
}

// SelectOption representa uma opcao no PromptSelect.
type SelectOption struct {
	Label string
	Value string
}

// Confirm pergunta S/n ou s/N e retorna true/false.
func Confirm(label string, defaultYes bool) bool {
	suffix := "S/n"
	if !defaultYes {
		suffix = "s/N"
	}

	fmt.Printf("%s [%s]: ", label, suffix)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(strings.ToLower(line))

	if line == "" {
		return defaultYes
	}

	return line == "s" || line == "sim" || line == "y" || line == "yes"
}

// Section imprime um cabecalho de secao.
func Section(title string) {
	fmt.Printf("\n=== %s ===\n\n", title)
}

// Success imprime uma mensagem de sucesso.
func Success(msg string) {
	fmt.Printf("OK! %s\n", msg)
}

// Warn imprime um aviso.
func Warn(msg string) {
	fmt.Printf("AVISO: %s\n", msg)
}

// Error imprime um erro.
func Error(msg string) {
	fmt.Printf("ERRO: %s\n", msg)
}
