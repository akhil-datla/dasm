package compiler

import (
	"strings"
	"unicode"
)

// TokenType represents the type of a token.
type TokenType uint8

const (
	TokenEOF TokenType = iota
	TokenNewline
	TokenIdent   // Opcode names
	TokenInt     // Integer literals
	TokenFloat   // Float literals
	TokenString  // "quoted strings"
	TokenComma   // ,
	TokenColon   // : (for labels)
	TokenComment // ; comment
	TokenRegR    // R0-R15
	TokenRegV    // V0-V7
	TokenRegF    // F0-F15
)

// String returns the string representation of a token type.
func (t TokenType) String() string {
	switch t {
	case TokenEOF:
		return "EOF"
	case TokenNewline:
		return "NEWLINE"
	case TokenIdent:
		return "IDENT"
	case TokenInt:
		return "INT"
	case TokenFloat:
		return "FLOAT"
	case TokenString:
		return "STRING"
	case TokenComma:
		return "COMMA"
	case TokenColon:
		return "COLON"
	case TokenComment:
		return "COMMENT"
	case TokenRegR:
		return "REG_R"
	case TokenRegV:
		return "REG_V"
	case TokenRegF:
		return "REG_F"
	default:
		return "UNKNOWN"
	}
}

// Token represents a lexical token.
type Token struct {
	Type  TokenType
	Value string
	Line  int
}

// Lexer tokenizes DFL assembly source code.
type Lexer struct {
	input  string
	pos    int
	line   int
	tokens []Token
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		pos:    0,
		line:   1,
		tokens: []Token{},
	}
}

// Tokenize tokenizes the entire input and returns the tokens.
func (l *Lexer) Tokenize() []Token {
	for l.pos < len(l.input) {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			break
		}

		ch := l.input[l.pos]

		switch {
		case ch == '\n':
			l.tokens = append(l.tokens, Token{Type: TokenNewline, Value: "\n", Line: l.line})
			l.line++
			l.pos++

		case ch == ';':
			// Comment - skip to end of line
			start := l.pos
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.pos++
			}
			// We could add the comment token, but we'll skip it for now
			_ = start

		case ch == ',':
			l.tokens = append(l.tokens, Token{Type: TokenComma, Value: ",", Line: l.line})
			l.pos++

		case ch == ':':
			l.tokens = append(l.tokens, Token{Type: TokenColon, Value: ":", Line: l.line})
			l.pos++

		case ch == '"':
			l.scanString()

		case ch == '-' || unicode.IsDigit(rune(ch)):
			l.scanNumber()

		case unicode.IsLetter(rune(ch)) || ch == '_':
			l.scanIdentOrRegister()

		default:
			// Unknown character, skip it
			l.pos++
		}
	}

	l.tokens = append(l.tokens, Token{Type: TokenEOF, Value: "", Line: l.line})
	return l.tokens
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\r' {
			l.pos++
		} else {
			break
		}
	}
}

func (l *Lexer) scanString() {
	l.pos++ // Skip opening quote
	start := l.pos

	for l.pos < len(l.input) && l.input[l.pos] != '"' {
		if l.input[l.pos] == '\n' {
			l.line++
		}
		l.pos++
	}

	value := l.input[start:l.pos]
	l.tokens = append(l.tokens, Token{Type: TokenString, Value: value, Line: l.line})

	if l.pos < len(l.input) {
		l.pos++ // Skip closing quote
	}
}

func (l *Lexer) scanNumber() {
	start := l.pos
	isFloat := false

	// Handle negative sign
	if l.input[l.pos] == '-' {
		l.pos++
	}

	// Scan digits
	for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
		l.pos++
	}

	// Check for decimal point
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		isFloat = true
		l.pos++

		// Scan decimal digits
		for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
			l.pos++
		}
	}

	value := l.input[start:l.pos]

	if isFloat {
		l.tokens = append(l.tokens, Token{Type: TokenFloat, Value: value, Line: l.line})
	} else {
		l.tokens = append(l.tokens, Token{Type: TokenInt, Value: value, Line: l.line})
	}
}

func (l *Lexer) scanIdentOrRegister() {
	start := l.pos

	// First character
	l.pos++

	// Continue with alphanumeric or underscore
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' {
			l.pos++
		} else {
			break
		}
	}

	value := l.input[start:l.pos]

	// Check if it's a register
	tokenType := l.classifyIdentOrRegister(value)
	l.tokens = append(l.tokens, Token{Type: tokenType, Value: value, Line: l.line})
}

func (l *Lexer) classifyIdentOrRegister(value string) TokenType {
	upper := strings.ToUpper(value)

	// Check for R registers (R0-R15)
	if len(upper) >= 2 && upper[0] == 'R' && unicode.IsDigit(rune(upper[1])) {
		return TokenRegR
	}

	// Check for V registers (V0-V7)
	if len(upper) >= 2 && upper[0] == 'V' && unicode.IsDigit(rune(upper[1])) {
		return TokenRegV
	}

	// Check for F registers (F0-F15)
	if len(upper) >= 2 && upper[0] == 'F' && unicode.IsDigit(rune(upper[1])) {
		return TokenRegF
	}

	return TokenIdent
}
