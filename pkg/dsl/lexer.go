package dsl

import (
	"strings"
	"unicode"
)

// Lexer tokenizes DFL high-level DSL source code.
type Lexer struct {
	input  string
	pos    int
	line   int
	col    int
	tokens []Token
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		pos:    0,
		line:   1,
		col:    1,
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

		ch := l.peek()

		switch {
		case ch == '\n':
			l.tokens = append(l.tokens, Token{Type: TokenNewline, Value: "\n", Line: l.line, Col: l.col})
			l.advance()
			l.line++
			l.col = 1

		case ch == '#':
			// Comment - skip to end of line
			l.scanComment()

		case ch == '"' || ch == '\'':
			l.scanString(ch)

		case ch == '|':
			if l.peekNext() == '>' {
				l.tokens = append(l.tokens, Token{Type: TokenPipe, Value: "|>", Line: l.line, Col: l.col})
				l.advance()
				l.advance()
			} else if l.peekNext() == '|' {
				l.tokens = append(l.tokens, Token{Type: TokenOr, Value: "||", Line: l.line, Col: l.col})
				l.advance()
				l.advance()
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenOr, Value: "|", Line: l.line, Col: l.col})
				l.advance()
			}

		case ch == '&':
			if l.peekNext() == '&' {
				l.tokens = append(l.tokens, Token{Type: TokenAnd, Value: "&&", Line: l.line, Col: l.col})
				l.advance()
				l.advance()
			} else {
				l.advance() // Skip unknown
			}

		case ch == '=':
			if l.peekNext() == '=' {
				l.tokens = append(l.tokens, Token{Type: TokenEQ, Value: "==", Line: l.line, Col: l.col})
				l.advance()
				l.advance()
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenAssign, Value: "=", Line: l.line, Col: l.col})
				l.advance()
			}

		case ch == '!':
			if l.peekNext() == '=' {
				l.tokens = append(l.tokens, Token{Type: TokenNE, Value: "!=", Line: l.line, Col: l.col})
				l.advance()
				l.advance()
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenNot, Value: "!", Line: l.line, Col: l.col})
				l.advance()
			}

		case ch == '<':
			if l.peekNext() == '=' {
				l.tokens = append(l.tokens, Token{Type: TokenLE, Value: "<=", Line: l.line, Col: l.col})
				l.advance()
				l.advance()
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenLT, Value: "<", Line: l.line, Col: l.col})
				l.advance()
			}

		case ch == '>':
			if l.peekNext() == '=' {
				l.tokens = append(l.tokens, Token{Type: TokenGE, Value: ">=", Line: l.line, Col: l.col})
				l.advance()
				l.advance()
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenGT, Value: ">", Line: l.line, Col: l.col})
				l.advance()
			}

		case ch == '+':
			l.tokens = append(l.tokens, Token{Type: TokenPlus, Value: "+", Line: l.line, Col: l.col})
			l.advance()

		case ch == '-':
			// Check if it's a negative number
			if unicode.IsDigit(rune(l.peekNext())) {
				l.scanNumber()
			} else {
				l.tokens = append(l.tokens, Token{Type: TokenMinus, Value: "-", Line: l.line, Col: l.col})
				l.advance()
			}

		case ch == '*':
			l.tokens = append(l.tokens, Token{Type: TokenStar, Value: "*", Line: l.line, Col: l.col})
			l.advance()

		case ch == '/':
			l.tokens = append(l.tokens, Token{Type: TokenSlash, Value: "/", Line: l.line, Col: l.col})
			l.advance()

		case ch == '%':
			l.tokens = append(l.tokens, Token{Type: TokenPercent, Value: "%", Line: l.line, Col: l.col})
			l.advance()

		case ch == '(':
			l.tokens = append(l.tokens, Token{Type: TokenLParen, Value: "(", Line: l.line, Col: l.col})
			l.advance()

		case ch == ')':
			l.tokens = append(l.tokens, Token{Type: TokenRParen, Value: ")", Line: l.line, Col: l.col})
			l.advance()

		case ch == '[':
			l.tokens = append(l.tokens, Token{Type: TokenLBracket, Value: "[", Line: l.line, Col: l.col})
			l.advance()

		case ch == ']':
			l.tokens = append(l.tokens, Token{Type: TokenRBracket, Value: "]", Line: l.line, Col: l.col})
			l.advance()

		case ch == ',':
			l.tokens = append(l.tokens, Token{Type: TokenComma, Value: ",", Line: l.line, Col: l.col})
			l.advance()

		case ch == ':':
			l.tokens = append(l.tokens, Token{Type: TokenColon, Value: ":", Line: l.line, Col: l.col})
			l.advance()

		case ch == '{':
			l.tokens = append(l.tokens, Token{Type: TokenLBrace, Value: "{", Line: l.line, Col: l.col})
			l.advance()

		case ch == '}':
			l.tokens = append(l.tokens, Token{Type: TokenRBrace, Value: "}", Line: l.line, Col: l.col})
			l.advance()

		case ch == '.':
			l.tokens = append(l.tokens, Token{Type: TokenDot, Value: ".", Line: l.line, Col: l.col})
			l.advance()

		case unicode.IsDigit(rune(ch)):
			l.scanNumber()

		case unicode.IsLetter(rune(ch)) || ch == '_':
			l.scanIdentifier()

		default:
			// Unknown character, skip it
			l.advance()
		}
	}

	l.tokens = append(l.tokens, Token{Type: TokenEOF, Value: "", Line: l.line, Col: l.col})
	return l.tokens
}

func (l *Lexer) peek() byte {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

func (l *Lexer) peekNext() byte {
	if l.pos+1 >= len(l.input) {
		return 0
	}
	return l.input[l.pos+1]
}

func (l *Lexer) advance() {
	if l.pos < len(l.input) {
		l.pos++
		l.col++
	}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\r' {
			l.col++
			l.pos++
		} else {
			break
		}
	}
}

func (l *Lexer) scanComment() {
	// Skip # and everything until newline
	for l.pos < len(l.input) && l.input[l.pos] != '\n' {
		l.pos++
		l.col++
	}
}

func (l *Lexer) scanString(quote byte) {
	startCol := l.col
	l.advance() // Skip opening quote
	start := l.pos

	for l.pos < len(l.input) && l.input[l.pos] != quote {
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 0
		}
		l.advance()
	}

	value := l.input[start:l.pos]
	l.tokens = append(l.tokens, Token{Type: TokenString, Value: value, Line: l.line, Col: startCol})

	if l.pos < len(l.input) {
		l.advance() // Skip closing quote
	}
}

func (l *Lexer) scanNumber() {
	startCol := l.col
	start := l.pos
	isFloat := false

	// Handle negative sign
	if l.peek() == '-' {
		l.advance()
	}

	// Scan digits
	for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
		l.advance()
	}

	// Check for decimal point
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		// Look ahead to make sure it's not a method call
		if l.pos+1 < len(l.input) && unicode.IsDigit(rune(l.input[l.pos+1])) {
			isFloat = true
			l.advance() // consume the dot

			// Scan decimal digits
			for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
				l.advance()
			}
		}
	}

	value := l.input[start:l.pos]

	if isFloat {
		l.tokens = append(l.tokens, Token{Type: TokenFloat, Value: value, Line: l.line, Col: startCol})
	} else {
		l.tokens = append(l.tokens, Token{Type: TokenInt, Value: value, Line: l.line, Col: startCol})
	}
}

func (l *Lexer) scanIdentifier() {
	startCol := l.col
	start := l.pos

	// First character
	l.advance()

	// Continue with alphanumeric or underscore
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' {
			l.advance()
		} else {
			break
		}
	}

	value := l.input[start:l.pos]
	tokenType := LookupIdent(strings.ToLower(value))
	l.tokens = append(l.tokens, Token{Type: tokenType, Value: value, Line: l.line, Col: startCol})
}
