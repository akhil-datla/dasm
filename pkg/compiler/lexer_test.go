package compiler

import (
	"testing"
)

func TestLexer_BasicTokens(t *testing.T) {
	input := `LOAD_CSV R0, "test.csv"`

	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	expected := []TokenType{TokenIdent, TokenRegR, TokenComma, TokenString, TokenEOF}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}

	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("token %d: expected %v, got %v", i, expected[i], tok.Type)
		}
	}
}

func TestLexer_Registers(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"R0", TokenRegR},
		{"R15", TokenRegR},
		{"V0", TokenRegV},
		{"V7", TokenRegV},
		{"F0", TokenRegF},
		{"F15", TokenRegF},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tokens := lexer.Tokenize()

			if len(tokens) < 1 {
				t.Fatal("expected at least one token")
			}
			if tokens[0].Type != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, tokens[0].Type)
			}
		})
	}
}

func TestLexer_Numbers(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"42", TokenInt},
		{"-42", TokenInt},
		{"3.14", TokenFloat},
		{"-3.14", TokenFloat},
		{"0", TokenInt},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tokens := lexer.Tokenize()

			if len(tokens) < 1 {
				t.Fatal("expected at least one token")
			}
			if tokens[0].Type != tt.expected {
				t.Errorf("expected %v, got %v (value: %q)", tt.expected, tokens[0].Type, tokens[0].Value)
			}
		})
	}
}

func TestLexer_Strings(t *testing.T) {
	input := `"hello world"`

	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	if len(tokens) < 1 {
		t.Fatal("expected at least one token")
	}
	if tokens[0].Type != TokenString {
		t.Errorf("expected TokenString, got %v", tokens[0].Type)
	}
	if tokens[0].Value != "hello world" {
		t.Errorf("expected 'hello world', got %q", tokens[0].Value)
	}
}

func TestLexer_Comments(t *testing.T) {
	input := `LOAD_CSV R0, "test.csv" ; this is a comment
SELECT_COL V0, R0, "price"`

	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	// Should have tokens for both lines, comment should be skipped or captured
	identCount := 0
	for _, tok := range tokens {
		if tok.Type == TokenIdent {
			identCount++
		}
	}

	if identCount != 2 {
		t.Errorf("expected 2 identifiers, got %d", identCount)
	}
}

func TestLexer_MultipleLines(t *testing.T) {
	input := `LOAD_CSV R0, "test.csv"
SELECT_COL V0, R0, "price"
HALT R0`

	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	identCount := 0
	newlineCount := 0
	for _, tok := range tokens {
		if tok.Type == TokenIdent {
			identCount++
		}
		if tok.Type == TokenNewline {
			newlineCount++
		}
	}

	if identCount != 3 {
		t.Errorf("expected 3 identifiers, got %d", identCount)
	}
	if newlineCount != 2 {
		t.Errorf("expected 2 newlines, got %d", newlineCount)
	}
}

func TestLexer_FullProgram(t *testing.T) {
	input := `; Calculate sum of prices where quantity > 10
LOAD_CSV      R0, "test.csv"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
LOAD_CONST    R1, 10
BROADCAST     V2, R1, V1
CMP_GT        V3, V1, V2
FILTER        V4, V0, V3
REDUCE_SUM_F  F0, V4
HALT_F        F0`

	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	// Count identifiers (opcodes)
	identCount := 0
	for _, tok := range tokens {
		if tok.Type == TokenIdent {
			identCount++
		}
	}

	if identCount != 9 {
		t.Errorf("expected 9 identifiers, got %d", identCount)
	}
}

func TestLexer_TokenLine(t *testing.T) {
	input := `LOAD_CSV R0, "test.csv"
SELECT_COL V0, R0, "price"`

	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	// First token should be on line 1
	if tokens[0].Line != 1 {
		t.Errorf("expected line 1, got %d", tokens[0].Line)
	}

	// Find first token on line 2
	for _, tok := range tokens {
		if tok.Type == TokenIdent && tok.Value == "SELECT_COL" {
			if tok.Line != 2 {
				t.Errorf("expected SELECT_COL on line 2, got %d", tok.Line)
			}
			break
		}
	}
}
