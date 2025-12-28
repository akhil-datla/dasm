package compiler

import (
	"fmt"
	"strconv"
)

// OperandType represents the type of an operand.
type OperandType uint8

const (
	OperandRegR OperandType = iota
	OperandRegV
	OperandRegF
	OperandInt
	OperandFloat
	OperandString
)

// Operand represents an instruction operand.
type Operand struct {
	Type     OperandType
	RegNum   uint8   // For registers
	IntVal   int64   // For integer literals
	FloatVal float64 // For float literals
	StrVal   string  // For string literals
}

// AsmInstruction represents a parsed assembly instruction.
type AsmInstruction struct {
	Opcode   string
	Operands []Operand
	Line     int
}

// AsmProgram represents a parsed assembly program.
type AsmProgram struct {
	Instructions []AsmInstruction
	Labels       map[string]int // label -> instruction index
}

// Parser parses DFL assembly source code.
type Parser struct {
	tokens  []Token
	pos     int
	program *AsmProgram
}

// NewParser creates a new parser for the given input.
func NewParser(input string) *Parser {
	lexer := NewLexer(input)
	tokens := lexer.Tokenize()
	return &Parser{
		tokens: tokens,
		pos:    0,
		program: &AsmProgram{
			Instructions: []AsmInstruction{},
			Labels:       make(map[string]int),
		},
	}
}

// Parse parses the entire input and returns the program.
func (p *Parser) Parse() (*AsmProgram, error) {
	for p.pos < len(p.tokens) {
		tok := p.tokens[p.pos]

		switch tok.Type {
		case TokenEOF:
			return p.program, nil

		case TokenNewline:
			p.pos++

		case TokenIdent:
			// This is an opcode
			inst, err := p.parseInstruction()
			if err != nil {
				return nil, err
			}
			p.program.Instructions = append(p.program.Instructions, inst)

		default:
			// Skip unknown tokens
			p.pos++
		}
	}

	return p.program, nil
}

func (p *Parser) parseInstruction() (AsmInstruction, error) {
	inst := AsmInstruction{
		Opcode:   p.tokens[p.pos].Value,
		Line:     p.tokens[p.pos].Line,
		Operands: []Operand{},
	}
	p.pos++ // Consume opcode

	// Parse operands until newline or EOF
	for p.pos < len(p.tokens) {
		tok := p.tokens[p.pos]

		if tok.Type == TokenNewline || tok.Type == TokenEOF {
			break
		}

		if tok.Type == TokenComma {
			p.pos++
			continue
		}

		operand, err := p.parseOperand()
		if err != nil {
			return inst, err
		}
		inst.Operands = append(inst.Operands, operand)
	}

	return inst, nil
}

func (p *Parser) parseOperand() (Operand, error) {
	tok := p.tokens[p.pos]

	switch tok.Type {
	case TokenRegR:
		regNum, err := p.parseRegisterNumber(tok.Value, 1)
		if err != nil {
			return Operand{}, err
		}
		p.pos++
		return Operand{Type: OperandRegR, RegNum: regNum}, nil

	case TokenRegV:
		regNum, err := p.parseRegisterNumber(tok.Value, 1)
		if err != nil {
			return Operand{}, err
		}
		p.pos++
		return Operand{Type: OperandRegV, RegNum: regNum}, nil

	case TokenRegF:
		regNum, err := p.parseRegisterNumber(tok.Value, 1)
		if err != nil {
			return Operand{}, err
		}
		p.pos++
		return Operand{Type: OperandRegF, RegNum: regNum}, nil

	case TokenInt:
		intVal, err := strconv.ParseInt(tok.Value, 10, 64)
		if err != nil {
			return Operand{}, fmt.Errorf("line %d: invalid integer: %s", tok.Line, tok.Value)
		}
		p.pos++
		return Operand{Type: OperandInt, IntVal: intVal}, nil

	case TokenFloat:
		floatVal, err := strconv.ParseFloat(tok.Value, 64)
		if err != nil {
			return Operand{}, fmt.Errorf("line %d: invalid float: %s", tok.Line, tok.Value)
		}
		p.pos++
		return Operand{Type: OperandFloat, FloatVal: floatVal}, nil

	case TokenString:
		p.pos++
		return Operand{Type: OperandString, StrVal: tok.Value}, nil

	default:
		return Operand{}, fmt.Errorf("line %d: unexpected token: %s", tok.Line, tok.Value)
	}
}

func (p *Parser) parseRegisterNumber(value string, offset int) (uint8, error) {
	if len(value) <= offset {
		return 0, fmt.Errorf("invalid register: %s", value)
	}
	numStr := value[offset:]
	num, err := strconv.ParseUint(numStr, 10, 8)
	if err != nil {
		return 0, fmt.Errorf("invalid register number: %s", value)
	}
	return uint8(num), nil
}
