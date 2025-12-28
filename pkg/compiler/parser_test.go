package compiler

import (
	"testing"
)

func TestParser_SimpleInstruction(t *testing.T) {
	input := `LOAD_CSV R0, "test.csv"`

	parser := NewParser(input)
	program, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(program.Instructions) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(program.Instructions))
	}

	inst := program.Instructions[0]
	if inst.Opcode != "LOAD_CSV" {
		t.Errorf("expected opcode LOAD_CSV, got %s", inst.Opcode)
	}
	if len(inst.Operands) != 2 {
		t.Errorf("expected 2 operands, got %d", len(inst.Operands))
	}
}

func TestParser_MultipleInstructions(t *testing.T) {
	input := `LOAD_CSV R0, "test.csv"
SELECT_COL V0, R0, "price"
HALT R0`

	parser := NewParser(input)
	program, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(program.Instructions) != 3 {
		t.Fatalf("expected 3 instructions, got %d", len(program.Instructions))
	}

	opcodes := []string{"LOAD_CSV", "SELECT_COL", "HALT"}
	for i, inst := range program.Instructions {
		if inst.Opcode != opcodes[i] {
			t.Errorf("instruction %d: expected %s, got %s", i, opcodes[i], inst.Opcode)
		}
	}
}

func TestParser_OperandTypes(t *testing.T) {
	input := `LOAD_CONST R0, 42
LOAD_CONST_F F0, 3.14`

	parser := NewParser(input)
	program, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// First instruction: LOAD_CONST R0, 42
	inst1 := program.Instructions[0]
	if inst1.Operands[1].Type != OperandInt {
		t.Errorf("expected int operand, got %v", inst1.Operands[1].Type)
	}
	if inst1.Operands[1].IntVal != 42 {
		t.Errorf("expected int value 42, got %d", inst1.Operands[1].IntVal)
	}

	// Second instruction: LOAD_CONST_F F0, 3.14
	inst2 := program.Instructions[1]
	if inst2.Operands[1].Type != OperandFloat {
		t.Errorf("expected float operand, got %v", inst2.Operands[1].Type)
	}
	if inst2.Operands[1].FloatVal != 3.14 {
		t.Errorf("expected float value 3.14, got %f", inst2.Operands[1].FloatVal)
	}
}

func TestParser_RegisterOperands(t *testing.T) {
	input := `VEC_ADD_I V0, V1, V2
ADD_R R0, R1, R2`

	parser := NewParser(input)
	program, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	inst1 := program.Instructions[0]
	if inst1.Operands[0].Type != OperandRegV {
		t.Errorf("expected V register, got %v", inst1.Operands[0].Type)
	}
	if inst1.Operands[0].RegNum != 0 {
		t.Errorf("expected register 0, got %d", inst1.Operands[0].RegNum)
	}

	inst2 := program.Instructions[1]
	if inst2.Operands[0].Type != OperandRegR {
		t.Errorf("expected R register, got %v", inst2.Operands[0].Type)
	}
}

func TestParser_Comments(t *testing.T) {
	input := `; This is a comment
LOAD_CSV R0, "test.csv" ; inline comment
; Another comment
HALT R0`

	parser := NewParser(input)
	program, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Should only have 2 instructions, comments are skipped
	if len(program.Instructions) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.Instructions))
	}
}

func TestParser_FullProgram(t *testing.T) {
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

	parser := NewParser(input)
	program, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(program.Instructions) != 9 {
		t.Fatalf("expected 9 instructions, got %d", len(program.Instructions))
	}

	// Verify opcode names
	opcodes := []string{
		"LOAD_CSV", "SELECT_COL", "SELECT_COL", "LOAD_CONST",
		"BROADCAST", "CMP_GT", "FILTER", "REDUCE_SUM_F", "HALT_F",
	}
	for i, inst := range program.Instructions {
		if inst.Opcode != opcodes[i] {
			t.Errorf("instruction %d: expected %s, got %s", i, opcodes[i], inst.Opcode)
		}
	}
}

func TestParser_EmptyInput(t *testing.T) {
	input := ``

	parser := NewParser(input)
	program, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(program.Instructions) != 0 {
		t.Errorf("expected 0 instructions, got %d", len(program.Instructions))
	}
}

func TestParser_CommentsOnly(t *testing.T) {
	input := `; Just a comment
; Another comment`

	parser := NewParser(input)
	program, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(program.Instructions) != 0 {
		t.Errorf("expected 0 instructions, got %d", len(program.Instructions))
	}
}

func TestParser_LineNumbers(t *testing.T) {
	input := `LOAD_CSV R0, "test.csv"

SELECT_COL V0, R0, "price"
HALT R0`

	parser := NewParser(input)
	program, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if program.Instructions[0].Line != 1 {
		t.Errorf("expected line 1, got %d", program.Instructions[0].Line)
	}
	if program.Instructions[1].Line != 3 {
		t.Errorf("expected line 3, got %d", program.Instructions[1].Line)
	}
}
