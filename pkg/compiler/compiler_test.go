package compiler

import (
	"testing"

	"github.com/akhildatla/dasm/pkg/vm"
)

func TestCompiler_SimpleProgram(t *testing.T) {
	input := `LOAD_CONST R0, 42
HALT R0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if len(program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.Code))
	}

	// Verify first instruction is LOAD_CONST
	if program.Code[0].Opcode() != vm.OpLoadConst {
		t.Errorf("expected OpLoadConst, got %v", program.Code[0].Opcode())
	}

	// Verify last instruction is HALT
	if program.Code[1].Opcode() != vm.OpHalt {
		t.Errorf("expected OpHalt, got %v", program.Code[1].Opcode())
	}
}

func TestCompiler_Constants(t *testing.T) {
	input := `LOAD_CONST R0, 42
LOAD_CONST R1, 100
HALT R0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	// Check constants pool
	if len(program.Constants) < 2 {
		t.Fatalf("expected at least 2 constants, got %d", len(program.Constants))
	}

	if program.Constants[0] != int64(42) {
		t.Errorf("expected constant 0 = 42, got %v", program.Constants[0])
	}
	if program.Constants[1] != int64(100) {
		t.Errorf("expected constant 1 = 100, got %v", program.Constants[1])
	}
}

func TestCompiler_FloatConstants(t *testing.T) {
	input := `LOAD_CONST_F F0, 3.14
HALT_F F0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if len(program.FloatConstants) < 1 {
		t.Fatalf("expected at least 1 float constant, got %d", len(program.FloatConstants))
	}

	if program.FloatConstants[0] != 3.14 {
		t.Errorf("expected float constant 0 = 3.14, got %v", program.FloatConstants[0])
	}
}

func TestCompiler_StringConstants(t *testing.T) {
	input := `LOAD_CSV R0, "test.csv"
SELECT_COL V0, R0, "price"
HALT R0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	// Check that string constants are in the pool
	found := map[string]bool{"test.csv": false, "price": false}
	for _, c := range program.Constants {
		if s, ok := c.(string); ok {
			found[s] = true
		}
	}

	for s, f := range found {
		if !f {
			t.Errorf("expected string constant %q not found", s)
		}
	}
}

func TestCompiler_VectorOperations(t *testing.T) {
	input := `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
SELECT_COL V1, R0, "b"
VEC_ADD_I V2, V0, V1
REDUCE_SUM R1, V2
HALT R1`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if len(program.Code) != 6 {
		t.Fatalf("expected 6 instructions, got %d", len(program.Code))
	}

	// Check opcode sequence
	expected := []vm.Opcode{
		vm.OpLoadFrame, vm.OpSelectCol, vm.OpSelectCol,
		vm.OpVecAddI, vm.OpReduceSum, vm.OpHalt,
	}
	for i, op := range expected {
		if program.Code[i].Opcode() != op {
			t.Errorf("instruction %d: expected %v, got %v", i, op, program.Code[i].Opcode())
		}
	}
}

func TestCompiler_FullProgram(t *testing.T) {
	input := `; Calculate sum of prices where quantity > 10
LOAD_FRAME    R0, "data"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
LOAD_CONST    R1, 10
BROADCAST     V2, R1, V1
CMP_GT        V3, V1, V2
FILTER        V4, V0, V3
REDUCE_SUM_F  F0, V4
HALT_F        F0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if len(program.Code) != 9 {
		t.Fatalf("expected 9 instructions, got %d", len(program.Code))
	}
}

func TestCompiler_EmptyProgram(t *testing.T) {
	input := ``

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if len(program.Code) != 0 {
		t.Errorf("expected 0 instructions, got %d", len(program.Code))
	}
}

func TestCompiler_InvalidOpcode(t *testing.T) {
	input := `INVALID_OPCODE R0, 42`

	_, err := Compile(input)
	if err == nil {
		t.Error("expected error for invalid opcode")
	}
}

func TestCompiler_ExecutableOutput(t *testing.T) {
	// Test that compiled output is actually executable by VM
	input := `LOAD_CONST R0, 10
LOAD_CONST R1, 5
ADD_R R2, R0, R1
HALT R2`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	machine := vm.NewVM()
	if err := machine.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := machine.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != int64(15) {
		t.Errorf("expected 15, got %v", result)
	}
}

// ===== Additional Compiler Tests =====

func TestCompiler_AllArithmeticOps(t *testing.T) {
	tests := []struct {
		name string
		op   string
	}{
		{"add", "ADD_R R2, R0, R1"},
		{"sub", "SUB_R R2, R0, R1"},
		{"mul", "MUL_R R2, R0, R1"},
		{"div", "DIV_R R2, R0, R1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := "LOAD_CONST R0, 10\nLOAD_CONST R1, 5\n" + tt.op + "\nHALT R2"
			_, err := Compile(input)
			if err != nil {
				t.Fatalf("Compile failed: %v", err)
			}
		})
	}
}

func TestCompiler_AllVectorOps(t *testing.T) {
	ops := []string{
		"VEC_ADD_I", "VEC_SUB_I", "VEC_MUL_I", "VEC_DIV_I", "VEC_MOD_I",
		"VEC_ADD_F", "VEC_SUB_F", "VEC_MUL_F", "VEC_DIV_F",
	}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			input := `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
SELECT_COL V1, R0, "b"
` + op + ` V2, V0, V1
HALT R0`
			_, err := Compile(input)
			if err != nil {
				t.Fatalf("Compile failed for %s: %v", op, err)
			}
		})
	}
}

func TestCompiler_AllComparisonOps(t *testing.T) {
	ops := []string{"CMP_EQ", "CMP_NE", "CMP_LT", "CMP_LE", "CMP_GT", "CMP_GE"}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			input := `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
SELECT_COL V1, R0, "b"
` + op + ` V2, V0, V1
HALT R0`
			_, err := Compile(input)
			if err != nil {
				t.Fatalf("Compile failed for %s: %v", op, err)
			}
		})
	}
}

func TestCompiler_LogicalOps(t *testing.T) {
	ops := []struct {
		name  string
		instr string
	}{
		{"and", "AND V3, V0, V1"},
		{"or", "OR V3, V0, V1"},
		{"not", "NOT V3, V0"},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			input := `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
SELECT_COL V1, R0, "b"
` + op.instr + `
HALT R0`
			_, err := Compile(input)
			if err != nil {
				t.Fatalf("Compile failed for %s: %v", op.name, err)
			}
		})
	}
}

func TestCompiler_AllAggregationOps(t *testing.T) {
	ops := []struct {
		name   string
		instr  string
		isHalt string
	}{
		{"reduce_sum", "REDUCE_SUM R1, V0", "HALT R1"},
		{"reduce_sum_f", "REDUCE_SUM_F F0, V0", "HALT_F F0"},
		{"reduce_count", "REDUCE_COUNT R1, V0", "HALT R1"},
		{"reduce_min", "REDUCE_MIN R1, V0", "HALT R1"},
		{"reduce_max", "REDUCE_MAX R1, V0", "HALT R1"},
		{"reduce_min_f", "REDUCE_MIN_F F0, V0", "HALT_F F0"},
		{"reduce_max_f", "REDUCE_MAX_F F0, V0", "HALT_F F0"},
		{"reduce_mean", "REDUCE_MEAN F0, V0", "HALT_F F0"},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			input := `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
` + op.instr + `
` + op.isHalt
			_, err := Compile(input)
			if err != nil {
				t.Fatalf("Compile failed for %s: %v", op.name, err)
			}
		})
	}
}

func TestCompiler_FrameOperations(t *testing.T) {
	input := `NEW_FRAME R0
LOAD_CONST R1, 10
HALT R1`

	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

func TestCompiler_GroupByOps(t *testing.T) {
	// Test GROUP_BY and GROUP_COUNT compilation
	input := `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "category"
SELECT_COL V1, R0, "amount"
GROUP_BY R1, V0
GROUP_COUNT V2, R1
GROUP_SUM V3, R1, V1
GROUP_KEYS V4, R1
HALT R0`
	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

func TestCompiler_StringOps(t *testing.T) {
	// Test string operation compilation
	input := `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "name"
STR_LEN V1, V0
STR_UPPER V2, V0
STR_LOWER V3, V0
STR_TRIM V4, V0
STR_CONTAINS V5, V0, "test"
STR_STARTS_WITH V6, V0, "abc"
HALT R0`
	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

func TestCompiler_JoinOps(t *testing.T) {
	// Test JOIN operation compilation
	input := `LOAD_FRAME R0, "left"
LOAD_FRAME R1, "right"
JOIN_INNER R2, R0, R1, "id"
JOIN_LEFT R3, R0, R1, "id"
HALT R0`
	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

func TestCompiler_AddColOp(t *testing.T) {
	// Test ADD_COL operation compilation
	input := `NEW_FRAME R0
LOAD_CONST R1, 42
BROADCAST V0, R1, V0
ADD_COL R0, V0, "value"
HALT R0`
	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

func TestCompiler_NOP(t *testing.T) {
	input := `NOP
LOAD_CONST R0, 42
NOP
HALT R0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if len(program.Code) != 4 {
		t.Errorf("expected 4 instructions, got %d", len(program.Code))
	}
}

func TestCompiler_Comments(t *testing.T) {
	input := `; This is a comment
LOAD_CONST R0, 42
; Another comment
HALT R0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	// Comments should not generate instructions
	if len(program.Code) != 2 {
		t.Errorf("expected 2 instructions (comments ignored), got %d", len(program.Code))
	}
}

func TestCompiler_WhitespaceHandling(t *testing.T) {
	input := `   LOAD_CONST    R0,    42
	HALT   R0  `

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if len(program.Code) != 2 {
		t.Errorf("expected 2 instructions, got %d", len(program.Code))
	}
}

func TestCompiler_NegativeConstants(t *testing.T) {
	input := `LOAD_CONST R0, -42
HALT R0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	found := false
	for _, c := range program.Constants {
		if v, ok := c.(int64); ok && v == -42 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected constant -42, got %v", program.Constants)
	}
}

func TestCompiler_NegativeFloatConstants(t *testing.T) {
	input := `LOAD_CONST_F F0, -3.14
HALT_F F0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if len(program.FloatConstants) < 1 || program.FloatConstants[0] != -3.14 {
		t.Errorf("expected float constant -3.14, got %v", program.FloatConstants)
	}
}

func TestCompiler_LargeConstants(t *testing.T) {
	input := `LOAD_CONST R0, 9223372036854775807
HALT R0`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	found := false
	for _, c := range program.Constants {
		if v, ok := c.(int64); ok && v == 9223372036854775807 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected max int64 constant")
	}
}

func TestCompiler_MoveOperations(t *testing.T) {
	input := `LOAD_CONST R0, 42
MOVE_R R1, R0
HALT R1`

	program, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if len(program.Code) != 3 {
		t.Errorf("expected 3 instructions, got %d", len(program.Code))
	}
}

func TestCompiler_FilterAndTake(t *testing.T) {
	input := `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
SELECT_COL V1, R0, "b"
CMP_GT V2, V0, V1
FILTER V3, V0, V2
HALT R0`

	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

func TestCompiler_BroadcastOperations(t *testing.T) {
	input := `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
LOAD_CONST R1, 10
BROADCAST V1, R1, V0
HALT R0`

	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

func TestCompiler_RowAndColCount(t *testing.T) {
	input := `LOAD_FRAME R0, "data"
ROW_COUNT R1, R0
COL_COUNT R2, R0
HALT R1`

	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

// ===== Additional coverage tests =====

func TestCompiler_FloatOps(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"LOAD_CONST_F", `LOAD_CONST_F F0, 3.14
HALT_F F0`},
		{"BROADCAST_F", `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
LOAD_CONST_F F0, 2.5
BROADCAST_F V1, F0, V0
HALT R0`},
		{"VEC_ADD_F", `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
SELECT_COL V1, R0, "b"
VEC_ADD_F V2, V0, V1
HALT R0`},
		{"REDUCE_SUM_F", `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
REDUCE_SUM_F F0, V0
HALT_F F0`},
		{"REDUCE_MEAN", `LOAD_FRAME R0, "data"
SELECT_COL V0, R0, "a"
REDUCE_MEAN F0, V0
HALT_F F0`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(tc.input)
			if err != nil {
				t.Fatalf("Compile failed: %v", err)
			}
		})
	}
}

func TestCompiler_NopAndHalt(t *testing.T) {
	input := `NOP
LOAD_CONST R0, 42
HALT R0`

	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

func TestCompiler_NewFrame(t *testing.T) {
	input := `NEW_FRAME R0
HALT R0`

	_, err := Compile(input)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
}

func TestCompiler_MoveOps(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"MOVE_R", `LOAD_CONST R0, 42
MOVE_R R1, R0
HALT R1`},
		{"MOVE_F", `LOAD_CONST_F F0, 3.14
MOVE_F F1, F0
HALT_F F1`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(tc.input)
			if err != nil {
				t.Fatalf("Compile failed: %v", err)
			}
		})
	}
}
