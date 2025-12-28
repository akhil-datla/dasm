package vm

import (
	"testing"
)

func TestSerializeDeserialize_Simple(t *testing.T) {
	// Create a simple program
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants:      []any{int64(42)},
		FloatConstants: []float64{},
	}

	// Serialize
	data, err := SerializeProgram(program)
	if err != nil {
		t.Fatalf("SerializeProgram failed: %v", err)
	}

	// Verify magic header
	if string(data[:4]) != BytecodeMagic {
		t.Errorf("expected magic %q, got %q", BytecodeMagic, string(data[:4]))
	}

	// Deserialize
	restored, err := DeserializeProgram(data)
	if err != nil {
		t.Fatalf("DeserializeProgram failed: %v", err)
	}

	// Verify
	if len(restored.Code) != len(program.Code) {
		t.Errorf("expected %d instructions, got %d", len(program.Code), len(restored.Code))
	}
	if len(restored.Constants) != len(program.Constants) {
		t.Errorf("expected %d constants, got %d", len(program.Constants), len(restored.Constants))
	}
}

func TestSerializeDeserialize_WithFloats(t *testing.T) {
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConstF, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants:      []any{},
		FloatConstants: []float64{3.14159, 2.71828},
	}

	data, err := SerializeProgram(program)
	if err != nil {
		t.Fatalf("SerializeProgram failed: %v", err)
	}

	restored, err := DeserializeProgram(data)
	if err != nil {
		t.Fatalf("DeserializeProgram failed: %v", err)
	}

	if len(restored.FloatConstants) != 2 {
		t.Errorf("expected 2 float constants, got %d", len(restored.FloatConstants))
	}
	if restored.FloatConstants[0] != 3.14159 {
		t.Errorf("expected 3.14159, got %v", restored.FloatConstants[0])
	}
	if restored.FloatConstants[1] != 2.71828 {
		t.Errorf("expected 2.71828, got %v", restored.FloatConstants[1])
	}
}

func TestSerializeDeserialize_WithStrings(t *testing.T) {
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadCSV, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants:      []any{"data.csv", "price"},
		FloatConstants: []float64{},
	}

	data, err := SerializeProgram(program)
	if err != nil {
		t.Fatalf("SerializeProgram failed: %v", err)
	}

	restored, err := DeserializeProgram(data)
	if err != nil {
		t.Fatalf("DeserializeProgram failed: %v", err)
	}

	if len(restored.Constants) != 2 {
		t.Errorf("expected 2 constants, got %d", len(restored.Constants))
	}
	if restored.Constants[0] != "data.csv" {
		t.Errorf("expected 'data.csv', got %v", restored.Constants[0])
	}
	if restored.Constants[1] != "price" {
		t.Errorf("expected 'price', got %v", restored.Constants[1])
	}
}

func TestSerializeDeserialize_ComplexProgram(t *testing.T) {
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadCSV, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpVecMulF, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants:      []any{"sales.csv", "price", "quantity", int64(100)},
		FloatConstants: []float64{1.5, 2.5, 3.5},
	}

	data, err := SerializeProgram(program)
	if err != nil {
		t.Fatalf("SerializeProgram failed: %v", err)
	}

	restored, err := DeserializeProgram(data)
	if err != nil {
		t.Fatalf("DeserializeProgram failed: %v", err)
	}

	// Verify all components
	if len(restored.Code) != 6 {
		t.Errorf("expected 6 instructions, got %d", len(restored.Code))
	}
	if len(restored.Constants) != 4 {
		t.Errorf("expected 4 constants, got %d", len(restored.Constants))
	}
	if len(restored.FloatConstants) != 3 {
		t.Errorf("expected 3 float constants, got %d", len(restored.FloatConstants))
	}

	// Verify instruction encoding preserved
	for i, inst := range program.Code {
		if restored.Code[i] != inst {
			t.Errorf("instruction %d: expected %v, got %v", i, inst, restored.Code[i])
		}
	}
}

func TestDeserialize_InvalidMagic(t *testing.T) {
	data := []byte("BAAD" + "\x00\x01" + "\x00\x00\x00\x00")
	_, err := DeserializeProgram(data)
	if err != ErrInvalidMagic {
		t.Errorf("expected ErrInvalidMagic, got %v", err)
	}
}

func TestDeserialize_InvalidVersion(t *testing.T) {
	data := []byte("DFBC" + "\xFF\x00") // version 255
	_, err := DeserializeProgram(data)
	if err != ErrInvalidVersion {
		t.Errorf("expected ErrInvalidVersion, got %v", err)
	}
}

func TestDeserialize_TruncatedData(t *testing.T) {
	// Just magic, no version
	data := []byte("DFBC")
	_, err := DeserializeProgram(data)
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestDisassemble_Simple(t *testing.T) {
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants:      []any{int64(42)},
		FloatConstants: []float64{},
	}

	asm := Disassemble(program)

	if asm == "" {
		t.Error("disassembly should not be empty")
	}
	if !contains(asm, "LOAD_CONST") {
		t.Error("disassembly should contain LOAD_CONST")
	}
	if !contains(asm, "HALT") {
		t.Error("disassembly should contain HALT")
	}
}

func TestDisassemble_AllOpcodes(t *testing.T) {
	// Test disassembly of various opcode types
	tests := []struct {
		name     string
		inst     Instruction
		consts   []any
		expected string
	}{
		{
			name:     "LOAD_CSV",
			inst:     EncodeInstruction(OpLoadCSV, 0, 0, 0, 0, 0),
			consts:   []any{"test.csv"},
			expected: "LOAD_CSV",
		},
		{
			name:     "VEC_ADD_I",
			inst:     EncodeInstruction(OpVecAddI, 0, 0, 1, 2, 0),
			consts:   []any{},
			expected: "VEC_ADD_I",
		},
		{
			name:     "CMP_GT",
			inst:     EncodeInstruction(OpCmpGT, 0, 0, 1, 2, 0),
			consts:   []any{},
			expected: "CMP_GT",
		},
		{
			name:     "FILTER",
			inst:     EncodeInstruction(OpFilter, 0, 0, 1, 2, 0),
			consts:   []any{},
			expected: "FILTER",
		},
		{
			name:     "REDUCE_SUM_F",
			inst:     EncodeInstruction(OpReduceSumF, 0, 0, 1, 0, 0),
			consts:   []any{},
			expected: "REDUCE_SUM_F",
		},
		{
			name:     "GROUP_BY",
			inst:     EncodeInstruction(OpGroupBy, 0, 0, 1, 0, 0),
			consts:   []any{},
			expected: "GROUP_BY",
		},
		{
			name:     "NOP",
			inst:     EncodeInstruction(OpNop, 0, 0, 0, 0, 0),
			consts:   []any{},
			expected: "NOP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			program := &Program{
				Code:           []Instruction{tt.inst},
				Constants:      tt.consts,
				FloatConstants: []float64{},
			}
			asm := Disassemble(program)
			if !contains(asm, tt.expected) {
				t.Errorf("expected disassembly to contain %q, got: %s", tt.expected, asm)
			}
		})
	}
}

func TestSerializeDeserialize_EmptyProgram(t *testing.T) {
	program := &Program{
		Code:           []Instruction{},
		Constants:      []any{},
		FloatConstants: []float64{},
	}

	data, err := SerializeProgram(program)
	if err != nil {
		t.Fatalf("SerializeProgram failed: %v", err)
	}

	restored, err := DeserializeProgram(data)
	if err != nil {
		t.Fatalf("DeserializeProgram failed: %v", err)
	}

	if len(restored.Code) != 0 {
		t.Errorf("expected 0 instructions, got %d", len(restored.Code))
	}
}

func TestDisassemble_JoinOps(t *testing.T) {
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpJoinInner, 0, 2, 0, 1, 0),
		},
		Constants:      []any{"key_col"},
		FloatConstants: []float64{},
	}

	asm := Disassemble(program)
	if !contains(asm, "JOIN_INNER") {
		t.Errorf("expected JOIN_INNER in disassembly, got: %s", asm)
	}
}

func TestDisassemble_StringOps(t *testing.T) {
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpStrLen, 0, 1, 0, 0, 0),
			EncodeInstruction(OpStrContains, 0, 1, 0, 0, 0),
		},
		Constants:      []any{"pattern"},
		FloatConstants: []float64{},
	}

	asm := Disassemble(program)
	if !contains(asm, "STR_LEN") {
		t.Errorf("expected STR_LEN in disassembly, got: %s", asm)
	}
	if !contains(asm, "STR_CONTAINS") {
		t.Errorf("expected STR_CONTAINS in disassembly, got: %s", asm)
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
