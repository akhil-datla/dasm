package vm

import (
	"testing"
)

func TestInstruction_Encode(t *testing.T) {
	// Test basic instruction encoding
	inst := EncodeInstruction(OpLoadConst, 0, 5, 0, 0, 100)

	if inst.Opcode() != OpLoadConst {
		t.Errorf("expected opcode %v, got %v", OpLoadConst, inst.Opcode())
	}
	if inst.Dst() != 5 {
		t.Errorf("expected dst 5, got %d", inst.Dst())
	}
	if inst.Imm16() != 100 {
		t.Errorf("expected imm16 100, got %d", inst.Imm16())
	}
}

func TestInstruction_EncodeWithSrc(t *testing.T) {
	// Test instruction with src1 and src2
	inst := EncodeInstruction(OpVecAddI, 0, 3, 1, 2, 0)

	if inst.Opcode() != OpVecAddI {
		t.Errorf("expected opcode %v, got %v", OpVecAddI, inst.Opcode())
	}
	if inst.Dst() != 3 {
		t.Errorf("expected dst 3, got %d", inst.Dst())
	}
	if inst.Src1() != 1 {
		t.Errorf("expected src1 1, got %d", inst.Src1())
	}
	if inst.Src2() != 2 {
		t.Errorf("expected src2 2, got %d", inst.Src2())
	}
}

func TestInstruction_EncodeWithModifier(t *testing.T) {
	// Test instruction with modifier
	inst := EncodeInstruction(OpCmpGT, 5, 3, 1, 2, 0)

	if inst.Modifier() != 5 {
		t.Errorf("expected modifier 5, got %d", inst.Modifier())
	}
}

// Note: imm16 and src1/src2/imm8 share the same bits.
// Instructions either use imm16 OR src1/src2/imm8, not both.

func TestInstruction_RoundTrip_Imm16Mode(t *testing.T) {
	// Test instructions that use imm16 (like LOAD_CONST, SELECT_COL, LOAD_CSV)
	tests := []struct {
		name     string
		opcode   Opcode
		modifier uint8
		dst      uint8
		imm16    uint16
	}{
		{"LoadConst", OpLoadConst, 0, 15, 65535},
		{"SelectCol", OpSelectCol, 0, 0, 12345},
		{"LoadCSV", OpLoadCSV, 0, 5, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst := EncodeInstruction(tt.opcode, tt.modifier, tt.dst, 0, 0, tt.imm16)

			if inst.Opcode() != tt.opcode {
				t.Errorf("opcode: expected %v, got %v", tt.opcode, inst.Opcode())
			}
			if inst.Modifier() != tt.modifier {
				t.Errorf("modifier: expected %d, got %d", tt.modifier, inst.Modifier())
			}
			if inst.Dst() != tt.dst {
				t.Errorf("dst: expected %d, got %d", tt.dst, inst.Dst())
			}
			if inst.Imm16() != tt.imm16 {
				t.Errorf("imm16: expected %d, got %d", tt.imm16, inst.Imm16())
			}
		})
	}
}

func TestInstruction_RoundTrip_SrcMode(t *testing.T) {
	// Test instructions that use src1/src2 (like VEC_ADD_I, CMP_GT, FILTER)
	tests := []struct {
		name     string
		opcode   Opcode
		modifier uint8
		dst      uint8
		src1     uint8
		src2     uint8
	}{
		{"VecAddI", OpVecAddI, 0, 7, 3, 5},
		{"CmpGT", OpCmpGT, 3, 4, 1, 2},
		{"Filter", OpFilter, 0, 0, 1, 2},
		{"Halt", OpHalt, 0, 10, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst := EncodeInstruction(tt.opcode, tt.modifier, tt.dst, tt.src1, tt.src2, 0)

			if inst.Opcode() != tt.opcode {
				t.Errorf("opcode: expected %v, got %v", tt.opcode, inst.Opcode())
			}
			if inst.Modifier() != tt.modifier {
				t.Errorf("modifier: expected %d, got %d", tt.modifier, inst.Modifier())
			}
			if inst.Dst() != tt.dst {
				t.Errorf("dst: expected %d, got %d", tt.dst, inst.Dst())
			}
			if inst.Src1() != tt.src1 {
				t.Errorf("src1: expected %d, got %d", tt.src1, inst.Src1())
			}
			if inst.Src2() != tt.src2 {
				t.Errorf("src2: expected %d, got %d", tt.src2, inst.Src2())
			}
		})
	}
}

func TestInstruction_Imm8(t *testing.T) {
	inst := EncodeInstruction(OpLoadConst, 0, 5, 0, 0, 0x1234)

	// imm8 is the lower 8 bits
	if inst.Imm8() != 0x34 {
		t.Errorf("expected imm8 0x34, got 0x%02X", inst.Imm8())
	}
}

func TestOpcodeString(t *testing.T) {
	tests := []struct {
		opcode   Opcode
		expected string
	}{
		{OpLoadCSV, "LOAD_CSV"},
		{OpLoadConst, "LOAD_CONST"},
		{OpSelectCol, "SELECT_COL"},
		{OpVecAddI, "VEC_ADD_I"},
		{OpCmpGT, "CMP_GT"},
		{OpFilter, "FILTER"},
		{OpReduceSum, "REDUCE_SUM"},
		{OpHalt, "HALT"},
		{OpHaltF, "HALT_F"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.opcode.String(); got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestOpcodeFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected Opcode
		ok       bool
	}{
		{"LOAD_CSV", OpLoadCSV, true},
		{"LOAD_CONST", OpLoadConst, true},
		{"VEC_ADD_I", OpVecAddI, true},
		{"CMP_GT", OpCmpGT, true},
		{"HALT", OpHalt, true},
		{"INVALID", 0, false},
		{"", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := OpcodeFromString(tt.input)
			if ok != tt.ok {
				t.Errorf("ok: expected %v, got %v", tt.ok, ok)
			}
			if ok && got != tt.expected {
				t.Errorf("opcode: expected %v, got %v", tt.expected, got)
			}
		})
	}
}
