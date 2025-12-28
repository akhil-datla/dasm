package vm

// Instruction represents a 32-bit encoded instruction.
//
// Layout:
// ┌─────────┬──────────┬─────┬──────┬──────┬─────────┐
// │ opcode  │ modifier │ dst │ src1 │ src2 │  imm8   │
// │ 8 bits  │  4 bits  │4 bit│4 bits│4 bits│ 8 bits  │
// └─────────┴──────────┴─────┴──────┴──────┴─────────┘
//
// For instructions using imm16, src1+src2+imm8 are combined:
// ┌─────────┬──────────┬─────┬──────────────────────┐
// │ opcode  │ modifier │ dst │       imm16          │
// │ 8 bits  │  4 bits  │4 bit│       16 bits        │
// └─────────┴──────────┴─────┴──────────────────────┘
type Instruction uint32

// EncodeInstruction creates an instruction from its components.
// When src1 or src2 are non-zero, uses src mode (src1+src2+imm8).
// Otherwise uses imm16 mode for the full 16-bit immediate.
func EncodeInstruction(opcode Opcode, modifier uint8, dst uint8, src1 uint8, src2 uint8, imm16 uint16) Instruction {
	var inst uint32

	// Opcode in bits 31-24
	inst |= uint32(opcode) << 24

	// Modifier in bits 23-20
	inst |= uint32(modifier&0xF) << 20

	// Dst in bits 19-16
	inst |= uint32(dst&0xF) << 16

	// For imm16, we use bits 15-0 directly
	// For src1/src2/imm8, we use bits 15-12, 11-8, 7-0
	if src1 != 0 || src2 != 0 {
		// Src mode: src1 + src2 + imm8 (from low 8 bits of imm16)
		inst |= uint32(src1&0xF) << 12
		inst |= uint32(src2&0xF) << 8
		inst |= uint32(imm16 & 0xFF)
	} else {
		// Imm16 mode: full 16-bit immediate
		inst |= uint32(imm16)
	}

	return Instruction(inst)
}

// Opcode returns the opcode (bits 31-24).
func (i Instruction) Opcode() Opcode {
	return Opcode(i >> 24)
}

// Modifier returns the modifier (bits 23-20).
func (i Instruction) Modifier() uint8 {
	return uint8((i >> 20) & 0xF)
}

// Dst returns the destination register (bits 19-16).
func (i Instruction) Dst() uint8 {
	return uint8((i >> 16) & 0xF)
}

// Src1 returns the first source register (bits 15-12).
func (i Instruction) Src1() uint8 {
	return uint8((i >> 12) & 0xF)
}

// Src2 returns the second source register (bits 11-8).
func (i Instruction) Src2() uint8 {
	return uint8((i >> 8) & 0xF)
}

// Imm8 returns the 8-bit immediate value (bits 7-0).
func (i Instruction) Imm8() uint8 {
	return uint8(i & 0xFF)
}

// Imm16 returns the 16-bit immediate value (bits 15-0).
// Used for instructions that need larger constants or indices.
func (i Instruction) Imm16() uint16 {
	return uint16(i & 0xFFFF)
}

// String returns a human-readable representation of the instruction.
func (i Instruction) String() string {
	return i.Opcode().String()
}
