package vm

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
)

// Bytecode file format:
// - Magic: "DFBC" (4 bytes)
// - Version: uint16
// - NumInstructions: uint32
// - Instructions: []uint64 (each instruction is 64-bit)
// - NumConstants: uint32
// - Constants: gob-encoded []any
// - NumFloatConstants: uint32
// - FloatConstants: []float64

const (
	BytecodeMagic   = "DFBC"
	BytecodeVersion = 1
)

var (
	ErrInvalidMagic   = errors.New("invalid bytecode magic")
	ErrInvalidVersion = errors.New("unsupported bytecode version")
)

// SerializeProgram serializes a Program to bytecode format.
func SerializeProgram(p *Program) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write magic
	buf.WriteString(BytecodeMagic)

	// Write version
	if err := binary.Write(buf, binary.LittleEndian, uint16(BytecodeVersion)); err != nil {
		return nil, fmt.Errorf("writing version: %w", err)
	}

	// Write instructions
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(p.Code))); err != nil {
		return nil, fmt.Errorf("writing instruction count: %w", err)
	}
	for _, inst := range p.Code {
		if err := binary.Write(buf, binary.LittleEndian, uint64(inst)); err != nil {
			return nil, fmt.Errorf("writing instruction: %w", err)
		}
	}

	// Write constants using gob encoding
	constBuf := new(bytes.Buffer)
	enc := gob.NewEncoder(constBuf)
	if err := enc.Encode(p.Constants); err != nil {
		return nil, fmt.Errorf("encoding constants: %w", err)
	}
	constBytes := constBuf.Bytes()
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(constBytes))); err != nil {
		return nil, fmt.Errorf("writing constants length: %w", err)
	}
	buf.Write(constBytes)

	// Write float constants
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(p.FloatConstants))); err != nil {
		return nil, fmt.Errorf("writing float constant count: %w", err)
	}
	for _, f := range p.FloatConstants {
		if err := binary.Write(buf, binary.LittleEndian, f); err != nil {
			return nil, fmt.Errorf("writing float constant: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// DeserializeProgram deserializes bytecode to a Program.
func DeserializeProgram(data []byte) (*Program, error) {
	buf := bytes.NewReader(data)

	// Read and verify magic
	magic := make([]byte, 4)
	if _, err := io.ReadFull(buf, magic); err != nil {
		return nil, fmt.Errorf("reading magic: %w", err)
	}
	if string(magic) != BytecodeMagic {
		return nil, ErrInvalidMagic
	}

	// Read and verify version
	var version uint16
	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("reading version: %w", err)
	}
	if version != BytecodeVersion {
		return nil, ErrInvalidVersion
	}

	// Read instructions
	var numInst uint32
	if err := binary.Read(buf, binary.LittleEndian, &numInst); err != nil {
		return nil, fmt.Errorf("reading instruction count: %w", err)
	}
	code := make([]Instruction, numInst)
	for i := range code {
		var inst uint64
		if err := binary.Read(buf, binary.LittleEndian, &inst); err != nil {
			return nil, fmt.Errorf("reading instruction %d: %w", i, err)
		}
		code[i] = Instruction(inst)
	}

	// Read constants
	var constLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &constLen); err != nil {
		return nil, fmt.Errorf("reading constants length: %w", err)
	}
	constBytes := make([]byte, constLen)
	if _, err := io.ReadFull(buf, constBytes); err != nil {
		return nil, fmt.Errorf("reading constants: %w", err)
	}
	var constants []any
	dec := gob.NewDecoder(bytes.NewReader(constBytes))
	if err := dec.Decode(&constants); err != nil {
		return nil, fmt.Errorf("decoding constants: %w", err)
	}

	// Read float constants
	var numFloats uint32
	if err := binary.Read(buf, binary.LittleEndian, &numFloats); err != nil {
		return nil, fmt.Errorf("reading float constant count: %w", err)
	}
	floatConstants := make([]float64, numFloats)
	for i := range floatConstants {
		if err := binary.Read(buf, binary.LittleEndian, &floatConstants[i]); err != nil {
			return nil, fmt.Errorf("reading float constant %d: %w", i, err)
		}
	}

	return &Program{
		Code:           code,
		Constants:      constants,
		FloatConstants: floatConstants,
	}, nil
}

// Disassemble converts a Program back to assembly source code.
func Disassemble(p *Program) string {
	var buf bytes.Buffer

	buf.WriteString("; Disassembled from DASM bytecode\n")
	buf.WriteString(fmt.Sprintf("; %d instructions, %d constants, %d float constants\n\n",
		len(p.Code), len(p.Constants), len(p.FloatConstants)))

	for i, inst := range p.Code {
		buf.WriteString(fmt.Sprintf("%04d: %s\n", i, disassembleInstruction(inst, p.Constants, p.FloatConstants)))
	}

	return buf.String()
}

func disassembleInstruction(inst Instruction, constants []any, floatConsts []float64) string {
	op := inst.Opcode()
	dst := inst.Dst()
	src1 := inst.Src1()
	src2 := inst.Src2()
	imm8 := inst.Imm8()
	imm16 := inst.Imm16()

	opName := op.String()

	switch op {
	// Data loading with string constant
	case OpLoadCSV, OpLoadFrame:
		constVal := ""
		if int(imm16) < len(constants) {
			constVal = fmt.Sprintf("%q", constants[imm16])
		}
		return fmt.Sprintf("%-14s R%d, %s", opName, dst, constVal)

	case OpLoadConst:
		constVal := ""
		if int(imm16) < len(constants) {
			constVal = fmt.Sprintf("%v", constants[imm16])
		}
		return fmt.Sprintf("%-14s R%d, %s", opName, dst, constVal)

	case OpLoadConstF:
		constVal := ""
		if int(imm16) < len(floatConsts) {
			constVal = fmt.Sprintf("%v", floatConsts[imm16])
		}
		return fmt.Sprintf("%-14s F%d, %s", opName, dst, constVal)

	case OpSelectCol:
		constVal := ""
		if int(imm8) < len(constants) {
			constVal = fmt.Sprintf("%q", constants[imm8])
		}
		return fmt.Sprintf("%-14s V%d, R%d, %s", opName, dst, src1, constVal)

	case OpBroadcast:
		return fmt.Sprintf("%-14s V%d, R%d, V%d", opName, dst, src1, src2)

	case OpBroadcastF:
		return fmt.Sprintf("%-14s V%d, F%d, V%d", opName, dst, src1, src2)

	// Vector binary ops
	case OpVecAddI, OpVecSubI, OpVecMulI, OpVecDivI, OpVecModI,
		OpVecAddF, OpVecSubF, OpVecMulF, OpVecDivF,
		OpCmpEQ, OpCmpNE, OpCmpLT, OpCmpLE, OpCmpGT, OpCmpGE,
		OpAnd, OpOr, OpFilter, OpTake, OpStrConcat:
		return fmt.Sprintf("%-14s V%d, V%d, V%d", opName, dst, src1, src2)

	// Vector unary ops
	case OpNot, OpStrLen, OpStrUpper, OpStrLower, OpStrTrim:
		return fmt.Sprintf("%-14s V%d, V%d", opName, dst, src1)

	// Reduce ops
	case OpReduceSum, OpReduceCount, OpReduceMin, OpReduceMax:
		return fmt.Sprintf("%-14s R%d, V%d", opName, dst, src1)

	case OpReduceSumF, OpReduceMinF, OpReduceMaxF, OpReduceMean:
		return fmt.Sprintf("%-14s F%d, V%d", opName, dst, src1)

	// Scalar ops
	case OpMoveR, OpRowCount, OpColCount:
		return fmt.Sprintf("%-14s R%d, R%d", opName, dst, src1)

	case OpMoveF:
		return fmt.Sprintf("%-14s F%d, F%d", opName, dst, src1)

	case OpAddR, OpSubR, OpMulR, OpDivR:
		return fmt.Sprintf("%-14s R%d, R%d, R%d", opName, dst, src1, src2)

	// Frame ops
	case OpNewFrame:
		return fmt.Sprintf("%-14s R%d", opName, dst)

	case OpAddCol:
		constVal := ""
		if int(imm8) < len(constants) {
			constVal = fmt.Sprintf("%q", constants[imm8])
		}
		return fmt.Sprintf("%-14s R%d, V%d, %s", opName, dst, src1, constVal)

	// GroupBy ops
	case OpGroupBy:
		return fmt.Sprintf("%-14s R%d, V%d", opName, dst, src1)

	case OpGroupCount, OpGroupKeys:
		return fmt.Sprintf("%-14s V%d, R%d", opName, dst, src1)

	case OpGroupSum, OpGroupSumF, OpGroupMin, OpGroupMax, OpGroupMinF, OpGroupMaxF, OpGroupMean:
		return fmt.Sprintf("%-14s V%d, R%d, V%d", opName, dst, src1, src2)

	// Join ops
	case OpJoinInner, OpJoinLeft, OpJoinRight, OpJoinOuter:
		constVal := ""
		if int(imm8) < len(constants) {
			constVal = fmt.Sprintf("%q", constants[imm8])
		}
		return fmt.Sprintf("%-14s R%d, R%d, R%d, %s", opName, dst, src1, src2, constVal)

	// String pattern ops
	case OpStrContains, OpStrStartsWith, OpStrEndsWith, OpStrSplit, OpStrReplace:
		constVal := ""
		if int(imm8) < len(constants) {
			constVal = fmt.Sprintf("%q", constants[imm8])
		}
		return fmt.Sprintf("%-14s V%d, V%d, %s", opName, dst, src1, constVal)

	// Control flow
	case OpNop:
		return opName

	case OpHalt:
		return fmt.Sprintf("%-14s R%d", opName, dst)

	case OpHaltF:
		return fmt.Sprintf("%-14s F%d", opName, dst)

	default:
		return fmt.Sprintf("%-14s 0x%08X", opName, uint64(inst))
	}
}
