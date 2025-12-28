package compiler

import (
	"fmt"
	"strings"

	"github.com/akhildatla/dasm/pkg/vm"
)

// Compile compiles DFL assembly source code to bytecode.
func Compile(source string) (*vm.Program, error) {
	parser := NewParser(source)
	asmProgram, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	compiler := &Compiler{
		constants:      []any{},
		floatConstants: []float64{},
		code:           []vm.Instruction{},
		constIndex:     make(map[any]uint16),
		floatIndex:     make(map[float64]uint16),
	}

	return compiler.compile(asmProgram)
}

// Compiler compiles parsed assembly to bytecode.
type Compiler struct {
	constants      []any
	floatConstants []float64
	code           []vm.Instruction
	constIndex     map[any]uint16
	floatIndex     map[float64]uint16
}

func (c *Compiler) compile(program *AsmProgram) (*vm.Program, error) {
	for _, inst := range program.Instructions {
		bytecode, err := c.compileInstruction(inst)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", inst.Line, err)
		}
		c.code = append(c.code, bytecode)
	}

	return &vm.Program{
		Code:           c.code,
		Constants:      c.constants,
		FloatConstants: c.floatConstants,
	}, nil
}

func (c *Compiler) compileInstruction(inst AsmInstruction) (vm.Instruction, error) {
	opcode, ok := vm.OpcodeFromString(strings.ToUpper(inst.Opcode))
	if !ok {
		return 0, fmt.Errorf("unknown opcode: %s", inst.Opcode)
	}

	switch opcode {
	// ===== Data Loading =====
	case vm.OpLoadCSV:
		return c.compileRegStrOp(opcode, inst)

	case vm.OpLoadConst:
		return c.compileLoadConst(inst)

	case vm.OpLoadConstF:
		return c.compileLoadConstF(inst)

	case vm.OpSelectCol:
		return c.compileSelectCol(inst)

	case vm.OpBroadcast, vm.OpBroadcastF:
		return c.compileBroadcast(opcode, inst)

	case vm.OpLoadFrame:
		return c.compileRegStrOp(opcode, inst)

	case vm.OpLoadJSON:
		return c.compileRegStrOp(opcode, inst)

	case vm.OpLoadParquet:
		return c.compileRegStrOp(opcode, inst)

	// ===== Vector Arithmetic =====
	case vm.OpVecAddI, vm.OpVecSubI, vm.OpVecMulI, vm.OpVecDivI, vm.OpVecModI,
		vm.OpVecAddF, vm.OpVecSubF, vm.OpVecMulF, vm.OpVecDivF:
		return c.compileVecBinaryOp(opcode, inst)

	// ===== Comparison =====
	case vm.OpCmpEQ, vm.OpCmpNE, vm.OpCmpLT, vm.OpCmpLE, vm.OpCmpGT, vm.OpCmpGE:
		return c.compileVecBinaryOp(opcode, inst)

	// ===== Logical =====
	case vm.OpAnd, vm.OpOr:
		return c.compileVecBinaryOp(opcode, inst)

	case vm.OpNot:
		return c.compileVecUnaryOp(opcode, inst)

	// ===== Filtering =====
	case vm.OpFilter, vm.OpTake:
		return c.compileVecBinaryOp(opcode, inst)

	// ===== Aggregations =====
	case vm.OpReduceSum, vm.OpReduceSumF, vm.OpReduceCount,
		vm.OpReduceMin, vm.OpReduceMax, vm.OpReduceMinF, vm.OpReduceMaxF, vm.OpReduceMean:
		return c.compileReduceOp(opcode, inst)

	// ===== Scalar Operations =====
	case vm.OpMoveR, vm.OpMoveF:
		return c.compileScalarUnaryOp(opcode, inst)

	case vm.OpAddR, vm.OpSubR, vm.OpMulR, vm.OpDivR:
		return c.compileScalarBinaryOp(opcode, inst)

	// ===== Frame Operations =====
	case vm.OpNewFrame:
		return c.compileSingleRegOp(opcode, inst)

	case vm.OpRowCount, vm.OpColCount:
		return c.compileScalarUnaryOp(opcode, inst)

	case vm.OpAddCol:
		return c.compileAddCol(inst)

	// ===== GroupBy Operations =====
	case vm.OpGroupBy:
		return c.compileGroupBy(inst)

	case vm.OpGroupSum, vm.OpGroupSumF, vm.OpGroupMin, vm.OpGroupMax,
		vm.OpGroupMinF, vm.OpGroupMaxF, vm.OpGroupMean:
		return c.compileGroupAgg(opcode, inst)

	case vm.OpGroupCount, vm.OpGroupKeys:
		return c.compileGroupUnary(opcode, inst)

	// ===== Join Operations =====
	case vm.OpJoinInner, vm.OpJoinLeft, vm.OpJoinRight, vm.OpJoinOuter:
		return c.compileJoin(opcode, inst)

	// ===== String Operations =====
	case vm.OpStrLen, vm.OpStrUpper, vm.OpStrLower, vm.OpStrTrim:
		return c.compileVecUnaryOp(opcode, inst)

	case vm.OpStrConcat:
		return c.compileVecBinaryOp(opcode, inst)

	case vm.OpStrContains, vm.OpStrStartsWith, vm.OpStrEndsWith, vm.OpStrSplit, vm.OpStrReplace:
		return c.compileStrPatternOp(opcode, inst)

	// ===== Control Flow =====
	case vm.OpNop:
		return vm.EncodeInstruction(opcode, 0, 0, 0, 0, 0), nil

	case vm.OpHalt, vm.OpHaltF, vm.OpHaltV:
		return c.compileSingleRegOp(opcode, inst)

	default:
		return 0, fmt.Errorf("unimplemented opcode: %s", opcode)
	}
}

// ===== Compile helpers =====

func (c *Compiler) addConstant(value any) uint16 {
	if idx, ok := c.constIndex[value]; ok {
		return idx
	}
	idx := uint16(len(c.constants))
	c.constants = append(c.constants, value)
	c.constIndex[value] = idx
	return idx
}

func (c *Compiler) addFloatConstant(value float64) uint16 {
	if idx, ok := c.floatIndex[value]; ok {
		return idx
	}
	idx := uint16(len(c.floatConstants))
	c.floatConstants = append(c.floatConstants, value)
	c.floatIndex[value] = idx
	return idx
}

func (c *Compiler) compileRegStrOp(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 2 {
		return 0, fmt.Errorf("expected 2 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum
	strVal := inst.Operands[1].StrVal
	constIdx := c.addConstant(strVal)

	return vm.EncodeInstruction(opcode, 0, dst, 0, 0, constIdx), nil
}

func (c *Compiler) compileLoadConst(inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 2 {
		return 0, fmt.Errorf("expected 2 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum
	intVal := inst.Operands[1].IntVal
	constIdx := c.addConstant(intVal)

	return vm.EncodeInstruction(vm.OpLoadConst, 0, dst, 0, 0, constIdx), nil
}

func (c *Compiler) compileLoadConstF(inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 2 {
		return 0, fmt.Errorf("expected 2 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum
	floatVal := inst.Operands[1].FloatVal
	constIdx := c.addFloatConstant(floatVal)

	return vm.EncodeInstruction(vm.OpLoadConstF, 0, dst, 0, 0, constIdx), nil
}

func (c *Compiler) compileSelectCol(inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 3 {
		return 0, fmt.Errorf("expected 3 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum // V register
	src := inst.Operands[1].RegNum // R register (frame)
	colName := inst.Operands[2].StrVal
	constIdx := c.addConstant(colName)

	return vm.EncodeInstruction(vm.OpSelectCol, 0, dst, src, 0, constIdx), nil
}

func (c *Compiler) compileBroadcast(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 3 {
		return 0, fmt.Errorf("expected 3 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum    // V register
	src := inst.Operands[1].RegNum    // R/F register (scalar value)
	lenSrc := inst.Operands[2].RegNum // V register (for length)

	return vm.EncodeInstruction(opcode, 0, dst, src, lenSrc, 0), nil
}

func (c *Compiler) compileVecBinaryOp(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 3 {
		return 0, fmt.Errorf("expected 3 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum
	src1 := inst.Operands[1].RegNum
	src2 := inst.Operands[2].RegNum

	return vm.EncodeInstruction(opcode, 0, dst, src1, src2, 0), nil
}

func (c *Compiler) compileVecUnaryOp(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 2 {
		return 0, fmt.Errorf("expected 2 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum
	src := inst.Operands[1].RegNum

	return vm.EncodeInstruction(opcode, 0, dst, src, 0, 0), nil
}

func (c *Compiler) compileReduceOp(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 2 {
		return 0, fmt.Errorf("expected 2 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum // R or F register
	src := inst.Operands[1].RegNum // V register

	return vm.EncodeInstruction(opcode, 0, dst, src, 0, 0), nil
}

func (c *Compiler) compileScalarUnaryOp(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 2 {
		return 0, fmt.Errorf("expected 2 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum
	src := inst.Operands[1].RegNum

	return vm.EncodeInstruction(opcode, 0, dst, src, 0, 0), nil
}

func (c *Compiler) compileScalarBinaryOp(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 3 {
		return 0, fmt.Errorf("expected 3 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum
	src1 := inst.Operands[1].RegNum
	src2 := inst.Operands[2].RegNum

	return vm.EncodeInstruction(opcode, 0, dst, src1, src2, 0), nil
}

func (c *Compiler) compileSingleRegOp(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 1 {
		return 0, fmt.Errorf("expected 1 operand, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum

	return vm.EncodeInstruction(opcode, 0, dst, 0, 0, 0), nil
}

// ADD_COL R[dst], V[src], "column_name"
func (c *Compiler) compileAddCol(inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 3 {
		return 0, fmt.Errorf("expected 3 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum // Frame register
	src := inst.Operands[1].RegNum // Vector register
	colName := inst.Operands[2].StrVal
	constIdx := c.addConstant(colName)

	// Use Imm8 encoding since Src1 is used
	if constIdx > 255 {
		return 0, fmt.Errorf("constant index %d exceeds 8-bit limit", constIdx)
	}

	return vm.EncodeInstruction(vm.OpAddCol, 0, dst, src, 0, constIdx), nil
}

// GROUP_BY R[dst], V[src] (src is key column)
func (c *Compiler) compileGroupBy(inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 2 {
		return 0, fmt.Errorf("expected 2 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum // Result register (groupby handle)
	src := inst.Operands[1].RegNum // Vector register (key column)

	return vm.EncodeInstruction(vm.OpGroupBy, 0, dst, src, 0, 0), nil
}

// GROUP_SUM V[dst], R[src1], V[src2] (src1=groupby handle, src2=value column)
func (c *Compiler) compileGroupAgg(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 3 {
		return 0, fmt.Errorf("expected 3 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum    // Result vector register
	gbSrc := inst.Operands[1].RegNum  // GroupBy handle register
	valSrc := inst.Operands[2].RegNum // Value vector register

	return vm.EncodeInstruction(opcode, 0, dst, gbSrc, valSrc, 0), nil
}

// GROUP_COUNT V[dst], R[src] or GROUP_KEYS V[dst], R[src]
func (c *Compiler) compileGroupUnary(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 2 {
		return 0, fmt.Errorf("expected 2 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum // Result register
	src := inst.Operands[1].RegNum // GroupBy handle register

	return vm.EncodeInstruction(opcode, 0, dst, src, 0, 0), nil
}

// JOIN_INNER R[dst], R[src1], R[src2], "key_column"
func (c *Compiler) compileJoin(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 4 {
		return 0, fmt.Errorf("expected 4 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum  // Result frame register
	src1 := inst.Operands[1].RegNum // Left frame register
	src2 := inst.Operands[2].RegNum // Right frame register
	keyName := inst.Operands[3].StrVal
	constIdx := c.addConstant(keyName)

	// Use Imm8 encoding since Src1 and Src2 are used
	if constIdx > 255 {
		return 0, fmt.Errorf("constant index %d exceeds 8-bit limit", constIdx)
	}

	return vm.EncodeInstruction(opcode, 0, dst, src1, src2, constIdx), nil
}

// STR_CONTAINS V[dst], V[src], "pattern"
func (c *Compiler) compileStrPatternOp(opcode vm.Opcode, inst AsmInstruction) (vm.Instruction, error) {
	if len(inst.Operands) < 3 {
		return 0, fmt.Errorf("expected 3 operands, got %d", len(inst.Operands))
	}

	dst := inst.Operands[0].RegNum // Result vector register
	src := inst.Operands[1].RegNum // Input vector register
	pattern := inst.Operands[2].StrVal
	constIdx := c.addConstant(pattern)

	// Use Imm8 encoding since Src1 is used
	if constIdx > 255 {
		return 0, fmt.Errorf("constant index %d exceeds 8-bit limit", constIdx)
	}

	return vm.EncodeInstruction(opcode, 0, dst, src, 0, constIdx), nil
}
