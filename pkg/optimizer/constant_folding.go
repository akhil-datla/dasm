package optimizer

import (
	"github.com/akhildatla/dasm/pkg/vm"
)

// constantFolding evaluates constant expressions at compile time.
// For example:
//
//	LOAD_CONST R0, 5
//	LOAD_CONST R1, 10
//	ADD_R R2, R0, R1
//
// Becomes:
//
//	LOAD_CONST R2, 15
func (o *Optimizer) constantFolding(program *vm.Program) *vm.Program {
	// Track which registers hold known constants
	regConstants := make(map[uint8]int64)    // R registers
	fregConstants := make(map[uint8]float64) // F registers

	newCode := make([]vm.Instruction, 0, len(program.Code))
	newConstants := make([]any, len(program.Constants))
	copy(newConstants, program.Constants)
	newFloatConstants := make([]float64, len(program.FloatConstants))
	copy(newFloatConstants, program.FloatConstants)

	for _, inst := range program.Code {
		op := inst.Opcode()
		dst := inst.Dst()

		switch op {
		case vm.OpLoadConst:
			// Track the constant value
			constIdx := inst.Imm16()
			if int(constIdx) < len(program.Constants) {
				if val, ok := program.Constants[constIdx].(int64); ok {
					regConstants[dst] = val
				}
			}
			newCode = append(newCode, inst)

		case vm.OpLoadConstF:
			// Track the float constant value
			constIdx := inst.Imm16()
			if int(constIdx) < len(program.FloatConstants) {
				fregConstants[dst] = program.FloatConstants[constIdx]
			}
			newCode = append(newCode, inst)

		case vm.OpAddR:
			src1, src2 := inst.Src1(), inst.Src2()
			val1, ok1 := regConstants[src1]
			val2, ok2 := regConstants[src2]

			if ok1 && ok2 {
				// Both operands are constants, fold them
				result := val1 + val2
				constIdx := uint16(len(newConstants))
				newConstants = append(newConstants, result)
				newInst := vm.EncodeInstruction(vm.OpLoadConst, 0, dst, 0, 0, constIdx)
				newCode = append(newCode, newInst)
				regConstants[dst] = result
			} else {
				// Invalidate the destination register
				delete(regConstants, dst)
				newCode = append(newCode, inst)
			}

		case vm.OpSubR:
			src1, src2 := inst.Src1(), inst.Src2()
			val1, ok1 := regConstants[src1]
			val2, ok2 := regConstants[src2]

			if ok1 && ok2 {
				result := val1 - val2
				constIdx := uint16(len(newConstants))
				newConstants = append(newConstants, result)
				newInst := vm.EncodeInstruction(vm.OpLoadConst, 0, dst, 0, 0, constIdx)
				newCode = append(newCode, newInst)
				regConstants[dst] = result
			} else {
				delete(regConstants, dst)
				newCode = append(newCode, inst)
			}

		case vm.OpMulR:
			src1, src2 := inst.Src1(), inst.Src2()
			val1, ok1 := regConstants[src1]
			val2, ok2 := regConstants[src2]

			if ok1 && ok2 {
				result := val1 * val2
				constIdx := uint16(len(newConstants))
				newConstants = append(newConstants, result)
				newInst := vm.EncodeInstruction(vm.OpLoadConst, 0, dst, 0, 0, constIdx)
				newCode = append(newCode, newInst)
				regConstants[dst] = result
			} else {
				delete(regConstants, dst)
				newCode = append(newCode, inst)
			}

		case vm.OpDivR:
			src1, src2 := inst.Src1(), inst.Src2()
			val1, ok1 := regConstants[src1]
			val2, ok2 := regConstants[src2]

			if ok1 && ok2 && val2 != 0 {
				result := val1 / val2
				constIdx := uint16(len(newConstants))
				newConstants = append(newConstants, result)
				newInst := vm.EncodeInstruction(vm.OpLoadConst, 0, dst, 0, 0, constIdx)
				newCode = append(newCode, newInst)
				regConstants[dst] = result
			} else {
				delete(regConstants, dst)
				newCode = append(newCode, inst)
			}

		case vm.OpMoveR:
			src := inst.Src1()
			if val, ok := regConstants[src]; ok {
				regConstants[dst] = val
			} else {
				delete(regConstants, dst)
			}
			newCode = append(newCode, inst)

		case vm.OpMoveF:
			src := inst.Src1()
			if val, ok := fregConstants[src]; ok {
				fregConstants[dst] = val
			} else {
				delete(fregConstants, dst)
			}
			newCode = append(newCode, inst)

		default:
			// For other instructions, invalidate affected registers
			// and pass through unchanged
			delete(regConstants, dst)
			delete(fregConstants, dst)
			newCode = append(newCode, inst)
		}
	}

	return &vm.Program{
		Code:           newCode,
		Constants:      newConstants,
		FloatConstants: newFloatConstants,
	}
}
