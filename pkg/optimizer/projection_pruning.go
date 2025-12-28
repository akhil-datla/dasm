package optimizer

import (
	"github.com/akhildatla/dasm/pkg/vm"
)

// projectionPruning removes unused column selections.
// If a column is loaded but never used, we can remove the SELECT_COL instruction.
//
// For example:
//
//	LOAD_CSV R0, "data.csv"
//	SELECT_COL V0, R0, "price"
//	SELECT_COL V1, R0, "quantity"  ; unused
//	SELECT_COL V2, R0, "name"      ; unused
//	REDUCE_SUM R1, V0
//	HALT R1
//
// Becomes:
//
//	LOAD_CSV R0, "data.csv"
//	SELECT_COL V0, R0, "price"
//	REDUCE_SUM R1, V0
//	HALT R1
func (o *Optimizer) projectionPruning(program *vm.Program) *vm.Program {
	if len(program.Code) == 0 {
		return program
	}

	// First pass: find all register uses
	usedVRegs := make(map[uint8]bool)
	usedRRegs := make(map[uint8]bool)
	usedFRegs := make(map[uint8]bool)

	for _, inst := range program.Code {
		op := inst.Opcode()
		src1 := inst.Src1()
		src2 := inst.Src2()

		// Track which source registers are used based on opcode
		switch op {
		// Vector operations use V registers as sources
		case vm.OpVecAddI, vm.OpVecSubI, vm.OpVecMulI, vm.OpVecDivI, vm.OpVecModI,
			vm.OpVecAddF, vm.OpVecSubF, vm.OpVecMulF, vm.OpVecDivF,
			vm.OpCmpEQ, vm.OpCmpNE, vm.OpCmpLT, vm.OpCmpLE, vm.OpCmpGT, vm.OpCmpGE,
			vm.OpAnd, vm.OpOr, vm.OpStrConcat:
			usedVRegs[src1] = true
			usedVRegs[src2] = true

		case vm.OpNot, vm.OpStrLen, vm.OpStrUpper, vm.OpStrLower, vm.OpStrTrim,
			vm.OpStrContains, vm.OpStrStartsWith, vm.OpStrEndsWith, vm.OpStrSplit, vm.OpStrReplace:
			usedVRegs[src1] = true

		case vm.OpFilter:
			usedVRegs[src1] = true
			usedVRegs[src2] = true

		case vm.OpReduceSum, vm.OpReduceSumF, vm.OpReduceCount,
			vm.OpReduceMin, vm.OpReduceMax, vm.OpReduceMinF, vm.OpReduceMaxF,
			vm.OpReduceMean:
			usedVRegs[src1] = true

		case vm.OpGroupBy:
			usedVRegs[src1] = true // key column

		case vm.OpGroupSum, vm.OpGroupSumF, vm.OpGroupMin, vm.OpGroupMax,
			vm.OpGroupMinF, vm.OpGroupMaxF, vm.OpGroupMean:
			usedRRegs[src1] = true // groupby result
			usedVRegs[src2] = true // value column

		case vm.OpGroupCount, vm.OpGroupKeys:
			usedRRegs[src1] = true

		// Scalar operations use R registers
		case vm.OpAddR, vm.OpSubR, vm.OpMulR, vm.OpDivR:
			usedRRegs[src1] = true
			usedRRegs[src2] = true

		case vm.OpMoveR:
			usedRRegs[src1] = true

		case vm.OpMoveF:
			usedFRegs[src1] = true

		// Join operations use R registers for frames
		case vm.OpJoinInner, vm.OpJoinLeft, vm.OpJoinRight, vm.OpJoinOuter:
			usedRRegs[src1] = true
			usedRRegs[src2] = true

		case vm.OpRowCount, vm.OpColCount:
			usedRRegs[src1] = true

		case vm.OpBroadcast:
			usedRRegs[src1] = true
			usedVRegs[src2] = true

		case vm.OpBroadcastF:
			usedFRegs[src1] = true
			usedVRegs[src2] = true

		case vm.OpHalt:
			usedRRegs[inst.Dst()] = true

		case vm.OpHaltF:
			usedFRegs[inst.Dst()] = true
		}
	}

	// Second pass: remove instructions that define unused registers
	newCode := make([]vm.Instruction, 0, len(program.Code))

	for _, inst := range program.Code {
		op := inst.Opcode()
		dst := inst.Dst()

		switch op {
		case vm.OpSelectCol:
			// Only include if the destination V register is used
			if usedVRegs[dst] {
				newCode = append(newCode, inst)
			}
			// Skip unused SELECT_COL

		case vm.OpBroadcast, vm.OpBroadcastF:
			if usedVRegs[dst] {
				newCode = append(newCode, inst)
			}

		default:
			// Keep all other instructions
			newCode = append(newCode, inst)
		}
	}

	return &vm.Program{
		Code:           newCode,
		Constants:      program.Constants,
		FloatConstants: program.FloatConstants,
	}
}
