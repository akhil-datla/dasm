package optimizer

import (
	"github.com/akhildatla/dasm/pkg/vm"
)

// WithDeadCodeElimination enables dead code elimination.
func WithDeadCodeElimination() Option {
	return func(o *Optimizer) {
		o.enableDeadCode = true
	}
}

// deadCodeElimination removes unreachable code and unused instructions.
func (o *Optimizer) deadCodeElimination(program *vm.Program) *vm.Program {
	if len(program.Code) == 0 {
		return program
	}

	// Track which registers are used before being written
	usedRegs := make(map[uint8]bool)   // R registers
	usedVecs := make(map[uint8]bool)   // V registers
	usedFloats := make(map[uint8]bool) // F registers

	// Find the HALT instruction and work backwards
	haltIdx := -1
	for i := len(program.Code) - 1; i >= 0; i-- {
		op := program.Code[i].Opcode()
		if op == vm.OpHalt || op == vm.OpHaltF {
			haltIdx = i
			break
		}
	}

	if haltIdx == -1 {
		// No HALT found, can't optimize
		return program
	}

	// Mark the result register of HALT as used
	haltInst := program.Code[haltIdx]
	haltOp := haltInst.Opcode()
	haltDst := haltInst.Dst()

	if haltOp == vm.OpHalt {
		usedRegs[haltDst] = true
	} else {
		usedFloats[haltDst] = true
	}

	// Work backwards from HALT to find all needed instructions
	needed := make([]bool, len(program.Code))
	needed[haltIdx] = true

	// Multiple passes to handle dependencies
	changed := true
	for changed {
		changed = false
		for i := haltIdx - 1; i >= 0; i-- {
			if needed[i] {
				continue
			}

			inst := program.Code[i]
			op := inst.Opcode()
			dst := inst.Dst()

			// Check if this instruction produces a value that's used
			isNeeded := false

			switch op {
			// Instructions that write to R registers
			case vm.OpLoadCSV, vm.OpLoadJSON, vm.OpLoadParquet, vm.OpLoadFrame, vm.OpLoadConst, vm.OpReduceSum,
				vm.OpReduceCount, vm.OpReduceMin, vm.OpReduceMax,
				vm.OpMoveR, vm.OpAddR, vm.OpSubR, vm.OpMulR, vm.OpDivR,
				vm.OpNewFrame, vm.OpRowCount, vm.OpColCount, vm.OpGroupBy:
				if usedRegs[dst] {
					isNeeded = true
				}

			// Instructions that write to F registers
			case vm.OpLoadConstF, vm.OpReduceSumF, vm.OpReduceMinF, vm.OpReduceMaxF,
				vm.OpReduceMean, vm.OpMoveF:
				if usedFloats[dst] {
					isNeeded = true
				}

			// Instructions that write to V registers
			case vm.OpSelectCol, vm.OpBroadcast, vm.OpBroadcastF,
				vm.OpVecAddI, vm.OpVecSubI, vm.OpVecMulI, vm.OpVecDivI, vm.OpVecModI,
				vm.OpVecAddF, vm.OpVecSubF, vm.OpVecMulF, vm.OpVecDivF,
				vm.OpCmpEQ, vm.OpCmpNE, vm.OpCmpLT, vm.OpCmpLE, vm.OpCmpGT, vm.OpCmpGE,
				vm.OpAnd, vm.OpOr, vm.OpNot, vm.OpFilter, vm.OpTake,
				vm.OpGroupSum, vm.OpGroupSumF, vm.OpGroupMin, vm.OpGroupMax,
				vm.OpGroupMinF, vm.OpGroupMaxF, vm.OpGroupMean, vm.OpGroupCount, vm.OpGroupKeys,
				vm.OpStrLen, vm.OpStrUpper, vm.OpStrLower, vm.OpStrTrim, vm.OpStrConcat,
				vm.OpStrContains, vm.OpStrStartsWith, vm.OpStrEndsWith, vm.OpStrSplit, vm.OpStrReplace:
				if usedVecs[dst] {
					isNeeded = true
				}

			// Instructions with side effects are always needed
			case vm.OpAddCol, vm.OpJoinInner, vm.OpJoinLeft, vm.OpJoinRight, vm.OpJoinOuter:
				isNeeded = true

			case vm.OpNop:
				// NOP is never needed
				isNeeded = false
			}

			if isNeeded {
				needed[i] = true
				changed = true

				// Mark source registers as used
				markSourcesUsed(inst, usedRegs, usedVecs, usedFloats)
			}
		}
	}

	// Also keep instructions that have side effects (like frame operations)
	for i := 0; i <= haltIdx; i++ {
		op := program.Code[i].Opcode()
		if op == vm.OpLoadCSV || op == vm.OpLoadJSON || op == vm.OpLoadParquet || op == vm.OpLoadFrame {
			// Check if the frame is used
			dst := program.Code[i].Dst()
			if usedRegs[dst] {
				needed[i] = true
				markSourcesUsed(program.Code[i], usedRegs, usedVecs, usedFloats)
			}
		}
	}

	// Count how many instructions we're keeping
	keepCount := 0
	for i := 0; i <= haltIdx; i++ {
		if needed[i] {
			keepCount++
		}
	}

	// If we're keeping everything, return original
	if keepCount == haltIdx+1 {
		return program
	}

	// Build new code without dead instructions
	newCode := make([]vm.Instruction, 0, keepCount)
	for i := 0; i <= haltIdx; i++ {
		if needed[i] {
			newCode = append(newCode, program.Code[i])
		}
	}

	return &vm.Program{
		Code:           newCode,
		Constants:      program.Constants,
		FloatConstants: program.FloatConstants,
	}
}

// markSourcesUsed marks source registers as used based on the instruction
func markSourcesUsed(inst vm.Instruction, usedRegs, usedVecs, usedFloats map[uint8]bool) {
	op := inst.Opcode()
	src1 := inst.Src1()
	src2 := inst.Src2()

	switch op {
	// Vector binary ops: V[src1], V[src2]
	case vm.OpVecAddI, vm.OpVecSubI, vm.OpVecMulI, vm.OpVecDivI, vm.OpVecModI,
		vm.OpVecAddF, vm.OpVecSubF, vm.OpVecMulF, vm.OpVecDivF,
		vm.OpCmpEQ, vm.OpCmpNE, vm.OpCmpLT, vm.OpCmpLE, vm.OpCmpGT, vm.OpCmpGE,
		vm.OpAnd, vm.OpOr, vm.OpFilter, vm.OpTake, vm.OpStrConcat:
		usedVecs[src1] = true
		usedVecs[src2] = true

	// Vector unary ops: V[src1]
	case vm.OpNot, vm.OpStrLen, vm.OpStrUpper, vm.OpStrLower, vm.OpStrTrim:
		usedVecs[src1] = true

	// String pattern ops: V[src1]
	case vm.OpStrContains, vm.OpStrStartsWith, vm.OpStrEndsWith, vm.OpStrSplit, vm.OpStrReplace:
		usedVecs[src1] = true

	// Reduce ops: V[src1]
	case vm.OpReduceSum, vm.OpReduceSumF, vm.OpReduceCount,
		vm.OpReduceMin, vm.OpReduceMax, vm.OpReduceMinF, vm.OpReduceMaxF, vm.OpReduceMean:
		usedVecs[src1] = true

	// SelectCol: R[src1] (frame)
	case vm.OpSelectCol:
		usedRegs[src1] = true

	// Broadcast: R[src1] (value), V[src2] (length)
	case vm.OpBroadcast:
		usedRegs[src1] = true
		usedVecs[src2] = true

	// BroadcastF: F[src1] (value), V[src2] (length)
	case vm.OpBroadcastF:
		usedFloats[src1] = true
		usedVecs[src2] = true

	// Scalar ops: R[src1], R[src2]
	case vm.OpAddR, vm.OpSubR, vm.OpMulR, vm.OpDivR:
		usedRegs[src1] = true
		usedRegs[src2] = true

	// Scalar unary: R[src1] or F[src1]
	case vm.OpMoveR, vm.OpRowCount, vm.OpColCount:
		usedRegs[src1] = true

	case vm.OpMoveF:
		usedFloats[src1] = true

	// GroupBy: V[src1]
	case vm.OpGroupBy:
		usedVecs[src1] = true

	// GroupAgg: R[src1] (gb), V[src2] (values)
	case vm.OpGroupSum, vm.OpGroupSumF, vm.OpGroupMin, vm.OpGroupMax,
		vm.OpGroupMinF, vm.OpGroupMaxF, vm.OpGroupMean:
		usedRegs[src1] = true
		usedVecs[src2] = true

	// GroupUnary: R[src1]
	case vm.OpGroupCount, vm.OpGroupKeys:
		usedRegs[src1] = true

	// Join: R[src1], R[src2]
	case vm.OpJoinInner, vm.OpJoinLeft, vm.OpJoinRight, vm.OpJoinOuter:
		usedRegs[src1] = true
		usedRegs[src2] = true

	// AddCol: R[dst] (frame), V[src1] (column)
	case vm.OpAddCol:
		usedVecs[src1] = true
	}
}
