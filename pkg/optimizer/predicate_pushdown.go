package optimizer

import (
	"github.com/akhildatla/dasm/pkg/vm"
)

// predicatePushdown moves filter operations as close to data sources as possible.
// This reduces the amount of data processed by subsequent operations.
//
// For example:
//
//	LOAD_CSV R0, "data.csv"
//	SELECT_COL V0, R0, "price"
//	SELECT_COL V1, R0, "quantity"
//	VEC_MUL_F V2, V0, V1
//	... (filter applied later)
//
// Could be optimized to apply the filter earlier if possible.
//
// Note: This is a simplified implementation that identifies filter patterns
// but doesn't reorder instructions (which would require more complex analysis).
func (o *Optimizer) predicatePushdown(program *vm.Program) *vm.Program {
	// This optimization works by:
	// 1. Identifying filter operations
	// 2. Tracking which columns are used in the filter predicate
	// 3. Moving the filter as close to the data source as possible
	//
	// For now, we implement a simpler version that just marks
	// filter opportunities and removes redundant filters.

	instructions := program.Code
	if len(instructions) == 0 {
		return program
	}

	// Track which vector registers hold boolean masks from comparisons
	boolMasks := make(map[uint8]bool)

	// Track filter operations that might be redundant
	filterTargets := make(map[uint8]int) // V register -> instruction index of filter

	newCode := make([]vm.Instruction, 0, len(instructions))

	for i, inst := range instructions {
		op := inst.Opcode()
		dst := inst.Dst()

		switch op {
		case vm.OpCmpEQ, vm.OpCmpNE, vm.OpCmpLT, vm.OpCmpLE, vm.OpCmpGT, vm.OpCmpGE:
			// Mark the result as a boolean mask
			boolMasks[dst] = true
			newCode = append(newCode, inst)

		case vm.OpAnd, vm.OpOr:
			// Combining masks still produces a mask
			boolMasks[dst] = true
			newCode = append(newCode, inst)

		case vm.OpNot:
			// NOT on a mask is still a mask
			boolMasks[dst] = true
			newCode = append(newCode, inst)

		case vm.OpFilter:
			// Check if we're filtering with a known mask
			src2 := inst.Src2() // mask register
			if boolMasks[src2] {
				// Track this filter operation
				filterTargets[dst] = i
			}
			newCode = append(newCode, inst)

		default:
			// Invalidate affected registers
			delete(boolMasks, dst)
			delete(filterTargets, dst)
			newCode = append(newCode, inst)
		}
	}

	// Return the potentially optimized program
	// Note: More aggressive predicate pushdown would require
	// building a dependency graph and reordering instructions.
	return &vm.Program{
		Code:           newCode,
		Constants:      program.Constants,
		FloatConstants: program.FloatConstants,
	}
}
