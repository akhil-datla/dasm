package optimizer

import (
	"testing"

	"github.com/akhildatla/dasm/pkg/vm"
)

func TestDeadCodeElimination_RemovesUnusedRegisters(t *testing.T) {
	// Program: LOAD_CONST R0, 42; LOAD_CONST R1, 100; HALT R0
	// R1 is never used, so the second LOAD_CONST should be removed
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0), // R0 = 42
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1), // R1 = 100 (dead)
			vm.EncodeInstruction(vm.OpHalt, 0, 0, 0, 0, 0),      // return R0
		},
		Constants:      []any{int64(42), int64(100)},
		FloatConstants: []float64{},
	}

	opt := New(WithDeadCodeElimination())
	result := opt.Optimize(program)

	if len(result.Code) != 2 {
		t.Errorf("expected 2 instructions after dead code elimination, got %d", len(result.Code))
	}
}

func TestDeadCodeElimination_KeepsUsedRegisters(t *testing.T) {
	// Program: LOAD_CONST R0, 1; LOAD_CONST R1, 2; ADD_R R2, R0, R1; HALT R2
	// All registers are used
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0), // R0 = 1
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1), // R1 = 2
			vm.EncodeInstruction(vm.OpAddR, 0, 2, 0, 1, 0),      // R2 = R0 + R1
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),      // return R2
		},
		Constants:      []any{int64(1), int64(2)},
		FloatConstants: []float64{},
	}

	opt := New(WithDeadCodeElimination())
	result := opt.Optimize(program)

	if len(result.Code) != 4 {
		t.Errorf("expected 4 instructions (none dead), got %d", len(result.Code))
	}
}

func TestDeadCodeElimination_EmptyProgram(t *testing.T) {
	program := &vm.Program{
		Code:           []vm.Instruction{},
		Constants:      []any{},
		FloatConstants: []float64{},
	}

	opt := New(WithDeadCodeElimination())
	result := opt.Optimize(program)

	if len(result.Code) != 0 {
		t.Errorf("expected empty program, got %d instructions", len(result.Code))
	}
}

func TestDeadCodeElimination_NoHalt(t *testing.T) {
	// Program without HALT - should be unchanged
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
		},
		Constants:      []any{int64(1), int64(2)},
		FloatConstants: []float64{},
	}

	opt := New(WithDeadCodeElimination())
	result := opt.Optimize(program)

	// Without HALT, can't determine what's dead, so nothing changes
	if len(result.Code) != 2 {
		t.Errorf("expected 2 instructions (no optimization without HALT), got %d", len(result.Code))
	}
}

func TestDeadCodeElimination_FloatRegisters(t *testing.T) {
	// Program: LOAD_CONST_F F0, 3.14; LOAD_CONST_F F1, 2.71; HALT_F F0
	// F1 is never used
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConstF, 0, 0, 0, 0, 0), // F0 = 3.14
			vm.EncodeInstruction(vm.OpLoadConstF, 0, 1, 0, 0, 1), // F1 = 2.71 (dead)
			vm.EncodeInstruction(vm.OpHaltF, 0, 0, 0, 0, 0),      // return F0
		},
		Constants:      []any{},
		FloatConstants: []float64{3.14, 2.71},
	}

	opt := New(WithDeadCodeElimination())
	result := opt.Optimize(program)

	if len(result.Code) != 2 {
		t.Errorf("expected 2 instructions after removing dead float register, got %d", len(result.Code))
	}
}

func TestDeadCodeElimination_VectorRegisters(t *testing.T) {
	// Complex test with vector operations
	// V0 and V1 are used, V2 is dead (computed but never used in result)
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0), // R0 = frame
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1), // V0 = R0.col1
			vm.EncodeInstruction(vm.OpSelectCol, 0, 1, 0, 0, 2), // V1 = R0.col2 (dead if not used)
			vm.EncodeInstruction(vm.OpReduceSum, 0, 1, 0, 0, 0), // R1 = sum(V0)
			vm.EncodeInstruction(vm.OpHalt, 0, 1, 0, 0, 0),      // return R1
		},
		Constants:      []any{"data", "col1", "col2"},
		FloatConstants: []float64{},
	}

	opt := New(WithDeadCodeElimination())
	result := opt.Optimize(program)

	// V1 is never used after SELECT_COL, so it should be removed
	if len(result.Code) != 4 {
		t.Errorf("expected 4 instructions after removing dead vector register, got %d", len(result.Code))
	}
}

func TestDeadCodeElimination_NopRemoval(t *testing.T) {
	// NOP instructions should always be removed
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0), // R0 = 42
			vm.EncodeInstruction(vm.OpNop, 0, 0, 0, 0, 0),       // NOP (dead)
			vm.EncodeInstruction(vm.OpNop, 0, 0, 0, 0, 0),       // NOP (dead)
			vm.EncodeInstruction(vm.OpHalt, 0, 0, 0, 0, 0),      // return R0
		},
		Constants:      []any{int64(42)},
		FloatConstants: []float64{},
	}

	opt := New(WithDeadCodeElimination())
	result := opt.Optimize(program)

	if len(result.Code) != 2 {
		t.Errorf("expected 2 instructions after removing NOPs, got %d", len(result.Code))
	}
}

func TestDeadCodeElimination_ChainedDependencies(t *testing.T) {
	// R0 -> R1 -> R2 -> HALT R2
	// All are needed
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0), // R0 = 1
			vm.EncodeInstruction(vm.OpMoveR, 0, 1, 0, 0, 0),     // R1 = R0
			vm.EncodeInstruction(vm.OpMoveR, 0, 2, 1, 0, 0),     // R2 = R1
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),      // return R2
		},
		Constants:      []any{int64(1)},
		FloatConstants: []float64{},
	}

	opt := New(WithDeadCodeElimination())
	result := opt.Optimize(program)

	if len(result.Code) != 4 {
		t.Errorf("expected 4 instructions (all in dependency chain), got %d", len(result.Code))
	}
}

func TestDeadCodeElimination_MultipleDeadBranches(t *testing.T) {
	// Multiple independent dead computations
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0), // R0 = used
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1), // R1 = dead
			vm.EncodeInstruction(vm.OpLoadConst, 0, 2, 0, 0, 2), // R2 = dead
			vm.EncodeInstruction(vm.OpLoadConst, 0, 3, 0, 0, 3), // R3 = dead
			vm.EncodeInstruction(vm.OpHalt, 0, 0, 0, 0, 0),      // return R0
		},
		Constants:      []any{int64(1), int64(2), int64(3), int64(4)},
		FloatConstants: []float64{},
	}

	opt := New(WithDeadCodeElimination())
	result := opt.Optimize(program)

	if len(result.Code) != 2 {
		t.Errorf("expected 2 instructions after removing 3 dead loads, got %d", len(result.Code))
	}
}
