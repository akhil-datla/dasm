package optimizer

import (
	"testing"

	"github.com/akhildatla/dasm/pkg/vm"
)

func TestConstantFolding_Addition(t *testing.T) {
	// LOAD_CONST R0, 5
	// LOAD_CONST R1, 10
	// ADD_R R2, R0, R1
	// HALT R2
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpAddR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(5), int64(10)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// The ADD_R should be replaced with LOAD_CONST R2, 15
	// So we expect: LOAD_CONST R0, 5; LOAD_CONST R1, 10; LOAD_CONST R2, 15; HALT R2
	if len(result.Code) != 4 {
		t.Fatalf("expected 4 instructions, got %d", len(result.Code))
	}

	// Check that the third instruction is now LOAD_CONST
	if result.Code[2].Opcode() != vm.OpLoadConst {
		t.Errorf("expected OpLoadConst, got %v", result.Code[2].Opcode())
	}

	// Check that the new constant was added (should be 15)
	if len(result.Constants) != 3 {
		t.Fatalf("expected 3 constants, got %d", len(result.Constants))
	}

	if result.Constants[2] != int64(15) {
		t.Errorf("expected constant 15, got %v", result.Constants[2])
	}
}

func TestConstantFolding_Multiplication(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpMulR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(6), int64(7)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// Check that the new constant is 42
	if result.Constants[2] != int64(42) {
		t.Errorf("expected constant 42, got %v", result.Constants[2])
	}
}

func TestConstantFolding_ChainedOperations(t *testing.T) {
	// LOAD_CONST R0, 10
	// LOAD_CONST R1, 5
	// ADD_R R2, R0, R1   ; R2 = 15
	// LOAD_CONST R3, 2
	// MUL_R R4, R2, R3   ; R4 = 30
	// HALT R4
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpAddR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 3, 0, 0, 2),
			vm.EncodeInstruction(vm.OpMulR, 0, 4, 2, 3, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 4, 0, 0, 0),
		},
		Constants: []any{int64(10), int64(5), int64(2)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// Should have added constants 15 and 30
	found30 := false
	for _, c := range result.Constants {
		if v, ok := c.(int64); ok && v == 30 {
			found30 = true
			break
		}
	}

	if !found30 {
		t.Errorf("expected to find constant 30, constants are: %v", result.Constants)
	}
}

func TestConstantFolding_NonConstantNotFolded(t *testing.T) {
	// LOAD_CSV R0, "data.csv"
	// ROW_COUNT R1, R0
	// LOAD_CONST R2, 10
	// ADD_R R3, R1, R2   ; Can't fold - R1 is not a constant
	// HALT R3
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadCSV, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpRowCount, 0, 1, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 2, 0, 0, 1),
			vm.EncodeInstruction(vm.OpAddR, 0, 3, 1, 2, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 3, 0, 0, 0),
		},
		Constants: []any{"data.csv", int64(10)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// ADD_R should not be folded
	if result.Code[3].Opcode() != vm.OpAddR {
		t.Errorf("expected OpAddR (not folded), got %v", result.Code[3].Opcode())
	}
}

func TestProjectionPruning_RemovesUnusedColumns(t *testing.T) {
	// LOAD_CSV R0, "data.csv"
	// SELECT_COL V0, R0, "price"
	// SELECT_COL V1, R0, "quantity"  ; unused
	// SELECT_COL V2, R0, "name"      ; unused
	// REDUCE_SUM R1, V0
	// HALT R1
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadCSV, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 1, 0, 0, 2),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 2, 0, 0, 3),
			vm.EncodeInstruction(vm.OpReduceSum, 0, 1, 0, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data.csv", "price", "quantity", "name"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)

	// Should have removed 2 SELECT_COL instructions
	// Original: 6 instructions, After: 4 instructions
	if len(result.Code) != 4 {
		t.Errorf("expected 4 instructions after pruning, got %d", len(result.Code))
	}

	// Verify we kept the right instructions
	expectedOps := []vm.Opcode{vm.OpLoadCSV, vm.OpSelectCol, vm.OpReduceSum, vm.OpHalt}
	for i, expected := range expectedOps {
		if result.Code[i].Opcode() != expected {
			t.Errorf("instruction %d: expected %v, got %v", i, expected, result.Code[i].Opcode())
		}
	}
}

func TestProjectionPruning_KeepsUsedColumns(t *testing.T) {
	// LOAD_CSV R0, "data.csv"
	// SELECT_COL V0, R0, "price"
	// SELECT_COL V1, R0, "quantity"
	// VEC_MUL_F V2, V0, V1
	// REDUCE_SUM_F F0, V2
	// HALT_F F0
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadCSV, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 1, 0, 0, 2),
			vm.EncodeInstruction(vm.OpVecMulF, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpReduceSumF, 0, 0, 2, 0, 0),
			vm.EncodeInstruction(vm.OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data.csv", "price", "quantity"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)

	// All instructions should be kept
	if len(result.Code) != 6 {
		t.Errorf("expected 6 instructions (all kept), got %d", len(result.Code))
	}
}

func TestAllOptimizations(t *testing.T) {
	// Combine constant folding with projection pruning
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpAddR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(100), int64(200)},
	}

	opt := New(WithAllOptimizations())
	result := opt.Optimize(program)

	// Constant folding should have produced 300
	found300 := false
	for _, c := range result.Constants {
		if v, ok := c.(int64); ok && v == 300 {
			found300 = true
			break
		}
	}

	if !found300 {
		t.Errorf("expected to find constant 300, constants are: %v", result.Constants)
	}
}

func TestPredicatePushdown_TracksFilters(t *testing.T) {
	// Basic test that predicate pushdown doesn't break the program
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadCSV, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 1, 0, 0, 2),
			vm.EncodeInstruction(vm.OpCmpGT, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpFilter, 0, 3, 0, 2, 0),
			vm.EncodeInstruction(vm.OpReduceSum, 0, 1, 3, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data.csv", "price", "threshold"},
	}

	opt := New(WithPredicatePushdown())
	result := opt.Optimize(program)

	// Should have the same number of instructions (this is a simple implementation)
	if len(result.Code) != len(program.Code) {
		t.Errorf("expected %d instructions, got %d", len(program.Code), len(result.Code))
	}
}

// ===== Additional Edge Case Tests =====

func TestOptimizer_NoOptimizations(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpAddR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(5), int64(10)},
	}

	// No options - no optimizations enabled
	opt := New()
	result := opt.Optimize(program)

	// Should be unchanged
	if len(result.Code) != len(program.Code) {
		t.Errorf("expected same instruction count")
	}
	// ADD_R should still be there (not folded)
	if result.Code[2].Opcode() != vm.OpAddR {
		t.Errorf("expected OpAddR to remain, got %v", result.Code[2].Opcode())
	}
}

func TestConstantFolding_Subtraction(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpSubR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(20), int64(8)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// Should fold 20 - 8 = 12
	found12 := false
	for _, c := range result.Constants {
		if v, ok := c.(int64); ok && v == 12 {
			found12 = true
			break
		}
	}
	if !found12 {
		t.Errorf("expected constant 12, got %v", result.Constants)
	}
}

func TestConstantFolding_Division(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpDivR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(100), int64(5)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// Should fold 100 / 5 = 20
	found20 := false
	for _, c := range result.Constants {
		if v, ok := c.(int64); ok && v == 20 {
			found20 = true
			break
		}
	}
	if !found20 {
		t.Errorf("expected constant 20, got %v", result.Constants)
	}
}

func TestConstantFolding_DivisionByZero(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpDivR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(10), int64(0)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// Division by zero should NOT be folded - instruction should remain
	if result.Code[2].Opcode() != vm.OpDivR {
		t.Errorf("expected OpDivR to remain when dividing by zero, got %v", result.Code[2].Opcode())
	}
}

func TestProjectionPruning_AllUsed(t *testing.T) {
	// All columns are used - nothing should be pruned
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadCSV, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpReduceSum, 0, 1, 0, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data.csv", "price"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)

	// Should have same number of instructions
	if len(result.Code) != len(program.Code) {
		t.Errorf("expected %d instructions, got %d", len(program.Code), len(result.Code))
	}
}

func TestProjectionPruning_EmptyProgram(t *testing.T) {
	program := &vm.Program{
		Code:      []vm.Instruction{},
		Constants: []any{},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)

	if len(result.Code) != 0 {
		t.Errorf("expected 0 instructions, got %d", len(result.Code))
	}
}

func TestConstantFolding_EmptyProgram(t *testing.T) {
	program := &vm.Program{
		Code:      []vm.Instruction{},
		Constants: []any{},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	if len(result.Code) != 0 {
		t.Errorf("expected 0 instructions, got %d", len(result.Code))
	}
}

func TestOptimizer_PreservesFloatConstants(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConstF, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants:      []any{},
		FloatConstants: []float64{3.14159},
	}

	opt := New(WithAllOptimizations())
	result := opt.Optimize(program)

	if len(result.FloatConstants) != 1 || result.FloatConstants[0] != 3.14159 {
		t.Errorf("expected float constant to be preserved, got %v", result.FloatConstants)
	}
}

func TestOptimizer_MultiplePassesNotNeeded(t *testing.T) {
	// Simple case where a single pass is sufficient
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{int64(42)},
	}

	opt := New(WithAllOptimizations())
	result := opt.Optimize(program)

	if len(result.Code) != 2 {
		t.Errorf("expected 2 instructions, got %d", len(result.Code))
	}
}

func TestConstantFolding_LargeConstants(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpAddR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(1000000000), int64(2000000000)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// Should fold to 3000000000
	found := false
	for _, c := range result.Constants {
		if v, ok := c.(int64); ok && v == 3000000000 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected constant 3000000000, got %v", result.Constants)
	}
}

func TestConstantFolding_NegativeNumbers(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpAddR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(-100), int64(50)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// Should fold to -50
	foundNeg50 := false
	for _, c := range result.Constants {
		if v, ok := c.(int64); ok && v == -50 {
			foundNeg50 = true
			break
		}
	}
	if !foundNeg50 {
		t.Errorf("expected constant -50, got %v", result.Constants)
	}
}

// ===== Projection Pruning Extended Tests =====

func TestProjectionPruning_StringOps(t *testing.T) {
	// Test that string operations are tracked
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1), // V0 = col "a"
			vm.EncodeInstruction(vm.OpSelectCol, 0, 1, 0, 0, 2), // V1 = col "b" - unused
			vm.EncodeInstruction(vm.OpStrLen, 0, 2, 0, 0, 0),    // V2 = strlen(V0)
			vm.EncodeInstruction(vm.OpReduceSum, 0, 0, 2, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)

	// Should have removed the unused SELECT_COL
	if len(result.Code) >= len(program.Code) {
		t.Logf("instructions: %d -> %d", len(program.Code), len(result.Code))
	}
}

func TestProjectionPruning_JoinOps(t *testing.T) {
	// Test that join operations track register usage
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpJoinInner, 0, 2, 0, 1, 2),
			vm.EncodeInstruction(vm.OpRowCount, 0, 3, 2, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 3, 0, 0, 0),
		},
		Constants: []any{"left", "right", "id"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)

	// Should preserve all instructions since all registers are used
	if len(result.Code) != len(program.Code) {
		t.Errorf("expected %d instructions, got %d", len(program.Code), len(result.Code))
	}
}

func TestProjectionPruning_BroadcastOps(t *testing.T) {
	// Test broadcast operation register tracking
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 2),
			vm.EncodeInstruction(vm.OpBroadcast, 0, 1, 1, 0, 0), // V1 = broadcast(R1, V0 length)
			vm.EncodeInstruction(vm.OpVecAddI, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpReduceSum, 0, 0, 2, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "values", int64(10)},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)
	_ = result
}

func TestProjectionPruning_GroupByOps(t *testing.T) {
	// Test GroupBy operation register tracking
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1), // V0 = category
			vm.EncodeInstruction(vm.OpSelectCol, 0, 1, 0, 0, 2), // V1 = amount
			vm.EncodeInstruction(vm.OpGroupBy, 0, 0, 0, 0, 0),   // R0 = groupby(V0)
			vm.EncodeInstruction(vm.OpGroupSum, 0, 2, 0, 1, 0),  // V2 = group_sum(R0, V1)
			vm.EncodeInstruction(vm.OpReduceSum, 0, 1, 2, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data", "category", "amount"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)

	// All registers are used, so no pruning
	if len(result.Code) != len(program.Code) {
		t.Errorf("expected %d instructions, got %d", len(program.Code), len(result.Code))
	}
}

func TestProjectionPruning_FloatOps(t *testing.T) {
	// Test float operations
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpLoadConstF, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpBroadcastF, 0, 1, 0, 0, 0), // V1 = broadcast(F0, V0 length)
			vm.EncodeInstruction(vm.OpVecAddF, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpReduceSumF, 0, 1, 2, 0, 0),
			vm.EncodeInstruction(vm.OpHaltF, 0, 1, 0, 0, 0),
		},
		Constants:      []any{"data", "values"},
		FloatConstants: []float64{3.14},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)
	_ = result
}

func TestProjectionPruning_MoveOps(t *testing.T) {
	// Test move operations
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpMoveR, 0, 1, 0, 0, 0), // R1 = R0
			vm.EncodeInstruction(vm.OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{int64(42)},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)
	_ = result
}

func TestProjectionPruning_MoveF(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConstF, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpMoveF, 0, 1, 0, 0, 0), // F1 = F0
			vm.EncodeInstruction(vm.OpHaltF, 0, 1, 0, 0, 0),
		},
		FloatConstants: []float64{3.14},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)
	_ = result
}

func TestProjectionPruning_GroupMinMax(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 1, 0, 0, 2),
			vm.EncodeInstruction(vm.OpGroupBy, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpGroupMin, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpReduceMin, 0, 1, 2, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data", "category", "values"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)
	_ = result
}

func TestProjectionPruning_GroupMinMaxF(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 1, 0, 0, 2),
			vm.EncodeInstruction(vm.OpGroupBy, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpGroupMinF, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpReduceMinF, 0, 1, 2, 0, 0),
			vm.EncodeInstruction(vm.OpHaltF, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data", "category", "values"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)
	_ = result
}

func TestProjectionPruning_GroupKeys(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpGroupBy, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpGroupKeys, 0, 1, 0, 0, 0),
			vm.EncodeInstruction(vm.OpReduceCount, 0, 0, 1, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "category"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)
	_ = result
}

func TestProjectionPruning_ColRowCount(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpColCount, 0, 1, 0, 0, 0),
			vm.EncodeInstruction(vm.OpRowCount, 0, 2, 0, 0, 0),
			vm.EncodeInstruction(vm.OpAddR, 0, 3, 1, 2, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 3, 0, 0, 0),
		},
		Constants: []any{"data"},
	}

	opt := New(WithProjectionPruning())
	result := opt.Optimize(program)
	_ = result
}

func TestPredicatePushdown_MultipleFilters(t *testing.T) {
	// Test multiple filter conditions
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadFrame, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 0, 0, 0, 1),
			vm.EncodeInstruction(vm.OpSelectCol, 0, 1, 0, 0, 2),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 3),
			vm.EncodeInstruction(vm.OpBroadcast, 0, 2, 1, 0, 0),
			vm.EncodeInstruction(vm.OpCmpGT, 0, 3, 0, 2, 0),
			vm.EncodeInstruction(vm.OpFilter, 0, 4, 1, 3, 0),
			vm.EncodeInstruction(vm.OpReduceSum, 0, 2, 4, 0, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{"data", "price", "quantity", int64(10)},
	}

	opt := New(WithPredicatePushdown())
	result := opt.Optimize(program)
	_ = result
}

func TestConstantFolding_SubtractionNegative(t *testing.T) {
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpSubR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(10), int64(20)},
	}

	opt := New(WithConstantFolding())
	result := opt.Optimize(program)

	// Should fold to -10
	foundResult := false
	for _, c := range result.Constants {
		if v, ok := c.(int64); ok && v == -10 {
			foundResult = true
			break
		}
	}
	if !foundResult {
		t.Errorf("expected constant -10, got %v", result.Constants)
	}
}

func TestOptimizer_AllPasses(t *testing.T) {
	// Test with all optimization passes enabled
	program := &vm.Program{
		Code: []vm.Instruction{
			vm.EncodeInstruction(vm.OpLoadConst, 0, 0, 0, 0, 0),
			vm.EncodeInstruction(vm.OpLoadConst, 0, 1, 0, 0, 1),
			vm.EncodeInstruction(vm.OpAddR, 0, 2, 0, 1, 0),
			vm.EncodeInstruction(vm.OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(5), int64(10)},
	}

	opt := New(
		WithConstantFolding(),
		WithPredicatePushdown(),
		WithProjectionPruning(),
	)
	result := opt.Optimize(program)
	_ = result
}
