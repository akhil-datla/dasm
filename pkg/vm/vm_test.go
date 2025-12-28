package vm

import (
	"context"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// ===== Integration Tests: Core Data Loading =====

func TestVM_LoadConst(t *testing.T) {
	vm := NewVM()
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{int64(42)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestVM_LoadConstF(t *testing.T) {
	vm := NewVM()
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConstF, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		FloatConstants: []float64{3.14159},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != 3.14159 {
		t.Errorf("expected 3.14159, got %v", result)
	}
}

func TestVM_LoadFrame(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 1.0, 2.0, 3.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpRowCount, 0, 1, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != int64(3) {
		t.Errorf("expected 3, got %v", result)
	}
}

// ===== Integration Tests: Column Selection & Broadcast =====

func TestVM_SelectCol(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"sales": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpReduceSumF, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"sales", "price"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := 60.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_Broadcast(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 1, 2, 3, 4, 5),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1), // V0 = data.value
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 2), // R1 = 10
			EncodeInstruction(OpBroadcast, 0, 1, 1, 0, 0), // V1 = broadcast(R1, len(V0))
			EncodeInstruction(OpVecAddI, 0, 2, 0, 1, 0),   // V2 = V0 + V1
			EncodeInstruction(OpReduceSum, 0, 2, 2, 0, 0), // R2 = sum(V2)
			EncodeInstruction(OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{"data", "value", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// (1+10) + (2+10) + (3+10) + (4+10) + (5+10) = 11+12+13+14+15 = 65
	expected := int64(65)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Integration Tests: Vector Arithmetic =====

func TestVM_VecAddI(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 1, 2, 3),
		dataframe.NewSeriesInt64("b", nil, 10, 20, 30),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1), // V0 = a
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2), // V1 = b
			EncodeInstruction(OpVecAddI, 0, 2, 0, 1, 0),   // V2 = V0 + V1
			EncodeInstruction(OpReduceSum, 0, 0, 2, 0, 0), // R0 = sum(V2)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// (1+10) + (2+20) + (3+30) = 11 + 22 + 33 = 66
	expected := int64(66)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_VecMulF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
		dataframe.NewSeriesFloat64("qty", nil, 2.0, 3.0, 4.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = price
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),  // V1 = qty
			EncodeInstruction(OpVecMulF, 0, 2, 0, 1, 0),    // V2 = V0 * V1
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0), // F0 = sum(V2)
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "price", "qty"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 10*2 + 20*3 + 30*4 = 20 + 60 + 120 = 200
	expected := 200.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_VecDivF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("a", nil, 100.0, 200.0),
		dataframe.NewSeriesFloat64("b", nil, 10.0, 20.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpVecDivF, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 100/10 + 200/20 = 10 + 10 = 20
	expected := 20.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Integration Tests: Comparisons =====

func TestVM_CmpGT(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 5, 15, 8, 25),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),   // V0 = value
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 2),   // R1 = 10
			EncodeInstruction(OpBroadcast, 0, 1, 1, 0, 0),   // V1 = broadcast(10)
			EncodeInstruction(OpCmpGT, 0, 2, 0, 1, 0),       // V2 = V0 > V1
			EncodeInstruction(OpReduceCount, 0, 0, 2, 0, 0), // R0 = count(true values)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Values > 10: 15, 25 = 2 values
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_CmpEQ(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 10, 20, 10, 30, 10),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 2),
			EncodeInstruction(OpBroadcast, 0, 1, 1, 0, 0),
			EncodeInstruction(OpCmpEQ, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceCount, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Values == 10: 3 values
	expected := int64(3)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Integration Tests: Filtering =====

func TestVM_Filter(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 5.0, 30.0),
		dataframe.NewSeriesInt64("quantity", nil, 5, 15, 3, 20),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = price
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),  // V1 = quantity
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 3),  // R1 = 10
			EncodeInstruction(OpBroadcast, 0, 2, 1, 1, 0),  // V2 = broadcast(10)
			EncodeInstruction(OpCmpGT, 0, 3, 1, 2, 0),      // V3 = quantity > 10
			EncodeInstruction(OpFilter, 0, 4, 0, 3, 0),     // V4 = filter(price, mask)
			EncodeInstruction(OpReduceSumF, 0, 0, 4, 0, 0), // F0 = sum(V4)
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "price", "quantity", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Prices where quantity > 10: 20.0 + 30.0 = 50.0
	expected := 50.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Integration Tests: Aggregations =====

func TestVM_ReduceSum(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 1, 2, 3, 4, 5),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpReduceSum, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := int64(15)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_ReduceMean(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 10.0, 20.0, 30.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpReduceMean, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := 20.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_ReduceMinMax(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 5, 2, 8, 1, 9),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	// Test Min
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpReduceMin, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != int64(1) {
		t.Errorf("expected min 1, got %v", result)
	}

	// Test Max
	vm2 := NewVM()
	vm2.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})
	program2 := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpReduceMax, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value"},
	}

	if err := vm2.Load(program2); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result2, err := vm2.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result2 != int64(9) {
		t.Errorf("expected max 9, got %v", result2)
	}
}

// ===== Integration Tests: Scalar Arithmetic =====

func TestVM_ScalarArithmetic(t *testing.T) {
	tests := []struct {
		name     string
		op       Opcode
		a, b     int64
		expected int64
	}{
		{"add", OpAddR, 10, 5, 15},
		{"sub", OpSubR, 10, 5, 5},
		{"mul", OpMulR, 10, 5, 50},
		{"div", OpDivR, 10, 5, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vm := NewVM()
			program := &Program{
				Code: []Instruction{
					EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0),
					EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 1),
					EncodeInstruction(tt.op, 0, 2, 0, 1, 0),
					EncodeInstruction(OpHalt, 0, 2, 0, 0, 0),
				},
				Constants: []any{tt.a, tt.b},
			}

			if err := vm.Load(program); err != nil {
				t.Fatalf("Load failed: %v", err)
			}

			result, err := vm.Execute()
			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// ===== Integration Tests: Logical Operations =====

func TestVM_LogicalAnd(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 5, 15, 25, 35),
		dataframe.NewSeriesInt64("b", nil, 100, 50, 200, 30),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),   // V0 = a
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),   // V1 = b
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 3),   // R1 = 10
			EncodeInstruction(OpBroadcast, 0, 2, 1, 0, 0),   // V2 = broadcast(10)
			EncodeInstruction(OpLoadConst, 0, 2, 0, 0, 4),   // R2 = 100
			EncodeInstruction(OpBroadcast, 0, 3, 2, 0, 0),   // V3 = broadcast(100)
			EncodeInstruction(OpCmpGT, 0, 4, 0, 2, 0),       // V4 = a > 10
			EncodeInstruction(OpCmpGT, 0, 5, 1, 3, 0),       // V5 = b > 100
			EncodeInstruction(OpAnd, 0, 6, 4, 5, 0),         // V6 = V4 AND V5
			EncodeInstruction(OpReduceCount, 0, 0, 6, 0, 0), // R0 = count(true)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b", int64(10), int64(100)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// a>10 AND b>100: only row 3 (25, 200) matches = 1
	expected := int64(1)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_LogicalOr(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 5, 15),
		dataframe.NewSeriesInt64("b", nil, 200, 50),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 3),
			EncodeInstruction(OpBroadcast, 0, 2, 1, 0, 0),
			EncodeInstruction(OpLoadConst, 0, 2, 0, 0, 4),
			EncodeInstruction(OpBroadcast, 0, 3, 2, 0, 0),
			EncodeInstruction(OpCmpGT, 0, 4, 0, 2, 0), // a > 10
			EncodeInstruction(OpCmpGT, 0, 5, 1, 3, 0), // b > 100
			EncodeInstruction(OpOr, 0, 6, 4, 5, 0),    // V4 OR V5
			EncodeInstruction(OpReduceCount, 0, 0, 6, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b", int64(10), int64(100)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Row 0: a=5 (not >10), b=200 (>100) → true
	// Row 1: a=15 (>10), b=50 (not >100) → true
	// Both rows match with OR
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_LogicalNot(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 5, 15, 25),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 2),
			EncodeInstruction(OpBroadcast, 0, 1, 1, 0, 0),
			EncodeInstruction(OpCmpGT, 0, 2, 0, 1, 0),       // V2 = value > 10
			EncodeInstruction(OpNot, 0, 3, 2, 0, 0),         // V3 = NOT V2
			EncodeInstruction(OpReduceCount, 0, 0, 3, 0, 0), // count where NOT (value > 10)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// NOT (value > 10): only 5 matches = 1
	expected := int64(1)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Integration Tests: Frame Operations =====

func TestVM_ColCount(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
		dataframe.NewSeriesFloat64("qty", nil, 2.0, 3.0, 4.0),
		dataframe.NewSeriesString("name", nil, "a", "b", "c"),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpColCount, 0, 1, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := int64(3)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_RowCount(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 1, 2, 3, 4, 5),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpRowCount, 0, 1, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := int64(5)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Integration Tests: Resource Limits =====

func TestVM_MaxSteps(t *testing.T) {
	vm := NewVM()
	vm.SetMaxSteps(5)

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0),
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 0),
			EncodeInstruction(OpLoadConst, 0, 2, 0, 0, 0),
			EncodeInstruction(OpLoadConst, 0, 3, 0, 0, 0),
			EncodeInstruction(OpLoadConst, 0, 4, 0, 0, 0),
			EncodeInstruction(OpLoadConst, 0, 5, 0, 0, 0), // 6th step should fail
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{int64(1)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	_, err := vm.Execute()
	if err == nil {
		t.Error("expected step limit error")
	}
}

func TestVM_Context_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	vm := NewVM()
	vm.SetContext(ctx)

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{int64(42)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	_, err := vm.Execute()
	if err == nil {
		t.Error("expected context cancellation error")
	}
}

// ===== Integration Tests: End-to-End Workflow =====

func TestVM_CompleteWorkflow_FilterAggregate(t *testing.T) {
	// Complete workflow: load frame, filter by condition, aggregate
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.5, 20.0, 5.0, 30.0, 15.0),
		dataframe.NewSeriesInt64("quantity", nil, 5, 15, 3, 20, 8),
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "C", "B"),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"sales": frame})

	// Calculate sum of prices where quantity > 10
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),  // R0 = sales
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = price
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),  // V1 = quantity
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 3),  // R1 = 10
			EncodeInstruction(OpBroadcast, 0, 2, 1, 1, 0),  // V2 = broadcast(10)
			EncodeInstruction(OpCmpGT, 0, 3, 1, 2, 0),      // V3 = quantity > 10
			EncodeInstruction(OpFilter, 0, 4, 0, 3, 0),     // V4 = filter(price, mask)
			EncodeInstruction(OpReduceSumF, 0, 0, 4, 0, 0), // F0 = sum(V4)
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"sales", "price", "quantity", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Prices where quantity > 10: 20.0 + 30.0 = 50.0
	expected := 50.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_CompleteWorkflow_ComputedColumn(t *testing.T) {
	// Complete workflow: compute total = price * quantity, then sum
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
		dataframe.NewSeriesFloat64("quantity", nil, 2.0, 3.0, 4.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"sales": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = price
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),  // V1 = quantity
			EncodeInstruction(OpVecMulF, 0, 2, 0, 1, 0),    // V2 = price * quantity
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0), // F0 = sum(V2)
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"sales", "price", "quantity"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 10*2 + 20*3 + 30*4 = 20 + 60 + 120 = 200
	expected := 200.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Integration Tests: Edge Cases =====

func TestVM_EmptyFrame(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpReduceSumF, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Sum of empty series should be 0
	expected := 0.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_SingleRow(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 42.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpReduceMean, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := 42.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Test: Nop Instruction =====

func TestVM_Nop(t *testing.T) {
	vm := NewVM()
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0),
			EncodeInstruction(OpNop, 0, 0, 0, 0, 0),
			EncodeInstruction(OpNop, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{int64(99)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != int64(99) {
		t.Errorf("expected 99, got %v", result)
	}
}

// ===== Test: MoveR Instruction =====

func TestVM_MoveR(t *testing.T) {
	vm := NewVM()
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0),
			EncodeInstruction(OpMoveR, 0, 5, 0, 0, 0), // R5 = R0
			EncodeInstruction(OpHalt, 0, 5, 0, 0, 0),
		},
		Constants: []any{int64(123)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != int64(123) {
		t.Errorf("expected 123, got %v", result)
	}
}

// ===== Additional Vector Operations Tests =====

func TestVM_VecSubI(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 100, 200, 300),
		dataframe.NewSeriesInt64("b", nil, 10, 20, 30),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpVecSubI, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceSum, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// (100-10) + (200-20) + (300-30) = 90 + 180 + 270 = 540
	expected := int64(540)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_VecMulI(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 2, 3, 4),
		dataframe.NewSeriesInt64("b", nil, 10, 20, 30),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpVecMulI, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceSum, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// (2*10) + (3*20) + (4*30) = 20 + 60 + 120 = 200
	expected := int64(200)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_VecDivI(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 100, 200, 300),
		dataframe.NewSeriesInt64("b", nil, 10, 20, 30),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpVecDivI, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceSum, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// (100/10) + (200/20) + (300/30) = 10 + 10 + 10 = 30
	expected := int64(30)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_VecModI(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 17, 25, 33),
		dataframe.NewSeriesInt64("b", nil, 5, 7, 10),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpVecModI, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceSum, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// (17%5) + (25%7) + (33%10) = 2 + 4 + 3 = 9
	expected := int64(9)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_VecAddF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("a", nil, 1.5, 2.5, 3.5),
		dataframe.NewSeriesFloat64("b", nil, 0.5, 0.5, 0.5),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpVecAddF, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// (1.5+0.5) + (2.5+0.5) + (3.5+0.5) = 2 + 3 + 4 = 9
	expected := 9.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_VecSubF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("a", nil, 10.0, 20.0, 30.0),
		dataframe.NewSeriesFloat64("b", nil, 1.0, 2.0, 3.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpVecSubF, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "a", "b"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// (10-1) + (20-2) + (30-3) = 9 + 18 + 27 = 54
	expected := 54.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Additional Comparison Tests =====

func TestVM_CmpNE(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 10, 20, 10, 30, 10),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 2),
			EncodeInstruction(OpBroadcast, 0, 1, 1, 0, 0),
			EncodeInstruction(OpCmpNE, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceCount, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Values != 10: 20, 30 = 2 values
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_CmpLT(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 5, 15, 8, 25),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 2),
			EncodeInstruction(OpBroadcast, 0, 1, 1, 0, 0),
			EncodeInstruction(OpCmpLT, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceCount, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Values < 10: 5, 8 = 2 values
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_CmpLE(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 5, 10, 15, 20),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 2),
			EncodeInstruction(OpBroadcast, 0, 1, 1, 0, 0),
			EncodeInstruction(OpCmpLE, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceCount, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Values <= 10: 5, 10 = 2 values
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_CmpGE(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("value", nil, 5, 10, 15, 20),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 2),
			EncodeInstruction(OpBroadcast, 0, 1, 1, 0, 0),
			EncodeInstruction(OpCmpGE, 0, 2, 0, 1, 0),
			EncodeInstruction(OpReduceCount, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value", int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Values >= 10: 10, 15, 20 = 3 values
	expected := int64(3)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Additional Float Aggregation Tests =====

func TestVM_ReduceMinF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 5.5, 2.2, 8.8, 1.1, 9.9),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpReduceMinF, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := 1.1
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_ReduceMaxF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 5.5, 2.2, 8.8, 1.1, 9.9),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpReduceMaxF, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := 9.9
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// Note: String operation tests (StrLen, StrUpper, StrLower) are in phase2_test.go

// ===== GroupBy Tests =====

func TestVM_GroupBy(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "C", "B"),
		dataframe.NewSeriesFloat64("amount", nil, 10.0, 20.0, 30.0, 40.0, 50.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),   // V0 = category
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),   // V1 = amount
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),     // R1 = groupby(V0)
			EncodeInstruction(OpGroupKeys, 0, 2, 1, 0, 0),   // V2 = group keys
			EncodeInstruction(OpReduceCount, 0, 0, 2, 0, 0), // R0 = count(keys) = number of groups
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "category", "amount"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 3 unique groups: A, B, C
	expected := int64(3)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_GroupSum(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "B"),
		dataframe.NewSeriesFloat64("amount", nil, 10.0, 20.0, 30.0, 40.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = category
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),  // V1 = amount
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),    // R1 = groupby(V0)
			EncodeInstruction(OpGroupSum, 0, 2, 1, 1, 0),   // V2 = group_sum(R1, V1)
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0), // F0 = sum(group sums)
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "category", "amount"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// A: 10+30=40, B: 20+40=60, total = 100
	expected := 100.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== BroadcastF Tests =====

func TestVM_BroadcastF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 1.0, 2.0, 3.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = value
			EncodeInstruction(OpLoadConstF, 0, 0, 0, 0, 0), // F0 = 10.5
			EncodeInstruction(OpBroadcastF, 0, 1, 0, 0, 0), // V1 = broadcast(F0, len(V0))
			EncodeInstruction(OpVecAddF, 0, 2, 0, 1, 0),    // V2 = V0 + V1
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0), // F0 = sum(V2)
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants:      []any{"data", "value"},
		FloatConstants: []float64{10.5},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// (1+10.5) + (2+10.5) + (3+10.5) = 11.5 + 12.5 + 13.5 = 37.5
	expected := 37.5
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== MoveF Tests =====

func TestVM_MoveF(t *testing.T) {
	vm := NewVM()
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConstF, 0, 0, 0, 0, 0), // F0 = 3.14
			EncodeInstruction(OpMoveF, 0, 5, 0, 0, 0),      // F5 = F0
			EncodeInstruction(OpHaltF, 0, 5, 0, 0, 0),
		},
		FloatConstants: []float64{3.14},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != 3.14 {
		t.Errorf("expected 3.14, got %v", result)
	}
}

// ===== Observability Tests =====

func TestVM_Stats_Enabled(t *testing.T) {
	vm := NewVM()
	vm.EnableStats()

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0), // R0 = 42
			EncodeInstruction(OpLoadConst, 0, 1, 0, 0, 1), // R1 = 10
			EncodeInstruction(OpAddR, 0, 2, 0, 1, 0),      // R2 = R0 + R1
			EncodeInstruction(OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{int64(42), int64(10)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != int64(52) {
		t.Errorf("expected 52, got %v", result)
	}

	stats := vm.Stats()
	if stats == nil {
		t.Fatal("expected stats to be non-nil")
	}

	if stats.StepsExecuted != 4 {
		t.Errorf("expected 4 steps, got %d", stats.StepsExecuted)
	}

	if stats.ExecutionTimeNs <= 0 {
		t.Errorf("expected positive execution time, got %d", stats.ExecutionTimeNs)
	}

	if stats.OpCounts["LOAD_CONST"] != 2 {
		t.Errorf("expected 2 LOAD_CONST ops, got %d", stats.OpCounts["LOAD_CONST"])
	}

	if stats.OpCounts["ADD_R"] != 1 {
		t.Errorf("expected 1 ADD_R op, got %d", stats.OpCounts["ADD_R"])
	}

	if stats.OpCounts["HALT"] != 1 {
		t.Errorf("expected 1 HALT op, got %d", stats.OpCounts["HALT"])
	}
}

func TestVM_Stats_Disabled(t *testing.T) {
	vm := NewVM()
	// Stats NOT enabled

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{int64(42)},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	_, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	stats := vm.Stats()
	if stats != nil {
		t.Error("expected stats to be nil when not enabled")
	}
}

func TestVM_Stats_WithFrames(t *testing.T) {
	vm := NewVM()
	vm.EnableStats()

	// Create a test frame
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 1.0, 2.0, 3.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),  // R0 = frame("data")
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = R0.value
			EncodeInstruction(OpReduceSumF, 0, 0, 0, 0, 0), // F0 = sum(V0)
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != 6.0 {
		t.Errorf("expected 6.0, got %v", result)
	}

	stats := vm.Stats()
	if stats == nil {
		t.Fatal("expected stats to be non-nil")
	}

	if stats.FramesLoaded != 1 {
		t.Errorf("expected 1 frame loaded, got %d", stats.FramesLoaded)
	}

	if stats.StepsExecuted != 4 {
		t.Errorf("expected 4 steps, got %d", stats.StepsExecuted)
	}
}
