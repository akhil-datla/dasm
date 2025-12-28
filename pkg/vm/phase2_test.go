package vm

import (
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// ===== GroupBy Tests =====

func TestVM_GroupBy_Sum(t *testing.T) {
	vm := NewVM()

	// Create frame with category and values
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "B", "A"),
		dataframe.NewSeriesInt64("value", nil, 10, 20, 30, 40, 50),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0), // R0 = frame "data"
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1), // V0 = category column
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2), // V1 = value column
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),   // R1 = groupby(R0, V0)
			EncodeInstruction(OpGroupSum, 0, 2, 1, 1, 0),  // V2 = sum(V1) per group
			EncodeInstruction(OpReduceSum, 0, 2, 2, 0, 0), // R2 = total sum of group sums (should equal original sum)
			EncodeInstruction(OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{"data", "category", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// A: 10+30+50=90, B: 20+40=60, total=150
	expected := int64(150)
	if result != expected {
		t.Errorf("expected %d, got %v", expected, result)
	}
}

func TestVM_GroupBy_Count(t *testing.T) {
	vm := NewVM()

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "B", "A"),
		dataframe.NewSeriesInt64("value", nil, 10, 20, 30, 40, 50),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),  // R0 = frame
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = category
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),    // R1 = groupby(R0, V0)
			EncodeInstruction(OpGroupCount, 0, 1, 1, 0, 0), // V1 = count per group
			EncodeInstruction(OpReduceSum, 0, 2, 1, 0, 0),  // R2 = total count (should be 5)
			EncodeInstruction(OpHalt, 0, 2, 0, 0, 0),
		},
		Constants: []any{"data", "category"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Total count should be 5 (3 A's + 2 B's)
	expected := int64(5)
	if result != expected {
		t.Errorf("expected %d, got %v", expected, result)
	}
}

func TestVM_GroupBy_Mean(t *testing.T) {
	vm := NewVM()

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "B"),
		dataframe.NewSeriesFloat64("value", nil, 10.0, 20.0, 30.0, 40.0),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),  // R0 = frame
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = category
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),  // V1 = value
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),    // R1 = groupby(R0, V0)
			EncodeInstruction(OpGroupMean, 0, 2, 1, 1, 0),  // V2 = mean(V1) per group
			EncodeInstruction(OpReduceMean, 0, 0, 2, 0, 0), // F0 = mean of group means
			EncodeInstruction(OpHaltF, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "category", "value"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// A: mean(10,30)=20, B: mean(20,40)=30, mean of means = 25
	expected := 25.0
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if got < expected-0.001 || got > expected+0.001 {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

// ===== String Operation Tests =====

func TestVM_StrLen(t *testing.T) {
	vm := NewVM()

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("name", nil, "a", "ab", "abc", "abcd"),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0), // R0 = frame
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1), // V0 = name column
			EncodeInstruction(OpStrLen, 0, 1, 0, 0, 0),    // V1 = strlen(V0)
			EncodeInstruction(OpReduceSum, 0, 1, 1, 0, 0), // R1 = sum of lengths
			EncodeInstruction(OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data", "name"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 1 + 2 + 3 + 4 = 10
	expected := int64(10)
	if result != expected {
		t.Errorf("expected %d, got %v", expected, result)
	}
}

func TestVM_StrUpper(t *testing.T) {
	vm := NewVM()

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("name", nil, "hello", "world"),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0), // R0 = frame
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1), // V0 = name
			EncodeInstruction(OpStrUpper, 0, 1, 0, 0, 0),  // V1 = upper(V0)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "name"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	_, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Check the result column
	result := vm.registers.V[1]
	val0, ok0 := getStringValue(result, 0)
	val1, ok1 := getStringValue(result, 1)
	if !ok0 || !ok1 || val0 != "HELLO" || val1 != "WORLD" {
		t.Errorf("expected [HELLO, WORLD], got [%s, %s]", val0, val1)
	}
}

func TestVM_StrLower(t *testing.T) {
	vm := NewVM()

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("name", nil, "HELLO", "WORLD"),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpStrLower, 0, 1, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "name"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	_, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	result := vm.registers.V[1]
	val0, ok0 := getStringValue(result, 0)
	val1, ok1 := getStringValue(result, 1)
	if !ok0 || !ok1 || val0 != "hello" || val1 != "world" {
		t.Errorf("expected [hello, world], got [%s, %s]", val0, val1)
	}
}

func TestVM_StrConcat(t *testing.T) {
	vm := NewVM()

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("first", nil, "Hello", "Good"),
		dataframe.NewSeriesString("second", nil, " World", " Morning"),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1), // V0 = first
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2), // V1 = second
			EncodeInstruction(OpStrConcat, 0, 2, 0, 1, 0), // V2 = concat(V0, V1)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "first", "second"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	_, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	result := vm.registers.V[2]
	val0, ok0 := getStringValue(result, 0)
	val1, ok1 := getStringValue(result, 1)
	if !ok0 || !ok1 || val0 != "Hello World" || val1 != "Good Morning" {
		t.Errorf("expected [Hello World, Good Morning], got [%s, %s]", val0, val1)
	}
}

func TestVM_StrContains(t *testing.T) {
	vm := NewVM()

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("text", nil, "hello world", "goodbye", "world peace"),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),   // V0 = text
			EncodeInstruction(OpStrContains, 0, 1, 0, 0, 2), // V1 = contains(V0, "world")
			EncodeInstruction(OpReduceCount, 0, 1, 1, 0, 0), // R1 = count of true
			EncodeInstruction(OpHalt, 0, 1, 0, 0, 0),
		},
		Constants: []any{"data", "text", "world"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// "hello world" contains "world", "world peace" contains "world"
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %d, got %v", expected, result)
	}
}

func TestVM_StrTrim(t *testing.T) {
	vm := NewVM()

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("name", nil, "  hello  ", "world  ", "  test"),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpStrTrim, 0, 1, 0, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "name"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	_, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	result := vm.registers.V[1]
	val0, ok0 := getStringValue(result, 0)
	val1, ok1 := getStringValue(result, 1)
	val2, ok2 := getStringValue(result, 2)
	if !ok0 || !ok1 || !ok2 || val0 != "hello" || val1 != "world" || val2 != "test" {
		t.Errorf("expected [hello, world, test], got [%s, %s, %s]", val0, val1, val2)
	}
}

// ===== Join Tests =====

func TestVM_JoinInner(t *testing.T) {
	vm := NewVM()

	// Left frame: id, value
	left := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("id", nil, 1, 2, 3, 4),
		dataframe.NewSeriesString("name", nil, "a", "b", "c", "d"),
	)

	// Right frame: id, score
	right := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("id", nil, 2, 3, 5),
		dataframe.NewSeriesInt64("score", nil, 100, 200, 300),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"left": left, "right": right})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0), // R0 = left
			EncodeInstruction(OpLoadFrame, 0, 1, 0, 0, 1), // R1 = right
			EncodeInstruction(OpJoinInner, 0, 2, 0, 1, 2), // R2 = inner_join(R0, R1) on "id"
			EncodeInstruction(OpRowCount, 0, 3, 2, 0, 0),  // R3 = row count
			EncodeInstruction(OpHalt, 0, 3, 0, 0, 0),
		},
		Constants: []any{"left", "right", "id"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Inner join on id: 2 and 3 are in both, so 2 rows
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %d, got %v", expected, result)
	}
}

func TestVM_JoinLeft(t *testing.T) {
	vm := NewVM()

	left := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("id", nil, 1, 2, 3),
		dataframe.NewSeriesString("name", nil, "a", "b", "c"),
	)

	right := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("id", nil, 2, 3, 4),
		dataframe.NewSeriesInt64("score", nil, 100, 200, 300),
	)

	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"left": left, "right": right})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpLoadFrame, 0, 1, 0, 0, 1),
			EncodeInstruction(OpJoinLeft, 0, 2, 0, 1, 2),
			EncodeInstruction(OpRowCount, 0, 3, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 3, 0, 0, 0),
		},
		Constants: []any{"left", "right", "id"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Left join keeps all from left (3 rows)
	expected := int64(3)
	if result != expected {
		t.Errorf("expected %d, got %v", expected, result)
	}
}
