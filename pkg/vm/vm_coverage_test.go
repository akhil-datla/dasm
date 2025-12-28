package vm

import (
	"context"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// ===== Tests for uncovered VM functions =====

func TestVM_SetMaxAlloc(t *testing.T) {
	vm := NewVM()
	vm.SetMaxAlloc(1024)
	// Just verify it doesn't panic
}

func TestVM_SetInstructionLimit(t *testing.T) {
	vm := NewVM()
	vm.SetInstructionLimit(1000)
	// Just verify it doesn't panic
}

func TestVM_SetMemoryLimit(t *testing.T) {
	vm := NewVM()
	vm.SetMemoryLimit(1024 * 1024)
	// Just verify it doesn't panic
}

func TestVM_SetSandbox(t *testing.T) {
	vm := NewVM()
	vm.SetSandbox(true, []string{"/tmp"})
	// Just verify it doesn't panic
}

func TestVM_isPathAllowed(t *testing.T) {
	vm := NewVM()
	vm.SetSandbox(true, []string{"/tmp", "/home/user"})

	tests := []struct {
		path    string
		allowed bool
	}{
		{"/tmp/file.csv", true},
		{"/tmp/subdir/file.csv", true},
		{"/home/user/data.csv", true},
		{"/etc/passwd", false},
		{"/var/log/syslog", false},
	}

	for _, tt := range tests {
		result := vm.isPathAllowed(tt.path)
		if result != tt.allowed {
			t.Errorf("isPathAllowed(%s) = %v, want %v", tt.path, result, tt.allowed)
		}
	}
}

func TestVM_TakeSeries(t *testing.T) {
	// Test the takeSeries helper function directly
	src := dataframe.NewSeriesFloat64("value", nil, 10.0, 20.0, 30.0, 40.0, 50.0)
	indices := dataframe.NewSeriesInt64("idx", nil, 0, 2, 4)

	vm := NewVM()
	result := vm.takeSeries(src, indices)

	if result == nil {
		t.Fatal("takeSeries returned nil")
	}
	if result.NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", result.NRows())
	}

	// Check values: indices 0, 2, 4 = values 10, 30, 50
	val0, _ := getFloat64Value(result, 0)
	val1, _ := getFloat64Value(result, 1)
	val2, _ := getFloat64Value(result, 2)

	if val0 != 10.0 || val1 != 30.0 || val2 != 50.0 {
		t.Errorf("unexpected values: %v, %v, %v", val0, val1, val2)
	}
}

// ===== GroupBy Extended Tests =====

func TestVM_GroupCount(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "A", "B"),
		dataframe.NewSeriesFloat64("value", nil, 1.0, 2.0, 3.0, 4.0, 5.0),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = category
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),    // R1 = groupby(V0)
			EncodeInstruction(OpGroupCount, 0, 2, 1, 0, 0), // V2 = group_count(R1)
			EncodeInstruction(OpReduceSum, 0, 0, 2, 0, 0),  // R0 = sum(counts)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
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

	// A: 3, B: 2, total = 5
	expected := int64(5)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_GroupMin(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "B"),
		dataframe.NewSeriesInt64("value", nil, 10, 20, 5, 15),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1), // V0 = category
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2), // V1 = value
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),   // R1 = groupby(V0)
			EncodeInstruction(OpGroupMin, 0, 2, 1, 1, 0),  // V2 = group_min(R1, V1)
			EncodeInstruction(OpReduceSum, 0, 0, 2, 0, 0), // R0 = sum(mins)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
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

	// A min: 5, B min: 15, total = 20
	expected := int64(20)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_GroupMax(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "B"),
		dataframe.NewSeriesInt64("value", nil, 10, 20, 5, 15),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),
			EncodeInstruction(OpGroupMax, 0, 2, 1, 1, 0),
			EncodeInstruction(OpReduceSum, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
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

	// A max: 10, B max: 20, total = 30
	expected := int64(30)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_GroupMinF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "B"),
		dataframe.NewSeriesFloat64("value", nil, 10.5, 20.5, 5.5, 15.5),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),
			EncodeInstruction(OpGroupMinF, 0, 2, 1, 1, 0),
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0),
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

	// A min: 5.5, B min: 15.5, total = 21.0
	expected := 21.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_GroupMaxF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "B"),
		dataframe.NewSeriesFloat64("value", nil, 10.5, 20.5, 5.5, 15.5),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2),
			EncodeInstruction(OpGroupBy, 0, 1, 0, 0, 0),
			EncodeInstruction(OpGroupMaxF, 0, 2, 1, 1, 0),
			EncodeInstruction(OpReduceSumF, 0, 0, 2, 0, 0),
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

	// A max: 10.5, B max: 20.5, total = 31.0
	expected := 31.0
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Join Extended Tests =====

func TestVM_JoinRight(t *testing.T) {
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
			EncodeInstruction(OpJoinRight, 0, 2, 0, 1, 2),
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

	// Right join keeps all right rows: 2, 3, 4 = 3 rows
	expected := int64(3)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_JoinOuter(t *testing.T) {
	vm := NewVM()
	left := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("id", nil, 1, 2),
		dataframe.NewSeriesString("name", nil, "a", "b"),
	)
	right := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("id", nil, 2, 3),
		dataframe.NewSeriesInt64("score", nil, 100, 200),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"left": left, "right": right})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpLoadFrame, 0, 1, 0, 0, 1),
			EncodeInstruction(OpJoinOuter, 0, 2, 0, 1, 2),
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

	// Outer join: 1(left only), 2(both), 3(right only) = 3 rows
	expected := int64(3)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== String Extended Tests =====

func TestVM_StrStartsWith(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("name", nil, "hello", "world", "help", "test"),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	// Test: check if names start with constant pattern "hel"
	// hello, help start with "hel" = 2 matches
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),     // R0 = frame "data"
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),     // V0 = column "name"
			EncodeInstruction(OpStrStartsWith, 0, 1, 0, 0, 2), // V1 = V0.startsWith("hel")
			EncodeInstruction(OpReduceCount, 0, 0, 1, 0, 0),   // R0 = count(V1)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "name", "hel"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// hello and help start with "hel" = 2 true values
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_StrEndsWith(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("name", nil, "hello", "world", "jello", "test"),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	// Test: check if names end with constant pattern "llo"
	// hello, jello end with "llo" = 2 matches
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),   // R0 = frame "data"
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),   // V0 = column "name"
			EncodeInstruction(OpStrEndsWith, 0, 1, 0, 0, 2), // V1 = V0.endsWith("llo")
			EncodeInstruction(OpReduceCount, 0, 0, 1, 0, 0), // R0 = count(V1)
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "name", "llo"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// hello and jello end with "llo" = 2 true values
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_StrSplit(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("text", nil, "a,b,c", "x,y", "1,2,3,4"),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpStrSplit, 0, 1, 0, 0, 2), // Split by ","
			EncodeInstruction(OpReduceCount, 0, 0, 1, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "text", ","},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Just verify it ran, split returns first part
	expected := int64(3)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestVM_StrReplace(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("text", nil, "hello", "world"),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),
			EncodeInstruction(OpStrReplace, 0, 1, 0, 0, 2), // Replace "l" with "L"
			EncodeInstruction(OpReduceCount, 0, 0, 1, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "text", "l", "L"},
	}

	if err := vm.Load(program); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	result, err := vm.Execute()
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := int64(2)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Instruction String test =====

func TestInstruction_String(t *testing.T) {
	inst := EncodeInstruction(OpLoadConst, 0, 0, 0, 0, 42)
	s := inst.String()
	if s != "LOAD_CONST" {
		t.Errorf("expected LOAD_CONST, got %s", s)
	}
}

// ===== DataType String test =====

func TestDataType_String(t *testing.T) {
	tests := []struct {
		dt       DataType
		expected string
	}{
		{TypeInt64, "int64"},
		{TypeFloat64, "float64"},
		{TypeString, "string"},
		{TypeBool, "bool"},
		{DataType(99), "unknown"},
	}

	for _, tt := range tests {
		result := tt.dt.String()
		if result != tt.expected {
			t.Errorf("DataType(%d).String() = %s, want %s", tt.dt, result, tt.expected)
		}
	}
}

// ===== Series helpers tests =====

func TestGetSeriesName(t *testing.T) {
	s := dataframe.NewSeriesInt64("test_name", nil, 1, 2, 3)
	name := getSeriesName(s)
	if name != "test_name" {
		t.Errorf("expected test_name, got %s", name)
	}
}

func TestCloneSeries(t *testing.T) {
	s := dataframe.NewSeriesInt64("original", nil, 1, 2, 3)
	cloned := cloneSeries(s)
	if cloned == nil {
		t.Fatal("cloneSeries returned nil")
	}
	if cloned.Name() != "original" {
		t.Errorf("expected name original, got %s", cloned.Name())
	}
	if cloned.NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", cloned.NRows())
	}
}

func TestCloneSeries_Nil(t *testing.T) {
	cloned := cloneSeries(nil)
	if cloned != nil {
		t.Error("expected nil for nil input")
	}
}

func TestCreateEmptySeries(t *testing.T) {
	// Test int64
	s1 := dataframe.NewSeriesInt64("test", nil, 1, 2, 3)
	empty1 := createEmptySeries(s1)
	if empty1 == nil {
		t.Error("expected non-nil series")
	}

	// Test float64
	s2 := dataframe.NewSeriesFloat64("test", nil, 1.0, 2.0)
	empty2 := createEmptySeries(s2)
	if empty2 == nil {
		t.Error("expected non-nil series")
	}

	// Test string
	s3 := dataframe.NewSeriesString("test", nil, "a", "b")
	empty3 := createEmptySeries(s3)
	if empty3 == nil {
		t.Error("expected non-nil series")
	}
}

func TestGetDataFrameColumnNames(t *testing.T) {
	df := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("id", nil, 1, 2),
		dataframe.NewSeriesString("name", nil, "a", "b"),
		dataframe.NewSeriesFloat64("value", nil, 1.0, 2.0),
	)
	names := getDataFrameColumnNames(df)
	if len(names) != 3 {
		t.Errorf("expected 3 names, got %d", len(names))
	}
	if names[0] != "id" || names[1] != "name" || names[2] != "value" {
		t.Errorf("unexpected column names: %v", names)
	}
}

func TestNewEmptyDataFrame(t *testing.T) {
	df := newEmptyDataFrame()
	if df == nil {
		t.Fatal("newEmptyDataFrame returned nil")
	}
	if len(df.Series) != 0 {
		t.Errorf("expected 0 series, got %d", len(df.Series))
	}
}

// ===== Opcode String test =====

func TestOpcode_String(t *testing.T) {
	tests := []struct {
		op       Opcode
		expected string
	}{
		{OpLoadCSV, "LOAD_CSV"},
		{OpLoadConst, "LOAD_CONST"},
		{OpLoadConstF, "LOAD_CONST_F"},
		{OpSelectCol, "SELECT_COL"},
		{OpHalt, "HALT"},
		{OpHaltF, "HALT_F"},
		{OpVecAddI, "VEC_ADD_I"},
		{OpVecMulF, "VEC_MUL_F"},
		{OpCmpGT, "CMP_GT"},
		{OpFilter, "FILTER"},
		{OpReduceSum, "REDUCE_SUM"},
		{OpGroupBy, "GROUP_BY"},
		{OpJoinInner, "JOIN_INNER"},
		{OpStrLen, "STR_LEN"},
	}

	for _, tt := range tests {
		result := tt.op.String()
		if result != tt.expected {
			t.Errorf("Opcode(%d).String() = %s, want %s", tt.op, result, tt.expected)
		}
	}
}

// TestOpcodeFromString is in instruction_test.go

// ===== Series Sum/Mean tests =====

func TestSeriesSum(t *testing.T) {
	s := dataframe.NewSeriesFloat64("value", nil, 1.0, 2.0, 3.0, 4.0)
	sum, err := seriesSum(context.TODO(), s)
	if err != nil {
		t.Fatalf("seriesSum failed: %v", err)
	}
	if sum != 10.0 {
		t.Errorf("expected 10.0, got %v", sum)
	}
}

func TestSeriesSumInt64(t *testing.T) {
	s := dataframe.NewSeriesInt64("value", nil, 1, 2, 3, 4)
	sum, err := seriesSum(context.TODO(), s)
	if err != nil {
		t.Fatalf("seriesSum failed: %v", err)
	}
	if sum != 10.0 {
		t.Errorf("expected 10.0, got %v", sum)
	}
}

func TestSeriesSumNil(t *testing.T) {
	sum, err := seriesSum(context.TODO(), nil)
	if err != nil {
		t.Fatalf("seriesSum failed: %v", err)
	}
	if sum != 0.0 {
		t.Errorf("expected 0.0 for nil series, got %v", sum)
	}
}

func TestSeriesMean(t *testing.T) {
	s := dataframe.NewSeriesFloat64("value", nil, 2.0, 4.0, 6.0, 8.0)
	mean, err := seriesMean(context.TODO(), s)
	if err != nil {
		t.Fatalf("seriesMean failed: %v", err)
	}
	if mean != 5.0 {
		t.Errorf("expected 5.0, got %v", mean)
	}
}

func TestSeriesMeanNil(t *testing.T) {
	mean, err := seriesMean(context.TODO(), nil)
	if err != nil {
		t.Fatalf("seriesMean failed: %v", err)
	}
	if mean != 0.0 {
		t.Errorf("expected 0.0 for nil series, got %v", mean)
	}
}

// ===== Additional getInt64Value / getFloat64Value tests =====

func TestGetInt64Value_FromFloat(t *testing.T) {
	s := dataframe.NewSeriesFloat64("value", nil, 1.0, 2.5, 3.9)
	val, ok := getInt64Value(s, 0)
	if !ok {
		t.Error("expected ok=true")
	}
	if val != 1 {
		t.Errorf("expected 1, got %d", val)
	}
}

func TestGetFloat64Value_FromInt(t *testing.T) {
	s := dataframe.NewSeriesInt64("value", nil, 1, 2, 3)
	val, ok := getFloat64Value(s, 1)
	if !ok {
		t.Error("expected ok=true")
	}
	if val != 2.0 {
		t.Errorf("expected 2.0, got %f", val)
	}
}

func TestGetStringValue(t *testing.T) {
	s := dataframe.NewSeriesString("name", nil, "hello", "world")
	val, ok := getStringValue(s, 0)
	if !ok {
		t.Error("expected ok=true")
	}
	if val != "hello" {
		t.Errorf("expected hello, got %s", val)
	}
}

func TestGetBoolValue(t *testing.T) {
	// Create bool series using SeriesGeneric
	s := newBoolSeries("flag", []bool{true, false, true})
	val, ok := getBoolValue(s, 0)
	if !ok {
		t.Error("expected ok=true")
	}
	if val != true {
		t.Errorf("expected true, got %v", val)
	}
}

// ===== getSeriesType extended test =====

func TestGetSeriesType(t *testing.T) {
	tests := []struct {
		name     string
		series   dataframe.Series
		expected DataType
	}{
		{"int64", dataframe.NewSeriesInt64("x", nil, 1), TypeInt64},
		{"float64", dataframe.NewSeriesFloat64("x", nil, 1.0), TypeFloat64},
		{"string", dataframe.NewSeriesString("x", nil, "a"), TypeString},
		{"bool", newBoolSeries("x", []bool{true}), TypeBool},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getSeriesType(tt.series)
			if result != tt.expected {
				t.Errorf("getSeriesType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// ===== GroupBy with float aggregation =====

func TestVM_GroupBySumF(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "B"),
		dataframe.NewSeriesFloat64("value", nil, 1.5, 2.5, 3.5, 4.5),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1), // V0 = category
			EncodeInstruction(OpGroupBy, 0, 0, 0, 0, 0),   // R0 = groupby(V0)
			EncodeInstruction(OpSelectCol, 0, 1, 0, 0, 2), // V1 = value
			EncodeInstruction(OpGroupSumF, 0, 2, 0, 1, 0), // V2 = group sum of V1
			EncodeInstruction(OpReduceCount, 0, 0, 2, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
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

	// 2 groups: A (1.5+3.5=5.0) and B (2.5+4.5=7.0)
	expected := int64(2)
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// ===== Opcode String coverage =====

func TestOpcodeString_AllCases(t *testing.T) {
	// Test various opcodes to improve String() coverage
	opcodes := []struct {
		op   Opcode
		want string
	}{
		{OpLoadCSV, "LOAD_CSV"},
		{OpLoadConst, "LOAD_CONST"},
		{OpLoadConstF, "LOAD_CONST_F"},
		{OpSelectCol, "SELECT_COL"},
		{OpBroadcast, "BROADCAST"},
		{OpBroadcastF, "BROADCAST_F"},
		{OpLoadFrame, "LOAD_FRAME"},
		{OpVecAddI, "VEC_ADD_I"},
		{OpVecSubI, "VEC_SUB_I"},
		{OpVecMulI, "VEC_MUL_I"},
		{OpVecDivI, "VEC_DIV_I"},
		{OpVecModI, "VEC_MOD_I"},
		{OpVecAddF, "VEC_ADD_F"},
		{OpVecSubF, "VEC_SUB_F"},
		{OpVecMulF, "VEC_MUL_F"},
		{OpVecDivF, "VEC_DIV_F"},
		{OpCmpEQ, "CMP_EQ"},
		{OpCmpNE, "CMP_NE"},
		{OpCmpLT, "CMP_LT"},
		{OpCmpLE, "CMP_LE"},
		{OpCmpGT, "CMP_GT"},
		{OpCmpGE, "CMP_GE"},
		{OpAnd, "AND"},
		{OpOr, "OR"},
		{OpNot, "NOT"},
		{OpFilter, "FILTER"},
		{OpTake, "TAKE"},
		{OpReduceSum, "REDUCE_SUM"},
		{OpReduceSumF, "REDUCE_SUM_F"},
		{OpReduceCount, "REDUCE_COUNT"},
		{OpReduceMin, "REDUCE_MIN"},
		{OpReduceMax, "REDUCE_MAX"},
		{OpReduceMinF, "REDUCE_MIN_F"},
		{OpReduceMaxF, "REDUCE_MAX_F"},
		{OpReduceMean, "REDUCE_MEAN"},
		{OpMoveR, "MOVE_R"},
		{OpMoveF, "MOVE_F"},
		{OpAddR, "ADD_R"},
		{OpSubR, "SUB_R"},
		{OpMulR, "MUL_R"},
		{OpDivR, "DIV_R"},
		{OpNewFrame, "NEW_FRAME"},
		{OpAddCol, "ADD_COL"},
		{OpColCount, "COL_COUNT"},
		{OpRowCount, "ROW_COUNT"},
		{OpGroupBy, "GROUP_BY"},
		{OpGroupSum, "GROUP_SUM"},
		{OpGroupSumF, "GROUP_SUM_F"},
		{OpGroupCount, "GROUP_COUNT"},
		{OpGroupMean, "GROUP_MEAN"},
		{OpGroupMin, "GROUP_MIN"},
		{OpGroupMax, "GROUP_MAX"},
		{OpGroupMinF, "GROUP_MIN_F"},
		{OpGroupMaxF, "GROUP_MAX_F"},
		{OpJoinInner, "JOIN_INNER"},
		{OpJoinLeft, "JOIN_LEFT"},
		{OpJoinRight, "JOIN_RIGHT"},
		{OpJoinOuter, "JOIN_OUTER"},
		{OpStrLen, "STR_LEN"},
		{OpStrUpper, "STR_UPPER"},
		{OpStrLower, "STR_LOWER"},
		{OpStrConcat, "STR_CONCAT"},
		{OpStrContains, "STR_CONTAINS"},
		{OpStrStartsWith, "STR_STARTS_WITH"},
		{OpStrEndsWith, "STR_ENDS_WITH"},
		{OpStrTrim, "STR_TRIM"},
		{OpStrSplit, "STR_SPLIT"},
		{OpStrReplace, "STR_REPLACE"},
		{OpNop, "NOP"},
		{OpHalt, "HALT"},
		{OpHaltF, "HALT_F"},
	}

	for _, tc := range opcodes {
		t.Run(tc.want, func(t *testing.T) {
			got := tc.op.String()
			if got != tc.want {
				t.Errorf("Opcode(%d).String() = %q, want %q", tc.op, got, tc.want)
			}
		})
	}

	// Test unknown opcode (use 0xFD which is not assigned)
	unknown := Opcode(0xFD)
	if got := unknown.String(); got != "UNKNOWN" {
		t.Errorf("unknown opcode String() = %q, want %q", got, "UNKNOWN")
	}
}

func TestOpcodeFromString_AllCases(t *testing.T) {
	// Test various opcode string conversions
	tests := []struct {
		input string
		want  Opcode
		ok    bool
	}{
		{"LOAD_CSV", OpLoadCSV, true},
		{"LOAD_CONST", OpLoadConst, true},
		{"LOAD_CONST_F", OpLoadConstF, true},
		{"SELECT_COL", OpSelectCol, true},
		{"BROADCAST", OpBroadcast, true},
		{"BROADCAST_F", OpBroadcastF, true},
		{"LOAD_FRAME", OpLoadFrame, true},
		{"VEC_ADD_I", OpVecAddI, true},
		{"VEC_SUB_I", OpVecSubI, true},
		{"VEC_MUL_I", OpVecMulI, true},
		{"VEC_DIV_I", OpVecDivI, true},
		{"VEC_MOD_I", OpVecModI, true},
		{"VEC_ADD_F", OpVecAddF, true},
		{"VEC_SUB_F", OpVecSubF, true},
		{"VEC_MUL_F", OpVecMulF, true},
		{"VEC_DIV_F", OpVecDivF, true},
		{"CMP_EQ", OpCmpEQ, true},
		{"CMP_NE", OpCmpNE, true},
		{"CMP_LT", OpCmpLT, true},
		{"CMP_LE", OpCmpLE, true},
		{"CMP_GT", OpCmpGT, true},
		{"CMP_GE", OpCmpGE, true},
		{"AND", OpAnd, true},
		{"OR", OpOr, true},
		{"NOT", OpNot, true},
		{"FILTER", OpFilter, true},
		{"TAKE", OpTake, true},
		{"REDUCE_SUM", OpReduceSum, true},
		{"REDUCE_SUM_F", OpReduceSumF, true},
		{"REDUCE_COUNT", OpReduceCount, true},
		{"REDUCE_MIN", OpReduceMin, true},
		{"REDUCE_MAX", OpReduceMax, true},
		{"REDUCE_MIN_F", OpReduceMinF, true},
		{"REDUCE_MAX_F", OpReduceMaxF, true},
		{"REDUCE_MEAN", OpReduceMean, true},
		{"MOVE_R", OpMoveR, true},
		{"MOVE_F", OpMoveF, true},
		{"ADD_R", OpAddR, true},
		{"SUB_R", OpSubR, true},
		{"MUL_R", OpMulR, true},
		{"DIV_R", OpDivR, true},
		{"NEW_FRAME", OpNewFrame, true},
		{"ADD_COL", OpAddCol, true},
		{"COL_COUNT", OpColCount, true},
		{"ROW_COUNT", OpRowCount, true},
		{"GROUP_BY", OpGroupBy, true},
		{"GROUP_SUM", OpGroupSum, true},
		{"GROUP_SUM_F", OpGroupSumF, true},
		{"GROUP_COUNT", OpGroupCount, true},
		{"GROUP_MEAN", OpGroupMean, true},
		{"GROUP_MIN", OpGroupMin, true},
		{"GROUP_MAX", OpGroupMax, true},
		{"GROUP_MIN_F", OpGroupMinF, true},
		{"GROUP_MAX_F", OpGroupMaxF, true},
		{"JOIN_INNER", OpJoinInner, true},
		{"JOIN_LEFT", OpJoinLeft, true},
		{"JOIN_RIGHT", OpJoinRight, true},
		{"JOIN_OUTER", OpJoinOuter, true},
		{"STR_LEN", OpStrLen, true},
		{"STR_UPPER", OpStrUpper, true},
		{"STR_LOWER", OpStrLower, true},
		{"STR_CONCAT", OpStrConcat, true},
		{"STR_CONTAINS", OpStrContains, true},
		{"STR_STARTS_WITH", OpStrStartsWith, true},
		{"STR_ENDS_WITH", OpStrEndsWith, true},
		{"STR_TRIM", OpStrTrim, true},
		{"STR_SPLIT", OpStrSplit, true},
		{"STR_REPLACE", OpStrReplace, true},
		{"NOP", OpNop, true},
		{"HALT", OpHalt, true},
		{"HALT_F", OpHaltF, true},
		{"INVALID_OPCODE", 0, false},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, ok := OpcodeFromString(tc.input)
			if ok != tc.ok {
				t.Errorf("OpcodeFromString(%q) ok = %v, want %v", tc.input, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Errorf("OpcodeFromString(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

// ===== addColumnToDataFrame test =====

func TestAddColumnToDataFrame(t *testing.T) {
	df := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 1, 2, 3),
	)

	newCol := dataframe.NewSeriesFloat64("b", nil, 1.1, 2.2, 3.3)
	err := addColumnToDataFrame(df, newCol)
	if err != nil {
		t.Fatalf("addColumnToDataFrame failed: %v", err)
	}

	if len(df.Series) != 2 {
		t.Errorf("expected 2 columns, got %d", len(df.Series))
	}

	// Check the new column exists
	found := false
	for _, s := range df.Series {
		if s.Name() == "b" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected column 'b' to be added")
	}

	// Test with nil
	err = addColumnToDataFrame(nil, newCol)
	if err != nil {
		t.Error("expected no error for nil dataframe")
	}
}

// ===== strReplace additional tests =====

func TestVM_StrReplace_MultiplePatterns(t *testing.T) {
	vm := NewVM()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesString("text", nil, "hello world", "foo bar baz", "test"),
	)
	vm.SetPredeclaredFrames(map[string]*dataframe.DataFrame{"data": frame})

	// Replace pattern in the format "old->new"
	program := &Program{
		Code: []Instruction{
			EncodeInstruction(OpLoadFrame, 0, 0, 0, 0, 0),
			EncodeInstruction(OpSelectCol, 0, 0, 0, 0, 1),  // V0 = text
			EncodeInstruction(OpStrReplace, 0, 1, 0, 0, 2), // V1 = replace "o" with ""
			EncodeInstruction(OpReduceCount, 0, 0, 1, 0, 0),
			EncodeInstruction(OpHalt, 0, 0, 0, 0, 0),
		},
		Constants: []any{"data", "text", "o->"},
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

// ===== Series helper edge cases =====

func TestSeriesHelpers_EdgeCases(t *testing.T) {
	// Test getSeriesLength with nil
	if getSeriesLength(nil) != 0 {
		t.Error("getSeriesLength(nil) should return 0")
	}

	// Test getSeriesName with nil
	if getSeriesName(nil) != "" {
		t.Error("getSeriesName(nil) should return empty string")
	}

	// Test isNil with nil series
	if !isNil(nil, 0) {
		t.Error("isNil(nil, 0) should return true")
	}

	// Test getInt64Value with nil
	val, ok := getInt64Value(nil, 0)
	if ok || val != 0 {
		t.Error("getInt64Value(nil, 0) should return 0, false")
	}

	// Test getFloat64Value with nil
	fval, fok := getFloat64Value(nil, 0)
	if fok || fval != 0 {
		t.Error("getFloat64Value(nil, 0) should return 0, false")
	}

	// Test getStringValue with nil
	sval, sok := getStringValue(nil, 0)
	if sok || sval != "" {
		t.Error("getStringValue(nil, 0) should return empty, false")
	}

	// Test getBoolValue with nil
	bval, bok := getBoolValue(nil, 0)
	if bok || bval != false {
		t.Error("getBoolValue(nil, 0) should return false, false")
	}
}

func TestCreateSeriesWithValues_AllTypes(t *testing.T) {
	// Test int64 series
	int64Template := dataframe.NewSeriesInt64("int_col", nil)
	int64Series := createSeriesWithValues(int64Template, []interface{}{int64(1), int64(2), int64(3)})
	if int64Series.NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", int64Series.NRows())
	}

	// Test float64 series
	float64Template := dataframe.NewSeriesFloat64("float_col", nil)
	float64Series := createSeriesWithValues(float64Template, []interface{}{1.1, 2.2, 3.3})
	if float64Series.NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", float64Series.NRows())
	}

	// Test string series
	stringTemplate := dataframe.NewSeriesString("str_col", nil)
	stringSeries := createSeriesWithValues(stringTemplate, []interface{}{"a", "b", "c"})
	if stringSeries.NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", stringSeries.NRows())
	}

	// Test bool series - needs a value in template for type detection
	boolTemplate := newBoolSeries("bool_col", []bool{false})
	boolSeries := createSeriesWithValues(boolTemplate, []interface{}{true, false, true})
	if boolSeries.NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", boolSeries.NRows())
	}
}

func TestFilterSeries_AllTypes(t *testing.T) {
	mask := NewBitmap(3)
	mask.Set(0)
	mask.Set(2)

	// Filter int64
	int64Series := dataframe.NewSeriesInt64("x", nil, 1, 2, 3)
	result := filterSeries(int64Series, mask)
	if result.NRows() != 2 {
		t.Errorf("expected 2 rows, got %d", result.NRows())
	}

	// Filter float64
	float64Series := dataframe.NewSeriesFloat64("x", nil, 1.1, 2.2, 3.3)
	result = filterSeries(float64Series, mask)
	if result.NRows() != 2 {
		t.Errorf("expected 2 rows, got %d", result.NRows())
	}

	// Filter string
	stringSeries := dataframe.NewSeriesString("x", nil, "a", "b", "c")
	result = filterSeries(stringSeries, mask)
	if result.NRows() != 2 {
		t.Errorf("expected 2 rows, got %d", result.NRows())
	}

	// Note: bool series filtering is tested implicitly through VM comparison ops
}

func TestCreateEmptySeries_AllTypes(t *testing.T) {
	// Int64
	int64Series := dataframe.NewSeriesInt64("x", nil, 1) // needs at least one value
	result := createEmptySeries(int64Series)
	if result.NRows() != 0 {
		t.Error("expected empty series")
	}

	// Float64
	float64Series := dataframe.NewSeriesFloat64("x", nil, 1.0)
	result = createEmptySeries(float64Series)
	if result.NRows() != 0 {
		t.Error("expected empty series")
	}

	// String
	stringSeries := dataframe.NewSeriesString("x", nil, "a")
	result = createEmptySeries(stringSeries)
	if result.NRows() != 0 {
		t.Error("expected empty series")
	}

	// Bool - need at least one value for type detection
	boolSeries := newBoolSeries("x", []bool{false})
	result = createEmptySeries(boolSeries)
	if result.NRows() != 0 {
		t.Error("expected empty series")
	}
}

func TestGetDataFrameColumn_EdgeCases(t *testing.T) {
	df := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 1, 2, 3),
	)

	// Test getting existing column
	col, ok := getDataFrameColumn(df, "a")
	if !ok || col == nil {
		t.Error("expected to find column 'a'")
	}

	// Test getting non-existent column
	col, ok = getDataFrameColumn(df, "nonexistent")
	if ok || col != nil {
		t.Error("expected to not find 'nonexistent' column")
	}

	// Test with nil dataframe
	col, ok = getDataFrameColumn(nil, "a")
	if ok || col != nil {
		t.Error("expected nil result for nil dataframe")
	}
}

func TestGetDataFrameLength_EdgeCases(t *testing.T) {
	df := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 1, 2, 3),
	)

	if getDataFrameLength(df) != 3 {
		t.Errorf("expected 3, got %d", getDataFrameLength(df))
	}

	// Empty dataframe
	emptyDf := dataframe.NewDataFrame()
	if getDataFrameLength(emptyDf) != 0 {
		t.Errorf("expected 0, got %d", getDataFrameLength(emptyDf))
	}

	// Nil dataframe
	if getDataFrameLength(nil) != 0 {
		t.Errorf("expected 0, got %d", getDataFrameLength(nil))
	}
}

func TestSeriesSum_AllTypes(t *testing.T) {
	ctx := context.Background()

	// Int64
	int64Series := dataframe.NewSeriesInt64("x", nil, 1, 2, 3)
	sum, err := seriesSum(ctx, int64Series)
	if err != nil || sum != 6.0 {
		t.Errorf("expected sum 6.0, got %v (err=%v)", sum, err)
	}

	// Float64
	float64Series := dataframe.NewSeriesFloat64("x", nil, 1.5, 2.5, 3.0)
	sum, err = seriesSum(ctx, float64Series)
	if err != nil || sum != 7.0 {
		t.Errorf("expected sum 7.0, got %v (err=%v)", sum, err)
	}

	// String (returns 0, nil - no error, just can't sum strings)
	stringSeries := dataframe.NewSeriesString("x", nil, "a", "b")
	sum, err = seriesSum(ctx, stringSeries)
	if err != nil {
		t.Errorf("unexpected error for string series: %v", err)
	}
	if sum != 0 {
		t.Errorf("expected sum 0 for string series, got %v", sum)
	}

	// Nil series
	sum, err = seriesSum(ctx, nil)
	if err != nil || sum != 0 {
		t.Errorf("expected 0, nil for nil series, got %v, %v", sum, err)
	}
}

func TestSeriesMean_AllTypes(t *testing.T) {
	ctx := context.Background()

	// Int64
	int64Series := dataframe.NewSeriesInt64("x", nil, 2, 4, 6)
	mean, err := seriesMean(ctx, int64Series)
	if err != nil || mean != 4.0 {
		t.Errorf("expected mean 4.0, got %v (err=%v)", mean, err)
	}

	// Float64
	float64Series := dataframe.NewSeriesFloat64("x", nil, 1.0, 2.0, 3.0)
	mean, err = seriesMean(ctx, float64Series)
	if err != nil || mean != 2.0 {
		t.Errorf("expected mean 2.0, got %v (err=%v)", mean, err)
	}

	// Empty series (returns 0, nil)
	emptySeries := dataframe.NewSeriesFloat64("x", nil)
	mean, err = seriesMean(ctx, emptySeries)
	if err != nil || mean != 0 {
		t.Errorf("expected 0, nil for empty series, got %v, %v", mean, err)
	}

	// Nil series
	mean, err = seriesMean(ctx, nil)
	if err != nil || mean != 0 {
		t.Errorf("expected 0, nil for nil series, got %v, %v", mean, err)
	}
}
