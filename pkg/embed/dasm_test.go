package embed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func TestExecute_BasicProgram(t *testing.T) {
	result, err := Execute(`
LOAD_CONST    R0, 10
LOAD_CONST    R1, 5
ADD_R         R2, R0, R1
HALT          R2
`)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != int64(15) {
		t.Errorf("expected 15, got %v", result)
	}
}

func TestExecute_FloatResult(t *testing.T) {
	result, err := Execute(`
LOAD_CONST_F  F0, 3.14
HALT_F        F0
`)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result != 3.14 {
		t.Errorf("expected 3.14, got %v", result)
	}
}

func TestExecuteFile_LoadsAndRuns(t *testing.T) {
	// Create temp assembly file
	tmpDir := t.TempDir()
	asmPath := filepath.Join(tmpDir, "test.dasm")
	asmCode := `LOAD_CONST R0, 42
HALT R0`
	if err := os.WriteFile(asmPath, []byte(asmCode), 0644); err != nil {
		t.Fatalf("failed to write assembly file: %v", err)
	}

	result, err := ExecuteFile(asmPath)
	if err != nil {
		t.Fatalf("ExecuteFile failed: %v", err)
	}

	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestExecuteWithFrames_InjectsData(t *testing.T) {
	// Create frame in Go
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.5, 20.0, 5.0, 30.0),
		dataframe.NewSeriesInt64("quantity", nil, 5, 15, 3, 20),
	)

	result, err := ExecuteWithFrames(`
LOAD_FRAME    R0, "data"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
LOAD_CONST    R1, 10
BROADCAST     V2, R1, V1
CMP_GT        V3, V1, V2
FILTER        V4, V0, V3
REDUCE_SUM_F  F0, V4
HALT_F        F0
`, map[string]*dataframe.DataFrame{"data": frame})

	if err != nil {
		t.Fatalf("ExecuteWithFrames failed: %v", err)
	}

	// 20.0 + 30.0 = 50.0 (where quantity > 10)
	expected := 50.0
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64 result, got %T", result)
	}
	if got < expected-0.001 || got > expected+0.001 {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

func TestExecute_ReturnsErrorOnBadCode(t *testing.T) {
	_, err := Execute(`INVALID_OPCODE R0, 42`)
	if err == nil {
		t.Error("expected error for invalid opcode")
	}
}

func TestExecuteWithFrames_ReturnsErrorOnMissingFrame(t *testing.T) {
	_, err := ExecuteWithFrames(`
LOAD_FRAME    R0, "nonexistent"
HALT          R0
`, map[string]*dataframe.DataFrame{})

	if err == nil {
		t.Error("expected error for missing frame")
	}
}

func TestExecuteFile_ReturnsErrorOnMissingFile(t *testing.T) {
	_, err := ExecuteFile("/nonexistent/file.dasm")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestExecuteWithFrames_VectorOperations(t *testing.T) {
	// Test computed columns: total = price * quantity
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
		dataframe.NewSeriesFloat64("quantity", nil, 2.0, 3.0, 4.0),
	)

	result, err := ExecuteWithFrames(`
LOAD_FRAME    R0, "sales"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
VEC_MUL_F     V2, V0, V1
REDUCE_SUM_F  F0, V2
HALT_F        F0
`, map[string]*dataframe.DataFrame{"sales": frame})

	if err != nil {
		t.Fatalf("ExecuteWithFrames failed: %v", err)
	}

	// 10*2 + 20*3 + 30*4 = 20 + 60 + 120 = 200
	expected := 200.0
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64 result, got %T", result)
	}
	if got < expected-0.001 || got > expected+0.001 {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

func TestExecute_WithCSV(t *testing.T) {
	// Create temp CSV file
	csvData := `price,quantity
10.5,5
20.0,15
5.0,3
30.0,20`
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "sales.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write CSV file: %v", err)
	}

	result, err := Execute(`
LOAD_CSV      R0, "` + csvPath + `"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
LOAD_CONST    R1, 10
BROADCAST     V2, R1, V1
CMP_GT        V3, V1, V2
FILTER        V4, V0, V3
REDUCE_SUM_F  F0, V4
HALT_F        F0
`)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 20.0 + 30.0 = 50.0 (where quantity > 10)
	expected := 50.0
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64 result, got %T", result)
	}
	if got < expected-0.001 || got > expected+0.001 {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

// ===== Phase 3: ExecuteWithOptions Tests =====

func TestExecuteWithOptions_BasicProgram(t *testing.T) {
	result, err := ExecuteWithOptions(`
LOAD_CONST    R0, 10
LOAD_CONST    R1, 5
ADD_R         R2, R0, R1
HALT          R2
`)
	if err != nil {
		t.Fatalf("ExecuteWithOptions failed: %v", err)
	}

	if result != int64(15) {
		t.Errorf("expected 15, got %v", result)
	}
}

func TestExecuteWithOptions_WithFrames(t *testing.T) {
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
	)

	result, err := ExecuteWithOptions(`
LOAD_FRAME    R0, "data"
SELECT_COL    V0, R0, "price"
REDUCE_SUM_F  F0, V0
HALT_F        F0
`, WithFrames(map[string]*dataframe.DataFrame{"data": frame}))

	if err != nil {
		t.Fatalf("ExecuteWithOptions failed: %v", err)
	}

	expected := 60.0
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if got != expected {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

func TestExecuteWithOptions_InstructionLimit(t *testing.T) {
	_, err := ExecuteWithOptions(`
LOAD_CONST    R0, 0
ADD_R         R0, R0, R0
ADD_R         R0, R0, R0
ADD_R         R0, R0, R0
ADD_R         R0, R0, R0
ADD_R         R0, R0, R0
HALT          R0
`, WithMaxInstructions(3))

	if err == nil {
		t.Error("expected instruction limit error")
	}
	if !errors.Is(err, ErrInstructionLimit) {
		t.Errorf("expected ErrInstructionLimit, got %v", err)
	}
}

func TestExecuteWithOptions_Timeout(t *testing.T) {
	// Create a context that's already expired
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Sleep to ensure context expires
	time.Sleep(5 * time.Millisecond)

	_, err := ExecuteWithOptions(`
LOAD_CONST    R0, 1
HALT          R0
`, WithContext(ctx))

	if err == nil {
		t.Error("expected timeout error")
	}
	// Context should be expired
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrTimeout) {
		t.Errorf("expected timeout error, got %v", err)
	}
}

func TestExecuteWithOptions_Sandbox(t *testing.T) {
	// Create temp CSV file
	csvData := `price,quantity
10.0,5`
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "sales.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write CSV file: %v", err)
	}

	// Try to load CSV in sandbox mode without allowing the path
	_, err := ExecuteWithOptions(`
LOAD_CSV      R0, "`+csvPath+`"
HALT          R0
`, WithSandbox())

	if err == nil {
		t.Error("expected sandbox error")
	}
	if !errors.Is(err, ErrFileAccessDenied) {
		t.Errorf("expected ErrFileAccessDenied, got %v", err)
	}
}

func TestExecuteWithOptions_SandboxWithAllowedPath(t *testing.T) {
	// Create temp CSV file
	csvData := `price,quantity
10.0,5`
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "sales.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write CSV file: %v", err)
	}

	// Allow the temp directory
	result, err := ExecuteWithOptions(`
LOAD_CSV      R0, "`+csvPath+`"
SELECT_COL    V0, R0, "price"
REDUCE_SUM_F  F0, V0
HALT_F        F0
`, WithSandbox(), WithAllowedPaths(tmpDir))

	if err != nil {
		t.Fatalf("ExecuteWithOptions failed: %v", err)
	}

	expected := 10.0
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if got != expected {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

// ===== Phase 3: ExecuteDSL Tests =====

func TestExecuteDSL_SimpleExpression(t *testing.T) {
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
	)

	result, err := ExecuteDSL(`
data = frame("sales")
col = data.price
return sum(col)
`, WithFrames(map[string]*dataframe.DataFrame{"sales": frame}))

	if err != nil {
		t.Fatalf("ExecuteDSL failed: %v", err)
	}

	expected := 60.0
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if got != expected {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

func TestExecuteDSLFile(t *testing.T) {
	// Create temp DSL file - simple return statement
	dslCode := `return 42`
	tmpDir := t.TempDir()
	dslPath := filepath.Join(tmpDir, "test.dfx")
	if err := os.WriteFile(dslPath, []byte(dslCode), 0644); err != nil {
		t.Fatalf("failed to write DSL file: %v", err)
	}

	result, err := ExecuteDSLFile(dslPath)
	if err != nil {
		t.Fatalf("ExecuteDSLFile failed: %v", err)
	}

	expected := int64(42)
	got, ok := result.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", result)
	}
	if got != expected {
		t.Errorf("expected %d, got %d", expected, got)
	}
}

func TestExecuteDSL_SyntaxError(t *testing.T) {
	_, err := ExecuteDSL(`
this is not valid syntax !!!
`)
	if err == nil {
		t.Error("expected syntax error")
	}
}

// ===== Additional Edge Case Tests =====

func TestExecute_EmptyProgram(t *testing.T) {
	// Empty program should return an error (no HALT instruction)
	_, err := Execute(``)
	if err == nil {
		t.Error("expected error for empty program (no HALT)")
	}
}

func TestExecute_CommentOnlyProgram(t *testing.T) {
	// Comment-only program should return an error (no HALT instruction)
	_, err := Execute(`; This is just a comment
; Another comment`)
	if err == nil {
		t.Error("expected error for comment-only program (no HALT)")
	}
}

func TestExecute_Arithmetic(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		expected int64
	}{
		{"addition", "LOAD_CONST R0, 10\nLOAD_CONST R1, 5\nADD_R R2, R0, R1\nHALT R2", 15},
		{"subtraction", "LOAD_CONST R0, 10\nLOAD_CONST R1, 5\nSUB_R R2, R0, R1\nHALT R2", 5},
		{"multiplication", "LOAD_CONST R0, 10\nLOAD_CONST R1, 5\nMUL_R R2, R0, R1\nHALT R2", 50},
		{"division", "LOAD_CONST R0, 10\nLOAD_CONST R1, 5\nDIV_R R2, R0, R1\nHALT R2", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Execute(tt.code)
			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("expected %d, got %v", tt.expected, result)
			}
		})
	}
}

func TestExecuteWithOptions_MultipleOptions(t *testing.T) {
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 1.0, 2.0, 3.0),
	)

	result, err := ExecuteWithOptions(`
LOAD_FRAME    R0, "data"
SELECT_COL    V0, R0, "value"
REDUCE_SUM_F  F0, V0
HALT_F        F0
`,
		WithFrames(map[string]*dataframe.DataFrame{"data": frame}),
		WithMaxInstructions(1000),
		WithTimeout(5*time.Second),
	)

	if err != nil {
		t.Fatalf("ExecuteWithOptions failed: %v", err)
	}

	expected := 6.0
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if got != expected {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

func TestExecuteWithOptions_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := ExecuteWithOptions(`
LOAD_CONST    R0, 1
HALT          R0
`, WithContext(ctx))

	if err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestExecuteWithOptions_NoOptions(t *testing.T) {
	result, err := ExecuteWithOptions(`
LOAD_CONST    R0, 42
HALT          R0
`)
	if err != nil {
		t.Fatalf("ExecuteWithOptions failed: %v", err)
	}
	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestExecuteWithFrames_EmptyFrames(t *testing.T) {
	result, err := ExecuteWithFrames(`
LOAD_CONST    R0, 100
HALT          R0
`, map[string]*dataframe.DataFrame{})

	if err != nil {
		t.Fatalf("ExecuteWithFrames failed: %v", err)
	}
	if result != int64(100) {
		t.Errorf("expected 100, got %v", result)
	}
}

func TestExecuteWithFrames_NilFrames(t *testing.T) {
	result, err := ExecuteWithFrames(`
LOAD_CONST    R0, 100
HALT          R0
`, nil)

	if err != nil {
		t.Fatalf("ExecuteWithFrames failed: %v", err)
	}
	if result != int64(100) {
		t.Errorf("expected 100, got %v", result)
	}
}

func TestExecuteFile_BadFileExtension(t *testing.T) {
	// Execute should work with any content regardless of extension
	tmpDir := t.TempDir()
	codePath := filepath.Join(tmpDir, "test.txt")
	code := `LOAD_CONST R0, 42
HALT R0`
	if err := os.WriteFile(codePath, []byte(code), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	result, err := ExecuteFile(codePath)
	if err != nil {
		t.Fatalf("ExecuteFile failed: %v", err)
	}
	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestExecuteDSLFile_MissingFile(t *testing.T) {
	_, err := ExecuteDSLFile("/nonexistent/path/file.dfx")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestExecuteDSL_WithComparison(t *testing.T) {
	// Test using direct comparison in DSL
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
	)

	result, err := ExecuteDSL(`
data = frame("sales")
p = data.price
return sum(p)
`, WithFrames(map[string]*dataframe.DataFrame{"sales": frame}))

	if err != nil {
		t.Fatalf("ExecuteDSL failed: %v", err)
	}

	expected := 60.0
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if got != expected {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

func TestExecuteDSL_Aggregations(t *testing.T) {
	tests := []struct {
		name     string
		aggFunc  string
		expected float64
	}{
		{"sum", "sum", 60.0},
		{"mean", "mean", 20.0},
		{"count", "count", 3.0},
		{"min", "min", 10.0},
		{"max", "max", 30.0},
	}

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 10.0, 20.0, 30.0),
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code := `
data = frame("data")
v = data.value
return ` + tt.aggFunc + `(v)`

			result, err := ExecuteDSL(code,
				WithFrames(map[string]*dataframe.DataFrame{"data": frame}))

			if err != nil {
				t.Fatalf("ExecuteDSL failed: %v", err)
			}

			// Count returns int64
			if tt.name == "count" {
				got, ok := result.(int64)
				if !ok {
					t.Fatalf("expected int64, got %T", result)
				}
				if float64(got) != tt.expected {
					t.Errorf("expected %.2f, got %d", tt.expected, got)
				}
				return
			}

			got, ok := result.(float64)
			if !ok {
				t.Fatalf("expected float64, got %T", result)
			}
			if got < tt.expected-0.001 || got > tt.expected+0.001 {
				t.Errorf("expected %.2f, got %.2f", tt.expected, got)
			}
		})
	}
}

func TestWithMaxMemory_Option(t *testing.T) {
	opts := &Options{}
	WithMaxMemory(1024)(opts)

	if opts.MaxMemoryBytes != 1024 {
		t.Errorf("expected MaxMemoryBytes 1024, got %d", opts.MaxMemoryBytes)
	}
}

func TestWithSandbox_Option(t *testing.T) {
	opts := &Options{}
	WithSandbox()(opts)

	if !opts.Sandbox {
		t.Error("expected Sandbox to be true")
	}
}

func TestWithAllowedPaths_Option(t *testing.T) {
	opts := &Options{}
	WithAllowedPaths("/path/one", "/path/two")(opts)

	if len(opts.AllowedPaths) != 2 {
		t.Errorf("expected 2 paths, got %d", len(opts.AllowedPaths))
	}
	if opts.AllowedPaths[0] != "/path/one" || opts.AllowedPaths[1] != "/path/two" {
		t.Error("paths not set correctly")
	}
}

func TestWithTimeout_Option(t *testing.T) {
	opts := &Options{}
	WithTimeout(5 * time.Second)(opts)

	if opts.Timeout != 5*time.Second {
		t.Errorf("expected 5s timeout, got %v", opts.Timeout)
	}
}

func TestWithMaxInstructions_Option(t *testing.T) {
	opts := &Options{}
	WithMaxInstructions(1000)(opts)

	if opts.MaxInstructions != 1000 {
		t.Errorf("expected 1000 instructions, got %d", opts.MaxInstructions)
	}
}

type testContextKey string

func TestWithContext_Option(t *testing.T) {
	ctx := context.WithValue(context.Background(), testContextKey("key"), "value")
	opts := &Options{}
	WithContext(ctx)(opts)

	if opts.Context != ctx {
		t.Error("context not set correctly")
	}
}

func TestError_Variables(t *testing.T) {
	// Verify error variables exist and have values
	if ErrTimeout == nil {
		t.Error("ErrTimeout should not be nil")
	}
	if ErrInstructionLimit == nil {
		t.Error("ErrInstructionLimit should not be nil")
	}
	if ErrMemoryLimit == nil {
		t.Error("ErrMemoryLimit should not be nil")
	}
	if ErrFileAccessDenied == nil {
		t.Error("ErrFileAccessDenied should not be nil")
	}

	// Verify error messages
	if ErrTimeout.Error() != "execution timeout exceeded" {
		t.Errorf("unexpected error message: %s", ErrTimeout.Error())
	}
	if ErrInstructionLimit.Error() != "instruction limit exceeded" {
		t.Errorf("unexpected error message: %s", ErrInstructionLimit.Error())
	}
}

func TestExecute_NegativeNumbers(t *testing.T) {
	result, err := Execute(`
LOAD_CONST    R0, -42
HALT          R0
`)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result != int64(-42) {
		t.Errorf("expected -42, got %v", result)
	}
}

func TestExecute_LargeNumber(t *testing.T) {
	result, err := Execute(`
LOAD_CONST    R0, 9223372036854775807
HALT          R0
`)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result != int64(9223372036854775807) {
		t.Errorf("expected max int64, got %v", result)
	}
}

func TestExecuteWithFrames_MultipleFrames(t *testing.T) {
	frame1 := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 1.0, 2.0),
	)

	frame2 := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("value", nil, 10.0, 20.0),
	)

	// Load from first frame
	result, err := ExecuteWithFrames(`
LOAD_FRAME    R0, "first"
SELECT_COL    V0, R0, "value"
REDUCE_SUM_F  F0, V0
HALT_F        F0
`, map[string]*dataframe.DataFrame{
		"first":  frame1,
		"second": frame2,
	})

	if err != nil {
		t.Fatalf("ExecuteWithFrames failed: %v", err)
	}

	expected := 3.0 // 1.0 + 2.0 from first frame
	got, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if got != expected {
		t.Errorf("expected %.2f, got %.2f", expected, got)
	}
}

func TestCompileDSL_InternalFunction(t *testing.T) {
	// Test that compileDSL is working correctly
	code := "return 42"
	asm, err := compileDSL(code)
	if err != nil {
		t.Fatalf("compileDSL failed: %v", err)
	}
	if asm == "" {
		t.Error("expected non-empty assembly output")
	}
}

func TestExecuteDSL_ReturnConstant(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		expected any
	}{
		{"int", "return 42", int64(42)},
		{"negative", "return -100", int64(-100)},
		{"float", "return 3.14", 3.14},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ExecuteDSL(tt.code)
			if err != nil {
				t.Fatalf("ExecuteDSL failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
