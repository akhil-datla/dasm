package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// buildDasm builds the dasm binary for testing
func buildDasm(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	binary := filepath.Join(tmpDir, "dasm")
	cmd := exec.Command("go", "build", "-o", binary, ".")
	cmd.Dir = "."
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build dasm: %v\n%s", err, output)
	}
	return binary
}

func TestCLI_Help(t *testing.T) {
	binary := buildDasm(t)

	cmd := exec.Command(binary, "help")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("help command failed: %v", err)
	}

	out := string(output)
	if !strings.Contains(out, "DASM") {
		t.Error("help output should contain DASM")
	}
	if !strings.Contains(out, "run") {
		t.Error("help output should contain run command")
	}
	if !strings.Contains(out, "compile") {
		t.Error("help output should contain compile command")
	}
}

func TestCLI_Version(t *testing.T) {
	binary := buildDasm(t)

	cmd := exec.Command(binary, "version")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}

	out := string(output)
	if !strings.Contains(out, "dasm version") {
		t.Errorf("expected version output, got: %s", out)
	}
}

func TestCLI_Run(t *testing.T) {
	binary := buildDasm(t)

	// Create a temporary dasm file
	tmpDir := t.TempDir()
	dasmFile := filepath.Join(tmpDir, "test.dasm")
	err := os.WriteFile(dasmFile, []byte(`
LOAD_CONST R0, 42
HALT R0
`), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	cmd := exec.Command(binary, "run", dasmFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run command failed: %v\n%s", err, output)
	}

	out := strings.TrimSpace(string(output))
	if out != "42" {
		t.Errorf("expected 42, got: %s", out)
	}
}

func TestCLI_CompileAndExec(t *testing.T) {
	binary := buildDasm(t)
	tmpDir := t.TempDir()

	// Create source file
	dasmFile := filepath.Join(tmpDir, "test.dasm")
	err := os.WriteFile(dasmFile, []byte(`
LOAD_CONST R0, 100
HALT R0
`), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Compile
	bytecodeFile := filepath.Join(tmpDir, "test.dfbc")
	cmd := exec.Command(binary, "compile", dasmFile, "-o", bytecodeFile)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compile failed: %v\n%s", err, output)
	}

	// Verify bytecode file exists
	if _, err := os.Stat(bytecodeFile); os.IsNotExist(err) {
		t.Fatal("bytecode file was not created")
	}

	// Execute bytecode
	cmd = exec.Command(binary, "exec", bytecodeFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("exec failed: %v\n%s", err, output)
	}

	out := strings.TrimSpace(string(output))
	if out != "100" {
		t.Errorf("expected 100, got: %s", out)
	}
}

func TestCLI_Disasm(t *testing.T) {
	binary := buildDasm(t)
	tmpDir := t.TempDir()

	// Create and compile source file
	dasmFile := filepath.Join(tmpDir, "test.dasm")
	err := os.WriteFile(dasmFile, []byte(`
LOAD_CONST R0, 55
HALT R0
`), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	bytecodeFile := filepath.Join(tmpDir, "test.dfbc")
	cmd := exec.Command(binary, "compile", dasmFile, "-o", bytecodeFile)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compile failed: %v\n%s", err, output)
	}

	// Disassemble
	cmd = exec.Command(binary, "disasm", bytecodeFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("disasm failed: %v\n%s", err, output)
	}

	out := string(output)
	if !strings.Contains(out, "LOAD_CONST") {
		t.Errorf("disasm output should contain LOAD_CONST, got: %s", out)
	}
	if !strings.Contains(out, "HALT") {
		t.Errorf("disasm output should contain HALT, got: %s", out)
	}
}

func TestCLI_RunWithCSV(t *testing.T) {
	binary := buildDasm(t)
	tmpDir := t.TempDir()

	// Create CSV file
	csvFile := filepath.Join(tmpDir, "test.csv")
	err := os.WriteFile(csvFile, []byte(`price,quantity
10.0,2
20.0,3
30.0,4
`), 0644)
	if err != nil {
		t.Fatalf("failed to create CSV: %v", err)
	}

	// Create dasm file referencing the CSV
	dasmFile := filepath.Join(tmpDir, "test.dasm")
	err = os.WriteFile(dasmFile, []byte(`
LOAD_CSV      R0, "`+csvFile+`"
SELECT_COL    V0, R0, "price"
REDUCE_SUM_F  F0, V0
HALT_F        F0
`), 0644)
	if err != nil {
		t.Fatalf("failed to create dasm file: %v", err)
	}

	cmd := exec.Command(binary, "run", dasmFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run failed: %v\n%s", err, output)
	}

	out := strings.TrimSpace(string(output))
	if out != "60" {
		t.Errorf("expected 60, got: %s", out)
	}
}

func TestCLI_RunWithExampleFrames(t *testing.T) {
	binary := buildDasm(t)
	tmpDir := t.TempDir()

	// Create dasm file that uses example frames
	dasmFile := filepath.Join(tmpDir, "test.dasm")
	err := os.WriteFile(dasmFile, []byte(`
LOAD_FRAME    R0, "sales"
SELECT_COL    V0, R0, "category"
SELECT_COL    V1, R0, "amount"
GROUP_BY      R1, V0
GROUP_SUM     V2, R1, V1
REDUCE_SUM    R2, V2
HALT          R2
`), 0644)
	if err != nil {
		t.Fatalf("failed to create dasm file: %v", err)
	}

	cmd := exec.Command(binary, "run", "-example-frames", dasmFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run failed: %v\n%s", err, output)
	}

	out := strings.TrimSpace(string(output))
	if out != "82" {
		t.Errorf("expected 82, got: %s", out)
	}
}

func TestCLI_UnknownCommand(t *testing.T) {
	binary := buildDasm(t)

	cmd := exec.Command(binary, "unknown")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Error("expected error for unknown command")
	}

	out := string(output)
	if !strings.Contains(out, "unknown command") {
		t.Errorf("expected 'unknown command' error, got: %s", out)
	}
}

func TestCLI_MissingFile(t *testing.T) {
	binary := buildDasm(t)

	cmd := exec.Command(binary, "run", "nonexistent.dasm")
	_, err := cmd.CombinedOutput()
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestCLI_CompileVerbose(t *testing.T) {
	binary := buildDasm(t)
	tmpDir := t.TempDir()

	dasmFile := filepath.Join(tmpDir, "test.dasm")
	err := os.WriteFile(dasmFile, []byte(`
LOAD_CONST R0, 1
LOAD_CONST R1, 2
ADD_R R2, R0, R1
HALT R2
`), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	cmd := exec.Command(binary, "compile", "-v", dasmFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("compile failed: %v\n%s", err, output)
	}

	out := string(output)
	if !strings.Contains(out, "instructions") {
		t.Errorf("verbose output should mention instructions, got: %s", out)
	}
}

func TestCLI_CompileWithOptimization(t *testing.T) {
	binary := buildDasm(t)
	tmpDir := t.TempDir()

	// Create a program with dead code that can be optimized
	dasmFile := filepath.Join(tmpDir, "test.dasm")
	err := os.WriteFile(dasmFile, []byte(`
LOAD_CONST R0, 42
LOAD_CONST R1, 100
LOAD_CONST R2, 200
HALT R0
`), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Compile with optimization and verbose
	bytecodeFile := filepath.Join(tmpDir, "test.dfbc")
	cmd := exec.Command(binary, "compile", "-O", "-v", dasmFile, "-o", bytecodeFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("compile with optimization failed: %v\n%s", err, output)
	}

	out := string(output)
	if !strings.Contains(out, "optimization") {
		t.Errorf("verbose output should mention optimization, got: %s", out)
	}

	// Execute the optimized bytecode
	cmd = exec.Command(binary, "exec", bytecodeFile)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("exec failed: %v\n%s", err, output)
	}

	result := strings.TrimSpace(string(output))
	if result != "42" {
		t.Errorf("expected 42, got: %s", result)
	}
}
