package repl

import (
	"bytes"
	"strings"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func TestREPL_New(t *testing.T) {
	r := New()
	if r == nil {
		t.Fatal("New returned nil")
	}
	if r.mode != ModeDSL {
		t.Errorf("expected DSL mode, got %v", r.mode)
	}
}

func TestREPL_SetMode(t *testing.T) {
	r := New()
	r.SetMode(ModeASM)
	if r.mode != ModeASM {
		t.Errorf("expected ASM mode, got %v", r.mode)
	}
	r.SetMode(ModeDSL)
	if r.mode != ModeDSL {
		t.Errorf("expected DSL mode, got %v", r.mode)
	}
}

func TestREPL_SetFrames(t *testing.T) {
	r := New()
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 1, 2, 3),
	)
	r.SetFrames(map[string]*dataframe.DataFrame{"test": frame})
	if len(r.frames) != 1 {
		t.Errorf("expected 1 frame, got %d", len(r.frames))
	}
}

func TestREPL_HandleCommand_Help(t *testing.T) {
	r := New()
	var out bytes.Buffer

	tests := []string{"help", "h", "?"}
	for _, cmd := range tests {
		out.Reset()
		handled := r.handleCommand(cmd, &out)
		if !handled {
			t.Errorf("expected help command '%s' to be handled", cmd)
		}
		if !strings.Contains(out.String(), "DASM REPL Commands") {
			t.Errorf("expected help text, got: %s", out.String())
		}
	}
}

func TestREPL_HandleCommand_Quit(t *testing.T) {
	r := New()
	var out bytes.Buffer

	tests := []string{"quit", "exit", "q"}
	for _, cmd := range tests {
		out.Reset()
		handled := r.handleCommand(cmd, &out)
		if !handled {
			t.Errorf("expected quit command '%s' to be handled", cmd)
		}
		if !strings.Contains(out.String(), "Goodbye") {
			t.Errorf("expected goodbye message, got: %s", out.String())
		}
	}
}

func TestREPL_HandleCommand_Mode(t *testing.T) {
	r := New()
	var out bytes.Buffer

	// Check current mode
	r.handleCommand("mode", &out)
	if !strings.Contains(out.String(), "DSL") {
		t.Errorf("expected current mode DSL, got: %s", out.String())
	}

	// Switch to ASM mode
	out.Reset()
	r.handleCommand("mode asm", &out)
	if r.mode != ModeASM {
		t.Error("expected ASM mode")
	}
	if !strings.Contains(out.String(), "assembly mode") {
		t.Errorf("expected switch confirmation, got: %s", out.String())
	}

	// Switch to DSL mode
	out.Reset()
	r.handleCommand("mode dsl", &out)
	if r.mode != ModeDSL {
		t.Error("expected DSL mode")
	}

	// Invalid mode
	out.Reset()
	r.handleCommand("mode invalid", &out)
	if !strings.Contains(out.String(), "Unknown mode") {
		t.Errorf("expected error message, got: %s", out.String())
	}
}

func TestREPL_HandleCommand_Frames(t *testing.T) {
	r := New()
	var out bytes.Buffer

	// No frames
	r.handleCommand("frames", &out)
	if !strings.Contains(out.String(), "No frames") {
		t.Errorf("expected no frames message, got: %s", out.String())
	}

	// With frames
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 1, 2, 3),
	)
	r.SetFrames(map[string]*dataframe.DataFrame{"test": frame})
	out.Reset()
	r.handleCommand("frames", &out)
	if !strings.Contains(out.String(), "test") {
		t.Errorf("expected frame name, got: %s", out.String())
	}
}

func TestREPL_HandleCommand_Vars(t *testing.T) {
	r := New()
	var out bytes.Buffer

	// No variables
	r.handleCommand("vars", &out)
	if !strings.Contains(out.String(), "No variables") {
		t.Errorf("expected no variables message, got: %s", out.String())
	}

	// With variables
	r.variables["x"] = int64(42)
	out.Reset()
	r.handleCommand("vars", &out)
	if !strings.Contains(out.String(), "x") {
		t.Errorf("expected variable name, got: %s", out.String())
	}
}

func TestREPL_HandleCommand_Clear(t *testing.T) {
	r := New()
	var out bytes.Buffer

	r.variables["x"] = int64(42)
	r.handleCommand("clear", &out)
	if len(r.variables) != 0 {
		t.Error("expected variables to be cleared")
	}
	if !strings.Contains(out.String(), "cleared") {
		t.Errorf("expected clear confirmation, got: %s", out.String())
	}
}

func TestREPL_HandleCommand_History(t *testing.T) {
	r := New()
	var out bytes.Buffer

	r.history = []string{"command1", "command2", "command3"}
	r.handleCommand("history", &out)
	output := out.String()
	if !strings.Contains(output, "command1") {
		t.Errorf("expected command1 in history, got: %s", output)
	}
	if !strings.Contains(output, "command2") {
		t.Errorf("expected command2 in history, got: %s", output)
	}
}

func TestREPL_HandleCommand_Load_Usage(t *testing.T) {
	r := New()
	var out bytes.Buffer

	r.handleCommand("load", &out)
	if !strings.Contains(out.String(), "Usage:") {
		t.Errorf("expected usage message, got: %s", out.String())
	}

	out.Reset()
	r.handleCommand("load onlyname", &out)
	if !strings.Contains(out.String(), "Usage:") {
		t.Errorf("expected usage message, got: %s", out.String())
	}
}

func TestREPL_HandleCommand_Empty(t *testing.T) {
	r := New()
	var out bytes.Buffer

	handled := r.handleCommand("", &out)
	if !handled {
		t.Error("empty command should be handled")
	}

	handled = r.handleCommand("   ", &out)
	if !handled {
		t.Error("whitespace command should be handled")
	}
}

func TestREPL_HandleCommand_Unknown(t *testing.T) {
	r := New()
	var out bytes.Buffer

	handled := r.handleCommand("unknowncommand", &out)
	if handled {
		t.Error("unknown command should not be handled")
	}
}

func TestREPL_Eval_Empty(t *testing.T) {
	r := New()
	var out bytes.Buffer

	r.eval("", &out)
	if out.Len() != 0 {
		t.Errorf("expected no output for empty input, got: %s", out.String())
	}

	r.eval("   ", &out)
	if out.Len() != 0 {
		t.Errorf("expected no output for whitespace input, got: %s", out.String())
	}
}

func TestREPL_Eval_ASM(t *testing.T) {
	r := New()
	r.SetMode(ModeASM)
	var out bytes.Buffer

	r.eval("LOAD_CONST R0, 42\nHALT R0", &out)
	if !strings.Contains(out.String(), "42") {
		t.Errorf("expected result 42, got: %s", out.String())
	}
}

func TestREPL_Eval_ASM_Error(t *testing.T) {
	r := New()
	r.SetMode(ModeASM)
	var out bytes.Buffer

	r.eval("INVALID_OPCODE", &out)
	if !strings.Contains(out.String(), "Error") {
		t.Errorf("expected error message, got: %s", out.String())
	}
}

func TestREPL_Eval_DSL(t *testing.T) {
	r := New()
	r.SetMode(ModeDSL)
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
	)
	r.SetFrames(map[string]*dataframe.DataFrame{"sales": frame})

	var out bytes.Buffer
	r.eval("data = frame(\"sales\")\ncol = data.price\nreturn sum(col)", &out)
	if !strings.Contains(out.String(), "60") {
		t.Errorf("expected result 60, got: %s", out.String())
	}
}

func TestREPL_Eval_History(t *testing.T) {
	r := New()
	r.SetMode(ModeASM)
	var out bytes.Buffer

	r.eval("LOAD_CONST R0, 1\nHALT R0", &out)
	r.eval("LOAD_CONST R0, 2\nHALT R0", &out)

	if len(r.history) != 2 {
		t.Errorf("expected 2 history entries, got %d", len(r.history))
	}
}

func TestREPL_Start_BasicInteraction(t *testing.T) {
	r := New()
	r.SetMode(ModeASM)

	// Simulate input: multiline ASM program (use backslash for continuation)
	// Then empty line to execute, then quit
	input := "LOAD_CONST R0, 42\\\nHALT R0\n\nquit\n"
	in := strings.NewReader(input)
	var out bytes.Buffer

	// Start will run until it hits quit or EOF
	r.Start(in, &out)

	output := out.String()
	if !strings.Contains(output, "DASM REPL") {
		t.Error("expected welcome message")
	}
	if !strings.Contains(output, "42") {
		t.Errorf("expected result 42, got: %s", output)
	}
}

func TestREPL_Start_MultilineInput(t *testing.T) {
	r := New()
	r.SetMode(ModeASM)

	// Use backslash for multiline, then empty line to execute
	input := "LOAD_CONST R0, 10\\\nHALT R0\n\nquit\n"
	in := strings.NewReader(input)
	var out bytes.Buffer

	r.Start(in, &out)

	output := out.String()
	if !strings.Contains(output, "10") {
		t.Errorf("expected result 10, got: %s", output)
	}
}

func TestREPL_Start_ModeSwitch(t *testing.T) {
	r := New()

	input := "mode asm\nmode dsl\nmode\nquit\n"
	in := strings.NewReader(input)
	var out bytes.Buffer

	r.Start(in, &out)

	output := out.String()
	if !strings.Contains(output, "assembly mode") {
		t.Error("expected ASM mode switch confirmation")
	}
	if !strings.Contains(output, "DSL mode") {
		t.Error("expected DSL mode switch confirmation")
	}
}

func TestREPL_PrintHelp(t *testing.T) {
	r := New()
	var out bytes.Buffer

	r.printHelp(&out)
	output := out.String()

	expectedStrings := []string{
		"DASM REPL Commands",
		"help",
		"quit",
		"mode",
		"frames",
		"vars",
		"load",
		"clear",
		"history",
		"DSL Examples",
		"ASM Examples",
		"LOAD_CSV",
		"Tips",
	}

	for _, s := range expectedStrings {
		if !strings.Contains(output, s) {
			t.Errorf("expected help to contain '%s'", s)
		}
	}
}

func TestREPL_ListFrames_Empty(t *testing.T) {
	r := New()
	var out bytes.Buffer

	r.listFrames(&out)
	if !strings.Contains(out.String(), "No frames") {
		t.Error("expected no frames message")
	}
}

func TestREPL_ListFrames_WithData(t *testing.T) {
	r := New()
	frame1 := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 1, 2, 3),
	)
	frame2 := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("b", nil, 1.5, 2.5),
		dataframe.NewSeriesFloat64("c", nil, 3.5, 4.5),
	)

	r.SetFrames(map[string]*dataframe.DataFrame{
		"frame1": frame1,
		"frame2": frame2,
	})

	var out bytes.Buffer
	r.listFrames(&out)
	output := out.String()

	if !strings.Contains(output, "frame1") {
		t.Error("expected frame1")
	}
	if !strings.Contains(output, "frame2") {
		t.Error("expected frame2")
	}
	if !strings.Contains(output, "3 rows") {
		t.Error("expected 3 rows for frame1")
	}
	if !strings.Contains(output, "2 columns") {
		t.Error("expected 2 columns for frame2")
	}
}

func TestREPL_ListVariables_Empty(t *testing.T) {
	r := New()
	var out bytes.Buffer

	r.listVariables(&out)
	if !strings.Contains(out.String(), "No variables") {
		t.Error("expected no variables message")
	}
}

func TestREPL_ListVariables_WithData(t *testing.T) {
	r := New()
	r.variables["x"] = int64(42)
	r.variables["y"] = 3.14
	r.variables["name"] = "test"

	var out bytes.Buffer
	r.listVariables(&out)
	output := out.String()

	if !strings.Contains(output, "x = 42") {
		t.Error("expected x = 42")
	}
	if !strings.Contains(output, "y = 3.14") {
		t.Error("expected y = 3.14")
	}
	if !strings.Contains(output, "name = test") {
		t.Error("expected name = test")
	}
}
