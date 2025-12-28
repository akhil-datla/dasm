package repl

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	dataframe "github.com/rocketlaunchr/dataframe-go"

	"github.com/akhildatla/dasm/pkg/compiler"
	"github.com/akhildatla/dasm/pkg/dsl"
	"github.com/akhildatla/dasm/pkg/vm"
)

const (
	promptDSL  = "dasm> "
	promptASM  = "asm> "
	promptCont = "...> "
)

// Mode represents the REPL input mode.
type Mode int

const (
	ModeDSL Mode = iota // High-level DSL mode
	ModeASM             // Assembly mode
)

// REPL provides an interactive Read-Eval-Print Loop.
type REPL struct {
	mode        Mode
	vm          *vm.VM
	frames      map[string]*dataframe.DataFrame
	variables   map[string]any
	history     []string
	multiline   strings.Builder
	inMultiline bool
}

// New creates a new REPL instance.
func New() *REPL {
	return &REPL{
		mode:      ModeDSL,
		vm:        vm.NewVM(),
		frames:    make(map[string]*dataframe.DataFrame),
		variables: make(map[string]any),
		history:   []string{},
	}
}

// SetFrames sets predeclared frames available in the REPL.
func (r *REPL) SetFrames(frames map[string]*dataframe.DataFrame) {
	r.frames = frames
}

// SetMode sets the REPL input mode.
func (r *REPL) SetMode(mode Mode) {
	r.mode = mode
}

// Start starts the REPL loop.
func (r *REPL) Start(in io.Reader, out io.Writer) {
	scanner := bufio.NewScanner(in)

	fmt.Fprintln(out, "DASM REPL v1.0 - Data Assembly Language")
	fmt.Fprintln(out, "Type 'help' for available commands, 'quit' to exit")
	fmt.Fprintln(out)

	for {
		if r.inMultiline {
			fmt.Fprint(out, promptCont)
		} else if r.mode == ModeDSL {
			fmt.Fprint(out, promptDSL)
		} else {
			fmt.Fprint(out, promptASM)
		}

		if !scanner.Scan() {
			break
		}

		line := scanner.Text()

		// Handle multiline input
		if r.inMultiline {
			if line == "" {
				// End multiline input
				r.inMultiline = false
				input := r.multiline.String()
				r.multiline.Reset()
				r.eval(input, out)
			} else {
				r.multiline.WriteString(line)
				r.multiline.WriteString("\n")
			}
			continue
		}

		// Check for special commands
		if handled := r.handleCommand(line, out); handled {
			continue
		}

		// Check for multiline start (ends with \)
		if strings.HasSuffix(line, "\\") {
			r.inMultiline = true
			r.multiline.WriteString(strings.TrimSuffix(line, "\\"))
			r.multiline.WriteString("\n")
			continue
		}

		r.eval(line, out)
	}
}

func (r *REPL) handleCommand(line string, out io.Writer) bool {
	trimmed := strings.TrimSpace(line)
	parts := strings.Fields(trimmed)

	if len(parts) == 0 {
		return true
	}

	switch parts[0] {
	case "quit", "exit", "q":
		fmt.Fprintln(out, "Goodbye!")
		return true // This will exit on next iteration when scanner fails

	case "help", "h", "?":
		r.printHelp(out)
		return true

	case "mode":
		if len(parts) > 1 {
			switch parts[1] {
			case "dsl":
				r.mode = ModeDSL
				fmt.Fprintln(out, "Switched to DSL mode")
			case "asm":
				r.mode = ModeASM
				fmt.Fprintln(out, "Switched to assembly mode")
			default:
				fmt.Fprintln(out, "Unknown mode. Use 'dsl' or 'asm'")
			}
		} else {
			if r.mode == ModeDSL {
				fmt.Fprintln(out, "Current mode: DSL")
			} else {
				fmt.Fprintln(out, "Current mode: ASM")
			}
		}
		return true

	case "frames":
		r.listFrames(out)
		return true

	case "vars":
		r.listVariables(out)
		return true

	case "load":
		if len(parts) > 2 {
			r.loadFrame(parts[1], parts[2], out)
		} else {
			fmt.Fprintln(out, "Usage: load <name> <path.csv>")
		}
		return true

	case "clear":
		r.variables = make(map[string]any)
		fmt.Fprintln(out, "Variables cleared")
		return true

	case "history":
		for i, cmd := range r.history {
			fmt.Fprintf(out, "%3d: %s\n", i+1, cmd)
		}
		return true
	}

	return false
}

func (r *REPL) eval(input string, out io.Writer) {
	if strings.TrimSpace(input) == "" {
		return
	}

	r.history = append(r.history, input)

	var result any
	var err error

	if r.mode == ModeDSL {
		result, err = r.evalDSL(input)
	} else {
		result, err = r.evalASM(input)
	}

	if err != nil {
		fmt.Fprintf(out, "Error: %v\n", err)
		return
	}

	if result != nil {
		fmt.Fprintf(out, "=> %v\n", result)
	}
}

func (r *REPL) evalDSL(input string) (any, error) {
	// Tokenize
	lexer := dsl.NewLexer(input)
	tokens := lexer.Tokenize()

	// Parse
	parser := dsl.NewParser(tokens)
	program, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	// Compile to assembly
	compiler := dsl.NewCompiler()
	asm, err := compiler.Compile(program)
	if err != nil {
		return nil, err
	}

	// Execute assembly
	return r.evalASM(asm)
}

func (r *REPL) evalASM(input string) (any, error) {
	// Compile assembly
	program, err := compiler.Compile(input)
	if err != nil {
		return nil, err
	}

	// Create fresh VM
	execVM := vm.NewVM()
	execVM.SetPredeclaredFrames(r.frames)

	// Load and execute
	if err := execVM.Load(program); err != nil {
		return nil, err
	}

	return execVM.Execute()
}

func (r *REPL) loadFrame(name, path string, out io.Writer) {
	// Use loader to load CSV
	frame, err := loadCSVFile(path)
	if err != nil {
		fmt.Fprintf(out, "Error loading %s: %v\n", path, err)
		return
	}

	r.frames[name] = frame
	numRows := 0
	numCols := len(frame.Series)
	if numCols > 0 {
		numRows = frame.Series[0].NRows()
	}
	fmt.Fprintf(out, "Loaded frame '%s' from %s (%d rows, %d columns)\n",
		name, path, numRows, numCols)
}

func loadCSVFile(_ string) (*dataframe.DataFrame, error) {
	// Import loader dynamically to avoid circular dependency
	return nil, fmt.Errorf("use dfl.Execute to load CSV files")
}

func (r *REPL) listFrames(out io.Writer) {
	if len(r.frames) == 0 {
		fmt.Fprintln(out, "No frames loaded")
		return
	}

	fmt.Fprintln(out, "Loaded frames:")
	for name, frame := range r.frames {
		numRows := 0
		numCols := len(frame.Series)
		if numCols > 0 {
			numRows = frame.Series[0].NRows()
		}
		fmt.Fprintf(out, "  %s: %d rows, %d columns\n",
			name, numRows, numCols)
	}
}

func (r *REPL) listVariables(out io.Writer) {
	if len(r.variables) == 0 {
		fmt.Fprintln(out, "No variables defined")
		return
	}

	fmt.Fprintln(out, "Variables:")
	for name, val := range r.variables {
		fmt.Fprintf(out, "  %s = %v\n", name, val)
	}
}

func (r *REPL) printHelp(out io.Writer) {
	help := `
DASM REPL Commands:
  help, h, ?      Show this help message
  quit, exit, q   Exit the REPL
  mode [dsl|asm]  Show or set input mode
  frames          List loaded data frames
  vars            List defined variables
  load <n> <path> Load CSV file as frame
  clear           Clear all variables
  history         Show command history

DSL Examples:
  data = load("sales.csv")
  data |> filter(quantity > 10) |> select(price, quantity)
  return sum(data.price)

ASM Examples:
  LOAD_CSV R0, "data.csv"
  SELECT_COL V0, R0, "price"
  REDUCE_SUM_F F0, V0
  HALT_F F0

Tips:
  - End a line with \ for multiline input
  - Press Enter twice to execute multiline input
`
	fmt.Fprint(out, help)
}
