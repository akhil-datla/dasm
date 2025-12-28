// Package main provides the CLI entry point for DASM (Data Assembly Language).
//
// Usage:
//
//	dasm run program.dasm          # Execute assembly file
//	dasm run program.dasm -v       # Execute with verbose output
//	dasm compile program.dasm      # Compile to bytecode (.dfbc)
//	dasm exec program.dfbc         # Execute compiled bytecode
//	dasm disasm program.dfbc       # Disassemble bytecode
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	dataframe "github.com/rocketlaunchr/dataframe-go"

	"github.com/akhildatla/dasm/pkg/compiler"
	"github.com/akhildatla/dasm/pkg/embed"
	"github.com/akhildatla/dasm/pkg/optimizer"
	"github.com/akhildatla/dasm/pkg/repl"
	"github.com/akhildatla/dasm/pkg/vm"
)

// Version info set by GoReleaser via ldflags
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return printUsage()
	}

	cmd := os.Args[1]

	switch cmd {
	case "run":
		return runCommand(os.Args[2:])
	case "compile":
		return compileCommand(os.Args[2:])
	case "exec":
		return execCommand(os.Args[2:])
	case "disasm":
		return disasmCommand(os.Args[2:])
	case "repl":
		return replCommand(os.Args[2:])
	case "version":
		fmt.Printf("dasm version %s\n", version)
		if commit != "none" {
			fmt.Printf("  commit: %s\n", commit)
		}
		if date != "unknown" {
			fmt.Printf("  built:  %s\n", date)
		}
		return nil
	case "help", "-h", "--help":
		return printUsage()
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func runCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	verbose := fs.Bool("v", false, "verbose output")
	useExampleFrames := fs.Bool("example-frames", false, "load built-in example frames (sales, people)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: dasm run <file.dasm>")
	}

	path := fs.Arg(0)
	var frames map[string]*dataframe.DataFrame

	if *verbose {
		fmt.Printf("Executing: %s\n", path)
		if *useExampleFrames {
			fmt.Println("Loading built-in example frames: sales, people")
		}
	}

	if *useExampleFrames {
		frames = loadExampleFrames()
	}

	var result any
	var err error

	if frames != nil {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		result, err = embed.ExecuteWithFrames(string(data), frames)
	} else {
		result, err = embed.ExecuteFile(path)
	}
	if err != nil {
		return err
	}

	// Print result
	switch v := result.(type) {
	case int64:
		fmt.Printf("%d\n", v)
	case float64:
		fmt.Printf("%.6g\n", v)
	case string:
		fmt.Printf("%s\n", v)
	case bool:
		fmt.Printf("%v\n", v)
	default:
		fmt.Printf("%v\n", result)
	}

	return nil
}

func compileCommand(args []string) error {
	fs := flag.NewFlagSet("compile", flag.ExitOnError)
	output := fs.String("o", "", "output file (default: input with .dfbc extension)")
	verbose := fs.Bool("v", false, "verbose output")
	optimize := fs.Bool("O", false, "enable optimizations (constant folding, dead code elimination)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: dasm compile <file.dasm> [-o output.dfbc]")
	}

	inputPath := fs.Arg(0)
	outputPath := *output

	if outputPath == "" {
		// Replace extension with .dfbc
		ext := filepath.Ext(inputPath)
		outputPath = strings.TrimSuffix(inputPath, ext) + ".dfbc"
	}

	if *verbose {
		fmt.Printf("Compiling: %s -> %s\n", inputPath, outputPath)
	}

	// Read source
	source, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("reading source: %w", err)
	}

	// Compile to program
	program, err := compiler.Compile(string(source))
	if err != nil {
		return fmt.Errorf("compiling: %w", err)
	}

	// Apply optimizations if requested
	if *optimize {
		if *verbose {
			fmt.Printf("Applying optimizations (before: %d instructions)\n", len(program.Code))
		}
		opt := optimizer.New(optimizer.WithAllOptimizations())
		program = opt.Optimize(program)
		if *verbose {
			fmt.Printf("After optimization: %d instructions\n", len(program.Code))
		}
	}

	// Serialize to bytecode
	bytecode, err := vm.SerializeProgram(program)
	if err != nil {
		return fmt.Errorf("serializing: %w", err)
	}

	// Write output
	if err := os.WriteFile(outputPath, bytecode, 0644); err != nil {
		return fmt.Errorf("writing bytecode: %w", err)
	}

	if *verbose {
		fmt.Printf("Compiled %d instructions, %d constants, %d float constants\n",
			len(program.Code), len(program.Constants), len(program.FloatConstants))
		fmt.Printf("Output: %s (%d bytes)\n", outputPath, len(bytecode))
	} else {
		fmt.Printf("Compiled: %s\n", outputPath)
	}

	return nil
}

func execCommand(args []string) error {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	verbose := fs.Bool("v", false, "verbose output")
	useExampleFrames := fs.Bool("example-frames", false, "load built-in example frames (sales, people)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: dasm exec <file.dfbc>")
	}

	path := fs.Arg(0)

	if *verbose {
		fmt.Printf("Executing bytecode: %s\n", path)
	}

	// Read bytecode
	bytecode, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading bytecode: %w", err)
	}

	// Deserialize program
	program, err := vm.DeserializeProgram(bytecode)
	if err != nil {
		return fmt.Errorf("deserializing: %w", err)
	}

	if *verbose {
		fmt.Printf("Loaded %d instructions, %d constants, %d float constants\n",
			len(program.Code), len(program.Constants), len(program.FloatConstants))
	}

	// Create and run VM
	v := vm.NewVM()

	if *useExampleFrames {
		v.SetPredeclaredFrames(loadExampleFrames())
	}

	if err := v.Load(program); err != nil {
		return fmt.Errorf("loading program: %w", err)
	}

	result, err := v.Execute()
	if err != nil {
		return fmt.Errorf("executing: %w", err)
	}

	// Print result
	switch val := result.(type) {
	case int64:
		fmt.Printf("%d\n", val)
	case float64:
		fmt.Printf("%.6g\n", val)
	case string:
		fmt.Printf("%s\n", val)
	case bool:
		fmt.Printf("%v\n", val)
	default:
		fmt.Printf("%v\n", result)
	}

	return nil
}

func disasmCommand(args []string) error {
	fs := flag.NewFlagSet("disasm", flag.ExitOnError)
	output := fs.String("o", "", "output file (default: stdout)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: dasm disasm <file.dfbc> [-o output.dasm]")
	}

	path := fs.Arg(0)

	// Read bytecode
	bytecode, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading bytecode: %w", err)
	}

	// Deserialize program
	program, err := vm.DeserializeProgram(bytecode)
	if err != nil {
		return fmt.Errorf("deserializing: %w", err)
	}

	// Disassemble
	asm := vm.Disassemble(program)

	// Output
	if *output != "" {
		if err := os.WriteFile(*output, []byte(asm), 0644); err != nil {
			return fmt.Errorf("writing output: %w", err)
		}
		fmt.Printf("Disassembled to: %s\n", *output)
	} else {
		fmt.Print(asm)
	}

	return nil
}

func replCommand(args []string) error {
	fs := flag.NewFlagSet("repl", flag.ExitOnError)
	useExampleFrames := fs.Bool("example-frames", false, "load built-in example frames")
	asmMode := fs.Bool("asm", false, "start in assembly mode (default: DSL mode)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	r := repl.New()

	if *useExampleFrames {
		r.SetFrames(loadExampleFrames())
	}

	if *asmMode {
		r.SetMode(repl.ModeASM)
	}

	r.Start(os.Stdin, os.Stdout)
	return nil
}

func printUsage() error {
	fmt.Println(`DASM (Data Assembly Language) - Assembly-like bytecode language for dataframe operations

Usage:
  dasm <command> [arguments]

Commands:
  run <file.dasm>       Execute a DASM assembly file
  compile <file.dasm>   Compile assembly to bytecode (.dfbc)
  exec <file.dfbc>      Execute compiled bytecode
  disasm <file.dfbc>    Disassemble bytecode to assembly
  repl                  Start interactive REPL
  version               Print version information
  help                  Show this help message

Run Options:
  -v                    Verbose output
  -example-frames       Load built-in example frames (sales, people, orders, customers, products)

Compile Options:
  -o <file>             Output file (default: input with .dfbc extension)
  -O                    Enable optimizations (constant folding, dead code elimination)
  -v                    Verbose output

Exec Options:
  -v                    Verbose output
  -example-frames       Load built-in example frames

Disasm Options:
  -o <file>             Output file (default: stdout)

REPL Options:
  -example-frames       Load built-in example frames
  -asm                  Start in assembly mode (default: DSL mode)

Examples:
  dasm run program.dasm
  dasm run -example-frames examples/groupby_aggregate.dasm
  dasm compile program.dasm -o program.dfbc
  dasm exec program.dfbc
  dasm disasm program.dfbc
  dasm repl
  dasm repl -example-frames -asm`)
	return nil
}

// loadExampleFrames constructs the built-in frames referenced by example programs.
func loadExampleFrames() map[string]*dataframe.DataFrame {
	frames := make(map[string]*dataframe.DataFrame)

	// "sales" frame used by examples/groupby_aggregate.dasm
	sales := dataframe.NewDataFrame(
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "C"),
		dataframe.NewSeriesFloat64("amount", nil, 10.0, 25.0, 7.5, 40.0),
	)
	frames["sales"] = sales

	// "people" frame used by examples/string_operations.dasm
	people := dataframe.NewDataFrame(
		dataframe.NewSeriesString("name", nil, "Johnson", "Anderson", "Lee", "Jackson", "Kim"),
		dataframe.NewSeriesInt64("age", nil, 34, 29, 41, 22, 37),
	)
	frames["people"] = people

	// "orders" frame used by examples/join_example.dasm
	orders := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("order_id", nil, 1, 2, 3, 4, 5),
		dataframe.NewSeriesInt64("customer_id", nil, 101, 102, 101, 103, 102),
		dataframe.NewSeriesFloat64("amount", nil, 150.0, 200.0, 75.0, 300.0, 125.0),
	)
	frames["orders"] = orders

	// "customers" frame used by examples/join_example.dasm
	customers := dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("customer_id", nil, 101, 102, 103),
		dataframe.NewSeriesString("name", nil, "Alice", "Bob", "Charlie"),
	)
	frames["customers"] = customers

	// "products" frame used by examples/multi_filter.dasm
	// Use SeriesGeneric for bool values (second param must be zero value)
	inStockVals := []interface{}{true, true, false, true, true}
	products := dataframe.NewDataFrame(
		dataframe.NewSeriesString("name", nil, "Widget", "Gadget", "Gizmo", "Thing", "Stuff"),
		dataframe.NewSeriesFloat64("price", nil, 25.0, 75.0, 100.0, 45.0, 60.0),
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "C", "B"),
		dataframe.NewSeriesGeneric("in_stock", false, nil, inStockVals...),
	)
	frames["products"] = products

	return frames
}
