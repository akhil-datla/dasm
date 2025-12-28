// Package embed provides the Go embedding API for DASM (Data Assembly Language).
//
// DASM is embeddable in Go applications. Pass a string, get a result.
//
// Basic usage:
//
//	result, err := dasm.Execute(`
//	    LOAD_CSV      R0, "sales.csv"
//	    SELECT_COL    V0, R0, "price"
//	    REDUCE_SUM_F  F0, V0
//	    HALT_F        F0
//	`)
//
// With pre-loaded DataFrames:
//
//	frame := dataframe.NewDataFrame(
//	    dataframe.NewSeriesFloat64("price", nil, prices...),
//	)
//
//	result, err := dasm.ExecuteWithFrames(`
//	    LOAD_FRAME    R0, "sales"
//	    SELECT_COL    V0, R0, "price"
//	    REDUCE_SUM_F  F0, V0
//	    HALT_F        F0
//	`, map[string]*dataframe.DataFrame{"sales": frame})
package embed

import (
	"context"
	"errors"
	"os"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"

	"github.com/akhildatla/dasm/pkg/compiler"
	"github.com/akhildatla/dasm/pkg/dsl"
	"github.com/akhildatla/dasm/pkg/vm"
)

// Common errors
var (
	ErrTimeout          = errors.New("execution timeout exceeded")
	ErrInstructionLimit = errors.New("instruction limit exceeded")
	ErrMemoryLimit      = errors.New("memory limit exceeded")
	ErrFileAccessDenied = errors.New("file access denied in sandbox mode")
)

// Execute compiles and runs DFL assembly code, returns the result.
func Execute(code string) (any, error) {
	return ExecuteWithFrames(code, nil)
}

// ExecuteFile reads a .dasm file and executes it.
func ExecuteFile(path string) (any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Execute(string(data))
}

// ExecuteWithFrames executes code with pre-loaded DataFrames.
// Scripts access these via LOAD_FRAME instruction.
func ExecuteWithFrames(code string, frames map[string]*dataframe.DataFrame) (any, error) {
	// Compile
	program, err := compiler.Compile(code)
	if err != nil {
		return nil, err
	}

	// Create VM
	machine := vm.NewVM()
	if frames != nil {
		machine.SetPredeclaredFrames(frames)
	}

	// Load program
	if err := machine.Load(program); err != nil {
		return nil, err
	}

	// Execute
	return machine.Execute()
}

// Options configures execution behavior for ExecuteWithOptions.
type Options struct {
	// Frames provides pre-loaded DataFrames accessible via LOAD_FRAME.
	Frames map[string]*dataframe.DataFrame

	// Timeout sets maximum execution time. Zero means no timeout.
	Timeout time.Duration

	// MaxInstructions limits the number of instructions executed.
	// Zero means unlimited.
	MaxInstructions int64

	// MaxMemoryBytes limits memory allocation.
	// Zero means unlimited.
	MaxMemoryBytes int64

	// Sandbox restricts file system access when true.
	// In sandbox mode, only pre-loaded frames can be used.
	Sandbox bool

	// AllowedPaths lists paths that can be accessed even in sandbox mode.
	// Supports exact paths only (no globs).
	AllowedPaths []string

	// Context for cancellation. If nil, context.Background() is used.
	Context context.Context
}

// Option is a functional option for configuring execution.
type Option func(*Options)

// WithFrames sets pre-loaded DataFrames.
func WithFrames(frames map[string]*dataframe.DataFrame) Option {
	return func(o *Options) {
		o.Frames = frames
	}
}

// WithTimeout sets execution timeout.
func WithTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.Timeout = d
	}
}

// WithMaxInstructions sets instruction limit.
func WithMaxInstructions(n int64) Option {
	return func(o *Options) {
		o.MaxInstructions = n
	}
}

// WithMaxMemory sets memory limit in bytes.
func WithMaxMemory(bytes int64) Option {
	return func(o *Options) {
		o.MaxMemoryBytes = bytes
	}
}

// WithSandbox enables sandbox mode.
func WithSandbox() Option {
	return func(o *Options) {
		o.Sandbox = true
	}
}

// WithAllowedPaths sets paths accessible in sandbox mode.
func WithAllowedPaths(paths ...string) Option {
	return func(o *Options) {
		o.AllowedPaths = paths
	}
}

// WithContext sets the context for cancellation.
func WithContext(ctx context.Context) Option {
	return func(o *Options) {
		o.Context = ctx
	}
}

// ExecuteWithOptions executes code with advanced configuration.
// Supports resource limits, timeouts, and sandboxing.
//
// Example:
//
//	result, err := dfl.ExecuteWithOptions(code,
//	    dfl.WithTimeout(5*time.Second),
//	    dfl.WithMaxInstructions(10000),
//	    dfl.WithSandbox(),
//	    dfl.WithFrames(map[string]*dataframe.DataFrame{"data": frame}),
//	)
func ExecuteWithOptions(code string, opts ...Option) (any, error) {
	// Apply options
	options := &Options{
		Context: context.Background(),
	}
	for _, opt := range opts {
		opt(options)
	}

	// Compile
	program, err := compiler.Compile(code)
	if err != nil {
		return nil, err
	}

	// Create VM with options
	machine := vm.NewVM()
	if options.Frames != nil {
		machine.SetPredeclaredFrames(options.Frames)
	}

	// Configure VM limits
	machine.SetInstructionLimit(options.MaxInstructions)
	machine.SetMemoryLimit(options.MaxMemoryBytes)
	machine.SetSandbox(options.Sandbox, options.AllowedPaths)

	// Load program
	if err := machine.Load(program); err != nil {
		return nil, err
	}

	// Setup timeout context
	ctx := options.Context
	if options.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, options.Timeout)
		defer cancel()
	}
	machine.SetContext(ctx)

	// Execute
	result, err := machine.Execute()
	if err != nil {
		// Map VM errors to embed package errors
		switch {
		case errors.Is(err, vm.ErrInstructionLimit):
			return nil, ErrInstructionLimit
		case errors.Is(err, vm.ErrMemoryLimit):
			return nil, ErrMemoryLimit
		case errors.Is(err, vm.ErrFileAccessDenied):
			return nil, ErrFileAccessDenied
		case errors.Is(err, context.DeadlineExceeded):
			return nil, ErrTimeout
		case errors.Is(err, context.Canceled):
			return nil, err
		}
		return nil, err
	}

	return result, nil
}

// ExecuteDSL compiles and runs high-level DSL code.
// The DSL is compiled to assembly first, then executed.
//
// Example:
//
//	result, err := dfl.ExecuteDSL(`
//	    data = load("sales.csv")
//	    data |> filter(quantity > 10) |> select(price, quantity)
//	    return sum(data.price)
//	`)
func ExecuteDSL(code string, opts ...Option) (any, error) {
	// Import DSL package inline to avoid circular dependency
	dslAsm, err := compileDSL(code)
	if err != nil {
		return nil, err
	}
	return ExecuteWithOptions(dslAsm, opts...)
}

// ExecuteDSLFile reads a .dfx file and executes it.
func ExecuteDSLFile(path string, opts ...Option) (any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ExecuteDSL(string(data), opts...)
}

// compileDSL compiles DSL code to assembly.
func compileDSL(code string) (string, error) {
	lexer := dsl.NewLexer(code)
	tokens := lexer.Tokenize()

	parser := dsl.NewParser(tokens)
	program, err := parser.Parse()
	if err != nil {
		return "", err
	}

	comp := dsl.NewCompiler()
	return comp.Compile(program)
}
