// Package vm implements the DASM virtual machine.
//
// The VM is a register-based bytecode interpreter with:
//   - 16 scalar registers (R0-R15) for 64-bit integers and frame references
//   - 16 float registers (F0-F15) for 64-bit floats
//   - 8 vector registers (V0-V7) for column data (dataframe.Series)
//
// Basic usage:
//
//	v := vm.NewVM()
//	v.Load(program)
//	result, err := v.Execute()
//
// With resource limits:
//
//	v := vm.NewVM()
//	v.SetMaxSteps(10000)
//	v.SetContext(ctx)
//	v.Load(program)
//	result, err := v.Execute()
package vm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"

	"github.com/akhildatla/dasm/pkg/loader"
)

// Error definitions
var (
	ErrNoHalt             = errors.New("program ended without HALT")
	ErrStepLimitExceeded  = errors.New("step limit exceeded")
	ErrAllocLimitExceeded = errors.New("allocation limit exceeded")
	ErrInvalidInstruction = errors.New("invalid instruction")
	ErrColumnNotFound     = errors.New("column not found")
	ErrFrameNotFound      = errors.New("frame not found")
	ErrTypeMismatch       = errors.New("type mismatch")
	ErrDivisionByZero     = errors.New("division by zero")
	ErrInvalidRegister    = errors.New("invalid register")

	// Resource limit errors (exported for embed package)
	ErrInstructionLimit = errors.New("instruction limit exceeded")
	ErrMemoryLimit      = errors.New("memory limit exceeded")
	ErrFileAccessDenied = errors.New("file access denied in sandbox mode")
)

// Program represents a compiled DFL program.
type Program struct {
	Code           []Instruction
	Constants      []any     // String and integer constant pool
	FloatConstants []float64 // Float constant pool
}

// ExecutionStats contains metrics about VM execution for observability.
type ExecutionStats struct {
	StepsExecuted   int64          // Total instructions executed
	ExecutionTimeNs int64          // Execution time in nanoseconds
	FramesLoaded    int            // Number of frames loaded
	RowsProcessed   int64          // Approximate rows processed
	PeakRegisters   int            // Peak number of registers used
	OpCounts        map[string]int // Count of each opcode executed
}

// GroupByResult holds the result of a GROUP_BY operation.
type GroupByResult struct {
	Keys      dataframe.Series // Unique keys
	Groups    map[any][]int    // Key -> row indices in original frame
	KeyOrder  []any            // Order of keys for deterministic iteration
	SourceCol dataframe.Series // Original key column for type info
}

// VM represents the virtual machine.
type VM struct {
	registers   RegisterFile
	code        []Instruction
	constants   []any
	floatConsts []float64
	frames      map[int]*dataframe.DataFrame    // Loaded dataframes (keyed by register)
	predeclared map[string]*dataframe.DataFrame // Pre-declared frames for embedding
	groupbys    map[int]*GroupByResult          // GroupBy results (keyed by register)
	ip          int                             // Instruction pointer

	// Resource limits (Starlark-style)
	maxSteps  int64
	stepCount int64
	maxAlloc  int64
	// allocCount is reserved for future memory limit tracking

	// Context for cancellation
	ctx context.Context

	// Sandbox mode
	sandbox      bool
	allowedPaths []string

	// Observability - execution statistics
	stats        ExecutionStats
	statsEnabled bool
}

// NewVM creates a new VM instance.
func NewVM() *VM {
	return &VM{
		registers:   RegisterFile{},
		frames:      make(map[int]*dataframe.DataFrame),
		predeclared: make(map[string]*dataframe.DataFrame),
		groupbys:    make(map[int]*GroupByResult),
	}
}

// Load loads a program into the VM.
func (vm *VM) Load(program *Program) error {
	vm.code = program.Code
	vm.constants = program.Constants
	vm.floatConsts = program.FloatConstants
	vm.ip = 0
	vm.stepCount = 0
	vm.registers.Reset()
	vm.frames = make(map[int]*dataframe.DataFrame)
	vm.groupbys = make(map[int]*GroupByResult)
	return nil
}

// SetMaxSteps sets the maximum number of execution steps.
func (vm *VM) SetMaxSteps(n int64) {
	vm.maxSteps = n
}

// SetMaxAlloc sets the maximum memory allocation.
func (vm *VM) SetMaxAlloc(bytes int64) {
	vm.maxAlloc = bytes
}

// SetInstructionLimit sets the maximum number of instructions (alias for SetMaxSteps).
func (vm *VM) SetInstructionLimit(n int64) {
	vm.maxSteps = n
}

// SetMemoryLimit sets the maximum memory allocation in bytes (alias for SetMaxAlloc).
func (vm *VM) SetMemoryLimit(bytes int64) {
	vm.maxAlloc = bytes
}

// SetContext sets the context for cancellation/timeout.
func (vm *VM) SetContext(ctx context.Context) {
	vm.ctx = ctx
}

// SetSandbox enables sandbox mode with optional allowed paths.
func (vm *VM) SetSandbox(enabled bool, allowedPaths []string) {
	vm.sandbox = enabled
	vm.allowedPaths = allowedPaths
}

// EnableStats enables execution statistics collection.
// When enabled, the VM tracks metrics like steps executed, timing, and opcode counts.
func (vm *VM) EnableStats() {
	vm.statsEnabled = true
	vm.stats = ExecutionStats{
		OpCounts: make(map[string]int),
	}
}

// Stats returns the execution statistics from the last Execute() call.
// Returns nil if stats were not enabled via EnableStats().
func (vm *VM) Stats() *ExecutionStats {
	if !vm.statsEnabled {
		return nil
	}
	return &vm.stats
}

// SetPredeclaredFrames sets frames that can be accessed via LOAD_FRAME.
func (vm *VM) SetPredeclaredFrames(frames map[string]*dataframe.DataFrame) {
	vm.predeclared = frames
}

// isPathAllowed checks if a file path is allowed in sandbox mode.
func (vm *VM) isPathAllowed(path string) bool {
	for _, allowed := range vm.allowedPaths {
		if path == allowed || strings.HasPrefix(path, allowed+"/") {
			return true
		}
	}
	return false
}

// Execute runs the loaded program and returns the result.
func (vm *VM) Execute() (any, error) {
	// Start timing if stats enabled
	var startTime time.Time
	if vm.statsEnabled {
		startTime = time.Now()
		vm.stats.StepsExecuted = 0
		vm.stats.FramesLoaded = 0
		vm.stats.RowsProcessed = 0
	}

	for vm.ip < len(vm.code) {
		// Context cancellation check
		if vm.ctx != nil {
			select {
			case <-vm.ctx.Done():
				return nil, vm.ctx.Err()
			default:
			}
		}

		// Resource limit check
		vm.stepCount++
		if vm.maxSteps > 0 && vm.stepCount > vm.maxSteps {
			return nil, ErrInstructionLimit
		}

		inst := vm.code[vm.ip]
		op := inst.Opcode()

		// Track opcode execution if stats enabled
		if vm.statsEnabled {
			vm.stats.StepsExecuted++
			opName := op.String()
			vm.stats.OpCounts[opName]++
		}

		switch op {
		// ===== Data Loading =====
		case OpLoadCSV:
			dst := inst.Dst()
			pathIdx := inst.Imm16()
			path := vm.constants[pathIdx].(string)

			// Sandbox check
			if vm.sandbox && !vm.isPathAllowed(path) {
				return nil, fmt.Errorf("%w: %s", ErrFileAccessDenied, path)
			}

			frame, err := loader.LoadCSV(path)
			if err != nil {
				return nil, fmt.Errorf("loading CSV %s: %w", path, err)
			}
			vm.frames[int(dst)] = frame
			vm.registers.R[dst] = int64(dst)

		case OpLoadJSON:
			dst := inst.Dst()
			pathIdx := inst.Imm16()
			path := vm.constants[pathIdx].(string)

			// Sandbox check
			if vm.sandbox && !vm.isPathAllowed(path) {
				return nil, fmt.Errorf("%w: %s", ErrFileAccessDenied, path)
			}

			frame, err := loader.LoadJSON(path)
			if err != nil {
				return nil, fmt.Errorf("loading JSON %s: %w", path, err)
			}
			vm.frames[int(dst)] = frame
			vm.registers.R[dst] = int64(dst)

		case OpLoadParquet:
			dst := inst.Dst()
			pathIdx := inst.Imm16()
			path := vm.constants[pathIdx].(string)

			// Sandbox check
			if vm.sandbox && !vm.isPathAllowed(path) {
				return nil, fmt.Errorf("%w: %s", ErrFileAccessDenied, path)
			}

			frame, err := loader.LoadParquet(path)
			if err != nil {
				return nil, fmt.Errorf("loading Parquet %s: %w", path, err)
			}
			vm.frames[int(dst)] = frame
			vm.registers.R[dst] = int64(dst)

		case OpLoadConst:
			dst := inst.Dst()
			constIdx := inst.Imm16()
			vm.registers.R[dst] = vm.constants[constIdx].(int64)

		case OpLoadConstF:
			dst := inst.Dst()
			constIdx := inst.Imm16()
			vm.registers.F[dst] = vm.floatConsts[constIdx]

		case OpLoadFrame:
			dst := inst.Dst()
			nameIdx := inst.Imm16()
			name := vm.constants[nameIdx].(string)
			frame, ok := vm.predeclared[name]
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrFrameNotFound, name)
			}
			vm.frames[int(dst)] = frame
			vm.registers.R[dst] = int64(dst)

		case OpSelectCol:
			dst := inst.Dst()
			frameSrc := inst.Src1()
			nameIdx := inst.Imm8() // Use Imm8 since Src1 is used
			colName := vm.constants[nameIdx].(string)
			frame := vm.frames[int(vm.registers.R[frameSrc])]
			col, ok := getDataFrameColumn(frame, colName)
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrColumnNotFound, colName)
			}
			vm.registers.V[dst] = col

		case OpBroadcast:
			dst := inst.Dst()
			src := inst.Src1()
			lenSrc := inst.Src2()
			value := vm.registers.R[src]
			length := getSeriesLength(vm.registers.V[lenSrc])
			data := make([]int64, length)
			for i := range data {
				data[i] = value
			}
			vm.registers.V[dst] = newInt64Series("broadcast", data)

		case OpBroadcastF:
			dst := inst.Dst()
			src := inst.Src1()
			lenSrc := inst.Src2()
			value := vm.registers.F[src]
			length := getSeriesLength(vm.registers.V[lenSrc])
			data := make([]float64, length)
			for i := range data {
				data[i] = value
			}
			vm.registers.V[dst] = newFloat64Series("broadcast", data)

		// ===== Vector Arithmetic =====
		case OpVecAddI:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorAddInt64(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpVecSubI:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorSubInt64(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpVecMulI:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorMulInt64(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpVecDivI:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result, err := vm.vectorDivInt64(vm.registers.V[src1], vm.registers.V[src2])
			if err != nil {
				return nil, err
			}
			vm.registers.V[dst] = result

		case OpVecModI:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result, err := vm.vectorModInt64(vm.registers.V[src1], vm.registers.V[src2])
			if err != nil {
				return nil, err
			}
			vm.registers.V[dst] = result

		case OpVecAddF:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorAddFloat64(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpVecSubF:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorSubFloat64(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpVecMulF:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorMulFloat64(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpVecDivF:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorDivFloat64(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		// ===== Comparison =====
		case OpCmpEQ:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorCmpEQ(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpCmpNE:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorCmpNE(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpCmpLT:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorCmpLT(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpCmpLE:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorCmpLE(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpCmpGT:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorCmpGT(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpCmpGE:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorCmpGE(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		// ===== Logical =====
		case OpAnd:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorAnd(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpOr:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			result := vm.vectorOr(vm.registers.V[src1], vm.registers.V[src2])
			vm.registers.V[dst] = result

		case OpNot:
			dst, src1 := inst.Dst(), inst.Src1()
			result := vm.vectorNot(vm.registers.V[src1])
			vm.registers.V[dst] = result

		// ===== Filtering =====
		case OpFilter:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			data := vm.registers.V[src1]
			mask := vm.registers.V[src2]
			result := vm.filterSeriesWithMask(data, mask)
			vm.registers.V[dst] = result

		case OpTake:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			data := vm.registers.V[src1]
			indices := vm.registers.V[src2]
			result := vm.takeSeries(data, indices)
			vm.registers.V[dst] = result

		// ===== Aggregations =====
		case OpReduceSum:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.R[dst] = vm.reduceSum(vm.registers.V[src])

		case OpReduceSumF:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.F[dst] = vm.reduceSumF(vm.registers.V[src])

		case OpReduceCount:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.R[dst] = vm.reduceCount(vm.registers.V[src])

		case OpReduceMin:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.R[dst] = vm.reduceMin(vm.registers.V[src])

		case OpReduceMax:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.R[dst] = vm.reduceMax(vm.registers.V[src])

		case OpReduceMinF:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.F[dst] = vm.reduceMinF(vm.registers.V[src])

		case OpReduceMaxF:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.F[dst] = vm.reduceMaxF(vm.registers.V[src])

		case OpReduceMean:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.F[dst] = vm.reduceMean(vm.registers.V[src])

		// ===== Scalar Operations =====
		case OpMoveR:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.R[dst] = vm.registers.R[src]

		case OpMoveF:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.F[dst] = vm.registers.F[src]

		case OpAddR:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			vm.registers.R[dst] = vm.registers.R[src1] + vm.registers.R[src2]

		case OpSubR:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			vm.registers.R[dst] = vm.registers.R[src1] - vm.registers.R[src2]

		case OpMulR:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			vm.registers.R[dst] = vm.registers.R[src1] * vm.registers.R[src2]

		case OpDivR:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			if vm.registers.R[src2] == 0 {
				return nil, ErrDivisionByZero
			}
			vm.registers.R[dst] = vm.registers.R[src1] / vm.registers.R[src2]

		// ===== Frame Operations =====
		case OpNewFrame:
			dst := inst.Dst()
			vm.frames[int(dst)] = newEmptyDataFrame()
			vm.registers.R[dst] = int64(dst)

		case OpAddCol:
			dst := inst.Dst()
			src := inst.Src1()
			nameIdx := inst.Imm8() // Use Imm8 since Src1 is used
			colName := vm.constants[nameIdx].(string)
			col := vm.registers.V[src]
			// Clone and rename the series
			cloned := cloneSeries(col)
			if cloned != nil {
				// Set the name using Rename
				cloned.Rename(colName)
			}
			frame := vm.frames[int(vm.registers.R[dst])]
			addColumnToDataFrame(frame, cloned)

		case OpColCount:
			dst, src := inst.Dst(), inst.Src1()
			frame := vm.frames[int(vm.registers.R[src])]
			if frame != nil {
				vm.registers.R[dst] = int64(len(frame.Series))
			} else {
				vm.registers.R[dst] = 0
			}

		case OpRowCount:
			dst, src := inst.Dst(), inst.Src1()
			frame := vm.frames[int(vm.registers.R[src])]
			vm.registers.R[dst] = int64(getDataFrameLength(frame))

		// ===== GroupBy Operations =====
		case OpGroupBy:
			dst, src := inst.Dst(), inst.Src1()
			keyCol := vm.registers.V[src]
			vm.groupbys[int(dst)] = vm.groupBy(keyCol)
			vm.registers.R[dst] = int64(dst)

		case OpGroupCount:
			dst, src := inst.Dst(), inst.Src1()
			gb := vm.groupbys[int(vm.registers.R[src])]
			vm.registers.V[dst] = vm.groupCount(gb)

		case OpGroupSum:
			dst, gbSrc, valSrc := inst.Dst(), inst.Src1(), inst.Src2()
			gb := vm.groupbys[int(vm.registers.R[gbSrc])]
			valCol := vm.registers.V[valSrc]
			vm.registers.V[dst] = vm.groupSum(gb, valCol)

		case OpGroupSumF:
			dst, gbSrc, valSrc := inst.Dst(), inst.Src1(), inst.Src2()
			gb := vm.groupbys[int(vm.registers.R[gbSrc])]
			valCol := vm.registers.V[valSrc]
			vm.registers.V[dst] = vm.groupSumF(gb, valCol)

		case OpGroupMin:
			dst, gbSrc, valSrc := inst.Dst(), inst.Src1(), inst.Src2()
			gb := vm.groupbys[int(vm.registers.R[gbSrc])]
			valCol := vm.registers.V[valSrc]
			vm.registers.V[dst] = vm.groupMin(gb, valCol)

		case OpGroupMax:
			dst, gbSrc, valSrc := inst.Dst(), inst.Src1(), inst.Src2()
			gb := vm.groupbys[int(vm.registers.R[gbSrc])]
			valCol := vm.registers.V[valSrc]
			vm.registers.V[dst] = vm.groupMax(gb, valCol)

		case OpGroupMinF:
			dst, gbSrc, valSrc := inst.Dst(), inst.Src1(), inst.Src2()
			gb := vm.groupbys[int(vm.registers.R[gbSrc])]
			valCol := vm.registers.V[valSrc]
			vm.registers.V[dst] = vm.groupMinF(gb, valCol)

		case OpGroupMaxF:
			dst, gbSrc, valSrc := inst.Dst(), inst.Src1(), inst.Src2()
			gb := vm.groupbys[int(vm.registers.R[gbSrc])]
			valCol := vm.registers.V[valSrc]
			vm.registers.V[dst] = vm.groupMaxF(gb, valCol)

		case OpGroupMean:
			dst, gbSrc, valSrc := inst.Dst(), inst.Src1(), inst.Src2()
			gb := vm.groupbys[int(vm.registers.R[gbSrc])]
			valCol := vm.registers.V[valSrc]
			vm.registers.V[dst] = vm.groupMean(gb, valCol)

		case OpGroupKeys:
			dst, src := inst.Dst(), inst.Src1()
			gb := vm.groupbys[int(vm.registers.R[src])]
			vm.registers.V[dst] = gb.Keys

		// ===== Join Operations =====
		case OpJoinInner:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			nameIdx := inst.Imm8() // Use Imm8 since src1/src2 are used
			keyName := vm.constants[nameIdx].(string)
			left := vm.frames[int(vm.registers.R[src1])]
			right := vm.frames[int(vm.registers.R[src2])]
			result := vm.joinInner(left, right, keyName)
			vm.frames[int(dst)] = result
			vm.registers.R[dst] = int64(dst)

		case OpJoinLeft:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			nameIdx := inst.Imm8() // Use Imm8 since src1/src2 are used
			keyName := vm.constants[nameIdx].(string)
			left := vm.frames[int(vm.registers.R[src1])]
			right := vm.frames[int(vm.registers.R[src2])]
			result := vm.joinLeft(left, right, keyName)
			vm.frames[int(dst)] = result
			vm.registers.R[dst] = int64(dst)

		case OpJoinRight:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			nameIdx := inst.Imm8() // Use Imm8 since src1/src2 are used
			keyName := vm.constants[nameIdx].(string)
			left := vm.frames[int(vm.registers.R[src1])]
			right := vm.frames[int(vm.registers.R[src2])]
			result := vm.joinRight(left, right, keyName)
			vm.frames[int(dst)] = result
			vm.registers.R[dst] = int64(dst)

		case OpJoinOuter:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			nameIdx := inst.Imm8() // Use Imm8 since src1/src2 are used
			keyName := vm.constants[nameIdx].(string)
			left := vm.frames[int(vm.registers.R[src1])]
			right := vm.frames[int(vm.registers.R[src2])]
			result := vm.joinOuter(left, right, keyName)
			vm.frames[int(dst)] = result
			vm.registers.R[dst] = int64(dst)

		// ===== String Operations =====
		case OpStrLen:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.V[dst] = vm.strLen(vm.registers.V[src])

		case OpStrUpper:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.V[dst] = vm.strUpper(vm.registers.V[src])

		case OpStrLower:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.V[dst] = vm.strLower(vm.registers.V[src])

		case OpStrConcat:
			dst, src1, src2 := inst.Dst(), inst.Src1(), inst.Src2()
			vm.registers.V[dst] = vm.strConcat(vm.registers.V[src1], vm.registers.V[src2])

		case OpStrContains:
			dst, src := inst.Dst(), inst.Src1()
			patternIdx := inst.Imm8() // Use Imm8 since Src1 is used
			pattern := vm.constants[patternIdx].(string)
			vm.registers.V[dst] = vm.strContains(vm.registers.V[src], pattern)

		case OpStrStartsWith:
			dst, src := inst.Dst(), inst.Src1()
			patternIdx := inst.Imm8() // Use Imm8 since Src1 is used
			pattern := vm.constants[patternIdx].(string)
			vm.registers.V[dst] = vm.strStartsWith(vm.registers.V[src], pattern)

		case OpStrEndsWith:
			dst, src := inst.Dst(), inst.Src1()
			patternIdx := inst.Imm8() // Use Imm8 since Src1 is used
			pattern := vm.constants[patternIdx].(string)
			vm.registers.V[dst] = vm.strEndsWith(vm.registers.V[src], pattern)

		case OpStrTrim:
			dst, src := inst.Dst(), inst.Src1()
			vm.registers.V[dst] = vm.strTrim(vm.registers.V[src])

		case OpStrSplit:
			dst, src := inst.Dst(), inst.Src1()
			delimIdx := inst.Imm8() // Use Imm8 since Src1 is used
			delim := vm.constants[delimIdx].(string)
			vm.registers.V[dst] = vm.strSplit(vm.registers.V[src], delim)

		case OpStrReplace:
			dst, src := inst.Dst(), inst.Src1()
			patternIdx := inst.Imm8() // Use Imm8 since Src1 is used
			pattern := vm.constants[patternIdx].(string)
			vm.registers.V[dst] = vm.strReplace(vm.registers.V[src], pattern)

		// ===== Control Flow =====
		case OpNop:
			// Do nothing

		case OpHalt:
			dst := inst.Dst()
			if vm.statsEnabled {
				vm.stats.ExecutionTimeNs = time.Since(startTime).Nanoseconds()
				vm.stats.FramesLoaded = len(vm.frames)
			}
			return vm.registers.R[dst], nil

		case OpHaltF:
			dst := inst.Dst()
			if vm.statsEnabled {
				vm.stats.ExecutionTimeNs = time.Since(startTime).Nanoseconds()
				vm.stats.FramesLoaded = len(vm.frames)
			}
			return vm.registers.F[dst], nil

		default:
			return nil, fmt.Errorf("%w: opcode 0x%02X", ErrInvalidInstruction, op)
		}

		vm.ip++
	}

	return nil, ErrNoHalt
}

// ===== Vector Operations =====

func (vm *VM) vectorAddInt64(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]int64, length)
	for i := 0; i < length; i++ {
		av, _ := getInt64Value(a, i)
		bv, _ := getInt64Value(b, i)
		data[i] = av + bv
	}
	return newInt64Series("result", data)
}

func (vm *VM) vectorSubInt64(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]int64, length)
	for i := 0; i < length; i++ {
		av, _ := getInt64Value(a, i)
		bv, _ := getInt64Value(b, i)
		data[i] = av - bv
	}
	return newInt64Series("result", data)
}

func (vm *VM) vectorMulInt64(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]int64, length)
	for i := 0; i < length; i++ {
		av, _ := getInt64Value(a, i)
		bv, _ := getInt64Value(b, i)
		data[i] = av * bv
	}
	return newInt64Series("result", data)
}

func (vm *VM) vectorDivInt64(a, b dataframe.Series) (dataframe.Series, error) {
	length := getSeriesLength(a)
	data := make([]int64, length)
	for i := 0; i < length; i++ {
		av, _ := getInt64Value(a, i)
		bv, _ := getInt64Value(b, i)
		if bv == 0 {
			return nil, ErrDivisionByZero
		}
		data[i] = av / bv
	}
	return newInt64Series("result", data), nil
}

func (vm *VM) vectorModInt64(a, b dataframe.Series) (dataframe.Series, error) {
	length := getSeriesLength(a)
	data := make([]int64, length)
	for i := 0; i < length; i++ {
		av, _ := getInt64Value(a, i)
		bv, _ := getInt64Value(b, i)
		if bv == 0 {
			return nil, ErrDivisionByZero
		}
		data[i] = av % bv
	}
	return newInt64Series("result", data), nil
}

func (vm *VM) vectorAddFloat64(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]float64, length)
	for i := 0; i < length; i++ {
		av, _ := getFloat64Value(a, i)
		bv, _ := getFloat64Value(b, i)
		data[i] = av + bv
	}
	return newFloat64Series("result", data)
}

func (vm *VM) vectorSubFloat64(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]float64, length)
	for i := 0; i < length; i++ {
		av, _ := getFloat64Value(a, i)
		bv, _ := getFloat64Value(b, i)
		data[i] = av - bv
	}
	return newFloat64Series("result", data)
}

func (vm *VM) vectorMulFloat64(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]float64, length)
	for i := 0; i < length; i++ {
		av, _ := getFloat64Value(a, i)
		bv, _ := getFloat64Value(b, i)
		data[i] = av * bv
	}
	return newFloat64Series("result", data)
}

func (vm *VM) vectorDivFloat64(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]float64, length)
	for i := 0; i < length; i++ {
		av, _ := getFloat64Value(a, i)
		bv, _ := getFloat64Value(b, i)
		if bv == 0 {
			data[i] = math.Inf(1)
		} else {
			data[i] = av / bv
		}
	}
	return newFloat64Series("result", data)
}

// ===== Comparison Operations =====

func (vm *VM) vectorCmpEQ(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]bool, length)
	for i := 0; i < length; i++ {
		data[i] = vm.compareValues(a, b, i) == 0
	}
	return newBoolSeries("result", data)
}

func (vm *VM) vectorCmpNE(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]bool, length)
	for i := 0; i < length; i++ {
		data[i] = vm.compareValues(a, b, i) != 0
	}
	return newBoolSeries("result", data)
}

func (vm *VM) vectorCmpLT(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]bool, length)
	for i := 0; i < length; i++ {
		data[i] = vm.compareValues(a, b, i) < 0
	}
	return newBoolSeries("result", data)
}

func (vm *VM) vectorCmpLE(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]bool, length)
	for i := 0; i < length; i++ {
		data[i] = vm.compareValues(a, b, i) <= 0
	}
	return newBoolSeries("result", data)
}

func (vm *VM) vectorCmpGT(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]bool, length)
	for i := 0; i < length; i++ {
		data[i] = vm.compareValues(a, b, i) > 0
	}
	return newBoolSeries("result", data)
}

func (vm *VM) vectorCmpGE(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]bool, length)
	for i := 0; i < length; i++ {
		data[i] = vm.compareValues(a, b, i) >= 0
	}
	return newBoolSeries("result", data)
}

// compareValues compares values at index i, returns -1, 0, or 1
func (vm *VM) compareValues(a, b dataframe.Series, i int) int {
	// Handle different types by converting to float64 for comparison
	av, _ := getFloat64Value(a, i)
	bv, _ := getFloat64Value(b, i)

	if av < bv {
		return -1
	} else if av > bv {
		return 1
	}
	return 0
}

// ===== Logical Operations =====

func (vm *VM) vectorAnd(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]bool, length)
	for i := 0; i < length; i++ {
		av, _ := getBoolValue(a, i)
		bv, _ := getBoolValue(b, i)
		data[i] = av && bv
	}
	return newBoolSeries("result", data)
}

func (vm *VM) vectorOr(a, b dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]bool, length)
	for i := 0; i < length; i++ {
		av, _ := getBoolValue(a, i)
		bv, _ := getBoolValue(b, i)
		data[i] = av || bv
	}
	return newBoolSeries("result", data)
}

func (vm *VM) vectorNot(a dataframe.Series) dataframe.Series {
	length := getSeriesLength(a)
	data := make([]bool, length)
	for i := 0; i < length; i++ {
		av, _ := getBoolValue(a, i)
		data[i] = !av
	}
	return newBoolSeries("result", data)
}

// ===== Filter Operations =====

func (vm *VM) filterSeriesWithMask(data, mask dataframe.Series) dataframe.Series {
	// Convert bool series to bitmap
	length := getSeriesLength(mask)
	bitmap := NewBitmap(length)
	for i := 0; i < length; i++ {
		if v, ok := getBoolValue(mask, i); ok && v {
			bitmap.Set(i)
		}
	}
	return filterSeries(data, bitmap)
}

func (vm *VM) takeSeries(data, indices dataframe.Series) dataframe.Series {
	// Take elements at specified indices
	n := getSeriesLength(indices)
	vals := make([]interface{}, n)
	for i := 0; i < n; i++ {
		idx, _ := getInt64Value(indices, i)
		vals[i] = data.Value(int(idx))
	}
	return createSeriesWithValues(data, vals)
}

// ===== Aggregation Operations =====

func (vm *VM) reduceSum(s dataframe.Series) int64 {
	var sum int64
	n := getSeriesLength(s)
	for i := 0; i < n; i++ {
		if v, ok := getInt64Value(s, i); ok {
			sum += v
		}
	}
	return sum
}

func (vm *VM) reduceSumF(s dataframe.Series) float64 {
	var sum float64
	n := getSeriesLength(s)
	for i := 0; i < n; i++ {
		if v, ok := getFloat64Value(s, i); ok {
			sum += v
		}
	}
	return sum
}

func (vm *VM) reduceCount(s dataframe.Series) int64 {
	// For bool series, count true values
	if getSeriesType(s) == TypeBool {
		var count int64
		n := getSeriesLength(s)
		for i := 0; i < n; i++ {
			if v, ok := getBoolValue(s, i); ok && v {
				count++
			}
		}
		return count
	}
	// For other series, count non-nil values
	var count int64
	n := getSeriesLength(s)
	for i := 0; i < n; i++ {
		if !isNil(s, i) {
			count++
		}
	}
	return count
}

func (vm *VM) reduceMin(s dataframe.Series) int64 {
	n := getSeriesLength(s)
	if n == 0 {
		return 0
	}
	min, _ := getInt64Value(s, 0)
	for i := 1; i < n; i++ {
		if v, ok := getInt64Value(s, i); ok && v < min {
			min = v
		}
	}
	return min
}

func (vm *VM) reduceMax(s dataframe.Series) int64 {
	n := getSeriesLength(s)
	if n == 0 {
		return 0
	}
	max, _ := getInt64Value(s, 0)
	for i := 1; i < n; i++ {
		if v, ok := getInt64Value(s, i); ok && v > max {
			max = v
		}
	}
	return max
}

func (vm *VM) reduceMinF(s dataframe.Series) float64 {
	n := getSeriesLength(s)
	if n == 0 {
		return 0
	}
	min, _ := getFloat64Value(s, 0)
	for i := 1; i < n; i++ {
		if v, ok := getFloat64Value(s, i); ok && v < min {
			min = v
		}
	}
	return min
}

func (vm *VM) reduceMaxF(s dataframe.Series) float64 {
	n := getSeriesLength(s)
	if n == 0 {
		return 0
	}
	max, _ := getFloat64Value(s, 0)
	for i := 1; i < n; i++ {
		if v, ok := getFloat64Value(s, i); ok && v > max {
			max = v
		}
	}
	return max
}

func (vm *VM) reduceMean(s dataframe.Series) float64 {
	var sum float64
	var count int
	n := getSeriesLength(s)
	for i := 0; i < n; i++ {
		if v, ok := getFloat64Value(s, i); ok {
			sum += v
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// ===== GroupBy Operations =====

func (vm *VM) groupBy(keyCol dataframe.Series) *GroupByResult {
	groups := make(map[any][]int)
	var keyOrder []any
	seen := make(map[any]bool)

	n := getSeriesLength(keyCol)
	for i := 0; i < n; i++ {
		key := keyCol.Value(i)
		if !seen[key] {
			seen[key] = true
			keyOrder = append(keyOrder, key)
		}
		groups[key] = append(groups[key], i)
	}

	// Build unique keys series
	keys := vm.buildKeysSeries(keyCol, keyOrder)

	return &GroupByResult{
		Keys:      keys,
		Groups:    groups,
		KeyOrder:  keyOrder,
		SourceCol: keyCol,
	}
}

func (vm *VM) buildKeysSeries(srcCol dataframe.Series, keyOrder []any) dataframe.Series {
	typ := getSeriesType(srcCol)
	switch typ {
	case TypeInt64:
		data := make([]int64, len(keyOrder))
		for i, k := range keyOrder {
			data[i] = k.(int64)
		}
		return newInt64Series("keys", data)
	case TypeFloat64:
		data := make([]float64, len(keyOrder))
		for i, k := range keyOrder {
			data[i] = k.(float64)
		}
		return newFloat64Series("keys", data)
	case TypeString:
		data := make([]string, len(keyOrder))
		for i, k := range keyOrder {
			data[i] = k.(string)
		}
		return newStringSeries("keys", data)
	default:
		return newInt64Series("keys", nil)
	}
}

func (vm *VM) groupCount(gb *GroupByResult) dataframe.Series {
	data := make([]int64, len(gb.KeyOrder))
	for i, key := range gb.KeyOrder {
		data[i] = int64(len(gb.Groups[key]))
	}
	return newInt64Series("count", data)
}

func (vm *VM) groupSum(gb *GroupByResult, valCol dataframe.Series) dataframe.Series {
	data := make([]int64, len(gb.KeyOrder))
	for i, key := range gb.KeyOrder {
		var sum int64
		for _, idx := range gb.Groups[key] {
			if v, ok := getInt64Value(valCol, idx); ok {
				sum += v
			}
		}
		data[i] = sum
	}
	return newInt64Series("sum", data)
}

func (vm *VM) groupSumF(gb *GroupByResult, valCol dataframe.Series) dataframe.Series {
	data := make([]float64, len(gb.KeyOrder))
	for i, key := range gb.KeyOrder {
		var sum float64
		for _, idx := range gb.Groups[key] {
			if v, ok := getFloat64Value(valCol, idx); ok {
				sum += v
			}
		}
		data[i] = sum
	}
	return newFloat64Series("sum", data)
}

func (vm *VM) groupMin(gb *GroupByResult, valCol dataframe.Series) dataframe.Series {
	data := make([]int64, len(gb.KeyOrder))
	for i, key := range gb.KeyOrder {
		indices := gb.Groups[key]
		if len(indices) > 0 {
			min, _ := getInt64Value(valCol, indices[0])
			for _, idx := range indices[1:] {
				if v, ok := getInt64Value(valCol, idx); ok && v < min {
					min = v
				}
			}
			data[i] = min
		}
	}
	return newInt64Series("min", data)
}

func (vm *VM) groupMax(gb *GroupByResult, valCol dataframe.Series) dataframe.Series {
	data := make([]int64, len(gb.KeyOrder))
	for i, key := range gb.KeyOrder {
		indices := gb.Groups[key]
		if len(indices) > 0 {
			max, _ := getInt64Value(valCol, indices[0])
			for _, idx := range indices[1:] {
				if v, ok := getInt64Value(valCol, idx); ok && v > max {
					max = v
				}
			}
			data[i] = max
		}
	}
	return newInt64Series("max", data)
}

func (vm *VM) groupMinF(gb *GroupByResult, valCol dataframe.Series) dataframe.Series {
	data := make([]float64, len(gb.KeyOrder))
	for i, key := range gb.KeyOrder {
		indices := gb.Groups[key]
		if len(indices) > 0 {
			min, _ := getFloat64Value(valCol, indices[0])
			for _, idx := range indices[1:] {
				if v, ok := getFloat64Value(valCol, idx); ok && v < min {
					min = v
				}
			}
			data[i] = min
		}
	}
	return newFloat64Series("min", data)
}

func (vm *VM) groupMaxF(gb *GroupByResult, valCol dataframe.Series) dataframe.Series {
	data := make([]float64, len(gb.KeyOrder))
	for i, key := range gb.KeyOrder {
		indices := gb.Groups[key]
		if len(indices) > 0 {
			max, _ := getFloat64Value(valCol, indices[0])
			for _, idx := range indices[1:] {
				if v, ok := getFloat64Value(valCol, idx); ok && v > max {
					max = v
				}
			}
			data[i] = max
		}
	}
	return newFloat64Series("max", data)
}

func (vm *VM) groupMean(gb *GroupByResult, valCol dataframe.Series) dataframe.Series {
	data := make([]float64, len(gb.KeyOrder))
	for i, key := range gb.KeyOrder {
		indices := gb.Groups[key]
		if len(indices) > 0 {
			var sum float64
			for _, idx := range indices {
				if v, ok := getFloat64Value(valCol, idx); ok {
					sum += v
				}
			}
			data[i] = sum / float64(len(indices))
		}
	}
	return newFloat64Series("mean", data)
}

// ===== Join Operations =====

func (vm *VM) joinInner(left, right *dataframe.DataFrame, keyName string) *dataframe.DataFrame {
	leftKey, _ := getDataFrameColumn(left, keyName)
	rightKey, _ := getDataFrameColumn(right, keyName)

	// Build right index
	rightIndex := vm.buildJoinIndex(rightKey)

	// Find matching rows
	var leftIndices, rightIndices []int
	n := getSeriesLength(leftKey)
	for i := 0; i < n; i++ {
		key := leftKey.Value(i)
		if matches, ok := rightIndex[key]; ok {
			for _, j := range matches {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, j)
			}
		}
	}

	return vm.buildJoinResult(left, right, keyName, leftIndices, rightIndices)
}

func (vm *VM) joinLeft(left, right *dataframe.DataFrame, keyName string) *dataframe.DataFrame {
	leftKey, _ := getDataFrameColumn(left, keyName)
	rightKey, _ := getDataFrameColumn(right, keyName)

	// Build right index
	rightIndex := vm.buildJoinIndex(rightKey)

	// Find matching rows, keeping all left rows
	var leftIndices, rightIndices []int
	n := getSeriesLength(leftKey)
	for i := 0; i < n; i++ {
		key := leftKey.Value(i)
		if matches, ok := rightIndex[key]; ok {
			for _, j := range matches {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, j)
			}
		} else {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1) // null marker
		}
	}

	return vm.buildJoinResultWithNulls(left, right, keyName, leftIndices, rightIndices, true, false)
}

func (vm *VM) joinRight(left, right *dataframe.DataFrame, keyName string) *dataframe.DataFrame {
	leftKey, _ := getDataFrameColumn(left, keyName)
	rightKey, _ := getDataFrameColumn(right, keyName)

	// Build left index
	leftIndex := vm.buildJoinIndex(leftKey)

	// Find matching rows, keeping all right rows
	var leftIndices, rightIndices []int
	n := getSeriesLength(rightKey)
	for j := 0; j < n; j++ {
		key := rightKey.Value(j)
		if matches, ok := leftIndex[key]; ok {
			for _, i := range matches {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, j)
			}
		} else {
			leftIndices = append(leftIndices, -1) // null marker
			rightIndices = append(rightIndices, j)
		}
	}

	return vm.buildJoinResultWithNulls(left, right, keyName, leftIndices, rightIndices, false, true)
}

func (vm *VM) joinOuter(left, right *dataframe.DataFrame, keyName string) *dataframe.DataFrame {
	leftKey, _ := getDataFrameColumn(left, keyName)
	rightKey, _ := getDataFrameColumn(right, keyName)

	rightIndex := vm.buildJoinIndex(rightKey)
	matchedRight := make(map[int]bool)

	var leftIndices, rightIndices []int

	// Match from left side
	n := getSeriesLength(leftKey)
	for i := 0; i < n; i++ {
		key := leftKey.Value(i)
		if matches, ok := rightIndex[key]; ok {
			for _, j := range matches {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, j)
				matchedRight[j] = true
			}
		} else {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
		}
	}

	// Add unmatched right rows
	m := getSeriesLength(rightKey)
	for j := 0; j < m; j++ {
		if !matchedRight[j] {
			leftIndices = append(leftIndices, -1)
			rightIndices = append(rightIndices, j)
		}
	}

	return vm.buildJoinResultWithNulls(left, right, keyName, leftIndices, rightIndices, true, true)
}

func (vm *VM) buildJoinIndex(col dataframe.Series) map[any][]int {
	index := make(map[any][]int)
	n := getSeriesLength(col)
	for i := 0; i < n; i++ {
		key := col.Value(i)
		index[key] = append(index[key], i)
	}
	return index
}

func (vm *VM) buildJoinResult(left, right *dataframe.DataFrame, keyName string, leftIndices, rightIndices []int) *dataframe.DataFrame {
	// Collect all series first, then create DataFrame
	var allSeries []dataframe.Series

	// Gather columns from left frame
	for _, s := range left.Series {
		colName := s.Name()
		dstCol := vm.gatherSeries(s, leftIndices, colName)
		allSeries = append(allSeries, dstCol)
	}

	// Gather columns from right frame (except key column)
	for _, s := range right.Series {
		colName := s.Name()
		if colName == keyName {
			continue
		}
		// Prefix with right_ to avoid collision
		dstCol := vm.gatherSeries(s, rightIndices, "right_"+colName)
		allSeries = append(allSeries, dstCol)
	}

	return dataframe.NewDataFrame(allSeries...)
}

func (vm *VM) buildJoinResultWithNulls(left, right *dataframe.DataFrame, keyName string, leftIndices, rightIndices []int, leftNulls, rightNulls bool) *dataframe.DataFrame {
	// Collect all series first, then create DataFrame
	var allSeries []dataframe.Series

	// Gather columns from left frame
	for _, s := range left.Series {
		colName := s.Name()
		dstCol := vm.gatherSeriesWithNulls(s, leftIndices, colName)
		allSeries = append(allSeries, dstCol)
	}

	// Gather columns from right frame (except key column)
	for _, s := range right.Series {
		colName := s.Name()
		if colName == keyName {
			continue
		}
		dstCol := vm.gatherSeriesWithNulls(s, rightIndices, "right_"+colName)
		allSeries = append(allSeries, dstCol)
	}

	return dataframe.NewDataFrame(allSeries...)
}

func (vm *VM) gatherSeries(src dataframe.Series, indices []int, name string) dataframe.Series {
	n := len(indices)
	vals := make([]interface{}, n)
	for i, idx := range indices {
		vals[i] = src.Value(idx)
	}
	s := createSeriesWithValues(src, vals)
	s.Rename(name)
	return s
}

func (vm *VM) gatherSeriesWithNulls(src dataframe.Series, indices []int, name string) dataframe.Series {
	n := len(indices)
	vals := make([]interface{}, n)
	for i, idx := range indices {
		if idx < 0 {
			vals[i] = nil
		} else {
			vals[i] = src.Value(idx)
		}
	}
	s := createSeriesWithValues(src, vals)
	s.Rename(name)
	return s
}

// ===== String Operations =====

func (vm *VM) strLen(s dataframe.Series) dataframe.Series {
	n := getSeriesLength(s)
	data := make([]int64, n)
	for i := 0; i < n; i++ {
		if v, ok := getStringValue(s, i); ok {
			data[i] = int64(len(v))
		}
	}
	return newInt64Series("strlen", data)
}

func (vm *VM) strUpper(s dataframe.Series) dataframe.Series {
	n := getSeriesLength(s)
	data := make([]string, n)
	for i := 0; i < n; i++ {
		if v, ok := getStringValue(s, i); ok {
			data[i] = strings.ToUpper(v)
		}
	}
	return newStringSeries("upper", data)
}

func (vm *VM) strLower(s dataframe.Series) dataframe.Series {
	n := getSeriesLength(s)
	data := make([]string, n)
	for i := 0; i < n; i++ {
		if v, ok := getStringValue(s, i); ok {
			data[i] = strings.ToLower(v)
		}
	}
	return newStringSeries("lower", data)
}

func (vm *VM) strConcat(a, b dataframe.Series) dataframe.Series {
	n := getSeriesLength(a)
	data := make([]string, n)
	for i := 0; i < n; i++ {
		av, _ := getStringValue(a, i)
		bv, _ := getStringValue(b, i)
		data[i] = av + bv
	}
	return newStringSeries("concat", data)
}

func (vm *VM) strContains(s dataframe.Series, pattern string) dataframe.Series {
	n := getSeriesLength(s)
	data := make([]bool, n)
	for i := 0; i < n; i++ {
		if v, ok := getStringValue(s, i); ok {
			data[i] = strings.Contains(v, pattern)
		}
	}
	return newBoolSeries("contains", data)
}

func (vm *VM) strStartsWith(s dataframe.Series, pattern string) dataframe.Series {
	n := getSeriesLength(s)
	data := make([]bool, n)
	for i := 0; i < n; i++ {
		if v, ok := getStringValue(s, i); ok {
			data[i] = strings.HasPrefix(v, pattern)
		}
	}
	return newBoolSeries("startswith", data)
}

func (vm *VM) strEndsWith(s dataframe.Series, pattern string) dataframe.Series {
	n := getSeriesLength(s)
	data := make([]bool, n)
	for i := 0; i < n; i++ {
		if v, ok := getStringValue(s, i); ok {
			data[i] = strings.HasSuffix(v, pattern)
		}
	}
	return newBoolSeries("endswith", data)
}

func (vm *VM) strTrim(s dataframe.Series) dataframe.Series {
	n := getSeriesLength(s)
	data := make([]string, n)
	for i := 0; i < n; i++ {
		if v, ok := getStringValue(s, i); ok {
			data[i] = strings.TrimSpace(v)
		}
	}
	return newStringSeries("trim", data)
}

func (vm *VM) strSplit(s dataframe.Series, delim string) dataframe.Series {
	// Returns first part after split for simplicity
	n := getSeriesLength(s)
	data := make([]string, n)
	for i := 0; i < n; i++ {
		if v, ok := getStringValue(s, i); ok {
			parts := strings.Split(v, delim)
			if len(parts) > 0 {
				data[i] = parts[0]
			}
		}
	}
	return newStringSeries("split", data)
}

func (vm *VM) strReplace(s dataframe.Series, pattern string) dataframe.Series {
	// Pattern format: "old|new"
	parts := strings.SplitN(pattern, "|", 2)
	if len(parts) != 2 {
		return s
	}
	oldStr, newStr := parts[0], parts[1]

	n := getSeriesLength(s)
	data := make([]string, n)
	for i := 0; i < n; i++ {
		if v, ok := getStringValue(s, i); ok {
			data[i] = strings.ReplaceAll(v, oldStr, newStr)
		}
	}
	return newStringSeries("replace", data)
}
