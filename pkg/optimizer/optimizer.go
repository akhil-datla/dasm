package optimizer

import (
	"github.com/akhildatla/dasm/pkg/vm"
)

// Optimizer applies optimizations to a compiled program.
type Optimizer struct {
	enableConstantFolding   bool
	enablePredicatePushdown bool
	enableProjectionPruning bool
	enableDeadCode          bool
}

// Option is a functional option for the Optimizer.
type Option func(*Optimizer)

// WithConstantFolding enables constant folding optimization.
func WithConstantFolding() Option {
	return func(o *Optimizer) {
		o.enableConstantFolding = true
	}
}

// WithPredicatePushdown enables predicate pushdown optimization.
func WithPredicatePushdown() Option {
	return func(o *Optimizer) {
		o.enablePredicatePushdown = true
	}
}

// WithProjectionPruning enables projection pruning optimization.
func WithProjectionPruning() Option {
	return func(o *Optimizer) {
		o.enableProjectionPruning = true
	}
}

// WithAllOptimizations enables all optimizations.
func WithAllOptimizations() Option {
	return func(o *Optimizer) {
		o.enableConstantFolding = true
		o.enablePredicatePushdown = true
		o.enableProjectionPruning = true
		o.enableDeadCode = true
	}
}

// New creates a new Optimizer with the given options.
func New(opts ...Option) *Optimizer {
	opt := &Optimizer{}
	for _, o := range opts {
		o(opt)
	}
	return opt
}

// Optimize applies enabled optimizations to the program.
func (o *Optimizer) Optimize(program *vm.Program) *vm.Program {
	result := program

	if o.enableConstantFolding {
		result = o.constantFolding(result)
	}

	if o.enablePredicatePushdown {
		result = o.predicatePushdown(result)
	}

	if o.enableProjectionPruning {
		result = o.projectionPruning(result)
	}

	if o.enableDeadCode {
		result = o.deadCodeElimination(result)
	}

	return result
}
