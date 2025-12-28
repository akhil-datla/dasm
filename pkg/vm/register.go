package vm

import (
	dataframe "github.com/rocketlaunchr/dataframe-go"
)

const (
	NumScalarRegs = 16 // R0-R15: 64-bit scalar registers
	NumVectorRegs = 8  // V0-V7: vector registers (Series)
)

// RegisterFile holds VM state.
type RegisterFile struct {
	R     [NumScalarRegs]int64            // Scalar registers
	V     [NumVectorRegs]dataframe.Series // Vector registers (dataframe-go Series)
	F     [NumScalarRegs]float64          // Floating-point scalars
	Flags uint8                           // Comparison flags
}

// Flag constants
const (
	FlagZero     uint8 = 1 << 0 // Result was zero
	FlagNegative uint8 = 1 << 1 // Result was negative
	FlagOverflow uint8 = 1 << 2 // Overflow occurred
)

// NewRegisterFile creates a new register file with all registers zeroed.
func NewRegisterFile() *RegisterFile {
	return &RegisterFile{}
}

// Reset clears all registers.
func (rf *RegisterFile) Reset() {
	for i := range rf.R {
		rf.R[i] = 0
	}
	for i := range rf.V {
		rf.V[i] = nil
	}
	for i := range rf.F {
		rf.F[i] = 0
	}
	rf.Flags = 0
}
