package vm

import (
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func TestRegisterFile_ScalarRegisters(t *testing.T) {
	rf := NewRegisterFile()

	// Test R registers
	rf.R[0] = 42
	rf.R[15] = 100

	if rf.R[0] != 42 {
		t.Errorf("expected R[0] = 42, got %d", rf.R[0])
	}
	if rf.R[15] != 100 {
		t.Errorf("expected R[15] = 100, got %d", rf.R[15])
	}
}

func TestRegisterFile_FloatRegisters(t *testing.T) {
	rf := NewRegisterFile()

	rf.F[0] = 3.14
	rf.F[15] = 2.718

	if rf.F[0] != 3.14 {
		t.Errorf("expected F[0] = 3.14, got %f", rf.F[0])
	}
	if rf.F[15] != 2.718 {
		t.Errorf("expected F[15] = 2.718, got %f", rf.F[15])
	}
}

func TestRegisterFile_VectorRegisters(t *testing.T) {
	rf := NewRegisterFile()

	series := dataframe.NewSeriesInt64("test", nil, 1, 2, 3, 4, 5)
	rf.V[0] = series

	if rf.V[0] != series {
		t.Error("expected V[0] to hold the series")
	}
	if rf.V[0].NRows() != 5 {
		t.Errorf("expected V[0].NRows() = 5, got %d", rf.V[0].NRows())
	}
}

func TestRegisterFile_Reset(t *testing.T) {
	rf := NewRegisterFile()

	// Set some values
	rf.R[0] = 42
	rf.F[0] = 3.14
	rf.V[0] = dataframe.NewSeriesInt64("test", nil, 1, 2, 3)
	rf.Flags = 0xFF

	// Reset
	rf.Reset()

	// Verify all cleared
	if rf.R[0] != 0 {
		t.Errorf("expected R[0] = 0 after reset, got %d", rf.R[0])
	}
	if rf.F[0] != 0 {
		t.Errorf("expected F[0] = 0 after reset, got %f", rf.F[0])
	}
	if rf.V[0] != nil {
		t.Error("expected V[0] = nil after reset")
	}
	if rf.Flags != 0 {
		t.Errorf("expected Flags = 0 after reset, got %d", rf.Flags)
	}
}

func TestRegisterFile_Constants(t *testing.T) {
	if NumScalarRegs != 16 {
		t.Errorf("expected NumScalarRegs = 16, got %d", NumScalarRegs)
	}
	if NumVectorRegs != 8 {
		t.Errorf("expected NumVectorRegs = 8, got %d", NumVectorRegs)
	}
}
