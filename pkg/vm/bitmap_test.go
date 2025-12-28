package vm

import (
	"testing"
)

func TestBitmap_NewBitmap(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"empty", 0},
		{"small", 10},
		{"exactly 64", 64},
		{"over 64", 100},
		{"large", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBitmap(tt.length)
			if b.Len() != tt.length {
				t.Errorf("expected length %d, got %d", tt.length, b.Len())
			}
		})
	}
}

func TestBitmap_SetClear(t *testing.T) {
	b := NewBitmap(100)

	// Initially all bits should be clear
	for i := 0; i < 100; i++ {
		if b.IsSet(i) {
			t.Errorf("bit %d should be clear initially", i)
		}
	}

	// Set some bits
	b.Set(0)
	b.Set(63)
	b.Set(64)
	b.Set(99)

	if !b.IsSet(0) {
		t.Error("bit 0 should be set")
	}
	if !b.IsSet(63) {
		t.Error("bit 63 should be set")
	}
	if !b.IsSet(64) {
		t.Error("bit 64 should be set")
	}
	if !b.IsSet(99) {
		t.Error("bit 99 should be set")
	}

	// Clear a bit
	b.Clear(63)
	if b.IsSet(63) {
		t.Error("bit 63 should be clear after Clear()")
	}
}

func TestBitmap_PopCount(t *testing.T) {
	b := NewBitmap(100)

	if b.PopCount() != 0 {
		t.Errorf("expected pop count 0, got %d", b.PopCount())
	}

	b.Set(0)
	b.Set(50)
	b.Set(99)

	if b.PopCount() != 3 {
		t.Errorf("expected pop count 3, got %d", b.PopCount())
	}

	// Set all bits
	b = NewBitmap(100)
	for i := 0; i < 100; i++ {
		b.Set(i)
	}
	if b.PopCount() != 100 {
		t.Errorf("expected pop count 100, got %d", b.PopCount())
	}
}

func TestBitmap_And(t *testing.T) {
	a := NewBitmap(10)
	b := NewBitmap(10)

	a.Set(0)
	a.Set(1)
	a.Set(2)

	b.Set(1)
	b.Set(2)
	b.Set(3)

	result := a.And(b)

	if result.IsSet(0) {
		t.Error("bit 0 should not be set in AND result")
	}
	if !result.IsSet(1) {
		t.Error("bit 1 should be set in AND result")
	}
	if !result.IsSet(2) {
		t.Error("bit 2 should be set in AND result")
	}
	if result.IsSet(3) {
		t.Error("bit 3 should not be set in AND result")
	}
}

func TestBitmap_Or(t *testing.T) {
	a := NewBitmap(10)
	b := NewBitmap(10)

	a.Set(0)
	a.Set(1)

	b.Set(1)
	b.Set(2)

	result := a.Or(b)

	if !result.IsSet(0) {
		t.Error("bit 0 should be set in OR result")
	}
	if !result.IsSet(1) {
		t.Error("bit 1 should be set in OR result")
	}
	if !result.IsSet(2) {
		t.Error("bit 2 should be set in OR result")
	}
	if result.IsSet(3) {
		t.Error("bit 3 should not be set in OR result")
	}
}

func TestBitmap_Not(t *testing.T) {
	b := NewBitmap(10)
	b.Set(0)
	b.Set(5)

	result := b.Not()

	if result.IsSet(0) {
		t.Error("bit 0 should not be set in NOT result")
	}
	if result.IsSet(5) {
		t.Error("bit 5 should not be set in NOT result")
	}
	if !result.IsSet(1) {
		t.Error("bit 1 should be set in NOT result")
	}
	if !result.IsSet(9) {
		t.Error("bit 9 should be set in NOT result")
	}
}

func TestBitmap_AllSet(t *testing.T) {
	b := NewBitmap(10)
	for i := 0; i < 10; i++ {
		b.Set(i)
	}

	all := NewAllSetBitmap(10)
	if all.PopCount() != 10 {
		t.Errorf("expected pop count 10, got %d", all.PopCount())
	}

	for i := 0; i < 10; i++ {
		if !all.IsSet(i) {
			t.Errorf("bit %d should be set", i)
		}
	}
}

func TestBitmap_Clone(t *testing.T) {
	b := NewBitmap(100)
	b.Set(0)
	b.Set(50)
	b.Set(99)

	clone := b.Clone()

	// Verify clone has same values
	if !clone.IsSet(0) || !clone.IsSet(50) || !clone.IsSet(99) {
		t.Error("clone should have same set bits")
	}

	// Modify original, clone should be unaffected
	b.Clear(0)
	if !clone.IsSet(0) {
		t.Error("clone should be independent of original")
	}
}
