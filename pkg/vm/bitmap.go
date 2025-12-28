package vm

import (
	"math/bits"
)

// Bitmap represents a bit vector for null tracking and filter masks.
// Bit = 1 means the value is valid (not null).
type Bitmap struct {
	bits   []uint64
	length int
}

// NewBitmap creates a new bitmap with all bits initially clear (0).
func NewBitmap(length int) *Bitmap {
	numWords := (length + 63) / 64
	return &Bitmap{
		bits:   make([]uint64, numWords),
		length: length,
	}
}

// NewAllSetBitmap creates a new bitmap with all bits set (1).
func NewAllSetBitmap(length int) *Bitmap {
	b := NewBitmap(length)
	for i := range b.bits {
		b.bits[i] = ^uint64(0) // All 1s
	}
	// Clear bits beyond length in the last word
	if length > 0 {
		remainder := length % 64
		if remainder != 0 {
			b.bits[len(b.bits)-1] &= (uint64(1) << remainder) - 1
		}
	}
	return b
}

// Len returns the length of the bitmap.
func (b *Bitmap) Len() int {
	return b.length
}

// Set sets the bit at index i to 1.
func (b *Bitmap) Set(i int) {
	if i < 0 || i >= b.length {
		return
	}
	wordIdx := i / 64
	bitIdx := i % 64
	b.bits[wordIdx] |= uint64(1) << bitIdx
}

// Clear sets the bit at index i to 0.
func (b *Bitmap) Clear(i int) {
	if i < 0 || i >= b.length {
		return
	}
	wordIdx := i / 64
	bitIdx := i % 64
	b.bits[wordIdx] &^= uint64(1) << bitIdx
}

// IsSet returns true if the bit at index i is 1.
func (b *Bitmap) IsSet(i int) bool {
	if i < 0 || i >= b.length {
		return false
	}
	wordIdx := i / 64
	bitIdx := i % 64
	return (b.bits[wordIdx] & (uint64(1) << bitIdx)) != 0
}

// PopCount returns the number of bits set to 1.
func (b *Bitmap) PopCount() int {
	count := 0
	for i, word := range b.bits {
		if i == len(b.bits)-1 && b.length%64 != 0 {
			// Mask off bits beyond length in last word
			mask := (uint64(1) << (b.length % 64)) - 1
			count += bits.OnesCount64(word & mask)
		} else {
			count += bits.OnesCount64(word)
		}
	}
	return count
}

// And returns a new bitmap that is the bitwise AND of b and other.
func (b *Bitmap) And(other *Bitmap) *Bitmap {
	length := b.length
	if other.length < length {
		length = other.length
	}
	result := NewBitmap(length)
	for i := range result.bits {
		if i < len(b.bits) && i < len(other.bits) {
			result.bits[i] = b.bits[i] & other.bits[i]
		}
	}
	return result
}

// Or returns a new bitmap that is the bitwise OR of b and other.
func (b *Bitmap) Or(other *Bitmap) *Bitmap {
	length := b.length
	if other.length > length {
		length = other.length
	}
	result := NewBitmap(length)
	for i := range result.bits {
		var bVal, oVal uint64
		if i < len(b.bits) {
			bVal = b.bits[i]
		}
		if i < len(other.bits) {
			oVal = other.bits[i]
		}
		result.bits[i] = bVal | oVal
	}
	return result
}

// Not returns a new bitmap that is the bitwise NOT of b.
func (b *Bitmap) Not() *Bitmap {
	result := NewBitmap(b.length)
	for i := range result.bits {
		result.bits[i] = ^b.bits[i]
	}
	// Clear bits beyond length in the last word
	if b.length > 0 {
		remainder := b.length % 64
		if remainder != 0 {
			result.bits[len(result.bits)-1] &= (uint64(1) << remainder) - 1
		}
	}
	return result
}

// Clone creates a copy of the bitmap.
func (b *Bitmap) Clone() *Bitmap {
	result := NewBitmap(b.length)
	copy(result.bits, b.bits)
	return result
}
