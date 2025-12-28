// Package testutil provides testing utilities for DASM tests.
package testutil

import (
	"os"
	"path/filepath"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// TempCSV creates a temporary CSV file and returns its path.
// The file is automatically cleaned up when the test finishes.
func TempCSV(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp CSV: %v", err)
	}
	return path
}

// TempFile creates a temporary file with the given content and extension.
func TempFile(t *testing.T, content, ext string) string {
	t.Helper()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test"+ext)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return path
}

// SalesCSV returns standard test CSV content for sales data.
func SalesCSV() string {
	return `price,quantity,category
10.5,5,A
20.0,15,B
5.0,3,A
30.0,20,C
15.0,8,B`
}

// SimpleCSV returns minimal test CSV content.
func SimpleCSV() string {
	return `a,b
1,2
3,4
5,6`
}

// MakeSalesFrame creates a standard sales test frame.
func MakeSalesFrame() *dataframe.DataFrame {
	return dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.5, 20.0, 5.0, 30.0, 15.0),
		dataframe.NewSeriesInt64("quantity", nil, 5, 15, 3, 20, 8),
		dataframe.NewSeriesString("category", nil, "A", "B", "A", "C", "B"),
	)
}

// MakeSimpleFrame creates a minimal test frame with int64 columns.
func MakeSimpleFrame() *dataframe.DataFrame {
	return dataframe.NewDataFrame(
		dataframe.NewSeriesInt64("a", nil, 1, 3, 5),
		dataframe.NewSeriesInt64("b", nil, 2, 4, 6),
	)
}

// AssertFloat64Near checks if two float64 values are approximately equal.
func AssertFloat64Near(t *testing.T, expected, actual, tolerance float64) {
	t.Helper()
	if actual < expected-tolerance || actual > expected+tolerance {
		t.Errorf("expected %.6f, got %.6f (tolerance: %.6f)", expected, actual, tolerance)
	}
}

// AssertInt64Equal checks if two int64 values are equal.
func AssertInt64Equal(t *testing.T, expected, actual int64) {
	t.Helper()
	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
	}
}
