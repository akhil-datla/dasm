package loader

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadJSON_Simple(t *testing.T) {
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "test.json")

	// Create test JSON file
	jsonData := `[
		{"name": "Alice", "age": 30, "score": 95.5},
		{"name": "Bob", "age": 25, "score": 87.0},
		{"name": "Charlie", "age": 35, "score": 92.5}
	]`

	err := os.WriteFile(jsonFile, []byte(jsonData), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	df, err := LoadJSON(jsonFile)
	if err != nil {
		t.Fatalf("LoadJSON failed: %v", err)
	}

	// Check number of rows
	if df.NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", df.NRows())
	}

	// Check number of columns
	if len(df.Series) < 3 {
		t.Errorf("expected at least 3 columns, got %d", len(df.Series))
	}
}

func TestLoadJSON_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "empty.json")

	err := os.WriteFile(jsonFile, []byte(""), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	_, err = LoadJSON(jsonFile)
	if err == nil {
		t.Error("expected error for empty file")
	}
}

func TestLoadJSON_EmptyArray(t *testing.T) {
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "empty_array.json")

	err := os.WriteFile(jsonFile, []byte("[]"), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	_, err = LoadJSON(jsonFile)
	if err == nil {
		t.Error("expected error for empty array")
	}
}

func TestLoadJSON_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "invalid.json")

	err := os.WriteFile(jsonFile, []byte("{invalid json}"), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	_, err = LoadJSON(jsonFile)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestLoadJSON_FileNotFound(t *testing.T) {
	_, err := LoadJSON("/nonexistent/path/file.json")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadJSON_Integers(t *testing.T) {
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "integers.json")

	jsonData := `[
		{"value": 100},
		{"value": 200},
		{"value": 300}
	]`

	err := os.WriteFile(jsonFile, []byte(jsonData), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	df, err := LoadJSON(jsonFile)
	if err != nil {
		t.Fatalf("LoadJSON failed: %v", err)
	}

	if df.NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", df.NRows())
	}
}

func TestLoadJSON_MixedTypes(t *testing.T) {
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "mixed.json")

	jsonData := `[
		{"id": 1, "name": "Item1", "price": 10.5, "active": true},
		{"id": 2, "name": "Item2", "price": 20.0, "active": false}
	]`

	err := os.WriteFile(jsonFile, []byte(jsonData), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	df, err := LoadJSON(jsonFile)
	if err != nil {
		t.Fatalf("LoadJSON failed: %v", err)
	}

	if df.NRows() != 2 {
		t.Errorf("expected 2 rows, got %d", df.NRows())
	}

	if len(df.Series) < 4 {
		t.Errorf("expected at least 4 columns, got %d", len(df.Series))
	}
}

// Parquet tests

func TestLoadParquet_FileNotFound(t *testing.T) {
	_, err := LoadParquet("/nonexistent/path/file.parquet")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadParquet_InvalidFile(t *testing.T) {
	tmpDir := t.TempDir()
	parquetFile := filepath.Join(tmpDir, "invalid.parquet")

	// Create an invalid parquet file (just text)
	err := os.WriteFile(parquetFile, []byte("not a parquet file"), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	_, err = LoadParquet(parquetFile)
	if err == nil {
		t.Error("expected error for invalid parquet file")
	}
}

func TestLoadParquet_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	parquetFile := filepath.Join(tmpDir, "empty.parquet")

	// Create an empty file
	err := os.WriteFile(parquetFile, []byte(""), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	_, err = LoadParquet(parquetFile)
	if err == nil {
		t.Error("expected error for empty parquet file")
	}
}
