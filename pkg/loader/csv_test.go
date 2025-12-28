package loader

import (
	"os"
	"path/filepath"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func TestLoadCSV_Basic(t *testing.T) {
	csvData := `id,name,value
1,alice,100
2,bob,200
3,charlie,300`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	// Check number of rows
	if df.Series[0].NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", df.Series[0].NRows())
	}

	// Check number of columns
	if len(df.Series) != 3 {
		t.Errorf("expected 3 columns, got %d", len(df.Series))
	}

	// Check column names
	names := make([]string, len(df.Series))
	for i, s := range df.Series {
		names[i] = s.Name()
	}
	if names[0] != "id" || names[1] != "name" || names[2] != "value" {
		t.Errorf("expected [id, name, value], got %v", names)
	}
}

func TestLoadCSV_TypeDetection_Int64(t *testing.T) {
	csvData := `id,count
1,100
2,200
3,300`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	// Find id column
	idIdx, err := df.NameToColumn("id")
	if err != nil {
		t.Fatal("expected 'id' column")
	}

	idCol := df.Series[idIdx]
	if _, ok := idCol.(*dataframe.SeriesInt64); !ok {
		t.Errorf("expected id column type SeriesInt64, got %T", idCol)
	}
}

func TestLoadCSV_TypeDetection_Float64(t *testing.T) {
	csvData := `price,discount
10.5,0.1
20.0,0.2
15.75,0.15`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	priceIdx, err := df.NameToColumn("price")
	if err != nil {
		t.Fatal("expected 'price' column")
	}

	priceCol := df.Series[priceIdx]
	if _, ok := priceCol.(*dataframe.SeriesFloat64); !ok {
		t.Errorf("expected price column type SeriesFloat64, got %T", priceCol)
	}

	// Check first value
	val := priceCol.Value(0)
	if v, ok := val.(float64); !ok || v != 10.5 {
		t.Errorf("expected price[0] = 10.5, got %v", val)
	}
}

func TestLoadCSV_TypeDetection_String(t *testing.T) {
	csvData := `id,name
1,alice
2,bob
3,charlie`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	nameIdx, err := df.NameToColumn("name")
	if err != nil {
		t.Fatal("expected 'name' column")
	}

	nameCol := df.Series[nameIdx]
	if _, ok := nameCol.(*dataframe.SeriesString); !ok {
		t.Errorf("expected name column type SeriesString, got %T", nameCol)
	}

	val := nameCol.Value(0)
	if v, ok := val.(string); !ok || v != "alice" {
		t.Errorf("expected name[0] = 'alice', got %v", val)
	}
}

func TestLoadCSV_EmptyFile(t *testing.T) {
	csvData := ``

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	_, err := LoadCSV(csvPath)
	if err == nil {
		t.Error("expected error for empty file")
	}
}

func TestLoadCSV_HeaderOnly(t *testing.T) {
	csvData := `id,name,value`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	// Check number of columns
	if len(df.Series) != 3 {
		t.Errorf("expected 3 columns, got %d", len(df.Series))
	}

	// Check number of rows (should be 0)
	if df.Series[0].NRows() != 0 {
		t.Errorf("expected 0 rows, got %d", df.Series[0].NRows())
	}
}

func TestLoadCSV_FileNotFound(t *testing.T) {
	_, err := LoadCSV("/nonexistent/file.csv")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadCSV_SalesData(t *testing.T) {
	// Test with the sales.csv format used in integration tests
	csvData := `price,quantity
10.5,5
20.0,15
5.0,3
30.0,20`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "sales.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	if df.Series[0].NRows() != 4 {
		t.Errorf("expected 4 rows, got %d", df.Series[0].NRows())
	}

	priceIdx, _ := df.NameToColumn("price")
	priceCol := df.Series[priceIdx]
	if _, ok := priceCol.(*dataframe.SeriesFloat64); !ok {
		t.Errorf("expected price column type SeriesFloat64, got %T", priceCol)
	}

	val := priceCol.Value(0)
	if v, ok := val.(float64); !ok || v != 10.5 {
		t.Errorf("expected price[0] = 10.5, got %v", val)
	}

	qtyIdx, _ := df.NameToColumn("quantity")
	qtyCol := df.Series[qtyIdx]
	if _, ok := qtyCol.(*dataframe.SeriesInt64); !ok {
		t.Errorf("expected quantity column type SeriesInt64, got %T", qtyCol)
	}
}

func TestLoadCSV_QuotedStrings(t *testing.T) {
	csvData := `name,description
"Alice","A person with comma, in name"
"Bob","Normal description"
"Charlie","Quote ""inside"" text"`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	descIdx, _ := df.NameToColumn("description")
	descCol := df.Series[descIdx]

	val0 := descCol.Value(0)
	if v, ok := val0.(string); !ok || v != "A person with comma, in name" {
		t.Errorf("expected comma to be preserved, got %q", val0)
	}

	val2 := descCol.Value(2)
	if v, ok := val2.(string); !ok || v != `Quote "inside" text` {
		t.Errorf("expected escaped quotes, got %q", val2)
	}
}

func TestLoadCSV_NegativeNumbers(t *testing.T) {
	csvData := `value,amount
-100,-50.5
200,100.0
-1,0`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	valueIdx, _ := df.NameToColumn("value")
	valueCol := df.Series[valueIdx]
	if _, ok := valueCol.(*dataframe.SeriesInt64); !ok {
		t.Errorf("expected int64 type, got %T", valueCol)
	}

	val := valueCol.Value(0)
	if v, ok := val.(int64); !ok || v != -100 {
		t.Errorf("expected -100, got %v", val)
	}

	amountIdx, _ := df.NameToColumn("amount")
	amountCol := df.Series[amountIdx]
	if _, ok := amountCol.(*dataframe.SeriesFloat64); !ok {
		t.Errorf("expected float64 type, got %T", amountCol)
	}

	amtVal := amountCol.Value(0)
	if v, ok := amtVal.(float64); !ok || v != -50.5 {
		t.Errorf("expected -50.5, got %v", amtVal)
	}
}

func TestLoadCSV_SingleColumn(t *testing.T) {
	csvData := `value
1
2
3`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	if len(df.Series) != 1 {
		t.Errorf("expected 1 column, got %d", len(df.Series))
	}
	if df.Series[0].NRows() != 3 {
		t.Errorf("expected 3 rows, got %d", df.Series[0].NRows())
	}
}

func TestLoadCSV_SingleRow(t *testing.T) {
	csvData := `a,b,c
1,2,3`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	if df.Series[0].NRows() != 1 {
		t.Errorf("expected 1 row, got %d", df.Series[0].NRows())
	}
}

func TestLoadCSV_Unicode(t *testing.T) {
	csvData := `name,emoji
日本語,🎉
中文,💻
한국어,🌍`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	nameIdx, _ := df.NameToColumn("name")
	nameCol := df.Series[nameIdx]
	if v, ok := nameCol.Value(0).(string); !ok || v != "日本語" {
		t.Errorf("expected 日本語, got %v", nameCol.Value(0))
	}

	emojiIdx, _ := df.NameToColumn("emoji")
	emojiCol := df.Series[emojiIdx]
	if v, ok := emojiCol.Value(0).(string); !ok || v != "🎉" {
		t.Errorf("expected 🎉, got %v", emojiCol.Value(0))
	}
}

func TestLoadCSV_ManyColumns(t *testing.T) {
	// Test with many columns
	csvData := `a,b,c,d,e,f,g,h,i,j
1,2,3,4,5,6,7,8,9,10
11,12,13,14,15,16,17,18,19,20`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	if len(df.Series) != 10 {
		t.Errorf("expected 10 columns, got %d", len(df.Series))
	}

	jIdx, _ := df.NameToColumn("j")
	jCol := df.Series[jIdx]
	if v, ok := jCol.Value(1).(int64); !ok || v != 20 {
		t.Errorf("expected j[1] = 20, got %v", jCol.Value(1))
	}
}

func TestLoadCSV_ManyRows(t *testing.T) {
	// Build CSV with many rows
	var lines []string
	lines = append(lines, "id,value")
	for i := 0; i < 1000; i++ {
		lines = append(lines, "1,100")
	}
	csvData := ""
	for i, line := range lines {
		if i > 0 {
			csvData += "\n"
		}
		csvData += line
	}

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	df, err := LoadCSV(csvPath)
	if err != nil {
		t.Fatalf("LoadCSV failed: %v", err)
	}

	if df.Series[0].NRows() != 1000 {
		t.Errorf("expected 1000 rows, got %d", df.Series[0].NRows())
	}
}

func TestLoadCSV_ErrorDefinitions(t *testing.T) {
	// Verify error variables exist and are not nil
	if ErrEmptyFile == nil {
		t.Error("ErrEmptyFile should not be nil")
	}
	if ErrNoHeader == nil {
		t.Error("ErrNoHeader should not be nil")
	}
	if ErrInvalidFormat == nil {
		t.Error("ErrInvalidFormat should not be nil")
	}
}
