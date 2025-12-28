package loader

import (
	"context"
	"errors"
	"os"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/imports"
)

// Error definitions
var (
	ErrEmptyFile     = errors.New("empty CSV file")
	ErrNoHeader      = errors.New("CSV file has no header")
	ErrInvalidFormat = errors.New("invalid CSV format")
)

// LoadCSV reads a CSV file and returns a DataFrame using dataframe-go.
// - First row is header (column names)
// - Auto-detects column types (int64, float64, bool, string)
// - Empty values become nil
func LoadCSV(path string) (*dataframe.DataFrame, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	ctx := context.Background()
	df, err := imports.LoadFromCSV(ctx, file, imports.CSVLoadOptions{
		// Auto-detect types (default behavior)
		InferDataTypes: true,
	})
	if err != nil {
		return nil, err
	}

	if df == nil || len(df.Series) == 0 {
		return nil, ErrEmptyFile
	}

	return df, nil
}
