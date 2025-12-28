package loader

import (
	"bytes"
	"context"
	"errors"
	"os"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/imports"
)

// JSON-specific errors
var (
	ErrEmptyJSON   = errors.New("empty JSON file")
	ErrInvalidJSON = errors.New("invalid JSON format")
)

// LoadJSON reads a JSON file containing an array of objects and returns a DataFrame.
// The JSON must be in the format: [{"col1": val1, "col2": val2}, ...]
// Column types are inferred automatically.
func LoadJSON(path string) (*dataframe.DataFrame, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, ErrEmptyJSON
	}

	reader := bytes.NewReader(data)
	ctx := context.Background()

	df, err := imports.LoadFromJSON(ctx, reader)
	if err != nil {
		return nil, err
	}

	if df == nil || len(df.Series) == 0 {
		return nil, ErrEmptyJSON
	}

	return df, nil
}
