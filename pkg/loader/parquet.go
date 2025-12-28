package loader

import (
	"context"
	"errors"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/imports"
	"github.com/xitongsys/parquet-go-source/local"
)

// Parquet-specific errors
var (
	ErrEmptyParquet   = errors.New("empty Parquet file")
	ErrInvalidParquet = errors.New("invalid Parquet format")
)

// LoadParquet reads a Parquet file and returns a DataFrame.
// Uses the dataframe-go imports package with parquet-go backend.
func LoadParquet(path string) (*dataframe.DataFrame, error) {
	// Open the parquet file using local file reader
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	ctx := context.Background()

	df, err := imports.LoadFromParquet(ctx, fr)
	if err != nil {
		return nil, err
	}

	if df == nil || len(df.Series) == 0 {
		return nil, ErrEmptyParquet
	}

	return df, nil
}
