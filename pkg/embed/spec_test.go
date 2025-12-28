package embed

import (
	"math"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// These tests use EXACT examples from CLAUDE.md as literal test cases.
// Per master-go-architect.md: "Specifications are the source of truth -
// test the spec, not the implementation; verify exact formats"

// TestSpec_SumPrices tests CLAUDE.md example: examples/sum_prices.dasm
// Expected: Sum all prices = 10.5 + 20.0 + 5.0 + 30.0 = 65.5
func TestSpec_SumPrices(t *testing.T) {
	// Uses same data as testdata/sales.csv but via LOAD_FRAME for test portability
	// Original CLAUDE.md code:
	//   LOAD_CSV      R0, "testdata/sales.csv"
	//   SELECT_COL    V0, R0, "price"
	//   REDUCE_SUM_F  F0, V0
	//   HALT_F        F0

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.5, 20.0, 5.0, 30.0),
		dataframe.NewSeriesFloat64("quantity", nil, 5, 15, 3, 20),
	)

	code := `
LOAD_FRAME    R0, "sales"
SELECT_COL    V0, R0, "price"
REDUCE_SUM_F  F0, V0
HALT_F        F0
`
	result, err := ExecuteWithFrames(code, map[string]*dataframe.DataFrame{"sales": frame})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := 65.5 // 10.5 + 20.0 + 5.0 + 30.0
	if val, ok := result.(float64); !ok || math.Abs(val-expected) > 0.001 {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// TestSpec_FilterAggregate tests CLAUDE.md example: examples/filter_aggregate.dasm
// Expected: Average price where quantity > 10 = mean(20.0, 30.0) = 25.0
func TestSpec_FilterAggregate(t *testing.T) {
	// Uses same data as testdata/sales.csv but via LOAD_FRAME for test portability
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.5, 20.0, 5.0, 30.0),
		dataframe.NewSeriesFloat64("quantity", nil, 5, 15, 3, 20),
	)

	code := `
LOAD_FRAME    R0, "sales"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
LOAD_CONST    R1, 10
BROADCAST     V2, R1, V1
CMP_GT        V3, V1, V2
FILTER        V4, V0, V3
REDUCE_MEAN   F0, V4
HALT_F        F0
`
	result, err := ExecuteWithFrames(code, map[string]*dataframe.DataFrame{"sales": frame})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := 25.0 // mean(20.0, 30.0) where quantity > 10
	if val, ok := result.(float64); !ok || math.Abs(val-expected) > 0.001 {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// TestSpec_ComputedColumn tests CLAUDE.md example: examples/computed_column.dasm
// Expected: Sum of price * quantity = 52.5 + 300 + 15 + 600 = 967.5
func TestSpec_ComputedColumn(t *testing.T) {
	// Uses same data as testdata/sales.csv but via LOAD_FRAME for test portability
	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.5, 20.0, 5.0, 30.0),
		dataframe.NewSeriesFloat64("quantity", nil, 5, 15, 3, 20),
	)

	code := `
LOAD_FRAME    R0, "sales"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
VEC_MUL_F     V2, V0, V1
REDUCE_SUM_F  F0, V2
HALT_F        F0
`
	result, err := ExecuteWithFrames(code, map[string]*dataframe.DataFrame{"sales": frame})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expected := 967.5 // 10.5*5 + 20.0*15 + 5.0*3 + 30.0*20
	if val, ok := result.(float64); !ok || math.Abs(val-expected) > 0.001 {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// TestSpec_EmbeddingAPI tests CLAUDE.md example: Go embedding API
// This verifies the exact embedding pattern from the spec works
func TestSpec_EmbeddingAPI(t *testing.T) {
	// From CLAUDE.md Phase 1 Success Criteria #2
	// The spec shows LOAD_FRAME with predeclared frames

	frame := dataframe.NewDataFrame(
		dataframe.NewSeriesFloat64("price", nil, 10.5, 20.0, 5.0, 30.0),
		dataframe.NewSeriesFloat64("quantity", nil, 5, 15, 3, 20),
	)

	code := `
LOAD_FRAME    R0, "data"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
LOAD_CONST    R1, 10
BROADCAST     V2, R1, V1
CMP_GT        V3, V1, V2
FILTER        V4, V0, V3
REDUCE_SUM_F  F0, V4
HALT_F        F0
`
	result, err := ExecuteWithFrames(code, map[string]*dataframe.DataFrame{"data": frame})
	if err != nil {
		t.Fatalf("ExecuteWithFrames failed: %v", err)
	}

	expected := 50.0 // sum of prices where quantity > 10 = 20.0 + 30.0
	if val, ok := result.(float64); !ok || math.Abs(val-expected) > 0.001 {
		t.Errorf("expected %v, got %v", expected, result)
	}
}
