package vm

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// DataType represents the type of data in a series.
type DataType uint8

const (
	TypeInt64 DataType = iota
	TypeFloat64
	TypeString
	TypeBool
	TypeUnknown
)

// String returns the string representation of the data type.
func (t DataType) String() string {
	switch t {
	case TypeInt64:
		return "int64"
	case TypeFloat64:
		return "float64"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	default:
		return "unknown"
	}
}

// getSeriesType returns the DataType for a dataframe-go Series.
func getSeriesType(s dataframe.Series) DataType {
	if s == nil {
		return TypeUnknown
	}
	switch s.(type) {
	case *dataframe.SeriesInt64:
		return TypeInt64
	case *dataframe.SeriesFloat64:
		return TypeFloat64
	case *dataframe.SeriesString:
		return TypeString
	default:
		// Check if it's a bool series (SeriesGeneric with bool type)
		if sg, ok := s.(*dataframe.SeriesGeneric); ok {
			if _, ok := sg.Value(0).(bool); ok {
				return TypeBool
			}
		}
		return TypeUnknown
	}
}

// getSeriesLength returns the number of rows in a Series.
func getSeriesLength(s dataframe.Series) int {
	if s == nil {
		return 0
	}
	return s.NRows()
}

// getSeriesName returns the name of a Series.
func getSeriesName(s dataframe.Series) string {
	if s == nil {
		return ""
	}
	return s.Name()
}

// isNil checks if the value at index i is nil.
func isNil(s dataframe.Series, i int) bool {
	if s == nil || i < 0 || i >= s.NRows() {
		return true
	}
	return s.Value(i) == nil
}

// getInt64Value extracts an int64 value from a Series at index i.
// Returns (value, ok) where ok is false if nil or wrong type.
func getInt64Value(s dataframe.Series, i int) (int64, bool) {
	if s == nil || i < 0 || i >= s.NRows() {
		return 0, false
	}
	v := s.Value(i)
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

// getFloat64Value extracts a float64 value from a Series at index i.
// Returns (value, ok) where ok is false if nil or wrong type.
func getFloat64Value(s dataframe.Series, i int) (float64, bool) {
	if s == nil || i < 0 || i >= s.NRows() {
		return 0, false
	}
	v := s.Value(i)
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	default:
		return 0, false
	}
}

// getStringValue extracts a string value from a Series at index i.
// Returns (value, ok) where ok is false if nil or wrong type.
func getStringValue(s dataframe.Series, i int) (string, bool) {
	if s == nil || i < 0 || i >= s.NRows() {
		return "", false
	}
	v := s.Value(i)
	if v == nil {
		return "", false
	}
	if str, ok := v.(string); ok {
		return str, true
	}
	return "", false
}

// getBoolValue extracts a bool value from a Series at index i.
// Returns (value, ok) where ok is false if nil or wrong type.
func getBoolValue(s dataframe.Series, i int) (bool, bool) {
	if s == nil || i < 0 || i >= s.NRows() {
		return false, false
	}
	v := s.Value(i)
	if v == nil {
		return false, false
	}
	if b, ok := v.(bool); ok {
		return b, true
	}
	return false, false
}

// newInt64Series creates a new SeriesInt64 with the given name and data.
func newInt64Series(name string, data []int64) *dataframe.SeriesInt64 {
	// Convert []int64 to []interface{} for the constructor
	vals := make([]interface{}, len(data))
	for i, v := range data {
		vals[i] = v
	}
	return dataframe.NewSeriesInt64(name, nil, vals...)
}

// newFloat64Series creates a new SeriesFloat64 with the given name and data.
func newFloat64Series(name string, data []float64) *dataframe.SeriesFloat64 {
	vals := make([]interface{}, len(data))
	for i, v := range data {
		vals[i] = v
	}
	return dataframe.NewSeriesFloat64(name, nil, vals...)
}

// newStringSeries creates a new SeriesString with the given name and data.
func newStringSeries(name string, data []string) *dataframe.SeriesString {
	vals := make([]interface{}, len(data))
	for i, v := range data {
		vals[i] = v
	}
	return dataframe.NewSeriesString(name, nil, vals...)
}

// newBoolSeries creates a new SeriesGeneric with bool values.
func newBoolSeries(name string, data []bool) dataframe.Series {
	vals := make([]interface{}, len(data))
	for i, v := range data {
		vals[i] = v
	}
	return dataframe.NewSeriesGeneric(name, false, nil, vals...)
}

// filterSeries applies a bitmap filter to a Series and returns a new filtered Series.
func filterSeries(s dataframe.Series, mask *Bitmap) dataframe.Series {
	if s == nil || mask == nil {
		return s
	}

	count := mask.PopCount()
	if count == 0 {
		// Return empty series of same type
		return createEmptySeries(s)
	}

	// Build filtered values
	vals := make([]interface{}, 0, count)
	n := s.NRows()
	for i := 0; i < n && len(vals) < count; i++ {
		if mask.IsSet(i) {
			vals = append(vals, s.Value(i))
		}
	}

	return createSeriesWithValues(s, vals)
}

// createEmptySeries creates an empty series of the same type as s.
func createEmptySeries(s dataframe.Series) dataframe.Series {
	name := s.Name()
	switch s.(type) {
	case *dataframe.SeriesInt64:
		return dataframe.NewSeriesInt64(name, nil)
	case *dataframe.SeriesFloat64:
		return dataframe.NewSeriesFloat64(name, nil)
	case *dataframe.SeriesString:
		return dataframe.NewSeriesString(name, nil)
	default:
		// For bool or other generic types
		if sg, ok := s.(*dataframe.SeriesGeneric); ok {
			return dataframe.NewSeriesGeneric(name, sg.Value(0), nil)
		}
		return dataframe.NewSeriesGeneric(name, nil, nil)
	}
}

// createSeriesWithValues creates a new series of the same type as s with the given values.
func createSeriesWithValues(s dataframe.Series, vals []interface{}) dataframe.Series {
	name := s.Name()
	switch s.(type) {
	case *dataframe.SeriesInt64:
		return dataframe.NewSeriesInt64(name, nil, vals...)
	case *dataframe.SeriesFloat64:
		return dataframe.NewSeriesFloat64(name, nil, vals...)
	case *dataframe.SeriesString:
		return dataframe.NewSeriesString(name, nil, vals...)
	default:
		// For bool or other generic types
		if sg, ok := s.(*dataframe.SeriesGeneric); ok {
			concreteType := sg.Value(0)
			return dataframe.NewSeriesGeneric(name, concreteType, nil, vals...)
		}
		return dataframe.NewSeriesGeneric(name, nil, nil, vals...)
	}
}

// cloneSeries creates a copy of a series.
func cloneSeries(s dataframe.Series) dataframe.Series {
	if s == nil {
		return nil
	}
	return s.Copy()
}

// seriesSum returns the sum of numeric values in a Series.
func seriesSum(_ context.Context, s dataframe.Series) (float64, error) {
	if s == nil {
		return 0, nil
	}
	n := s.NRows()
	if n == 0 {
		return 0, nil
	}

	var sum float64
	switch ss := s.(type) {
	case *dataframe.SeriesInt64:
		for i := 0; i < n; i++ {
			v := ss.Value(i)
			if v != nil {
				sum += float64(v.(int64))
			}
		}
	case *dataframe.SeriesFloat64:
		for i := 0; i < n; i++ {
			v := ss.Value(i)
			if v != nil {
				sum += v.(float64)
			}
		}
	default:
		// Try to extract float values
		for i := 0; i < n; i++ {
			if val, ok := getFloat64Value(s, i); ok {
				sum += val
			}
		}
	}
	return sum, nil
}

// seriesMean returns the mean of numeric values in a Series.
func seriesMean(_ context.Context, s dataframe.Series) (float64, error) {
	if s == nil {
		return 0, nil
	}
	n := s.NRows()
	if n == 0 {
		return 0, nil
	}

	sum, err := seriesSum(context.TODO(), s)
	if err != nil {
		return 0, err
	}

	// Count non-nil values
	count := 0
	for i := 0; i < n; i++ {
		if s.Value(i) != nil {
			count++
		}
	}
	if count == 0 {
		return 0, nil
	}
	return sum / float64(count), nil
}

// getDataFrameColumn retrieves a Series from a DataFrame by name.
func getDataFrameColumn(df *dataframe.DataFrame, name string) (dataframe.Series, bool) {
	if df == nil {
		return nil, false
	}
	idx, err := df.NameToColumn(name)
	if err != nil {
		return nil, false
	}
	return df.Series[idx], true
}

// getDataFrameColumnNames returns the names of all Series in a DataFrame.
func getDataFrameColumnNames(df *dataframe.DataFrame) []string {
	if df == nil {
		return nil
	}
	names := make([]string, len(df.Series))
	for i, s := range df.Series {
		names[i] = s.Name()
	}
	return names
}

// getDataFrameLength returns the number of rows in a DataFrame.
func getDataFrameLength(df *dataframe.DataFrame) int {
	if df == nil || len(df.Series) == 0 {
		return 0
	}
	return df.Series[0].NRows()
}

// newEmptyDataFrame creates a new empty DataFrame.
func newEmptyDataFrame() *dataframe.DataFrame {
	return dataframe.NewDataFrame()
}

// addColumnToDataFrame adds a Series to a DataFrame.
func addColumnToDataFrame(df *dataframe.DataFrame, s dataframe.Series) error {
	if df == nil || s == nil {
		return nil
	}
	return df.AddSeries(s, nil)
}
