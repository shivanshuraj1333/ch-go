package proto

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTime_Conversion(t *testing.T) {
	// Test Time conversion
	now := time.Date(2023, 1, 1, 14, 30, 45, 123456789, time.UTC)

	// Convert to Time
	t1 := ToTime(now)

	// Convert back to time.Time
	result := t1.Time()

	// Check that the time components match (ignoring date)
	assert.Equal(t, now.Hour(), result.Hour())
	assert.Equal(t, now.Minute(), result.Minute())
	assert.Equal(t, now.Second(), result.Second())
	assert.Equal(t, now.Nanosecond(), result.Nanosecond())
}

func TestTime64_Conversion(t *testing.T) {
	// Test Time64 conversion with different precisions
	now := time.Date(2023, 1, 1, 14, 30, 45, 123456789, time.UTC)

	testCases := []Precision{
		PrecisionSecond,
		PrecisionMilli,
		PrecisionMicro,
		PrecisionNano,
	}

	for _, precision := range testCases {
		t.Run(fmt.Sprintf("precision_%d", precision), func(t *testing.T) {
			// Convert to Time64
			t64 := ToTime64(now, precision)

			// Convert back to time.Time
			result := t64.Time(precision)

			// Check that the time components match (ignoring date)
			assert.Equal(t, now.Hour(), result.Hour())
			assert.Equal(t, now.Minute(), result.Minute())
			assert.Equal(t, now.Second(), result.Second())

			// For nanosecond precision, check nanoseconds too
			if precision == PrecisionNano {
				assert.Equal(t, now.Nanosecond(), result.Nanosecond())
			}
		})
	}
}

func TestColTime_Basic(t *testing.T) {
	col := &ColTime{}

	// Test appending values
	now := time.Date(2023, 1, 1, 14, 30, 45, 123456789, time.UTC)
	col.Append(now)

	assert.Equal(t, 1, col.Rows())
	assert.Equal(t, ColumnTypeTime, col.Type())

	// Test retrieving value
	result := col.Row(0)
	assert.Equal(t, now.Hour(), result.Hour())
	assert.Equal(t, now.Minute(), result.Minute())
	assert.Equal(t, now.Second(), result.Second())
}

func TestColTime64_Basic(t *testing.T) {
	col := &ColTime64{}
	col.WithPrecision(PrecisionNano)

	// Test appending values
	now := time.Date(2023, 1, 1, 14, 30, 45, 123456789, time.UTC)
	col.Append(now)

	assert.Equal(t, 1, col.Rows())
	assert.Equal(t, ColumnTypeTime64.With("9"), col.Type())

	// Test retrieving value
	result := col.Row(0)
	assert.Equal(t, now.Hour(), result.Hour())
	assert.Equal(t, now.Minute(), result.Minute())
	assert.Equal(t, now.Second(), result.Second())
	assert.Equal(t, now.Nanosecond(), result.Nanosecond())
}
