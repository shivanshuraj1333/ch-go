package proto

import (
	"time"
)

// Time represents Time type.
//
// See https://clickhouse.com/docs/en/sql-reference/data-types/time/.
type Time int64

// ToTime converts time.Time to Time.
func ToTime(t time.Time) Time {
	if t.IsZero() {
		return 0
	}
	// Time represents time of day, so we extract hours, minutes, seconds, and nanoseconds
	// and convert to nanoseconds since midnight
	hour := t.Hour()
	minute := t.Minute()
	second := t.Second()
	nsec := t.Nanosecond()
	
	// Convert to nanoseconds since midnight
	totalNsec := int64(hour)*3600*1e9 + int64(minute)*60*1e9 + int64(second)*1e9 + int64(nsec)
	return Time(totalNsec)
}

// Time returns Time as time.Time.
func (t Time) Time() time.Time {
	if t == 0 {
		return time.Time{}
	}
	
	// Convert nanoseconds since midnight to time components
	totalNsec := int64(t)
	hour := int(totalNsec / (3600 * 1e9))
	totalNsec %= 3600 * 1e9
	minute := int(totalNsec / (60 * 1e9))
	totalNsec %= 60 * 1e9
	second := int(totalNsec / 1e9)
	nsec := int(totalNsec % 1e9)
	
	// Create time.Time for today with the extracted time components
	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), hour, minute, second, nsec, time.Local)
}

// Time64 represents Time64 type.
//
// See https://clickhouse.com/docs/en/sql-reference/data-types/time64/.
type Time64 int64

// ToTime64 converts time.Time to Time64.
func ToTime64(t time.Time, p Precision) Time64 {
	if t.IsZero() {
		return 0
	}
	// Time64 represents time of day with precision, so we extract time components
	// and convert to the specified precision since midnight
	hour := t.Hour()
	minute := t.Minute()
	second := t.Second()
	nsec := t.Nanosecond()
	
	// Convert to nanoseconds since midnight
	totalNsec := int64(hour)*3600*1e9 + int64(minute)*60*1e9 + int64(second)*1e9 + int64(nsec)
	
	// Scale to the specified precision
	return Time64(totalNsec / p.Scale())
}

// Time returns Time64 as time.Time.
func (t Time64) Time(p Precision) time.Time {
	if t == 0 {
		return time.Time{}
	}
	
	// Convert scaled value back to nanoseconds since midnight
	totalNsec := int64(t) * p.Scale()
	
	// Convert nanoseconds since midnight to time components
	hour := int(totalNsec / (3600 * 1e9))
	totalNsec %= 3600 * 1e9
	minute := int(totalNsec / (60 * 1e9))
	totalNsec %= 60 * 1e9
	second := int(totalNsec / 1e9)
	nsec := int(totalNsec % 1e9)
	
	// Create time.Time for today with the extracted time components
	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), hour, minute, second, nsec, time.Local)
} 