package proto

import (
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
)

var (
	_ ColumnOf[time.Time] = (*ColTime64)(nil)
	_ Inferable           = (*ColTime64)(nil)
	_ Column              = (*ColTime64)(nil)
)

// ColTime64 implements ColumnOf[time.Time].
//
// If Precision is not set, Append and Row() panics.
// Use ColTime64Raw to work with raw Time64 values.
type ColTime64 struct {
	Data         []Time64
	Precision    Precision
	PrecisionSet bool
}

func (c *ColTime64) WithPrecision(p Precision) *ColTime64 {
	c.Precision = p
	c.PrecisionSet = true
	return c
}

func (c ColTime64) Rows() int {
	return len(c.Data)
}

func (c *ColTime64) Reset() {
	c.Data = c.Data[:0]
}

func (c ColTime64) Type() ColumnType {
	var elems []string
	if p := c.Precision; c.PrecisionSet {
		elems = append(elems, strconv.Itoa(int(p)))
	}
	return ColumnTypeTime64.With(elems...)
}

func (c *ColTime64) Infer(t ColumnType) error {
	elem := string(t.Elem())
	if elem == "" {
		return errors.Errorf("invalid Time64: no elements in %q", t)
	}
	pStr := strings.Trim(elem, `' `)
	n, err := strconv.ParseUint(pStr, 10, 8)
	if err != nil {
		return errors.Wrap(err, "parse precision")
	}
	p := Precision(n)
	if !p.Valid() {
		return errors.Errorf("precision %d is invalid", n)
	}
	c.Precision = p
	c.PrecisionSet = true
	return nil
}

func (c ColTime64) Row(i int) time.Time {
	if !c.PrecisionSet {
		panic("Time64: no precision set")
	}
	return c.Data[i].Time(c.Precision)
}

func (c *ColTime64) AppendRaw(v Time64) {
	c.Data = append(c.Data, v)
}

func (c *ColTime64) Append(v time.Time) {
	if !c.PrecisionSet {
		panic("Time64: no precision set")
	}
	c.AppendRaw(ToTime64(v, c.Precision))
}

func (c *ColTime64) AppendArr(v []time.Time) {
	if !c.PrecisionSet {
		panic("Time64: no precision set")
	}

	for _, item := range v {
		c.AppendRaw(ToTime64(item, c.Precision))
	}
}

// Raw version of ColTime64 for ColumnOf[Time64].
func (c ColTime64) Raw() *ColTime64Raw {
	return &ColTime64Raw{ColTime64: c}
}

func (c *ColTime64) Nullable() *ColNullable[time.Time] {
	return &ColNullable[time.Time]{Values: c}
}

func (c *ColTime64) Array() *ColArr[time.Time] {
	return &ColArr[time.Time]{Data: c}
}

var (
	_ ColumnOf[Time64] = (*ColTime64Raw)(nil)
	_ Inferable        = (*ColTime64Raw)(nil)
	_ Column           = (*ColTime64Raw)(nil)
)

// ColTime64Raw is Time64 wrapper to implement ColumnOf[Time64].
type ColTime64Raw struct {
	ColTime64
}

func (c *ColTime64Raw) Append(v Time64) { c.AppendRaw(v) }
func (c *ColTime64Raw) AppendArr(vs []Time64) {
	for _, v := range vs {
		c.AppendRaw(v)
	}
}
func (c ColTime64Raw) Row(i int) Time64 { return c.Data[i] } 