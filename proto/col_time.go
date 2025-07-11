package proto

import (
	"encoding/binary"
	"time"

	"github.com/go-faster/errors"
)

var (
	_ ColumnOf[time.Time] = (*ColTime)(nil)
	_ Inferable           = (*ColTime)(nil)
	_ Column              = (*ColTime)(nil)
)

// ColTime implements ColumnOf[time.Time].
type ColTime struct {
	Data []Time
}

func (c ColTime) Rows() int {
	return len(c.Data)
}

func (c *ColTime) Reset() {
	c.Data = c.Data[:0]
}

func (c ColTime) Type() ColumnType {
	return ColumnTypeTime
}

func (c *ColTime) Infer(t ColumnType) error {
	if t.Base() != ColumnTypeTime {
		return errors.Errorf("invalid type: %s", t)
	}
	return nil
}

func (c ColTime) Row(i int) time.Time {
	return c.Data[i].Time()
}

func (c *ColTime) AppendRaw(v Time) {
	c.Data = append(c.Data, v)
}

func (c *ColTime) Append(v time.Time) {
	c.AppendRaw(ToTime(v))
}

func (c *ColTime) AppendArr(v []time.Time) {
	for _, item := range v {
		c.AppendRaw(ToTime(item))
	}
}

// Raw version of ColTime for ColumnOf[Time].
func (c ColTime) Raw() *ColTimeRaw {
	return &ColTimeRaw{ColTime: c}
}

func (c *ColTime) Nullable() *ColNullable[time.Time] {
	return &ColNullable[time.Time]{Values: c}
}

func (c *ColTime) Array() *ColArr[time.Time] {
	return &ColArr[time.Time]{Data: c}
}

var (
	_ ColumnOf[Time] = (*ColTimeRaw)(nil)
	_ Inferable      = (*ColTimeRaw)(nil)
	_ Column         = (*ColTimeRaw)(nil)
)

// ColTimeRaw is Time wrapper to implement ColumnOf[Time].
type ColTimeRaw struct {
	ColTime
}

func (c *ColTimeRaw) Append(v Time) { c.AppendRaw(v) }
func (c *ColTimeRaw) AppendArr(vs []Time) {
	for _, v := range vs {
		c.AppendRaw(v)
	}
}
func (c ColTimeRaw) Row(i int) Time { return c.Data[i] }

// DecodeColumn decodes Time rows from *Reader.
func (c *ColTime) DecodeColumn(r *Reader, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 64 / 8
	data, err := r.ReadRaw(rows * size)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	v := c.Data
	// Move bound check out of loop.
	//
	// See https://github.com/golang/go/issues/30945.
	_ = data[len(data)-size]
	for i := 0; i <= len(data)-size; i += size {
		v = append(v,
			Time(binary.LittleEndian.Uint64(data[i:i+size])),
		)
	}
	c.Data = v
	return nil
}

// EncodeColumn encodes Time rows to *Buffer.
func (c ColTime) EncodeColumn(b *Buffer) {
	v := c.Data
	if len(v) == 0 {
		return
	}
	const size = 64 / 8
	offset := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, size*len(v))...)
	for _, vv := range v {
		binary.LittleEndian.PutUint64(
			b.Buf[offset:offset+size],
			uint64(vv),
		)
		offset += size
	}
}

func (c ColTime) WriteColumn(w *Writer) {
	w.ChainBuffer(c.EncodeColumn)
}
