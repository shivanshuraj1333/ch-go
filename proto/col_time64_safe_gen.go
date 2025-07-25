//go:build !(amd64 || arm64 || riscv64) || purego

// Code generated by ./cmd/ch-gen-col, DO NOT EDIT.

package proto

import (
	"encoding/binary"

	"github.com/go-faster/errors"
)

var _ = binary.LittleEndian // clickHouse uses LittleEndian

// DecodeColumn decodes Time64 rows from *Reader.
func (c *ColTime64) DecodeColumn(r *Reader, rows int) error {
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
			Time64(binary.LittleEndian.Uint64(data[i:i+size])),
		)
	}
	c.Data = v
	return nil
}

// EncodeColumn encodes Time64 rows to *Buffer.
func (c ColTime64) EncodeColumn(b *Buffer) {
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

func (c ColTime64) WriteColumn(w *Writer) {
	w.ChainBuffer(c.EncodeColumn)
}
