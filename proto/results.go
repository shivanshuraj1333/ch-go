package proto

import (
	"fmt"

	"github.com/go-faster/errors"
)

// Result of Query.
type Result interface {
	DecodeResult(r *Reader, version int, b Block) error
}

// Results wrap []ResultColumn to implement Result.
type Results []ResultColumn

type autoResults struct {
	results *Results
}

func (s autoResults) DecodeResult(r *Reader, version int, b Block) error {
	return s.results.decodeAuto(r, version, b)
}

func (s Results) Rows() int {
	if len(s) == 0 {
		return 0
	}
	return s[0].Data.Rows()
}

func (s *Results) Auto() Result {
	return autoResults{results: s}
}

func (s *Results) decodeAuto(r *Reader, version int, b Block) error {
	if len(*s) > 0 {
		// Already inferred.
		return s.DecodeResult(r, version, b)
	}
	for i := 0; i < b.Columns; i++ {
		r.DebugPeek(64)
		columnName, err := r.Str()
		if err != nil {
			return errors.Wrapf(err, "column [%d] name", i)
		}

		var colType ColumnType

		// Check for custom serialization if the protocol version supports it
		if FeatureCustomSerialization.In(version) {
			// Try to peek at the next byte to see if it's a boolean value
			peek, err := r.raw.Peek(1)
			if err != nil {
				// If we can't peek, assume legacy format
				columnTypeRaw, err := r.Str()
				if err != nil {
					return errors.Wrapf(err, "column [%d] type", i)
				}
				colType = ColumnType(columnTypeRaw)
			} else if peek[0] == boolTrue || peek[0] == boolFalse {
				// This looks like a boolean value, so read it
				customSerialization, err := r.Bool()
				if err != nil {
					return errors.Wrapf(err, "column [%d] custom serialization flag", i)
				}
				if customSerialization {
					// Handle binary type codes
					typeCode, err := r.ReadByte()
					if err != nil {
						return errors.Wrapf(err, "column [%d] type code", i)
					}
					switch typeCode {
					case 0x32: // TimeUTC
						colType = ColumnTypeTime
					case 0x33: // TimeWithTimezone
						colType = ColumnTypeTime
						// TODO: read timezone string if needed
					case 0x34: // Time64UTC
						precision, err := r.UInt8()
						if err != nil {
							return errors.Wrapf(err, "column [%d] Time64 precision", i)
						}
						colType = ColumnTypeTime64.With(fmt.Sprintf("%d", precision))
					case 0x35: // Time64WithTimezone
						precision, err := r.UInt8()
						if err != nil {
							return errors.Wrapf(err, "column [%d] Time64 precision", i)
						}
						tz, err := r.Str()
						if err != nil {
							return errors.Wrapf(err, "column [%d] Time64 timezone", i)
						}
						colType = ColumnTypeTime64.With(fmt.Sprintf("%d, '%s'", precision, tz))
					default:
						return errors.Errorf("column [%d]: unknown type code 0x%x", i, typeCode)
					}
				} else {
					// Read type string for non-custom serialization
					columnTypeRaw, err := r.Str()
					if err != nil {
						return errors.Wrapf(err, "column [%d] type", i)
					}
					colType = ColumnType(columnTypeRaw)
				}
			}
		} else {
			// Legacy protocol - always read type string
			columnTypeRaw, err := r.Str()
			if err != nil {
				return errors.Wrapf(err, "column [%d] type", i)
			}
			colType = ColumnType(columnTypeRaw)
		}

		col := &ColAuto{}
		if err := col.Infer(colType); err != nil {
			return errors.Wrap(err, "column type inference")
		}
		col.Data.Reset()
		if b.Rows != 0 {
			if s, ok := col.Data.(Stateful); ok {
				if err := s.DecodeState(r); err != nil {
					return errors.Wrapf(err, "%s state", columnName)
				}
			}
			if err := col.Data.DecodeColumn(r, b.Rows); err != nil {
				return errors.Wrap(err, columnName)
			}
		}
		*s = append(*s, ResultColumn{
			Name: columnName,
			Data: col.Data,
		})
	}
	return nil
}

func (s Results) DecodeResult(r *Reader, version int, b Block) error {
	var (
		noTarget        = len(s) == 0
		noRows          = b.Rows == 0
		columnsMismatch = b.Columns != len(s)
		allowMismatch   = noTarget && noRows
	)
	if columnsMismatch && !allowMismatch {
		return errors.Errorf("%d (columns) != %d (target)", b.Columns, len(s))
	}
	for i := 0; i < b.Columns; i++ {
		columnName, err := r.Str()
		if err != nil {
			return errors.Wrapf(err, "column [%d] name", i)
		}

		var columnType ColumnType

		// Check for custom serialization if the protocol version supports it
		if FeatureCustomSerialization.In(version) {
			// Try to peek at the next byte to see if it's a boolean value
			peek, err := r.raw.Peek(1)
			if err != nil {
				// If we can't peek, assume legacy format
				columnTypeRaw, err := r.Str()
				if err != nil {
					return errors.Wrapf(err, "column [%d] type", i)
				}
				columnType = ColumnType(columnTypeRaw)
			} else if peek[0] == boolTrue || peek[0] == boolFalse {
				// This looks like a boolean value, so read it
				customSerialization, err := r.Bool()
				if err != nil {
					return errors.Wrapf(err, "column [%d] custom serialization flag", i)
				}
				if customSerialization {
					// Handle binary type codes
					typeCode, err := r.ReadByte()
					if err != nil {
						return errors.Wrapf(err, "column [%d] type code", i)
					}

					switch typeCode {
					case 0x32: // TimeUTC
						columnType = ColumnTypeTime
					case 0x33: // TimeWithTimezone
						columnType = ColumnTypeTime
						// TODO: read timezone string if needed
					case 0x34: // Time64UTC
						precision, err := r.UInt8()
						if err != nil {
							return errors.Wrapf(err, "column [%d] Time64 precision", i)
						}
						columnType = ColumnTypeTime64.With(fmt.Sprintf("%d", precision))
					case 0x35: // Time64WithTimezone
						precision, err := r.UInt8()
						if err != nil {
							return errors.Wrapf(err, "column [%d] Time64 precision", i)
						}
						tz, err := r.Str()
						if err != nil {
							return errors.Wrapf(err, "column [%d] Time64 timezone", i)
						}
						columnType = ColumnTypeTime64.With(fmt.Sprintf("%d, '%s'", precision, tz))
					default:
						return errors.Errorf("column [%d]: unknown type code 0x%x", i, typeCode)
					}
				} else {
					// Read type string for non-custom serialization
					columnTypeRaw, err := r.Str()
					if err != nil {
						return errors.Wrapf(err, "column [%d] type", i)
					}
					columnType = ColumnType(columnTypeRaw)
				}
			}
		} else {
			// Legacy protocol - always read type string
			columnTypeRaw, err := r.Str()
			if err != nil {
				return errors.Wrapf(err, "column [%d] type", i)
			}
			columnType = ColumnType(columnTypeRaw)
		}

		if noTarget {
			// Just reading types and names.
			continue
		}

		// Checking column name and type.
		t := s[i]
		if t.Name == "" {
			// Inferring column name.
			t.Name = columnName
			s[i] = t
		}
		if t.Name != columnName {
			return errors.Errorf("[%d]: unexpected column %q (%q expected)", i, columnName, t.Name)
		}
		if infer, ok := t.Data.(Inferable); ok {
			if err := infer.Infer(columnType); err != nil {
				return errors.Wrap(err, "infer")
			}
		}
		hasType := t.Data.Type()
		if columnType.Conflicts(hasType) {
			return errors.Errorf("[%d]: %s: unexpected type %q (got) instead of %q (has)",
				i, columnName, columnType, hasType,
			)
		}
		t.Data.Reset()
		if b.Rows == 0 {
			continue
		}
		if s, ok := t.Data.(StateDecoder); ok {
			if err := s.DecodeState(r); err != nil {
				return errors.Wrapf(err, "%s state", columnName)
			}
		}
		if err := t.Data.DecodeColumn(r, b.Rows); err != nil {
			return errors.Wrap(err, columnName)
		}
	}

	return nil
}
