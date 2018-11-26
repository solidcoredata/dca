// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ts

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

type FieldCoder interface {
	BitSize() int64 // Zero if variable length.

	// Encode should try to encode the value into writeTo and return the same value.
	//
	// Values smaller then 8 bits may be OR'ed to gether with the previous value.
	Encode(col *Col, writeTo []byte, value interface{}) ([]byte, error)

	// TODO(kardianos): write decoder interface along with reeading / scanning table interface.
}

const hashSizeBits = 256
const hashSizeBytes = 256 / 8

type coderHash struct{}

func (coderHash) BitSize() int64 {
	return hashSizeBits
}
func (coderHash) Encode(col *Col, writeTo []byte, value interface{}) ([]byte, error) {
	if cap(writeTo) < hashSizeBytes {
		writeTo = make([]byte, hashSizeBytes)
	} else {
		writeTo = writeTo[:hashSizeBytes]
	}
	switch v := value.(type) {
	default:
		return writeTo, fmt.Errorf("ts: unknown value type %#v", value)
	case []byte:
		copy(writeTo, v)
	case [8]byte:
		copy(writeTo, v[:])
	}
	return writeTo, nil
}

type coderInt64 struct{}

func (coderInt64) BitSize() int64 {
	return 64
}
func (coderInt64) Encode(col *Col, writeTo []byte, value interface{}) ([]byte, error) {
	if cap(writeTo) < 8 {
		writeTo = make([]byte, 8)
	} else {
		writeTo = writeTo[:8]
	}
	switch v := value.(type) {
	default:
		return writeTo, fmt.Errorf("ts: unknown value type %#v", value)
	case int64:
		binary.LittleEndian.PutUint64(writeTo, uint64(v))
	case int:
		binary.LittleEndian.PutUint64(writeTo, uint64(v))
	}
	return writeTo, nil
}

type coderBool struct{}

func (coderBool) BitSize() int64 {
	return 1
}
func (coderBool) Encode(col *Col, writeTo []byte, value interface{}) ([]byte, error) {
	if cap(writeTo) < 1 {
		writeTo = make([]byte, 1)
	} else {
		writeTo = writeTo[:1]
	}
	switch v := value.(type) {
	default:
		return writeTo, fmt.Errorf("ts: unknown value type %#v", value)
	case bool:
		if v {
			writeTo[0] = 1
		} else {
			writeTo[0] = 0
		}
	}
	return writeTo, nil
}

type coderString struct{}

func (coderString) BitSize() int64 {
	return 0
}
func (coderString) Encode(col *Col, writeTo []byte, value interface{}) ([]byte, error) {
	var runeCount int64
	switch v := value.(type) {
	default:
		return writeTo, fmt.Errorf("ts: unknown value type %#v", value)
	case string:
		if cap(writeTo) < len(v) {
			writeTo = make([]byte, len(v))
		}
		writeTo = writeTo[:len(v)]
		for i, r := range v {
			if r == utf8.RuneError {
				return nil, fmt.Errorf("ts: invalid utf8 string, invalid rune at byte index %d", i)
			}
			runeCount++
			utf8.EncodeRune(writeTo[i:], r)
		}
	case []byte:
		if cap(writeTo) < len(v) {
			writeTo = make([]byte, len(v))
		}
		writeTo = writeTo[:len(v)]
		i := 0
		for {
			r, sz := utf8.DecodeRune(v[i:])
			if r == utf8.RuneError {
				return nil, fmt.Errorf("ts: invalid utf8 string, invalid rune at byte index %d", i)
			}
			runeCount++
			utf8.EncodeRune(writeTo[i:], r)
			i += sz
		}
	}
	if col.Length > 0 && runeCount > col.Length {
		return nil, fmt.Errorf("ts: value for %q contains %d runes, max allowed is %d", col.Name, runeCount, col.Length)
	}
	return writeTo, nil
}

type coderBytes struct{}

func (coderBytes) BitSize() int64 {
	return 0
}
func (coderBytes) Encode(col *Col, writeTo []byte, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	default:
		return writeTo, fmt.Errorf("ts: unknown value type %#v", value)
	case string:
		if cap(writeTo) < len(v) {
			writeTo = []byte(v)
		} else {
			n := copy(writeTo, []byte(v))
			writeTo = writeTo[:n]
		}
	case []byte:
		if cap(writeTo) < len(v) {
			writeTo = make([]byte, len(v))
		}
		writeTo = writeTo[:len(v)]
		n := copy(writeTo, v)
		writeTo = writeTo[:n]
	}
	return writeTo, nil
}

type coderAny struct{}

func (coderAny) BitSize() int64 {
	return 0
}
func (coderAny) Encode(col *Col, writeTo []byte, value interface{}) ([]byte, error) {
	return writeTo[:0], nil
}
