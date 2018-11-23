// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ts

import (
	"encoding/binary"
	"fmt"
)

type FieldCoder interface {
	BitSize() int64 // Zero if variable length.

	// Encode should try to encode the value into writeTo and return the same value.
	//
	// Values smaller then 8 bits may be OR'ed to gether with the previous value.
	Encode(writeTo []byte, value interface{}) ([]byte, error)

	// TODO(kardianos): write decoder interface along with reeading / scanning table interface.
}

const hashSizeBits = 256
const hashSizeBytes = 256 / 8

type coderHash struct{}

func (coderHash) BitSize() int64 {
	return hashSizeBits
}
func (coderHash) Encode(writeTo []byte, value interface{}) ([]byte, error) {
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

// w.Insert(fieldtype, Hash, 256, "hash")
// w.Insert(fieldtype, Int64, 64, "int64")
// w.Insert(fieldtype, Bool, 1, "bool")
// w.Insert(fieldtype, String, 0, "string")
// w.Insert(fieldtype, Bytes, 0, "bytes")
// w.Insert(fieldtype, Any, 0, "any")

type coderInt64 struct{}

func (coderInt64) BitSize() int64 {
	return 64
}
func (coderInt64) Encode(writeTo []byte, value interface{}) ([]byte, error) {
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
	}
	return writeTo, nil
}

type coderBool struct{}
type coderString struct{}
type coderBytes struct{}
type coderAny struct{}
