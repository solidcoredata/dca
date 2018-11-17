// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package ts does table serialization to and from a binary stream.
//
/*

	let control/version table {
		version hash
	} {
		{0x00...00},
	}
	let control/table table {
		id int64 key
		version hash // Hash of all columns in the table, ordered by column sort_order.
		name string length=1000
		comment string default zero
	} {
		{1, 0x00...00, "control.version"},
		{2, 0x00...00, "control.table"},
		{3, 0x00...00, "control.fieldtype"},
		{4, 0x00...00, "control.tag"},
		{5, 0x00...00, "control.column"},
		{6, 0x00...00, "control.column/tag"},
	}
	let control/fieldtype table {
		id int64 key
		name string
		length_prefix bool
		bit_size int64
	} {
		{1, "hash", false, 256},
		{2, "int64", false, 64},
		{3, "bool", false, 1},
		{4, "string", true, 0},
		{5, "bytes", true, 0},
		{6, "any", true, 0},
	}
	let control/tag table {
		id int64 key
		name string
	} {
		{1, "hidden"},
	}
	let control/column table {
		id int64 key
		version hash
		table *control.table
		fieldtype *control.fieldtype
		link *control.table nullable
		name string
		key bool default zero
		nullable bool default zero

		// Maximum number of runes to encode into field.
		// Max byte storage could be 4x this number.
		max_runes int64

		// This is written by the encoder and read by the decoder.
		// This is not set by the user.
		// For variable length fields, the encoder decides how much to write into
		// the row. For fixed length fields, this is still populated. That way a decoder that
		// will always have the correct length and can generally read with a mmap.
		fixed_bit_size int64 :hidden

		 // The preferred order this column should appear, relative to other columns
		// in the same table.
		sort_order int
		default any
		comment string default zero
	} {
		{1, 1, 3, "version", 4, false, true, 0x00...00},
		{2, 2, 1, "id", 0, false, true, 0x00...00},
		{3, 2, 3, "name", 1000, false, false, 0x00...00},
		...
	}
	let control/column/tag table {
		id int64 key
		column *control/column
		tag *control/tag
	}


Variable length columns have the following layout:
	<value-id><total-value-size><value-data-fixed>

Field Lendth notes:
	max rune count (integer): user
	field data size in bytes (integer): user / framework / encoder
	field length in bits (integer): encoder

The data for the schemas are written first, followed by the data for all other tables.

	VERSION=[]byte("DC00")
	CHUNK_TABLE=[]byte("CT")<table-id><chunk-size-bytes><table-data>
	CHUNK_VALUE=[]byte("CV")<value-id><value-offset-bytes><value-size-bytes><value-data>
	CHUNK_SUM=[]byte("ST")<hash-sum-of-preceding-chunk>
	ROW=[]byte("RM")<value-mask><row-data>
	CANCEL=[]byte{255, 255}
	EOF=[]byte{0, 0}

	{VERSION}
	[for each schema data table, including control tables]
		[N chunks]
			{CHUNK_TABLE}
			[M rows]
				{ROW}
			[/M rows]
			{CHUNK_SUM}
			[K values]
				{CHUNK_VALUE}
				{CHUNK_SUM}
			[/K values]
		[/N chunks]
	[/for each schema data table]
	[optional]
		{CANCEL}
	[/optional]
	{EOF}

*/
package ts

import (
	"errors"
	"fmt"
	"io"
)

var ErrStreamCancel = errors.New("ts: stream cancel")

const (
	controlVersionID = 1
	controlTableID   = 2
	controlFieldType = 3
	controlTag       = 4
	controlColumn    = 5
	controlColumnTag = 6
)

type Type int64

const (
	Hash   Type = 1
	Int64  Type = 2
	Bool   Type = 3
	String Type = 4
	Bytes  Type = 5
	Any    Type = 6
)

type Encoder struct {
	err error
	w   io.Writer

	rowID map[int64]int64
}
type Decoder struct {
	table map[int64][]chunk
}

type chunk struct {
	readOffset int64
	values     map[int64]valueChunk
	rowCount   int64
}

type valueChunk struct {
	readOffset  int64 // Read offset from top of file.
	valueID     int64
	valueOffset int64
	valueLength int64
}

func NewEncoder(w io.Writer) *Encoder {
	e := &Encoder{
		w:     w,
		rowID: make(map[int64]int64, 10),
	}
	e.initControl()
	return e
}

func (e *Encoder) initControl() {
	ver := e.Table("control/version",
		Col{Name: "version", Type: Hash},
	)
	e.Insert(ver, 0)
	if ver.id != controlVersionID {
		panic("control/version.id incorrect")
	}
	// TODO(kardianos): finish setting up remaining control tables.
}

type Table struct {
	id      int64
	all     []string // Names of all valid columns.
	col     []string // Names of the columns to work with from table.
	invalid []string // Invalid names.
}

func (t Table) Use(columns ...string) Table {
	return Table{
		id:  t.id,
		all: t.all,
		col: columns, // TODO(kardianos): ensure all columns are valid names.
	}
}

type Col struct {
	Name string
	Type Type
}

type Row struct {
	table int64
	id    int64
}

var errTable = Table{id: -1}
var errRow = Row{id: -1}

func (e *Encoder) Table(name string, cols ...Col) Table {
	if e.err != nil {
		return errTable
	}
	// TODO(kardianos): encode column schema, store schema in Encoder.
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return Table{
		id:  e.nextRowID(controlTableID),
		all: names,
		col: names,
	}
}

func (e *Encoder) nextRowID(tid int64) int64 {
	rid := e.rowID[tid]
	rid++
	e.rowID[tid] = rid
	return rid
}

func (e *Encoder) Err() error {
	return e.err
}

func (e *Encoder) Insert(t Table, values ...interface{}) Row {
	if e.err != nil {
		return errRow
	}
	if len(t.invalid) > 0 {
		e.err = fmt.Errorf("st: invalid table names: %q", t.invalid)
		return errRow
	}
	return Row{
		table: e.nextRowID(t.id),
	}
}

func NewDecoder(r io.ReadSeeker) *Decoder {
	return nil
}

// indexTable reads through the entire data structure, seeking each
// new token until the EOF is reached.
func (d *Decoder) indexTable() error {
	return nil
}
