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

	SOH = 1 : Start of Header
	STX = 2 : Start of Text
	EOT = 4 : End of Transmission
	SO = 14 : Shift out
	DLE = 16 : Data Link Escape
	CAN = 24 : Cancel
	DC1 = 17 : Device Control 1
	DC2 = 18 : Device Control 2
	DC3 = 19 : Device Control 3
	DC4 = 20 : Device Control 4
	SUB = 26 : Substitute
	FS = 28 : File Sep
	GS = 29 : Group Sep
	RS = 30 : Row Sep
	US = 31 : Unit Sep

	---

	VERSION = SOH "SCD01" NULL STX
	PADDING = FS SO <chunk-length> (begin-chunk) NUL * CHUNK_LENGTH (end-chunk)

	The chunk header contains an index of all rows within it.
	Each Row has a specific type prior to the offset list.
	These types may include:
	 * Data Row
	 * Field Value
	 * Delta (insert/update/delete)
	 * Validation
	 * ? Error code + Error message ?
	 * Reference Data Row

	CHUNK = FS DC1 <chunk-length> (begin-chunk) <table-id><row-count><row-offset-list><row-data> (end-chunk)
		<row-offset-list> = [N]<row-type><row-offset-from-chunk-start>[/N]

		ROW = DLE RS <row-data>
			variable length field = <value-size-bytes><value-id><value-data>
		VALUE = FS DC2 <chunk-length> (begin-chunk) <value-id><value-offset-bytes><value-data> (end-chunk)

	CANCEL = DLE CAN
	EOF = DLE EOT

	{VERSION}
	[for each schema data table, including control tables]
		[N chunks]
			{CHUNK}
			[M rows]
				{ROW}
			[/M rows]
			[K values]
				{VALUE}
			[/K values]
			{/Chunk}
		[/N chunks]
	[/for each schema data table]
	[optional]
		{CANCEL}
	[/optional]
	{EOF}
*/
package ts

import (
	"fmt"
	"io"
	"sort"
)

type Writer struct {
	err error
	w   io.Writer

	table map[int64][]Col
	rowID map[int64]int64

	schemaWritten map[int64]bool
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

func NewWriter(w io.Writer) *Writer {
	e := &Writer{
		w:             w,
		rowID:         make(map[int64]int64, 10),
		table:         make(map[int64][]Col, 10),
		schemaWritten: make(map[int64]bool, 10),
	}
	e.initControl()
	return e
}

func (w *Writer) tableIDList() []int64 {
	tt := make([]int64, 0, len(w.table))
	for tid := range w.table {
		tt = append(tt, tid)
	}
	sort.Slice(tt, func(i, j int) bool {
		return tt[i] < tt[j]
	})
	return tt
}

func (w *Writer) initControl() {
	version := w.Define(Table{Name: "control/version"},
		Col{Name: "version", Type: Hash},
	)
	if version.id != controlVersionID {
		panic("control/version.id incorrect")
	}
	tag := w.Define(Table{Name: "control/tag"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "name", Type: String},
	)
	if tag.id != controlTagID {
		panic("control/tag.id incorrect")
	}
	table := w.Define(Table{Name: "control/table"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "version", Type: Hash, Default: Zero},
		Col{Name: "name", Type: String},
		Col{Name: "comment", Type: String, Default: Zero},
	)
	if table.id != controlTableID {
		panic("control/table.id incorrect")
	}
	tableTag := w.Define(Table{Name: "control/table/tag"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "table", Type: Int64},
		Col{Name: "tag", Type: Int64},
	)
	if tableTag.id != controlTableTagID {
		panic("control/table/tag.id incorrect")
	}

	fieldtype := w.Define(Table{Name: "control/fieldtype"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "bit_size", Type: Int64},
		Col{Name: "name", Type: String},
	)
	if fieldtype.id != controlFieldTypeID {
		panic("control/fieldtype.id incorrect")
	}

	column := w.Define(Table{Name: "control/column"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "version", Type: Hash, Default: Zero, Tags: Tags{TagHidden}},
		Col{Name: "table", Type: Int64},
		Col{Name: "fieldtype", Type: Int64},
		Col{Name: "link", Type: Int64, Nullable: true},
		Col{Name: "key", Type: Bool, Default: Zero},
		Col{Name: "nullable", Type: Bool, Default: Zero},
		Col{Name: "length", Type: Int64, Default: Zero, Comment: "For strings this is the number of allowed runes. For bytes it is the byte count."},
		Col{Name: "fixed_bit_size", Type: Int64, Default: Zero, Tags: Tags{TagHidden}},
		Col{Name: "sort_order", Type: Int64, Default: Zero},
		Col{Name: "name", Type: String},
		Col{Name: "default", Type: Any, Nullable: true},
		Col{Name: "comment", Type: String, Default: Zero},
	)
	if column.id != controlColumnID {
		panic("control/column.id incorrect")
	}

	columnTag := w.Define(Table{Name: "control/column/tag"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "column", Type: Int64},
		Col{Name: "tag", Type: Int64},
	)
	if columnTag.id != controlColumnTagID {
		panic("control/column/tag.id incorrect")
	}

	w.Flush()

	// TODO(kardianos): Calculate hash of control/*.
	w.Insert(version, 0)
}

type TableRef struct {
	id      int64
	all     map[string]bool // Names of all valid columns.
	col     []string        // Names of the columns to work with from table.
	invalid []string        // Invalid names.
}

func (t TableRef) Use(columns ...string) TableRef {
	ut := TableRef{
		id:  t.id,
		all: t.all,
		col: columns,
	}
	for _, c := range columns {
		if !t.all[c] {
			ut.invalid = append(ut.invalid, c)
		}
	}
	return ut
}

type Table struct {
	Name    string
	Comment string
	Tags    Tags
}

type Col struct {
	Name string
	Type Type

	Link      int64 // column.id
	Key       bool
	Nullable  bool
	MaxRunes  int64
	SortOrder int64
	Default   interface{}
	Comment   string

	Tags Tags
}

type RowRef struct {
	table int64
	id    int64
}

var errTable = TableRef{id: -1}
var errRow = RowRef{id: -1}

func (w *Writer) Define(t Table, cols ...Col) TableRef {
	if w.err != nil {
		return errTable
	}
	// TODO(kardianos): encode column schema, store schema in Encoder.
	tid := w.nextRowID(controlTableID)
	names := make([]string, len(cols))
	lookup := make(map[string]bool, len(cols))
	for i, c := range cols {
		names[i] = c.Name
		lookup[c.Name] = true
	}
	w.table[tid] = cols
	return TableRef{
		id:  tid,
		all: lookup,
		col: names,
	}
}

func (w *Writer) Flush() {
	if w.err != nil {
		return
	}

	// TODO(kardianos): Don't add data into column, table/tag, or column/tag directlly. Add from previous table definitions.
	// Gather all un-written schema changes and write them first.
	encodeTID := make([]int64, 0)
	for _, tid := range w.tableIDList() {
		if !w.schemaWritten[tid] {
			continue
		}
		encodeTID = append(encodeTID, tid)
		w.schemaWritten[tid] = true

	}
	for _, tid := range encodeTID {
		// TODO(kardianos): write table block.
	}
	for _, tid := range encodeTID {
		cc := w.table[tid]
		for i, c := range cc {
			// TODO(kardianos): write column block.
		}
	}

	// Then write any un-written data changes.

	/*
		w.Insert(table, controlVersionID, 0, "control/version", "")
		w.Insert(table, controlTableID, 0, "control/table", "")
		w.Insert(table, controlFieldTypeID, 0, "control/fieldtype", "")
		w.Insert(table, controlTagID, 0, "control/tag", "")
		w.Insert(table, controlColumnID, 0, "control/column", "")
		w.Insert(table, controlColumnTagID, 0, "control/column/Tag", "")

		w.Insert(fieldtype, Hash, 256, "hash")
		w.Insert(fieldtype, Int64, 64, "int64")
		w.Insert(fieldtype, Bool, 1, "bool")
		w.Insert(fieldtype, String, 0, "string")
		w.Insert(fieldtype, Bytes, 0, "bytes")
		w.Insert(fieldtype, Any, 0, "any")

		w.Insert(tag, TagHidden, "hidden")

		// c2 := column.Use("table", "fieldtype", "key", "nullable", "name")
		// w.Insert(c2, controlVersionID, Hash, false, false, "version")
	*/
}

func (w *Writer) nextRowID(tid int64) int64 {
	rid := w.rowID[tid]
	rid++
	w.rowID[tid] = rid
	return rid
}

func (w *Writer) Error() error {
	return w.err
}

func (w *Writer) Insert(t TableRef, values ...interface{}) RowRef {
	if w.err != nil {
		return errRow
	}
	if len(t.invalid) > 0 {
		w.err = fmt.Errorf("st: invalid table names: %q", t.invalid)
		return errRow
	}
	return RowRef{
		table: w.nextRowID(t.id),
	}
}
