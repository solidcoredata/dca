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

0	00	00000000	NUL	null
1	01	00000001	SOH	start of header
2	02	00000010	STX	start of text
3	03	00000011	ETX	end of text
4	04	00000100	EOT	end of transmission
5	05	00000101	ENQ	enquiry
6	06	00000110	ACK	acknowledge
7	07	00000111	BEL	bell
8	08	00001000	BS	backspace
9	09	00001001	HT	horizontal tab
10	0A	00001010	LF	line feed
11	0B	00001011	VT	vertical tab
12	0C	00001100	FF	form feed
13	0D	00001101	CR	enter / carriage return
14	0E	00001110	SO	shift out
15	0F	00001111	SI	shift in
16	10	00010000	DLE	data link escape
17	11	00010001	DC1	device control 1
18	12	00010010	DC2	device control 2
19	13	00010011	DC3	device control 3
20	14	00010100	DC4	device control 4
21	15	00010101	NAK	negative acknowledge
22	16	00010110	SYN	synchronize
23	17	00010111	ETB	end of trans. block
24	18	00011000	CAN	cancel
25	19	00011001	EM	end of medium
26	1A	00011010	SUB	substitute
27	1B	00011011	 ESC	escape
28	1C	00011100	 FS	file separator
29	1D	00011101	GS	group separator
30	1E	00011110	RS	record separator
31	1F	00011111	US	unit separator
127	7F	01111111	DEL	delete

VERSION = SOH "SCD01" STX
CANCEL = DLE CAN
ROW = DLE RS
CHUNK_TABLE = DLE DC1
CHUNK_VALUE = DLE DC2
CHUNK_SUM = DLE DC3
EOF = DLE EOT

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
	"sort"
)

var ErrStreamCancel = errors.New("ts: stream cancel")

const (
	controlVersionID   = 1
	controlTagID       = 2
	controlTableID     = 3
	controlTableTagID  = 4
	controlFieldTypeID = 5
	controlColumnID    = 6
	controlColumnTagID = 7
)

type Type int64

type zero struct{}

var Zero = zero{}

const (
	Hash   Type = 1
	Int64  Type = 2
	Bool   Type = 3
	String Type = 4
	Bytes  Type = 5
	Any    Type = 6
)

type Tag int64

type Tags []Tag

const (
	TagHidden Tag = 1
)

type Writer struct {
	err error
	w   io.Writer

	table map[int64][]Col
	rowID map[int64]int64
}
type Reader struct {
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

func NewWriter(w io.Writer) *Writer {
	e := &Writer{
		w:     w,
		rowID: make(map[int64]int64, 10),
		table: make(map[int64][]Col, 10),
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

	/*
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
		}
	*/
	column := w.Define(Table{Name: "control/column"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "version", Type: Hash, Default: Zero},
		Col{Name: "table", Type: Int64},
		Col{Name: "fieldtype", Type: Int64},
		Col{Name: "link", Type: Int64, Nullable: true},
		Col{Name: "key", Type: Bool, Default: Zero},
		Col{Name: "nullable", Type: Bool, Default: Zero},
		Col{Name: "max_runes", Type: Int64, Default: Zero},
		Col{Name: "fixed_bit_size", Type: Int64, Default: Zero, Tags: Tags{TagHidden}},
		Col{Name: "sort_order", Type: Int64, Default: Zero},
		Col{Name: "name", Type: String},
		Col{Name: "default", Type: Any},
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

	w.Insert(version, 0)

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
	// TODO(kardianos): Don't add data into column, table/tag, or column/tag directlly. Add from previous table definitions.
	for _, tid := range w.tableIDList() {
		cc := w.table[tid]
	}
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

func NewReader(r io.Reader) *Reader {
	return nil
}

// indexTable reads through the entire data structure, seeking each
// new token until the EOF is reached.
func (r *Reader) indexTable() error {
	return nil
}
