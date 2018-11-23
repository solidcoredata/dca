// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ts

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

type tableInfo struct {
	ID int64
	Table
	Columns      []Col
	ColumnByName map[string]*Col
}

type Writer struct {
	err error
	w   io.Writer

	chunksWritten int64
	chunkBuffer   *bytes.Buffer

	table   map[int64]*tableInfo
	rowID   map[int64]int64
	control map[int64]TableRef

	// rowBuffer is written to by the Insert call, then written to disk
	// and emptied on Flush.
	rowBuffer map[int64][][]byte // map[tableID][]RowData
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
		w:           w,
		chunkBuffer: &bytes.Buffer{},
		rowID:       make(map[int64]int64, 10),
		table:       make(map[int64]*tableInfo, 10),
		control:     make(map[int64]TableRef, 10),
		rowBuffer:   make(map[int64][][]byte, 10),
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

func (w *Writer) rowBufferTID() []int64 {
	tt := make([]int64, 0, len(w.rowBuffer))
	for tid := range w.rowBuffer {
		tt = append(tt, tid)
	}
	sort.Slice(tt, func(i, j int) bool {
		return tt[i] < tt[j]
	})
	return tt
}

func (w *Writer) csetup(tid int64, t Table, c ...Col) TableRef {
	if w.err != nil {
		panic(w.err)
	}
	tref := w.cdefine(tid, t, c...)
	if tref.id != tid {
		panic(fmt.Errorf("%s.id incorrect: wanted %d, got %d", t.Name, tid, tref.id))
	}
	w.control[tid] = tref
	return tref
}

// initControl created the control tables and initial data. This must be done
// in two steps, the first to define all the internal structures, the second
// to create the rows within the internal structures.
func (w *Writer) initControl() {
	version := w.csetup(controlVersionID, Table{Name: "control/version"},
		Col{Name: "version", Type: Hash},
	)

	tag := w.csetup(controlTagID, Table{Name: "control/tag"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "name", Type: String},
	)

	table := w.csetup(controlTableID, Table{Name: "control/table"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "version", Type: Hash, Default: Zero},
		Col{Name: "name", Type: String},
		Col{Name: "comment", Type: String, Default: Zero},
	)

	tableTag := w.csetup(controlTableTagID, Table{Name: "control/table/tag"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "table", Type: Int64},
		Col{Name: "tag", Type: Int64},
	)

	fieldtype := w.csetup(controlFieldTypeID, Table{Name: "control/fieldtype"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "bit_size", Type: Int64},
		Col{Name: "name", Type: String},
	)

	column := w.csetup(controlColumnID, Table{Name: "control/column"},
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

	columnTag := w.csetup(controlColumnTagID, Table{Name: "control/column/tag"},
		Col{Name: "id", Type: Int64, Key: true},
		Col{Name: "column", Type: Int64},
		Col{Name: "tag", Type: Int64},
	)
	_ = table
	_ = tableTag
	_ = column
	_ = columnTag
	_ = fieldtype

	// Loop through all the tables added so far and insert the table and column rows.
	for _, tid := range w.tableIDList() {
		w.insertControl(w.table[tid])
	}

	w.Insert(tag, TagHidden, "hidden")

	// TODO(kardianos): Register encoders to types.
	w.addFieldType(Hash, "hash", coderHash{})
	w.addFieldType(Int64, "int64", coderInt64{})

	// w.Insert(fieldtype, Hash, 256, "hash")
	// w.Insert(fieldtype, Int64, 64, "int64")
	// w.Insert(fieldtype, Bool, 1, "bool")
	// w.Insert(fieldtype, String, 0, "string")
	// w.Insert(fieldtype, Bytes, 0, "bytes")
	// w.Insert(fieldtype, Any, 0, "any")

	w.Flush()

	// TODO(kardianos): Calculate hash of control/*.
	w.Insert(version, 0)
}

func (w *Writer) addFieldType(ftid Type, name string, fc FieldCoder) {
	w.Insert(w.control[controlFieldTypeID], int64(ftid), fc.BitSize(), name)
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
	Length    int64 // Number of runes if text, or number of bytes if bytes.
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

func (w *Writer) cdefine(tid int64, t Table, cols ...Col) TableRef {
	if w.err != nil {
		return errTable
	}

	names := make([]string, len(cols))
	lookup := make(map[string]bool, len(cols))

	ti := &tableInfo{
		ID:           tid,
		Table:        t,
		Columns:      cols,
		ColumnByName: make(map[string]*Col, len(cols)),
	}
	w.table[tid] = ti

	for i, c := range cols {
		names[i] = c.Name
		lookup[c.Name] = true
		ti.ColumnByName[c.Name] = &ti.Columns[i]
	}

	return TableRef{
		id:  tid,
		all: lookup,
		col: names,
	}
}

func (w *Writer) insertControl(ti *tableInfo) {
	tref := w.control[controlTableID]
	ttagref := w.control[controlTableTagID]
	cref := w.control[controlColumnID]
	ctagref := w.control[controlColumnTagID]
	w.Insert(tref, ti.ID, 0, ti.Name, ti.Comment)

	for _, tag := range ti.Tags {
		// TODO(kardianos): Verify tag is valid.
		ttagid := w.nextRowID(controlTableTagID)
		w.Insert(ttagref, ttagid, ti.ID, tag)
	}
	for i, c := range ti.Columns {
		rid := w.nextRowID(controlColumnID)
		fixed_bit_size := int64(0) // TODO(kardianos): Calc hash and fixed_bit_size.
		sort_order := int64(i + 1)

		w.Insert(cref, rid, 0, ti.ID, c.Type, c.Link, c.Key, c.Nullable, c.Length, fixed_bit_size, sort_order, c.Name, c.Default, c.Comment)

		for _, tag := range c.Tags {
			// TODO(kardianos): Verify tag is valid.
			rtagid := w.nextRowID(controlColumnTagID)
			w.Insert(ctagref, rtagid, rid, tag)
		}
	}
}

func (w *Writer) Define(t Table, cols ...Col) TableRef {
	if w.err != nil {
		return errTable
	}

	tid := w.nextRowID(controlTableID)
	ref := w.cdefine(tid, t, cols...)
	w.insertControl(w.table[tid])

	return ref
}

func (w *Writer) Flush() {
	if w.err != nil {
		return
	}
	if len(w.rowBuffer) == 0 {
		return
	}

	if w.chunksWritten == 0 {
		w.w.Write(fileHeader)
	}

	type offset struct {
		Type   byte
		Offset int64
	}

	cb := w.chunkBuffer

	for _, tid := range w.rowBufferTID() {
		rows := w.rowBuffer[tid]
		delete(w.rowBuffer, tid)

		sizeOfRowOffset := 8
		sizeOfRowType := 1

		sizeOfTableID := 8
		sizeOfRowCount := 8
		sizeOfPerRowHeader := sizeOfRowType + sizeOfRowOffset

		// TODO(kardianos): In the future there may be a another loop to split many buffered rows into multiple chunks.

		headerSize := sizeOfTableID + sizeOfRowCount + (len(rows) * sizeOfPerRowHeader)
		chunkSize := int64(headerSize)
		oo := make([]offset, len(rows))
		for ri, r := range rows {
			if len(r) < 2 {
				w.err = fmt.Errorf("invalid row length (%d) for tid=%d", len(r), tid)
				return
			}
			oo[ri].Type = r[1]
			oo[ri].Offset = chunkSize
			chunkSize += int64(len(r))
		}

		cb.Reset()
		cb.Write(markerChunk)
		binary.Write(cb, binary.LittleEndian, chunkSize)
		binary.Write(cb, binary.LittleEndian, tid)
		binary.Write(cb, binary.LittleEndian, len(rows))

		for _, r := range rows {
			cb.Write(r)
		}
		_, err := cb.WriteTo(w.w)
		if err != nil {
			w.err = err
			return
		}
		w.chunksWritten++
	}
	cb.Reset()
}

func (w *Writer) Cancel() error {
	if w.err != nil {
		return w.err
	}
	_, err := w.w.Write(fileCancel)
	if err != nil {
		w.err = err
	}
	w.err = io.EOF
	return nil
}

func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}
	_, err := w.w.Write(fileEOF)
	if err != nil {
		w.err = err
	}
	w.err = io.EOF
	return nil
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

	if len(t.col) != len(values) {
		w.err = fmt.Errorf("ts: expected %d values, got %d values", len(t.col), len(values))
		return errRow
	}
	// TODO(kardianos): Encode values row to w.rowBuffer.
	cb := w.chunkBuffer
	cb.Reset()
	cb.Write(markerRow)

	// Decide which columns have values.
	// Encode the value bit-mask prefix.
	// Loop through each column and write it to the buffer.

	rowdata := make([]byte, cb.Len())
	copy(rowdata, cb.Bytes())
	w.rowBuffer[t.id] = append(w.rowBuffer[t.id], rowdata)

	return RowRef{
		table: t.id,
		id:    -1, // TODO(kardianos): Determine the correct ID, ensure it is greater or equal to the current row table ID.
	}
}
