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

	CHUNK = FS "C" <chunk-length> (begin-chunk) <table-id><row-count><row-offset-list><row-data> (end-chunk)
		<row-offset-list> = [N]<row-type><row-offset-from-chunk-start>[/N]

		ROW = RS "R" <row-data>
			variable length field = <value-size-bytes><value-id><value-data>
		VALUE = RS "F" <value-id><value-offset-bytes><value-data>

	CANCEL = FS CAN
	EOF = FS EOT

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
