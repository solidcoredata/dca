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
		{2, 0x00...00, "control.version"},
		{2, 0x00...00, "control.table"},
		{3, 0x00...00, "control.fieldtype"},
		{4, 0x00...00, "control.column"},
	}
	let control/fieldtype table {
		id int64 key
		name string
		length_prefix bool
		bit_size int64
	} {
		{0, "hash", false, 256},
		{1, "int64", false, 64},
		{2, "bool", false, 1},
		{3, "string", true, 0},
		{4, "bytes", true, 0},
		{5, "any", true, 0},
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
