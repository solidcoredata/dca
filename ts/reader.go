// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ts

import (
	"io"
)

type Reader struct {
	table map[int64][]chunk
}

func NewReader(r io.Reader) *Reader {
	return nil
}

// indexTable reads through the entire data structure, seeking each
// new token until the EOF is reached.
func (r *Reader) indexTable() error {
	return nil
}
