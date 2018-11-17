// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ts

import (
	"bytes"
	"testing"
)

func TestEncode(t *testing.T) {
	buf := &bytes.Buffer{}
	e := NewWriter(buf)
	e.Flush()
	if err := e.Error(); err != nil {
		t.Fatal(err)
	}
	t.Log(buf.Bytes())
}
