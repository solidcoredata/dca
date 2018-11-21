// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ts

import (
	"errors"
)

var ErrStreamCancel = errors.New("ts: stream cancel")

const (
	asciiFS = 28
	asciiGS = 29
	asciiRS = 30
	asciiUS = 31
)
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
