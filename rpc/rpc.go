// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpc

import (
	"context"
)

type ConfigService interface {
	Alive(ctx context.Context, req *AliveRequest) (*AliveResponse, error)
}

type AliveRequest struct {
}
type AliveResponse struct {
}

type TSMsg struct {
	Version      string // = 1 // Only sent in first message.
	FirstMessage bool   // = 2
	LastMessage  bool   // = 3
	Cancel       bool   // = 4
	Chunk        []byte // = 10
}
