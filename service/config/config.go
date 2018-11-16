// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package config

import (
	"context"
	"errors"
	"flag"
	"time"
)

var config = flag.String("config", "", "configuration directory")

func Run(ctx context.Context) error {
	if len(*config) == 0 {
		return errors.New("missing configuration directory")
	}
	select {
	case <-time.After(time.Second * 5):
	case <-ctx.Done():
	}
	return nil
}
