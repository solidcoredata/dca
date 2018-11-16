// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/solidcoredata/dca/internal/start"
	"github.com/solidcoredata/dca/service/config"
)

func main() {
	flag.Parse()
	err := start.Start(context.Background(), time.Second*5, run)
	if err != nil {
		log.Print(err)
	}
}

func run(ctx context.Context) error {
	return start.RunAll(ctx,
		config.Run,
	)
}
