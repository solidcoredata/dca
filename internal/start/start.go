// Copyright 2018 The Solid Core Data Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package start

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

type StartFunc func(ctx context.Context) error

func Start(ctx context.Context, stopTimeout time.Duration, run StartFunc) error {
	notify := make(chan os.Signal, 3)
	signal.Notify(notify, os.Interrupt)
	ctx, cancel := context.WithCancel(ctx)
	once := &sync.Once{}
	fin := make(chan bool)
	unlock := func() {
		close(fin)
	}
	unlockOnce := func() {
		once.Do(unlock)
	}
	runErr := atomic.Value{}
	go func() {
		err := run(ctx)
		if err != nil {
			runErr.Store(err)
		}
		unlockOnce()
	}()
	select {
	case <-notify:
	case <-fin:
	}
	cancel()
	go func() {
		<-time.After(stopTimeout)
		unlockOnce()
	}()
	<-fin
	if err, ok := runErr.Load().(error); ok {
		return err
	}
	return nil
}

func RunAll(ctx context.Context, runs ...func(ctx context.Context) error) error {
	group, ctx := errgroup.WithContext(ctx)
	for _, run := range runs {
		run := run
		group.Go(func() error { return run(ctx) })
	}

	return group.Wait()
}
