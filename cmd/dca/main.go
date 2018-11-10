package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/solidcoredata/dca/config"
	"github.com/solidcoredata/dca/internal/start"
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
