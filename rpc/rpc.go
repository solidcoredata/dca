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
