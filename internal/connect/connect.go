package connect

import (
	"time"
)

/*
	The client connects to the server.
	The client periodically sends a heartbeat to the server to signal it
	is still alive.
	The server notifies the clients of a change.
	The client fetches the change through a different interface.
	The client notifies the the server 5/10 connections have been updated.
	The client notifies the server all active connections are on v X.

	Each heartbeat from the client anounces the internal version state for
	the given component: struct{Parts int, Current []struct{Version string, Parts int}}.
	Similarly the server anounces in each heartbeat the 5 most recent versions:
	struct{Stack []struct{Version string, Current bool, Scheduled *time.Time}}
	The server announce should only contain relevant versions, which may be defined as
	any future versions not yet started and any past versions within the same
	change group.

	Upon startup, a client should choose a UUID to send with each request.
	No "connect" message should be sent, if the server doesn't know the UUID,
	it assumes it is effectivly "new".

	The client and server should send a "disconnect" message when they want
	to go away, though it is not required.

	(p) Update() {
		// New requests still get old version.
		p.GetVersion(nextCurrent)
		// New requests start to get nextCurrent version.
	}

	Use in-memory gRPC connection such as: google.golang.org/grpc/test/bufconn
	or github.com/akutz/memconn for connecting comments.
*/

type NotifyToServer struct {
	Disconnect   bool
	NextAnnounce *time.Time

	Parts   int
	Current []struct {
		Version string
		Parts   int
	}
}

type NotifyToClient struct {
	Disconnect   bool
	NextAnnounce *time.Time // TODO(kardianos): Is this needed?

	Stack []struct {
		Version   string
		Current   bool
		Scheduled *time.Time
	}
}

type Notify interface {
	Subscribe(toServer chan NotifyToServer, toClient chan NotifyToClient) error
}

type NotifyServer struct{}

// Serve runs the notification server and blocks until the server is closed down.
func (n *NotifyServer) Serve(ns NotifyServer) {}
