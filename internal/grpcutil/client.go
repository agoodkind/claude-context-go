// Package grpcutil contains client helpers for talking to the daemon.
package grpcutil

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/gklog/correlation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DialDaemon creates a gRPC client connection to the local daemon
// socket. Callers should wrap their context with [WithCorrelation]
// (or call [correlation.NewOutgoingContext] directly) so the daemon
// receives the trace, span, and request identifiers.
func DialDaemon(ctx context.Context, socketPath string) (*grpc.ClientConn, pb.ClaudeContextDaemonServiceClient, error) {
	connection, err := grpc.NewClient(
		"passthrough:///unix",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		}),
	)
	if err != nil {
		slog.ErrorContext(ctx, "create gRPC client failed", "socket_path", socketPath, "err", err)
		return nil, nil, fmt.Errorf("create gRPC client for %s: %w", socketPath, err)
	}
	connection.Connect()
	return connection, pb.NewClaudeContextDaemonServiceClient(connection), nil
}

// WithCorrelation builds an outgoing-metadata context that carries
// the [correlation.Context] from ctx, building a fresh one when ctx
// has none. Daemon callers wrap each gRPC client invocation with this
// helper instead of using a global interceptor.
func WithCorrelation(ctx context.Context) context.Context {
	ctx, _ = correlation.Ensure(ctx, "")
	return correlation.NewOutgoingContext(ctx)
}
