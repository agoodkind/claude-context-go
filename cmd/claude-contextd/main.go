// Command claude-contextd runs the local gRPC daemon.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/daemon"
	"goodkind.io/claude-context-go/internal/store"
	"goodkind.io/gklog"
	"goodkind.io/gklog/correlation"
	"google.golang.org/grpc"
)

func main() {
	rootContext := installCorrelationLogger("daemon-boot")
	if err := run(rootContext); err != nil {
		slog.ErrorContext(rootContext, "daemon failed", "err", err)
		os.Exit(1)
	}
}

func correlationHandlerOptions() correlation.HandlerOptions {
	return correlation.HandlerOptions{
		Strict:   true,
		Required: []string{"trace_id", "span_id"},
	}
}

// installCorrelationLogger wraps the default JSON slog handler with a
// correlation handler in strict mode and returns a root context that
// carries the given origin so boot records inherit a trace_id. The boot
// logger writes only to stderr; once the state paths are known,
// installConcernRouter swaps in per-concern files.
func installCorrelationLogger(origin string) context.Context {
	jsonHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(correlation.SlogHandler(jsonHandler, correlationHandlerOptions())))
	rootCorrelation := correlation.New("").WithIdentityAttributes(
		correlation.IdentityAttribute{Key: "origin", Value: origin},
	)
	return correlation.WithContext(context.Background(), rootCorrelation)
}

// installConcernRouter swaps the default logger so records fan out to
// per-concern JSONL files under logsDir while the combined stream stays on
// stderr for the service log. The concern is the first dot-separated segment
// of each message; the daemon concern catches anything without a dot.
func installConcernRouter(logsDir string) {
	combined := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	router := gklog.NewRouter(logsDir, slog.LevelInfo, combined, gklog.RouterOptions{FallbackConcern: "daemon", Rotation: gklog.RotationConfig{}})
	slog.SetDefault(slog.New(correlation.SlogHandler(router, correlationHandlerOptions())))
}

func run(rootContext context.Context) error {
	slog.InfoContext(rootContext, "start daemon")

	cfg, err := config.Default()
	if err != nil {
		slog.ErrorContext(rootContext, "load config failed", "err", err)
		return fmt.Errorf("load default config: %w", err)
	}

	socketPath := flag.String("socket", cfg.SocketPath, "unix socket path")
	stateRoot := flag.String("state-root", cfg.StateRoot, "state root")
	flag.Parse()

	cfg.StateRoot = *stateRoot
	cfg.SocketPath = *socketPath
	cfg.RegistryPath = filepath.Join(cfg.StateRoot, "registry.json")
	cfg.JobsPath = filepath.Join(cfg.StateRoot, "jobs.jsonl")
	cfg.EventsPath = filepath.Join(cfg.StateRoot, "events.jsonl")
	cfg.SocketsDir = filepath.Dir(cfg.SocketPath)
	cfg.LogsDir = filepath.Join(cfg.StateRoot, "logs")
	cfg.LogPath = filepath.Join(cfg.LogsDir, "claude-contextd.log")
	cfg.MerkleDir = filepath.Join(cfg.StateRoot, "merkle")
	cfg.LocksDir = filepath.Join(cfg.StateRoot, "locks")
	cfg.ChunksDir = filepath.Join(cfg.StateRoot, "chunks")

	for _, path := range []string{cfg.StateRoot, cfg.SocketsDir, cfg.LogsDir, cfg.MerkleDir, cfg.LocksDir, cfg.ChunksDir} {
		if err := store.EnsureDir(path); err != nil {
			slog.ErrorContext(rootContext, "ensure state directory failed", "path", path, "err", err)
			return fmt.Errorf("ensure state directory %s: %w", path, err)
		}
	}

	installConcernRouter(cfg.LogsDir)

	if err := os.RemoveAll(cfg.SocketPath); err != nil {
		slog.ErrorContext(rootContext, "remove stale socket failed", "path", cfg.SocketPath, "err", err)
		return fmt.Errorf("remove stale socket %s: %w", cfg.SocketPath, err)
	}

	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(rootContext, "unix", cfg.SocketPath)
	if err != nil {
		slog.ErrorContext(rootContext, "listen on unix socket failed", "path", cfg.SocketPath, "err", err)
		return fmt.Errorf("listen on unix socket %s: %w", cfg.SocketPath, err)
	}
	defer listener.Close()

	manager, err := daemon.NewManager(rootContext, cfg)
	if err != nil {
		slog.ErrorContext(rootContext, "create manager failed", "err", err)
		return fmt.Errorf("create manager: %w", err)
	}

	runtimeContext, cancelRuntime := context.WithCancel(rootContext)
	defer cancelRuntime()
	manager.ResumeOrphanedJobs(runtimeContext)
	daemon.NewBackgroundSync(cfg, manager).Start(runtimeContext)

	server := grpc.NewServer()
	shutdownCh := make(chan struct{}, 1)
	pb.RegisterClaudeContextDaemonServiceServer(server, daemon.NewGRPCServer(manager, func() {
		select {
		case shutdownCh <- struct{}{}:
		default:
		}
	}))

	serveErrCh := make(chan error, 1)
	goSafe(rootContext, func() {
		if serveErr := server.Serve(listener); serveErr != nil {
			serveErrCh <- fmt.Errorf("serve gRPC on %s: %w", cfg.SocketPath, serveErr)
		}
	})

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serveErrCh:
		cancelRuntime()
		server.Stop()
		return err
	case <-signalCh:
	case <-shutdownCh:
	}

	cancelRuntime()
	server.GracefulStop()
	return nil
}

func goSafe(ctx context.Context, run func()) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "goroutine panic", "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		run()
	}()
}
