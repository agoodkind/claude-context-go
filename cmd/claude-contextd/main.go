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
	"time"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/daemon"
	"goodkind.io/claude-context-go/internal/debugserver"
	"goodkind.io/claude-context-go/internal/metrics"
	"goodkind.io/claude-context-go/internal/store"
	"goodkind.io/gklog"
	"goodkind.io/gklog/correlation"
	"goodkind.io/gklog/version"
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

	cfg = applyStatePaths(cfg, *stateRoot, *socketPath)

	for _, path := range []string{cfg.StateRoot, cfg.SocketsDir, cfg.LogsDir, cfg.MerkleDir, cfg.LocksDir, cfg.ChunksDir} {
		if err := store.EnsureDir(path); err != nil {
			slog.ErrorContext(rootContext, "ensure state directory failed", "path", path, "err", err)
			return fmt.Errorf("ensure state directory %s: %w", path, err)
		}
	}

	installConcernRouter(cfg.LogsDir)
	metrics.Register()

	slog.InfoContext(rootContext, "daemon identity", "build", version.String(), "commit", version.Commit, "socket", cfg.SocketPath, "state_root", cfg.StateRoot, "pid", os.Getpid())

	if err := refuseIfDaemonAlreadyServing(rootContext, cfg.SocketPath); err != nil {
		return err
	}
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

	metrics.StartReporter(runtimeContext, time.Duration(cfg.PerfCountersIntervalMS)*time.Millisecond)
	var debugSrv *debugserver.Server
	if cfg.DebugListenerEnabled {
		if debugSrv, err = startDebugServer(runtimeContext, cfg); err != nil {
			return err
		}
		defer stopDebugServer(rootContext, debugSrv)
	}

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

// applyStatePaths derives every state-relative path from the resolved state
// root and socket so the rest of startup reads a fully-populated config.
func applyStatePaths(cfg config.Config, stateRoot string, socketPath string) config.Config {
	cfg.StateRoot = stateRoot
	cfg.SocketPath = socketPath
	cfg.RegistryPath = filepath.Join(cfg.StateRoot, "registry.json")
	cfg.JobsPath = filepath.Join(cfg.StateRoot, "jobs.jsonl")
	cfg.EventsPath = filepath.Join(cfg.StateRoot, "events.jsonl")
	cfg.SocketsDir = filepath.Dir(cfg.SocketPath)
	cfg.LogsDir = filepath.Join(cfg.StateRoot, "logs")
	cfg.LogPath = filepath.Join(cfg.LogsDir, "claude-contextd.log")
	cfg.MerkleDir = filepath.Join(cfg.StateRoot, "merkle")
	cfg.LocksDir = filepath.Join(cfg.StateRoot, "locks")
	cfg.ChunksDir = filepath.Join(cfg.StateRoot, "chunks")
	return cfg
}

// debugShutdownTimeout bounds how long daemon exit waits on the debug
// listener so an in-flight profile request cannot delay shutdown.
const debugShutdownTimeout = 2 * time.Second

// startDebugServer binds the loopback pprof and expvar listener. The bind is
// eager so a port conflict surfaces during startup rather than inside the
// serving goroutine.
// refuseIfDaemonAlreadyServing returns an error when another claude-contextd is
// already accepting connections on socketPath, so a second instance never
// clobbers the live one's socket and steals its clients. A stale socket file
// with no listener (the normal case after a crash or a kill-and-restart) fails
// to dial and is cleared by the caller before binding.
func refuseIfDaemonAlreadyServing(ctx context.Context, socketPath string) error {
	dialContext, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	var dialer net.Dialer
	conn, err := dialer.DialContext(dialContext, "unix", socketPath)
	if err != nil {
		return nil
	}
	if closeErr := conn.Close(); closeErr != nil {
		slog.WarnContext(ctx, "close probe connection failed", "path", socketPath, "err", closeErr)
	}
	conflict := fmt.Errorf("another claude-contextd is already listening on %s", socketPath)
	slog.ErrorContext(ctx, "another claude-contextd is already serving this socket; refusing to start", "path", socketPath, "err", conflict)
	return conflict
}

func startDebugServer(ctx context.Context, cfg config.Config) (*debugserver.Server, error) {
	srv, err := debugserver.New(cfg.DebugListenAddr)
	if err != nil {
		slog.ErrorContext(ctx, "create debug listener failed", "addr", cfg.DebugListenAddr, "err", err)
		return nil, fmt.Errorf("create debug listener on %s: %w", cfg.DebugListenAddr, err)
	}
	if err := srv.Start(ctx); err != nil {
		slog.ErrorContext(ctx, "start debug listener failed", "addr", cfg.DebugListenAddr, "err", err)
		return nil, fmt.Errorf("start debug listener on %s: %w", cfg.DebugListenAddr, err)
	}
	slog.InfoContext(ctx, "debug listener started", "addr", srv.Addr())
	return srv, nil
}

// stopDebugServer shuts the debug listener within a bounded window. A nil
// server is a no-op so callers need not branch on whether it was started.
func stopDebugServer(ctx context.Context, srv *debugserver.Server) {
	if srv == nil {
		return
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, debugShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.WarnContext(ctx, "debug listener shutdown failed", "err", err)
	}
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
