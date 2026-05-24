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

	pb "github.com/zilliztech/claude-context-go/gen/go/claudecontext/v1"
	"github.com/zilliztech/claude-context-go/internal/config"
	"github.com/zilliztech/claude-context-go/internal/daemon"
	"github.com/zilliztech/claude-context-go/internal/store"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("daemon failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	slog.Info("start daemon")

	cfg, err := config.Default()
	if err != nil {
		slog.Error("load config failed", "err", err)
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
			slog.Error("ensure state directory failed", "path", path, "err", err)
			return fmt.Errorf("ensure state directory %s: %w", path, err)
		}
	}

	if err := os.RemoveAll(cfg.SocketPath); err != nil {
		slog.Error("remove stale socket failed", "path", cfg.SocketPath, "err", err)
		return fmt.Errorf("remove stale socket %s: %w", cfg.SocketPath, err)
	}

	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(context.Background(), "unix", cfg.SocketPath)
	if err != nil {
		slog.Error("listen on unix socket failed", "path", cfg.SocketPath, "err", err)
		return fmt.Errorf("listen on unix socket %s: %w", cfg.SocketPath, err)
	}
	defer listener.Close()

	manager, err := daemon.NewManager(cfg)
	if err != nil {
		slog.Error("create manager failed", "err", err)
		return fmt.Errorf("create manager: %w", err)
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
	goSafe(func() {
		if serveErr := server.Serve(listener); serveErr != nil {
			serveErrCh <- fmt.Errorf("serve gRPC on %s: %w", cfg.SocketPath, serveErr)
		}
	})

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serveErrCh:
		server.Stop()
		return err
	case <-signalCh:
	case <-shutdownCh:
	}

	server.GracefulStop()
	return nil
}

func goSafe(run func()) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("goroutine panic", "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		run()
	}()
}
