# claude-context-go

A ground-up Go rewrite of the Claude Context runtime, owning the daemon, the operator CLI, and the MCP adapter; VS Code and Chrome extension clients are out of scope here.

Original TypeScript implementation: `github.com/zilliztech/claude-context`.

## Transport Contract

The daemon transport is `gRPC` only, with protobuf definitions managed by `buf`, and this repo does not define or accept a JSON-RPC control plane.

Sources:

- `proto/claudecontext/v1/service.proto`
- `buf.yaml`
- `buf.gen.yaml`

## Binaries

- `claude-contextd`: long-lived daemon.
- `claude-context`: operator CLI.
- `claude-context-mcp`: MCP stdio adapter that forwards every tool call to the daemon over its unix socket.

## Build

All validation should use the local `go-makefile` checkout:

```sh
GO_MK_DEV_DIR=$HOME/Sites/go-makefile make check
GO_MK_DEV_DIR=$HOME/Sites/go-makefile make test
GO_MK_DEV_DIR=$HOME/Sites/go-makefile make build
GO_MK_DEV_DIR=$HOME/Sites/go-makefile make staticcheck-extra
```

## Install And Deploy

This repo follows the same local deploy shape as `~/Sites/agent-gate`:

- `make install` installs the daemon binary through the shared `go-build.mk` path.
- `make install-clients` installs `claude-context` and `claude-context-mcp`.
- `make deploy` performs local install, restarts or installs the user service, waits for readiness, and then reports daemon status.
- `make deploy-service` uses the shared `go-service.mk` restart-first, install-on-failure pattern.
- `make daemon-status` checks the daemon through the installed CLI.
- `make daemon-wait` polls the installed CLI until the daemon is reachable.
- `make kill-orphans` is a paranoia cleanup that SIGKILLs any `claude-context-mcp` process whose PPID is `1`; live sessions with a real parent stay untouched.

## Embeddings

The Go port supports exactly one embedding provider, an OpenAI-compatible HTTP adapter, and `OPENAI_BASE_URL` can point at any endpoint that speaks the OpenAI embeddings API (OpenAI itself, an OpenRouter proxy, a self-hosted Ollama with its `/v1/embeddings` shim, a local `lmd-serve`, and similar). Provider-specific clients for VoyageAI, Gemini, and native Ollama have been removed in favor of this single path.

Configuration is read in this order (highest precedence first):

1. Process environment variables.
2. `~/.context/.env` (KEY=VALUE pairs, comments via `#`, respects already-set env vars).
3. `~/.contextd/config.json` (persisted daemon defaults).

Required keys:

- `EMBEDDING_PROVIDER=OpenAI`
- `OPENAI_API_KEY=<key>`
- `OPENAI_BASE_URL=<base URL>` (omit when using the public OpenAI endpoint).
- `EMBEDDING_MODEL=<model id served by the upstream>`
- `MILVUS_ADDRESS=<host:port>` for the vector backend.

Optional:

- `EMBEDDING_DIMENSION=<int>` requests a non-default output dimension.
- `EMBEDDING_BATCH_SIZE=<int>` (default 32) is the number of chunks per embedding HTTP call.
- `CUSTOM_EXTENSIONS=.ext1,.ext2,...` adds extra file extensions to discovery.
- `CUSTOM_IGNORE_PATTERNS=glob1,glob2,...` adds extra ignore patterns.
- `HYBRID_MODE=true|false` (default true) toggles Milvus BM25 + dense hybrid.
- `CODE_CHUNKS_COLLECTION_NAME_OVERRIDE` adds a readable infix to collection names.
- `CLAUDE_CONTEXT_BACKGROUND_SYNC=true|false` (default true) enables periodic sync.
- `CLAUDE_CONTEXT_TRIGGER_WATCHER=true|false` (default true) watches `~/.context/.sync-trigger`.
- `CLAUDE_CONTEXT_SYNC_INTERVAL_MS` and `CLAUDE_CONTEXT_SYNC_LOCK_STALE_MS` tune sync cadence and lock recovery.

## Splitter

- **AST** (default): `tree-sitter` parsers for JavaScript, TypeScript, Python, Java, C, C++, Go, Rust, Scala, and C#, with chunks falling on class, function, method, and interface declarations.
- **`langchain`** (opt-in via `splitter: "langchain"` per index request): a recursive separator splitter that mirrors LangChain JS `RecursiveCharacterTextSplitter.fromLanguage`, with per-language separator tables for js, python, java, cpp, go, rust, php, ruby, swift, scala, html, markdown, latex, and sol.

## Incremental Sync

Per-codebase Merkle snapshots live under `~/.contextd/merkle/<codebase-id>.json`.

On every `sync` request:

1. The daemon captures a new snapshot of the codebase.
2. `merkle.DiffSnapshots` computes `{Added, Modified, Removed}`.
3. An empty diff completes the job as a no-op tagged "already up to date".
4. The indexer otherwise processes only added and modified files, and `semantic.Reindex` deletes existing Milvus rows for `relative_path in (removed + modified)` before upserting the new chunks.
5. The new snapshot is persisted only after success.

A missing previous snapshot or a missing semantic collection routes back to a full `Replace`.

## Idempotency

Concurrent MCP requests for the same codebase deduplicate against any in-flight job with a matching effective config, and that includes `force=true` requests so N parallel `index_codebase(force=true)` calls collapse to a single embedding pass instead of cancelling each other in sequence. This defensive shape prevents the machine-blowing-up failure mode where a client fan-out can otherwise launch arbitrary parallel work.

## Orphan Guard

The MCP adapter must exit when its parent process (Claude Code, Cursor, an editor) exits. Three independent defenses make sure that happens:

1. **stdin EOF**: the parent closes its end of the pipe, the read loop returns, and the process unwinds.
2. **PPID watcher**: `internal/mcpserver/orphan_guard.go` polls `os.Getppid()` every 2 seconds and cancels the run context when it returns `1` (reparented to init).
3. **Panic recovery**: `cmd/claude-context-mcp/main.go` wraps `mcpserver.Run` in a deferred recover that forces `os.Exit(1)` so a panic in any goroutine takes the whole process down rather than leaving a half-dead orphan.

`make kill-orphans` remains as a paranoia cleanup for any orphan that somehow escapes all three defenses.

## Deliberately Not Supported

The Go port is local- and self-hosted-only. The following upstream surfaces are intentionally absent:

- Zilliz Cloud auto-provisioning, `ClusterManager`, free-cluster creation.
- `checkCollectionLimit()` and the Zilliz pricing surface.
- `syncIndexedCodebasesFromCloud()` and description-based recovery.
- `MILVUS_TOKEN`-based address auto-resolution.
- The `MilvusRestfulVectorDatabase` REST client.
- VS Code and Chrome extension packages.
- Telemetry and hosted-service hooks.
- Dedicated VoyageAI, Gemini, and Ollama embedding clients (use an OpenAI-compatible proxy with `OPENAI_BASE_URL` instead).
