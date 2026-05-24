# claude-context-go

`claude-context-go` is a ground-up Go rewrite of the Claude Context runtime.
This repo owns the daemon, operator CLI, and MCP adapter. VS Code and Chrome
clients are intentionally out of scope here.

## Transport Contract

The daemon transport is `gRPC` only, with protobuf definitions managed by
`buf`. This repo does not define or accept a JSON-RPC control plane.

The primary service contract lives in:

- `proto/claudecontext/v1/service.proto`

Code generation is driven by:

- `buf.yaml`
- `buf.gen.yaml`

## Binaries

- `claude-contextd`: long-lived daemon
- `claude-context`: operator CLI
- `claude-context-mcp`: MCP stdio adapter that forwards to the daemon

## Build

All validation should use the local `go-makefile` checkout:

```sh
GO_MK_DEV_DIR=/Users/agoodkind/Sites/go-makefile make check
GO_MK_DEV_DIR=/Users/agoodkind/Sites/go-makefile make test
GO_MK_DEV_DIR=/Users/agoodkind/Sites/go-makefile make build
GO_MK_DEV_DIR=/Users/agoodkind/Sites/go-makefile make staticcheck-extra
```
