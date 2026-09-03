# `make help` is the canonical source of truth for every target this repo
# supports. Run it before adding anything new. Lint, build, test, deadcode,
# release, baseline, and service-install all live in the central go-makefile
# pipeline fetched at parse time. Do NOT add project-local lint, deadcode,
# audit, fmt, vet, or staticcheck targets here. They duplicate the central
# pipeline and let agents bypass strict rules.

# Identity. This repo has no own version package; it cross-stamps gklog/version.
BINARY := lm-semantic-search-daemon
CMD    := ./cmd/lm-semantic-search-daemon
GKLOG_VPKG := goodkind.io/gklog/version
CLI_BINARY := lm-semantic-search
CLI_CMD := ./cmd/$(CLI_BINARY)
MCP_BINARY := lm-semantic-search-mcp
MCP_CMD := ./cmd/$(MCP_BINARY)

# make install builds and installs the daemon plus both client binaries.
INSTALL_BINS := $(BINARY):$(CMD) $(CLI_BINARY):$(CLI_CMD) $(MCP_BINARY):$(MCP_CMD)
RELEASE_BINS := $(INSTALL_BINS)
# The go-makefile compile matrix sets RELEASE_PLATFORMS per job to that job's
# single target, so use ?= to defer to that environment value. A plain := would
# override it and make every job rebuild all listed platforms with one
# toolchain, so the Linux job would cross-build darwin with gcc and fail on the
# Darwin-only -arch flag. This default drives local make release. darwin/amd64
# is omitted because ONNX Runtime 1.27.0 ships no osx-x64 binary; the CI and
# release workflow inputs (release_platforms / platforms) exclude it from the
# matrix.
RELEASE_PLATFORMS ?= darwin/arm64 linux/amd64 linux/arm64
# go-makefile resolves codesign and quill through PATH. These generated command
# adapters add the daemon entitlement without applying it to either client.
# The release package job runs quill on Linux, so this path must not depend on
# the build host operating system.
SIGNING_WRAPPER_DIR := $(CURDIR)/.make/signing-bin
export PATH := $(SIGNING_WRAPPER_DIR):$(PATH)

# Pipeline modules. Add go-service.mk if this binary ships as a daemon and
# set LAUNCHD_LABEL, SYSTEMD_UNIT, LOG_PATH before -include $(GO_MK).
GO_MK_MODULES := go-build.mk go-release.mk go-service.mk
export GO_MK_PLATFORM_STUB_OPTIONAL := goodkind.io/lm-semantic-search/internal/platformactivity
BUILD_CHECKS := true
STATICCHECK_EXTRA_FLAGS = $(STATICCHECK_EXTRA_CORE_FLAGS) $(STATICCHECK_EXTRA_STRICT_FLAGS)
# Cold Linux runners need more than the generated three-minute default.
GOLANGCI_LINT_RUN_FLAGS = $(GOLANGCI_LINT_FLAGS) --allow-parallel-runners --timeout=10m \
	$(if $(filter-out 0 auto,$(strip $(LINT_CONCURRENCY))),--concurrency=$(LINT_CONCURRENCY))
# go.mk owns the rest of the cgo contract: the per-target GO_MK_CGO_PREFIX with
# its host fallback, the PKG_CONFIG_PATH export, the go-mk-cgo-deps prerequisite
# on every compile-bearing target, and the GO_MK_CC/GO_MK_CXX toolchain
# resolution into CC/CXX for the dep recipe.
GO_MK_CGO_DEPS := cbm onnxruntime tokenizers
GO_MK_CGO_CACHE_VERSIONS := onnxruntime=1.27.0 tokenizers=1.27.0
GO_MK_CGO_CACHE_INPUTS := cmd/onnxruntime-dep cmd/tokenizers-dep
export CGO_LDFLAGS_ALLOW := -Wl,-rpath,@loader_path
ifeq ($(shell uname),Darwin)
GO_MK_INSTALL_POST_CMD = \
	if [ -w "$(INSTALL_DIR)" ]; then \
		cp -P "$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.1.27.0.dylib" \
			"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.1.dylib" \
			"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.dylib" "$(INSTALL_DIR)/"; \
	else \
		sudo cp -P "$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.1.27.0.dylib" \
			"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.1.dylib" \
			"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.dylib" "$(INSTALL_DIR)/"; \
	fi
endif
ifeq ($(shell uname),Linux)
GO_MK_INSTALL_POST_CMD = \
	if [ -w "$(INSTALL_DIR)" ]; then \
		cp -P "$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.so.1.27.0" \
			"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.so.1" \
			"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.so" "$(INSTALL_DIR)/"; \
	else \
		sudo cp -P "$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.so.1.27.0" \
			"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.so.1" \
			"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.so" "$(INSTALL_DIR)/"; \
	fi
endif

LAUNCHD_LABEL := io.goodkind.lm-semantic-search-daemon
SYSTEMD_UNIT := lm-semantic-search-daemon.service
# macOS launchd captures the daemon's stderr to this file; Linux logs to journald
# (the systemd unit sets no file path), so LOG_PATH there is only a harmless default.
ifeq ($(shell uname),Darwin)
LOG_PATH := $(HOME)/Library/Logs/lm-semantic-search-daemon.log
else
LOG_PATH := $(HOME)/.lm-semantic-search/logs/lm-semantic-search-daemon.log
endif

# go.mk runs these as order-only prerequisites of every build, lint, vet, test,
# and govulncheck target (and install/release via the modules). GO_MK_GENERATE
# generates the tree-sitter parser; GO_MK_WORKSPACE_USE materializes a gitignored
# go.work that routes the gksyntax submodule into the build, so both exist before
# any target compiles the grammar packages.
GO_MK_GENERATE := gksyntax-grammars
GO_MK_GENERATE_INPUTS := third_party/gksyntax
GO_MK_GENERATE_OUTPUTS := \
	third_party/gksyntax/treesitter/grammars/swift/upstream/src/parser.c \
	third_party/gksyntax/treesitter/grammars/swift/upstream/src/tree_sitter/parser.h \
	third_party/gksyntax/treesitter/grammars/swift/upstream/src/tree_sitter/array.h \
	third_party/gksyntax/treesitter/grammars/swift/upstream/src/tree_sitter/alloc.h
GO_MK_WORKSPACE_USE := . third_party/gksyntax

daemon-entitlements-signer:
	@mkdir -p "$(SIGNING_WRAPPER_DIR)"
	go build -o "$(SIGNING_WRAPPER_DIR)/codesign" ./cmd/daemon-entitlements-signer
	@ln -sf codesign "$(SIGNING_WRAPPER_DIR)/quill"

go-mk-cgo-dep-cbm: scripts/setup-cgo-cbm.sh scripts/cbm-lib.mk third_party/cbm/Makefile.cbm
	"$(CURDIR)/scripts/setup-cgo-cbm.sh"

go-mk-cgo-dep-onnxruntime:
	CGO_ENABLED=0 go run ./cmd/onnxruntime-dep

go-mk-cgo-dep-tokenizers:
	CGO_ENABLED=0 go run ./cmd/tokenizers-dep

# bootstrap.mk fetches go.mk + golangci.yml + every module in GO_MK_MODULES
# at parse time and -includes them. Update path: edit go-makefile/bootstrap.mk,
# then refresh consumer copies (one-off cp; not enshrined as infrastructure).
include bootstrap.mk

build install release: | daemon-entitlements-signer

.DEFAULT_GOAL := check

# ---------------------------------------------------------------------------
# Project-local
# ---------------------------------------------------------------------------

.PHONY: daemon-entitlements-signer go-mk-cgo-dep-cbm go-mk-cgo-dep-onnxruntime go-mk-cgo-dep-tokenizers deploy deploy-service daemon-wait daemon-status kill-orphans live offline-live service-activity-live restart-acceptance-unit restart-acceptance proto

# live runs the opt-in conversation-marker validation suite against a real local
# Milvus, fully isolated from the operator's daemon (build tag `live`). It reuses
# the go.mk order-only prerequisites so the gksyntax grammars, go.work routing,
# and cgo libraries exist before the suite compiles. Milvus must be reachable; when
# it is not, each test skips with an environment note rather than failing.
live: | $(GO_MK_PREREQS)
	go test -tags live -count=1 ./test/live/

# offline-live runs the self-contained offline-profile acceptance suite against
# an isolated in-process daemon, embedded vector store, and embedded ONNX model.
offline-live: | $(GO_MK_PREREQS)
	go test -tags offlinelive -count=1 ./test/offlinelive/

# service-activity-live reads the default installed daemon only. It never starts
# a replacement process or writes operator indexing state.
service-activity-live: | $(GO_MK_PREREQS)
	go test -tags serviceactivitylive -count=1 ./test/serviceactivity/

# restart-acceptance-unit runs the deterministic restart acceptance tests.
restart-acceptance-unit: | $(GO_MK_PREREQS)
	go test -tags restartacceptance -count=1 ./test/restartacceptance/

# restart-acceptance runs the opt-in acceptance harness only against the restored clone.
restart-acceptance: | $(GO_MK_PREREQS)
	@test "$$LMS_RESTART_ACCEPTANCE_CONFIRM" = "isolated-clone" || { \
		echo 'restart-acceptance: LMS_RESTART_ACCEPTANCE_CONFIRM must equal "isolated-clone"' >&2; \
		exit 1; \
	}
	go test -timeout=2h -tags 'restartacceptance restartacceptancelive' -count=1 -run '^TestRestartAcceptance$$' ./test/restartacceptance/
# daemon-status and daemon-wait call the installed CLI; kill-orphans matches the
# installed MCP binary by name.
CLI_INSTALL_BIN := $(INSTALL_DIR)/$(CLI_BINARY)

# ---------------------------------------------------------------------------
# Protobuf
# ---------------------------------------------------------------------------
# proto regenerates the Go bindings under gen/go from proto/lmsemanticsearch/v1,
# using the module declared in buf.yaml and the plugins declared in
# buf.gen.yaml. It is a manual target, run by hand after editing a .proto file,
# not a GO_MK_GENERATE prerequisite: gen/go is committed rather than produced by
# every build, lint, and test run, so wiring codegen in there would run buf on
# every target for no reason. Commit the regenerated files under gen/go after
# running this; nothing under gen/ is hand-edited.
#
# This fails loudly instead of silently doing nothing when buf is missing,
# because a no-op codegen target would let stale generated code pass as
# regenerated.
proto:
	@command -v buf >/dev/null 2>&1 || { \
		echo "proto: buf is not installed on PATH; install it (for example: brew install bufbuild/buf/buf) and re-run make proto" >&2; \
		exit 1; \
	}
	buf generate

# ---------------------------------------------------------------------------
# gksyntax submodule grammars
# ---------------------------------------------------------------------------
# The AST splitter and tree-sitter grammars live in goodkind.io/gksyntax, a git
# submodule under third_party/ routed through a generated, gitignored go.work
# (GO_MK_WORKSPACE_USE above). A plain module require is not possible because
# gksyntax vendors the dart and swift grammars as its own submodules, whose C
# sources are absent from a Go module zip, and a go.mod replace is rejected by
# gomoddirectives. gksyntax commits only the swift grammar definition, not the
# generated parser, so the parser is produced from the pinned submodule by the
# tree-sitter CLI. The generated parser stays inside the submodule working tree
# (gitignored there) and is never committed.
GKS_DIR := third_party/gksyntax
SWIFT_GRAMMAR_DIR := $(GKS_DIR)/treesitter/grammars/swift/upstream
SWIFT_GRAMMAR_DEF := $(SWIFT_GRAMMAR_DIR)/src/grammar.json
SWIFT_GRAMMAR_PARSER := $(SWIFT_GRAMMAR_DIR)/src/parser.c
TREE_SITTER_ABI ?= 14
# tree-sitter CLI lands here when the host has none on PATH. Gitignored.
TREE_SITTER_LOCAL_DIR := $(CURDIR)/.bin

.PHONY: gksyntax-grammars
gksyntax-grammars:
	@status="$$(git submodule status --recursive $(GKS_DIR))"; \
	if printf '%s\n' "$$status" | grep -q '^U'; then \
		echo "gksyntax-grammars: $(GKS_DIR) has unresolved submodule conflicts"; \
		exit 1; \
	fi; \
	if printf '%s\n' "$$status" | grep -Eq '^[+-]'; then \
		git -c url.https://github.com/.insteadOf=git@github.com: submodule update --init --recursive $(GKS_DIR); \
	fi
	@if [ ! -f "$(SWIFT_GRAMMAR_DEF)" ]; then \
		echo "gksyntax-grammars: $(SWIFT_GRAMMAR_DIR) is empty; run 'git submodule update --init --recursive'"; \
		exit 1; \
	fi
	@ts_bin="$$(command -v tree-sitter 2>/dev/null || true)"; \
	if [ -z "$$ts_bin" ]; then \
		"$(GKS_DIR)/scripts/install-tree-sitter.sh" "$(TREE_SITTER_LOCAL_DIR)"; \
		ts_bin="$(TREE_SITTER_LOCAL_DIR)/tree-sitter"; \
	fi; \
	if [ ! -f "$(SWIFT_GRAMMAR_PARSER)" ] || [ "$(SWIFT_GRAMMAR_DEF)" -nt "$(SWIFT_GRAMMAR_PARSER)" ]; then \
		echo "gksyntax-grammars: generating Swift parser (abi $(TREE_SITTER_ABI))"; \
		( cd "$(SWIFT_GRAMMAR_DIR)" && "$$ts_bin" generate src/grammar.json --abi $(TREE_SITTER_ABI) ); \
		git -C "$(SWIFT_GRAMMAR_DIR)" checkout -- . >/dev/null 2>&1 || true; \
	else \
		echo "gksyntax-grammars: Swift parser already generated"; \
	fi

# The order-only prerequisite that runs gksyntax-grammars before every compile,
# vet, lint, test, install, and release target is wired centrally in go.mk via
# GO_MK_GENERATE (set above), so no per-target list is maintained here.

# install runs as the inherited `deploy: install` prerequisite from
# go-build.mk; repeating it here as a recipe line runs the whole
# build-check + codesign pipeline a second time in a fresh sub-make.
deploy:
	$(MAKE) deploy-service
	$(MAKE) daemon-wait
	$(MAKE) daemon-status

# Probe loadedness silently instead of letting service-restart fail with
# a make error on a host where the service is not installed yet.
deploy-service:
	@if [ "$$(uname)" = "Darwin" ]; then \
		if launchctl print "$(LAUNCHD_DOMAIN)/$(LAUNCHD_LABEL)" >/dev/null 2>&1; then \
			$(MAKE) service-restart; \
		else \
			echo "deploy-service: $(LAUNCHD_LABEL) not loaded; installing user service"; \
			launchctl enable "$(LAUNCHD_DOMAIN)/$(LAUNCHD_LABEL)" >/dev/null 2>&1 || true; \
			$(MAKE) service-install; \
		fi; \
	else \
		if systemctl --user cat "$(SYSTEMD_UNIT)" >/dev/null 2>&1; then \
			$(MAKE) service-restart; \
		else \
			echo "deploy-service: $(SYSTEMD_UNIT) not installed; installing user service"; \
			$(MAKE) service-install; \
		fi; \
	fi

daemon-status:
	"$(CLI_INSTALL_BIN)" daemon status

daemon-wait:
	@for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
		if status_output="$$( "$(CLI_INSTALL_BIN)" daemon status )"; then \
			exit 0; \
		fi; \
		sleep 0.25; \
	done; \
	"$(CLI_INSTALL_BIN)" daemon status

# kill-orphans walks every running lm-semantic-search-mcp process and sends
# SIGKILL when its parent PID is 1 (init). Active sessions with a live parent
# stay untouched. This is the mitigation for the orphan-pile failure mode
# (199 zombies on the host pushing system load to 28) that bit the upstream
# TS adapter; the Go adapter avoids this by exiting on stdin EOF, PPID poll,
# and panic recovery, but this target stays as a paranoia cleanup.
kill-orphans:
	@killed=0; preserved=0; \
	for pid in $$(pgrep -x $(MCP_BINARY) || true); do \
		parent=$$(ps -o ppid= -p "$$pid" | tr -d ' '); \
		if [ "$$parent" = "1" ]; then \
			echo "kill-orphans: SIGKILL pid=$$pid (orphan, ppid=1)"; \
			kill -9 "$$pid" && killed=$$((killed + 1)); \
		else \
			parent_command=$$(ps -o comm= -p "$$parent" 2>&1 | head -n1); \
			echo "kill-orphans: preserve pid=$$pid (ppid=$$parent $$parent_command)"; \
			preserved=$$((preserved + 1)); \
		fi; \
	done; \
	echo "kill-orphans: killed $$killed orphan(s), preserved $$preserved live adapter(s)"
