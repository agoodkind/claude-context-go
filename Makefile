# `make help` is the canonical source of truth for every target this repo
# supports. Run it before adding anything new. Lint, build, test, deadcode,
# release, baseline, and service-install all live in the central go-makefile
# pipeline fetched at parse time. Do NOT add project-local lint, deadcode,
# audit, fmt, vet, or staticcheck targets here. They duplicate the central
# pipeline and let agents bypass strict rules.

# Identity
BINARY := claude-contextd
CMD    := ./cmd/claude-contextd
VPKG   := goodkind.io/claude-context-go/internal/version
CLI_BINARY := claude-context
CLI_CMD := ./cmd/$(CLI_BINARY)
MCP_BINARY := claude-context-mcp
MCP_CMD := ./cmd/$(MCP_BINARY)

# Pipeline modules. Add go-service.mk if this binary ships as a daemon and
# set LAUNCHD_LABEL, SYSTEMD_UNIT, LOG_PATH before -include $(GO_MK).
GO_MK_MODULES := go-build.mk go-release.mk go-service.mk
BUILD_CHECKS := true
STATICCHECK_EXTRA_FLAGS = $(STATICCHECK_EXTRA_CORE_FLAGS) $(STATICCHECK_EXTRA_STRICT_FLAGS)

LAUNCHD_LABEL := io.zilliz.claude-contextd
SYSTEMD_UNIT := claude-contextd.service
LOG_PATH := $(HOME)/.contextd/logs/claude-contextd.log

# bootstrap.mk fetches go.mk + golangci.yml + every module in GO_MK_MODULES
# at parse time and -includes them. Update path: edit go-makefile/bootstrap.mk,
# then refresh consumer copies (one-off cp; not enshrined as infrastructure).
include bootstrap.mk

.DEFAULT_GOAL := check

# ---------------------------------------------------------------------------
# Project-local
# ---------------------------------------------------------------------------

.PHONY: build-clients install-clients deploy deploy-service daemon-wait daemon-status kill-orphans

CLI_DIST_BIN := $(DIST_DIR)/$(CLI_BINARY)
MCP_DIST_BIN := $(DIST_DIR)/$(MCP_BINARY)
CLI_INSTALL_BIN := $(INSTALL_DIR)/$(CLI_BINARY)
MCP_INSTALL_BIN := $(INSTALL_DIR)/$(MCP_BINARY)

build-clients:
	@mkdir -p "$(DIST_DIR)"
	go build $(GO_BUILD_FLAGS) -o "$(CLI_DIST_BIN)" $(CLI_CMD)
	@echo "built: $(CLI_DIST_BIN)"
	$(call codesign_binary,$(CLI_DIST_BIN))
	go build $(GO_BUILD_FLAGS) -o "$(MCP_DIST_BIN)" $(MCP_CMD)
	@echo "built: $(MCP_DIST_BIN)"
	$(call codesign_binary,$(MCP_DIST_BIN))

install-clients: build-clients
	@printf 'install: installing %s to %s\n' '$(CLI_BINARY)' '$(CLI_INSTALL_BIN)'
	@mkdir -p "$(INSTALL_DIR)"
	@out="$$(mktemp "$(CLI_INSTALL_BIN)".new.XXXXXX)"; \
	trap 'rm -f "$$out"' EXIT; \
	cp -f "$(CLI_DIST_BIN)" "$$out"; \
	chmod 0755 "$$out"; \
	test -s "$$out"; \
	mv -f "$$out" "$(CLI_INSTALL_BIN)"
	@printf 'install: installing %s to %s\n' '$(MCP_BINARY)' '$(MCP_INSTALL_BIN)'
	@out="$$(mktemp "$(MCP_INSTALL_BIN)".new.XXXXXX)"; \
	trap 'rm -f "$$out"' EXIT; \
	cp -f "$(MCP_DIST_BIN)" "$$out"; \
	chmod 0755 "$$out"; \
	test -s "$$out"; \
	mv -f "$$out" "$(MCP_INSTALL_BIN)"

deploy:
	$(MAKE) install
	$(MAKE) install-clients
	$(MAKE) deploy-service
	$(MAKE) daemon-wait
	$(MAKE) daemon-status

deploy-service:
	@$(MAKE) service-restart || { \
		echo "service restart failed; installing user service"; \
		$(MAKE) service-install; \
		if [ "$$(uname)" = "Darwin" ]; then \
			launchctl enable "$(LAUNCHD_DOMAIN)/$(LAUNCHD_LABEL)" || true; \
		fi; \
		$(MAKE) service-restart; \
	}

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

# kill-orphans walks every running claude-context-mcp process and sends
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
