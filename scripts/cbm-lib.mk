LMS_AR ?= $(AR)
LMS_OBJCOPY ?= objcopy
LMS_NM ?= $(NM)

# Mirror the engine binary's own source set (PROD_SRCS plus the extraction and
# vendored-compression sources), excluding only src/main.c, which is the CLI
# entrypoint and is not part of PROD_SRCS. CLI_SRCS/UI_SRCS stay in: src/cli/cli.c
# defines library helpers (cbm_config_get_*, cbm_cli_get_version,
# cbm_compare_versions) that the retained core objects reference, so dropping them
# leaves the archive with undefined symbols. The keep-list still localizes the
# final archive to the 14 cbm_* API symbols at the relocatable link.
LMS_API_C_SRCS := internal/cbm/cbm.c src/store/store.c src/pipeline/pipeline.c src/mcp/mcp.c src/cypher/cypher.c
LMS_LIB_C_SRCS := $(filter-out $(LMS_API_C_SRCS),$(filter %.c,$(PROD_SRCS) $(EXTRACTION_SRCS) $(AC_LZ4_SRCS) $(ZSTD_SRCS) $(SQLITE_WRITER_SRC)))
LMS_OBJ_DIR := $(BUILD_DIR)/lms
LMS_LIB_OBJS := $(patsubst %.c,$(LMS_OBJ_DIR)/%.o,$(LMS_LIB_C_SRCS))
LMS_API_OBJS := $(patsubst %.c,$(LMS_OBJ_DIR)/%.o,$(LMS_API_C_SRCS))
LMS_VENDORED_OBJS := $(filter-out $(MIMALLOC_OBJ_PROD),$(OBJS_VENDORED_PROD)) $(LMS_OBJ_DIR)/lms_mimalloc.o
LMS_KEEP_LIST := $(LMS_OBJ_DIR)/cbm-exported-symbols.txt
LMS_COMBINED_OBJ := $(LMS_OBJ_DIR)/libcbm_engine.o

LIBGIT2_FLAGS :=
LIBGIT2_LIBS :=

ifeq ($(LMS_TARGET_GOOS),darwin)
LMS_SYMBOL_PREFIX := _
ifeq ($(LMS_TARGET_GOARCH),arm64)
LMS_DARWIN_ARCH := arm64
else ifeq ($(LMS_TARGET_GOARCH),amd64)
LMS_DARWIN_ARCH := x86_64
else
$(error unsupported darwin GOARCH $(LMS_TARGET_GOARCH))
endif
else ifeq ($(LMS_TARGET_GOOS),linux)
LMS_SYMBOL_PREFIX :=
else
$(error unsupported GOOS $(LMS_TARGET_GOOS))
endif

.PHONY: lms-cbm-lib

$(LMS_API_OBJS): $(LMS_OBJ_DIR)/%.o: %.c | $(BUILD_DIR)
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS_PROD) -fvisibility=hidden -fvisibility=default -fno-common -c -o $@ $<

$(LMS_OBJ_DIR)/%.o: %.c | $(BUILD_DIR)
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS_PROD) -fvisibility=hidden -fno-common -c -o $@ $<

$(LMS_OBJ_DIR)/lms_mimalloc.o: $(MIMALLOC_SRC) | $(BUILD_DIR)
	mkdir -p $(dir $@)
	$(CC) -std=c11 -O2 -w -Ivendored/mimalloc/include -Ivendored/mimalloc/src -DMI_OVERRIDE=0 -fvisibility=hidden -fno-common -c -o $@ $<

$(LMS_KEEP_LIST): | $(BUILD_DIR)
	mkdir -p $(dir $@)
	{ \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_alloc_init'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_store_open_path'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_store_open_path_query'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_store_close'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_pipeline_new'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_pipeline_run'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_pipeline_free'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_mcp_server_new'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_mcp_server_set_project'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_mcp_server_set_config'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_mcp_handle_tool'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_mcp_server_free'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_cypher_execute'; \
	    printf '%s\n' '$(LMS_SYMBOL_PREFIX)cbm_cypher_result_free'; \
	} >$@

ifeq ($(LMS_TARGET_GOOS),darwin)
$(LMS_COMBINED_OBJ): $(LMS_LIB_OBJS) $(LMS_API_OBJS) $(LMS_VENDORED_OBJS) $(LMS_KEEP_LIST) | $(BUILD_DIR)
	mkdir -p $(dir $@)
	$(CC) -r -arch $(LMS_DARWIN_ARCH) -Wl,-exported_symbols_list,$(LMS_KEEP_LIST) -o $@ $(LMS_LIB_OBJS) $(LMS_API_OBJS) $(LMS_VENDORED_OBJS)
else ifeq ($(LMS_TARGET_GOOS),linux)
$(LMS_COMBINED_OBJ): $(LMS_LIB_OBJS) $(LMS_API_OBJS) $(LMS_VENDORED_OBJS) $(LMS_KEEP_LIST) | $(BUILD_DIR)
	mkdir -p $(dir $@)
	$(CC) -r -nostdlib -o $@ $(LMS_LIB_OBJS) $(LMS_API_OBJS) $(LMS_VENDORED_OBJS)
	$(LMS_OBJCOPY) --keep-global-symbols=$(LMS_KEEP_LIST) $@
endif

lms-cbm-lib: $(LMS_COMBINED_OBJ)
	mkdir -p $(dir $(LMS_ARCHIVE))
	rm -f $(LMS_ARCHIVE)
	$(LMS_AR) crs $(LMS_ARCHIVE) $(LMS_COMBINED_OBJ)
	@symbols="$$($(LMS_NM) $(if $(filter darwin,$(LMS_TARGET_GOOS)),-gU,-g --defined-only) $(LMS_ARCHIVE))"; \
	while IFS= read -r symbol; do \
		count="$$(printf '%s\n' "$$symbols" | awk -v symbol="$$symbol" '$$NF == symbol { count += 1 } END { print count + 0 }')"; \
		if [ "$$count" -ne 1 ]; then \
			printf '%s\\n' "lms-cbm-lib: expected exactly one definition of $$symbol, found $$count" >&2; \
			exit 1; \
		fi; \
	done <$(LMS_KEEP_LIST)
