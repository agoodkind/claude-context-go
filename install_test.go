package lmsemanticsearch_test

import (
	"os"
	"strings"
	"testing"
)

func TestInstallerSystemdRestartPolicyAlwaysRelaunchesDaemon(t *testing.T) {
	data, err := os.ReadFile("install.sh")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	if !strings.Contains(contents, "Restart=always") {
		t.Fatalf("systemd unit does not restart the daemon after a clean shutdown")
	}
	if strings.Contains(contents, "Restart=on-failure") {
		t.Fatalf("systemd unit still uses on-failure restart policy")
	}
}

func TestInstallerDaemonServiceNameUsesCurrentProduct(t *testing.T) {
	data, err := os.ReadFile("install.sh")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	if strings.Contains(contents, "Claude Context") {
		t.Fatalf("install.sh contains stale product name Claude Context")
	}
	if !strings.Contains(contents, "Description=lm-semantic-search daemon") {
		t.Fatalf("systemd unit description does not name lm-semantic-search daemon")
	}
}

func TestDependencyToolsRunWithoutInheritedCgo(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	for _, command := range []string{
		"CGO_ENABLED=0 go run ./cmd/onnxruntime-dep",
		"CGO_ENABLED=0 go run ./cmd/tokenizers-dep",
	} {
		if !strings.Contains(contents, command) {
			t.Fatalf("Makefile does not isolate dependency tool command %q from cgo", command)
		}
	}
}

func TestLinuxONNXBridgeUsesOriginRelativeRunpath(t *testing.T) {
	data, err := os.ReadFile("internal/embedding/onnx.go")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	const directive = "#cgo linux LDFLAGS: -Wl,-rpath,$ORIGIN"
	if !strings.Contains(contents, directive) {
		t.Fatalf("ONNX cgo bridge is missing Linux runpath directive %q", directive)
	}
}

func TestDarwinONNXBridgeUsesLoaderRelativeRunpath(t *testing.T) {
	data, err := os.ReadFile("internal/embedding/onnx.go")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	const directive = "#cgo darwin LDFLAGS: -Wl,-rpath,@loader_path"
	if !strings.Contains(contents, directive) {
		t.Fatalf("ONNX cgo bridge is missing Darwin runpath directive %q", directive)
	}
}

func TestDarwinONNXBridgeAllowsLoaderRelativeRunpath(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	const allowPattern = "export CGO_LDFLAGS_ALLOW := -Wl,-rpath,@loader_path"
	if !strings.Contains(contents, allowPattern) {
		t.Fatalf("Makefile is missing cgo linker allow pattern %q", allowPattern)
	}
}

func TestPlatformActivityDeliberatePlatformSplitIsExported(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	const declaration = "export GO_MK_PLATFORM_STUB_OPTIONAL := goodkind.io/lm-semantic-search/internal/platformactivity"
	if !strings.Contains(contents, declaration) {
		t.Fatalf("Makefile is missing platform stub declaration %q", declaration)
	}
}

func TestLinuxSourceInstallStagesONNXRuntimeBesideDaemon(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	requiredText := []string{
		"ifeq ($(shell uname),Linux)",
		"GO_MK_INSTALL_POST_CMD",
		"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.so.1.27.0",
		"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.so.1",
		"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.so",
		"$(INSTALL_DIR)/",
	}
	for _, text := range requiredText {
		if !strings.Contains(contents, text) {
			t.Fatalf("Makefile is missing Linux ONNX Runtime install text %q", text)
		}
	}
}

func TestDarwinSourceInstallStagesONNXRuntimeBesideDaemon(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	requiredText := []string{
		"ifeq ($(shell uname),Darwin)",
		"GO_MK_INSTALL_POST_CMD",
		"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.1.27.0.dylib",
		"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.1.dylib",
		"$(GO_MK_CGO_PREFIX)/lib/libonnxruntime.dylib",
		"$(INSTALL_DIR)/",
	}
	for _, text := range requiredText {
		if !strings.Contains(contents, text) {
			t.Fatalf("Makefile is missing Darwin ONNX Runtime install text %q", text)
		}
	}
}

func TestReleasePlatformsExcludeUnsupportedDarwinAMD64(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	// The compile matrix sets RELEASE_PLATFORMS per job, so the Makefile uses ?=
	// to supply only a local default. Match the assignment line exactly, which
	// proves the value carries the three supported platforms and omits the
	// unsupported darwin/amd64 target without colliding with explanatory comments
	// that name it.
	const releasePlatforms = "RELEASE_PLATFORMS ?= darwin/arm64 linux/amd64 linux/arm64"
	var found bool
	for _, line := range strings.Split(contents, "\n") {
		if !strings.HasPrefix(line, "RELEASE_PLATFORMS") {
			continue
		}
		if line != releasePlatforms {
			t.Fatalf("RELEASE_PLATFORMS line = %q, want %q", line, releasePlatforms)
		}
		found = true
	}
	if !found {
		t.Fatalf("Makefile is missing %q", releasePlatforms)
	}
}

func TestLinuxInstallerStagesPinnedONNXRuntimeBesideDaemon(t *testing.T) {
	data, err := os.ReadFile("install.sh")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	requiredText := []string{
		"onnxruntime-linux-x64-1.27.0.tgz",
		"547e40a48f1fe73e3f812d7c88a948612c23f896b91e4e2ee1e232d7b468246f",
		"onnxruntime-linux-aarch64-1.27.0.tgz",
		"3e4d83ac06924a32a07b6d7f91ce6f852876153fc0bbdf931bf517a140bfbe48",
		"install_linux_onnxruntime",
		"libonnxruntime.so",
	}
	for _, text := range requiredText {
		if !strings.Contains(contents, text) {
			t.Fatalf("install.sh is missing Linux ONNX Runtime contract text %q", text)
		}
	}
}

func TestDarwinInstallerStagesPinnedONNXRuntimeBesideDaemon(t *testing.T) {
	data, err := os.ReadFile("install.sh")
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	contents := string(data)

	requiredText := []string{
		"onnxruntime-osx-arm64-1.27.0.tgz",
		"545e81c58152353acb0d1e8bd6ce4b62f830c0961f5b3acfedc790ffd76e477a",
		"install_darwin_onnxruntime",
		"libonnxruntime.1.27.0.dylib",
		"libonnxruntime.1.dylib",
		"libonnxruntime.dylib",
	}
	for _, text := range requiredText {
		if !strings.Contains(contents, text) {
			t.Fatalf("install.sh is missing Darwin ONNX Runtime contract text %q", text)
		}
	}
}
