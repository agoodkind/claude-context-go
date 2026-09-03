//go:build restartacceptance

package restartacceptance

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"goodkind.io/lm-semantic-search/test/sandboxharness"
)

const (
	minioUserEnvironment     = "LMS_RESTART_MINIO_USER"
	minioPasswordEnvironment = "LMS_RESTART_MINIO_PASSWORD"
	hostUserEnvironment      = "LMS_RESTART_HOST_UID"
	maximumTreeRemovalTime   = 2 * time.Minute
)

type recordedImage struct {
	Tag string
	ID  string
}

var (
	etcdImage = recordedImage{
		Tag: "quay.io/coreos/etcd:v3.5.18",
		ID:  "sha256:b2317748a97a43c82040cdfacb68041b83bb6a01072a7da564a4c2562b3a3bcb",
	}
	minioImage = recordedImage{
		Tag: "minio/minio:RELEASE.2024-12-18T13-15-44Z",
		ID:  "sha256:8ce3a2b6c83521fabbde58b4f06e5fb7f95e64bfc05bc10def264479327c407b",
	}
	milvusImage = recordedImage{
		Tag: "milvusdb/milvus:v2.6.18",
		ID:  "sha256:fea7ed1d226c93292f476e4283584ce01f0ff01e45762206b9cd7486f9c68c5d",
	}
)

type runPaths struct {
	RunRoot            string
	Restore            string
	Source             string
	SourceEtcd         string
	SourceMilvus       string
	SourceMinIO        string
	SourceMinIODefault string
	Cases              string
	LMSState           string
	LMSContext         string
	LMSSocket          string
	LMSLogs            string
	ClydeConfig        string
	ClydeData          string
	ClydeState         string
	ClydeCache         string
	ClydeRuntime       string
	ClydeHome          string
	ClydeConfigFile    string
	Artifacts          string
	EventsJSONL        string
	ResultJSON         string
	ResultMarkdown     string
	ComposeFile        string
}

func pathsForRun(runRoot string) runPaths {
	restore := filepath.Join(runRoot, "restore")
	source := filepath.Join(restore, "source")
	lms := filepath.Join(runRoot, "lms")
	clyde := filepath.Join(runRoot, "clyde")
	artifacts := filepath.Join(runRoot, "artifacts")
	return runPaths{
		RunRoot:            runRoot,
		Restore:            restore,
		Source:             source,
		SourceEtcd:         filepath.Join(source, "etcd"),
		SourceMilvus:       filepath.Join(source, "milvus"),
		SourceMinIO:        filepath.Join(source, "minio"),
		SourceMinIODefault: filepath.Join(source, "minio-default"),
		Cases:              filepath.Join(restore, "cases"),
		LMSState:           filepath.Join(lms, "state"),
		LMSContext:         filepath.Join(lms, "context"),
		LMSSocket:          filepath.Join(lms, "daemon.sock"),
		LMSLogs:            filepath.Join(lms, "logs"),
		ClydeConfig:        filepath.Join(clyde, "config"),
		ClydeData:          filepath.Join(clyde, "data"),
		ClydeState:         filepath.Join(clyde, "state"),
		ClydeCache:         filepath.Join(clyde, "cache"),
		ClydeRuntime:       filepath.Join(clyde, "runtime"),
		ClydeHome:          filepath.Join(clyde, "home"),
		ClydeConfigFile:    filepath.Join(clyde, "config", "clyde", "config.toml"),
		Artifacts:          artifacts,
		EventsJSONL:        filepath.Join(artifacts, "events.jsonl"),
		ResultJSON:         filepath.Join(artifacts, "result.json"),
		ResultMarkdown:     filepath.Join(artifacts, "result.md"),
		ComposeFile:        filepath.Join(runRoot, "compose.yaml"),
	}
}

func createIsolationLayout(paths runPaths) error {
	directories := []string{
		paths.SourceEtcd,
		paths.SourceMilvus,
		paths.SourceMinIO,
		paths.SourceMinIODefault,
		paths.Cases,
		paths.LMSState,
		paths.LMSContext,
		paths.LMSLogs,
		filepath.Dir(paths.ClydeConfigFile),
		paths.ClydeData,
		paths.ClydeState,
		paths.ClydeCache,
		paths.ClydeRuntime,
		paths.ClydeHome,
		paths.Artifacts,
	}
	for _, directory := range directories {
		if !pathWithin(paths.RunRoot, directory) {
			return fmt.Errorf("layout path %q escapes run root %q", directory, paths.RunRoot)
		}
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return fmt.Errorf("create isolation directory %q: %w", directory, err)
		}
	}
	collectionID := "restart-" + filepath.Base(paths.RunRoot)
	configuration := fmt.Sprintf("[conversation.semantic]\nenabled = true\nsearch_enabled = true\nsocket_path = %q\ncollection_id = %q\n\n[adapter]\nenabled = false\n\n[mitm]\nenabled_default = false\n", paths.LMSSocket, collectionID)
	if err := os.WriteFile(paths.ClydeConfigFile, []byte(configuration), 0o600); err != nil {
		return fmt.Errorf("write isolated Clyde configuration: %w", err)
	}
	return nil
}

func pathWithin(root string, path string) bool {
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil {
		return false
	}
	return relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func restoreImmutableArchive(ctx context.Context, archivePath string, destination string) error {
	archive, err := openNoFollowRegular(archivePath)
	if err != nil {
		return fmt.Errorf("open restore archive: %w", err)
	}
	defer func() { _ = archive.Close() }()
	return restoreImmutableArchiveReader(ctx, archive, destination)
}

func restoreImmutableArchiveReader(ctx context.Context, archive io.Reader, destination string) error {
	if err := context.Cause(ctx); err != nil {
		return fmt.Errorf("restore archive: %w", err)
	}
	if _, err := os.Lstat(destination); err == nil {
		entries, readErr := os.ReadDir(destination)
		if readErr != nil {
			return fmt.Errorf("inspect restore destination: %w", readErr)
		}
		if len(entries) != 0 {
			return fmt.Errorf("restore destination %q is not empty", destination)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect restore destination: %w", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return fmt.Errorf("create restore destination: %w", err)
	}
	reader := tar.NewReader(&contextReader{ctx: ctx, reader: archive})
	for {
		if err := context.Cause(ctx); err != nil {
			return fmt.Errorf("restore archive: %w", err)
		}
		header, readErr := reader.Next()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read restore archive: %w", readErr)
		}
		cleanName := filepath.Clean(header.Name)
		if cleanName == "." {
			if header.Typeflag != tar.TypeDir {
				return fmt.Errorf("restore archive root entry %q is not a directory", header.Name)
			}
			continue
		}
		if filepath.IsAbs(header.Name) || cleanName == ".." || strings.HasPrefix(cleanName, ".."+string(filepath.Separator)) {
			return fmt.Errorf("restore archive entry %q escapes destination", header.Name)
		}
		target := filepath.Join(destination, cleanName)
		if !pathWithin(destination, target) {
			return fmt.Errorf("restore archive entry %q escapes destination", header.Name)
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return fmt.Errorf("create restored directory: %w", err)
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return fmt.Errorf("create restored parent: %w", err)
			}
			file, openErr := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if openErr != nil {
				return fmt.Errorf("create restored file: %w", openErr)
			}
			_, copyErr := io.CopyN(file, &contextReader{ctx: ctx, reader: reader}, header.Size)
			closeErr := file.Close()
			if copyErr != nil {
				return fmt.Errorf("extract restored file: %w", copyErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close restored file: %w", closeErr)
			}
		default:
			return fmt.Errorf("restore archive entry %q has unsupported type %d", header.Name, header.Typeflag)
		}
	}
	if err := filepath.WalkDir(destination, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("restored source contains symlink %q", path)
		}
		mode := fs.FileMode(0o400)
		if entry.IsDir() {
			mode = 0o500
		}
		return os.Chmod(path, mode)
	}); err != nil {
		return fmt.Errorf("make restored source immutable: %w", err)
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(body []byte) (int, error) {
	if err := context.Cause(reader.ctx); err != nil {
		return 0, err
	}
	return reader.reader.Read(body)
}

func cloneWritableTree(source string, destination string) error {
	return sandboxharness.CloneTree(source, destination)
}

func removeTree(ctx context.Context, root string) error {
	return removeTreeWith(ctx, root, os.RemoveAll, 100*time.Millisecond, maximumTreeRemovalTime)
}

func removeTreeWith(
	ctx context.Context,
	root string,
	removeAll func(string) error,
	retryDelay time.Duration,
	maximumDuration time.Duration,
) error {
	ctx, cancel := context.WithTimeout(ctx, maximumDuration)
	defer cancel()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		return os.Chmod(path, 0o700)
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("make cleanup tree writable: %w", err)
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return fmt.Errorf("remove cleanup tree: %w", err)
		}
		err := removeAll(root)
		if err == nil {
			return nil
		}
		if !errors.Is(err, syscall.ENOTEMPTY) {
			return fmt.Errorf("remove cleanup tree: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("remove cleanup tree: %w", context.Cause(ctx))
		case <-time.After(retryDelay):
		}
	}
}

func renderCompose(paths runPaths, caseName string) string {
	caseRoot := filepath.Join(paths.Cases, caseName)
	return fmt.Sprintf(`services:
  etcd:
    image: %s
    pull_policy: never
    user: "${%s:?required}:0"
    command: etcd -advertise-client-urls=http://etcd:2379 -listen-client-urls=http://0.0.0.0:2379 --data-dir=/etcd
    ports:
      - "127.0.0.1:%d:2379"
    volumes:
      - "%s/etcd:/etcd"
    healthcheck:
      test: ["CMD", "etcdctl", "endpoint", "health"]
      interval: 5s
      timeout: 5s
      retries: 12
  minio:
    image: %s
    pull_policy: never
    user: "${%s:?required}:0"
    command: minio server /minio_data --console-address :9001
    environment:
      MINIO_ROOT_USER: ${%s:?required}
      MINIO_ROOT_PASSWORD: ${%s:?required}
    ports:
      - "127.0.0.1:%d:9000"
      - "127.0.0.1:%d:9001"
    volumes:
      - "%s/minio:/minio_data"
      - "%s/minio-default:/data"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 5s
      retries: 12
  standalone:
    image: %s
    pull_policy: never
    user: "${%s:?required}:0"
    command: ["milvus", "run", "standalone"]
    security_opt:
      - seccomp:unconfined
    environment:
      ETCD_ENDPOINTS: etcd:2379
      MINIO_ADDRESS: minio:9000
      MINIO_ACCESS_KEY_ID: ${%s:?required}
      MINIO_SECRET_ACCESS_KEY: ${%s:?required}
    ports:
      - "127.0.0.1:%d:19530"
      - "127.0.0.1:%d:9091"
    volumes:
      - "%s/milvus:/var/lib/milvus"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9091/healthz"]
      interval: 5s
      timeout: 5s
      retries: 30
    depends_on:
      etcd:
        condition: service_healthy
      minio:
        condition: service_healthy
`, etcdImage.Tag, hostUserEnvironment, etcdClientPort, caseRoot, minioImage.Tag,
		hostUserEnvironment, minioUserEnvironment, minioPasswordEnvironment, minioAPIPort,
		minioConsolePort, caseRoot, caseRoot, milvusImage.Tag, hostUserEnvironment,
		minioUserEnvironment, minioPasswordEnvironment, milvusGRPCPort, milvusHealthPort,
		caseRoot)
}

type imageConfigIDSource interface {
	ImageConfigID(context.Context, string) (string, error)
}

func validateRecordedImages(ctx context.Context, source imageConfigIDSource) error {
	if source == nil {
		return fmt.Errorf("recorded image validation requires an image config ID source")
	}
	for _, image := range []recordedImage{etcdImage, minioImage, milvusImage} {
		actual, err := source.ImageConfigID(ctx, image.Tag)
		if err != nil {
			return fmt.Errorf("read recorded image %q config ID: %w", image.Tag, err)
		}
		if actual != image.ID {
			return fmt.Errorf("recorded image %q has local ID %q, want %q", image.Tag, actual, image.ID)
		}
	}
	return nil
}

func (execCommandRunner) ImageConfigID(ctx context.Context, tag string) (string, error) {
	commandContext, cancel := context.WithCancel(ctx)
	defer cancel()
	command := exec.CommandContext(commandContext, "docker", "image", "save", tag)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("open Docker image archive: %w", err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return "", fmt.Errorf("start Docker image archive: %w", err)
	}
	configID, readErr := readDockerSaveConfigID(stdout)
	if readErr != nil {
		cancel()
	}
	waitErr := command.Wait()
	if readErr != nil {
		if waitErr != nil {
			return "", errors.Join(readErr, fmt.Errorf("stop Docker image archive: %w", waitErr))
		}
		return "", readErr
	}
	if waitErr != nil {
		message := strings.TrimSpace(stderr.String())
		if message != "" {
			return "", fmt.Errorf("save Docker image: %w; output: %s", waitErr, message)
		}
		return "", fmt.Errorf("save Docker image: %w", waitErr)
	}
	return configID, nil
}

func readDockerSaveConfigID(reader io.Reader) (string, error) {
	archive := tar.NewReader(reader)
	configID := ""
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			if configID == "" {
				return "", fmt.Errorf("Docker image archive is missing manifest.json")
			}
			return configID, nil
		}
		if err != nil {
			return "", fmt.Errorf("read Docker image archive: %w", err)
		}
		if header.Name != "manifest.json" {
			continue
		}
		if configID != "" {
			return "", fmt.Errorf("Docker image archive contains duplicate manifest.json")
		}
		var manifest []struct {
			Config string `json:"Config"`
		}
		if err := json.NewDecoder(archive).Decode(&manifest); err != nil {
			return "", fmt.Errorf("decode Docker image manifest: %w", err)
		}
		if len(manifest) != 1 {
			return "", fmt.Errorf("Docker image manifest contains %d images, want 1", len(manifest))
		}
		const configPrefix = "blobs/sha256/"
		configDigest, found := strings.CutPrefix(manifest[0].Config, configPrefix)
		if !found {
			configDigest, found = strings.CutSuffix(manifest[0].Config, ".json")
			found = found && filepath.Base(manifest[0].Config) == manifest[0].Config
		}
		if !found || len(configDigest) != sha256.Size*2 {
			return "", fmt.Errorf("Docker image manifest has invalid config %q", manifest[0].Config)
		}
		configID = "sha256:" + configDigest
	}
}

type evidenceEvent struct {
	RecordedAt time.Time         `json:"recorded_at"`
	Phase      string            `json:"phase"`
	Status     string            `json:"status"`
	Details    map[string]string `json:"details,omitempty"`
}

type acceptanceResult struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type evidenceRecorder struct {
	paths runPaths
	now   func() time.Time
}

func newEvidenceRecorder(paths runPaths, now func() time.Time) *evidenceRecorder {
	return &evidenceRecorder{paths: paths, now: now}
}

func (recorder *evidenceRecorder) Record(phase string, status string, details map[string]string) error {
	event := evidenceEvent{RecordedAt: recorder.now().UTC(), Phase: phase, Status: status, Details: details}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode evidence event: %w", err)
	}
	file, err := os.OpenFile(recorder.paths.EventsJSONL, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open evidence events: %w", err)
	}
	if _, err := file.Write(append(body, '\n')); err != nil {
		_ = file.Close()
		return fmt.Errorf("write evidence event: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close evidence events: %w", err)
	}
	return nil
}

func (recorder *evidenceRecorder) Finish(result acceptanceResult) error {
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode acceptance result: %w", err)
	}
	body = append(body, '\n')
	if err := os.WriteFile(recorder.paths.ResultJSON, body, 0o600); err != nil {
		return fmt.Errorf("write JSON acceptance result: %w", err)
	}
	markdown := fmt.Sprintf("# Restart acceptance\n\nRun: %s\n\nStatus: %s\n", result.RunID, result.Status)
	if result.Error != "" {
		markdown += "\nError: " + result.Error + "\n"
	}
	if err := os.WriteFile(recorder.paths.ResultMarkdown, []byte(markdown), 0o600); err != nil {
		return fmt.Errorf("write Markdown acceptance result: %w", err)
	}
	return nil
}

type commandRunner interface {
	Run(context.Context, map[string]string, string, ...string) ([]byte, error)
}

type harness struct {
	paths          runPaths
	composeProject string
	runner         commandRunner
	valueEntropy   io.Reader
	valueMutex     sync.Mutex
	runtimeValues  composeRuntimeValues
	archiveSizes   []int64
	availableBytes func(string) (int64, error)
	census         collectionCensusFunc
	readiness      func(context.Context) error
}

type composeRuntimeValues struct {
	userValue string
	keyValue  string
}

func newComposeRuntimeValues(entropy io.Reader) (composeRuntimeValues, error) {
	var randomBytes [32]byte
	if _, err := io.ReadFull(entropy, randomBytes[:]); err != nil {
		return composeRuntimeValues{}, fmt.Errorf("generate acceptance runtime values: %w", err)
	}
	return composeRuntimeValues{
		userValue: hex.EncodeToString(randomBytes[:8]),
		keyValue:  hex.EncodeToString(randomBytes[8:]),
	}, nil
}

func (h *harness) setRuntimeValues(values composeRuntimeValues) {
	h.runtimeValues = values
}

func (h *harness) composeEnvironment() (map[string]string, error) {
	h.valueMutex.Lock()
	defer h.valueMutex.Unlock()
	if h.runtimeValues.userValue == "" || h.runtimeValues.keyValue == "" {
		entropy := h.valueEntropy
		if entropy == nil {
			entropy = rand.Reader
		}
		values, err := newComposeRuntimeValues(entropy)
		if err != nil {
			return nil, err
		}
		h.setRuntimeValues(values)
	}
	return map[string]string{
		minioUserEnvironment:     h.runtimeValues.userValue,
		minioPasswordEnvironment: h.runtimeValues.keyValue,
		hostUserEnvironment:      strconv.Itoa(os.Getuid()),
	}, nil
}

func (h *harness) runCompose(ctx context.Context, caseName string) (runErr error) {
	return h.withCompose(ctx, caseName, func(context.Context) error { return nil })
}

func (h *harness) withCompose(
	ctx context.Context,
	caseName string,
	scenario func(context.Context) error,
) (runErr error) {
	if !validComposeProject(h.composeProject, filepath.Base(h.paths.RunRoot)) {
		return fmt.Errorf("compose project %q is not tagged for run %q", h.composeProject, filepath.Base(h.paths.RunRoot))
	}
	if h.census == nil {
		return fmt.Errorf("clone collection census is unavailable")
	}
	if h.readiness == nil {
		return fmt.Errorf("clone readiness check is unavailable")
	}
	composeEnvironment, err := h.composeEnvironment()
	if err != nil {
		return err
	}
	caseRoot, err := resolveCaseRoot(h.paths.Cases, caseName)
	if err != nil {
		return err
	}
	spaceReader := h.availableBytes
	if spaceReader == nil {
		spaceReader = availableDiskBytes
	}
	available, err := spaceReader(h.paths.RunRoot)
	if err != nil {
		return err
	}
	if err := validateCaseFreeSpace(available, h.archiveSizes); err != nil {
		return fmt.Errorf("recheck case free space: %w", err)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if removeErr := removeTree(cleanupContext, caseRoot); removeErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("clean case tree: %w", removeErr))
		}
	}()
	for _, name := range []string{"etcd", "milvus", "minio", "minio-default"} {
		if err := cloneWritableTree(filepath.Join(h.paths.Source, name), filepath.Join(caseRoot, name)); err != nil {
			return fmt.Errorf("create writable %s copy: %w", name, err)
		}
	}
	if err := os.WriteFile(h.paths.ComposeFile, []byte(renderCompose(h.paths, caseName)), 0o600); err != nil {
		return fmt.Errorf("write compose file: %w", err)
	}
	project := sandboxharness.ComposeProject{
		Runner:         h.runner,
		Name:           h.composeProject,
		File:           h.paths.ComposeFile,
		Environment:    composeEnvironment,
		CleanupTimeout: 2 * time.Minute,
	}
	return project.Run(ctx, func(composeContext context.Context) (scenarioErr error) {
		if err := h.readiness(composeContext); err != nil {
			return fmt.Errorf("check clone readiness after startup: %w", err)
		}
		baseline, err := h.census(composeContext)
		if err != nil {
			return fmt.Errorf("capture clone inventory before case: %w", err)
		}
		defer func() {
			auditContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			after, censusErr := retryCloneCensus(auditContext, h.census, baseline)
			if censusErr == nil {
				censusErr = auditCloneMutation(baseline, after)
			}
			if censusErr != nil {
				scenarioErr = errors.Join(scenarioErr, fmt.Errorf("audit clone after case: %w", censusErr))
			}
		}()
		if err := scenario(composeContext); err != nil {
			return fmt.Errorf("run restart acceptance scenario: %w", err)
		}
		return nil
	})
}

func retryCloneCensus(
	ctx context.Context,
	census collectionCensusFunc,
	baseline cloneMilvusCensus,
) (cloneMilvusCensus, error) {
	const retryDelay = 500 * time.Millisecond
	var lastErr error
	for {
		result, err := census(ctx)
		if err == nil {
			err = auditCloneMutation(baseline, result)
		}
		if err == nil {
			return result, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return cloneMilvusCensus{}, errors.Join(lastErr, context.Cause(ctx))
		case <-time.After(retryDelay):
		}
	}
}

var caseNamePattern = regexp.MustCompile(`^[a-i](-[a-z0-9]+)?$`)

func resolveCaseRoot(casesRoot string, caseName string) (string, error) {
	if !caseNamePattern.MatchString(caseName) {
		return "", fmt.Errorf("case name %q is invalid", caseName)
	}
	resolvedCases, err := filepath.EvalSymlinks(casesRoot)
	if err != nil {
		return "", fmt.Errorf("resolve cases root: %w", err)
	}
	caseRoot := filepath.Join(resolvedCases, caseName)
	if !pathWithin(resolvedCases, caseRoot) {
		return "", fmt.Errorf("case root %q escapes cases root", caseRoot)
	}
	if _, err := os.Lstat(caseRoot); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return "", fmt.Errorf("case root %q already exists", caseRoot)
		}
		return "", fmt.Errorf("inspect case root: %w", err)
	}
	return caseRoot, nil
}

func validComposeProject(project string, runID string) bool {
	if !runIDPattern.MatchString(runID) {
		return false
	}
	suffix := runID[len(runID)-8:]
	return project == "lms-restart-"+suffix
}

func isolatedLMSEnvironment(paths runPaths) map[string]string {
	return map[string]string{
		"XDG_STATE_HOME":               paths.LMSState,
		"CLAUDE_CONTEXTD_CONTEXT_ROOT": paths.LMSContext,
		"CLAUDE_CONTEXTD_SOCKET_PATH":  paths.LMSSocket,
		"MILVUS_ADDRESS":               fmt.Sprintf("127.0.0.1:%d", milvusProxyPort),
		"MILVUS_DATABASE":              cloneMilvusDatabase,
		"OPENAI_BASE_URL":              fmt.Sprintf("http://127.0.0.1:%d", embeddingProxyPort),
		"OPENAI_API_KEY":               "restart-acceptance-local",
		"EMBEDDING_MODEL":              cloneEmbeddingModel,
		"EMBEDDING_DIMENSION":          strconv.Itoa(cloneEmbeddingDimension),
	}
}

func isolatedClydeEnvironment(paths runPaths) map[string]string {
	return map[string]string{
		"HOME":                        paths.ClydeHome,
		"XDG_CONFIG_HOME":             paths.ClydeConfig,
		"XDG_DATA_HOME":               paths.ClydeData,
		"XDG_STATE_HOME":              paths.ClydeState,
		"XDG_CACHE_HOME":              paths.ClydeCache,
		"XDG_RUNTIME_DIR":             paths.ClydeRuntime,
		"CLAUDE_CONTEXTD_SOCKET_PATH": paths.LMSSocket,
	}
}

type collectionCensusFunc func(context.Context) (cloneMilvusCensus, error)

type cloneMilvusCensus struct {
	Databases   []string            `json:"databases"`
	Collections collectionCensus    `json:"collections"`
	Samples     collectionCensus    `json:"samples,omitempty"`
	RowCounts   collectionRowCounts `json:"row_counts"`
}

type collectionIdentity struct {
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

type collectionCensus map[collectionIdentity]string

type collectionRowCounts map[collectionIdentity]int64

type collectionCensusEntry struct {
	Database   string `json:"database"`
	Collection string `json:"collection"`
	Hash       string `json:"hash"`
}

func (census collectionCensus) MarshalJSON() ([]byte, error) {
	entries := make([]collectionCensusEntry, 0, len(census))
	for identity, hash := range census {
		entries = append(entries, collectionCensusEntry{
			Database: identity.Database, Collection: identity.Collection, Hash: hash,
		})
	}
	sort.Slice(entries, func(left int, right int) bool {
		if entries[left].Database != entries[right].Database {
			return entries[left].Database < entries[right].Database
		}
		return entries[left].Collection < entries[right].Collection
	})
	return json.Marshal(entries)
}

func (counts collectionRowCounts) MarshalJSON() ([]byte, error) {
	entries := make([]collectionRowCountEntry, 0, len(counts))
	for identity, count := range counts {
		entries = append(entries, collectionRowCountEntry{
			Database: identity.Database, Collection: identity.Collection, Count: count,
		})
	}
	sort.Slice(entries, func(left int, right int) bool {
		if entries[left].Database != entries[right].Database {
			return entries[left].Database < entries[right].Database
		}
		return entries[left].Collection < entries[right].Collection
	})
	return json.Marshal(entries)
}

type collectionRowCountEntry struct {
	Database   string `json:"database"`
	Collection string `json:"collection"`
	Count      int64  `json:"count"`
}

func cloneCollectionCensus(collections collectionCensus) collectionCensus {
	result := make(collectionCensus, len(collections))
	for identity, hash := range collections {
		result[identity] = hash
	}
	return result
}

func cloneCollectionRowCounts(counts collectionRowCounts) collectionRowCounts {
	result := make(collectionRowCounts, len(counts))
	for identity, count := range counts {
		result[identity] = count
	}
	return result
}

func auditCloneMutation(before cloneMilvusCensus, after cloneMilvusCensus) error {
	beforeDatabases := slices.Clone(before.Databases)
	afterDatabases := slices.Clone(after.Databases)
	slices.Sort(beforeDatabases)
	slices.Sort(afterDatabases)
	if !slices.Equal(beforeDatabases, afterDatabases) {
		return fmt.Errorf("clone database set changed from %v to %v", beforeDatabases, afterDatabases)
	}
	for identity := range before.Collections {
		_, exists := after.Collections[identity]
		if !exists {
			return fmt.Errorf("restored clone collection %q/%q was removed", identity.Database, identity.Collection)
		}
		beforeSample, sampledBefore := before.Samples[identity]
		afterSample, sampledAfter := after.Samples[identity]
		if sampledBefore && sampledAfter && beforeSample != afterSample {
			return fmt.Errorf("restored clone collection %q/%q sampled vectors changed", identity.Database, identity.Collection)
		}
		beforeCount, countedBefore := before.RowCounts[identity]
		afterCount, countedAfter := after.RowCounts[identity]
		if countedBefore && !countedAfter {
			return fmt.Errorf("restored clone collection %q/%q row count is missing", identity.Database, identity.Collection)
		}
		if countedBefore && afterCount < beforeCount {
			return fmt.Errorf(
				"restored clone collection %q/%q row count fell from %d to %d",
				identity.Database,
				identity.Collection,
				beforeCount,
				afterCount,
			)
		}
	}
	return nil
}
