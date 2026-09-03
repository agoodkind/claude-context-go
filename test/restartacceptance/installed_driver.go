//go:build restartacceptance

package restartacceptance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/semantic/milvusgrpc"
	"goodkind.io/lm-semantic-search/internal/tshash"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	cloneMilvusAddress                   = "127.0.0.1:29530"
	fixtureQuery                         = "restart acceptance"
	scenarioBMetadataTimeoutMilliseconds = 10000
	scenarioGLoadTimeoutMilliseconds     = 12000
	scenarioGCallerWaitMilliseconds      = 7000
	scenarioGIdleTimeoutMilliseconds     = 60000
	embeddingReadinessTimeout            = 30 * time.Second
	embeddingReadinessResponseLimit      = 4 << 20
	daemonProcessExitTimeout             = 30 * time.Second
)

type realAcceptanceDriver struct {
	runner                 execCommandRunner
	startScenarioBDaemon   func(context.Context, installedProcess, string) (*daemonRuntime, error)
	openScenarioBObservers func(context.Context, *daemonRuntime, acceptanceRun, acceptanceFixture) (rowSnapshotObserver, checkpointSnapshotObserver, func(), error)
	mutex                  sync.Mutex
	teardownError          error
}

func newRealAcceptanceLifecycleOperations() (acceptanceLifecycleOperations, error) {
	driver := &realAcceptanceDriver{}
	return acceptanceLifecycleOperations{
		Prepare: func(ctx context.Context) (acceptanceRun, error) {
			return prepareAcceptanceRun(ctx, time.Now(), rand.Reader)
		},
		RunCase: func(ctx context.Context, run acceptanceRun, name string) error {
			return driver.runCase(ctx, run, name)
		},
		ConfirmClone: driver.confirmClone,
		Cleanup:      cleanupAcceptanceRun,
		Finish: func(run acceptanceRun, result acceptanceResult) error {
			return newEvidenceRecorder(run.Paths, time.Now).Finish(result)
		},
	}, nil
}

func runPathsForID(parent string, runID string) runPaths {
	return pathsForRun(filepath.Join(parent, runID))
}

func acceptanceCollectionIdentities(run acceptanceRun) map[collectionIdentity]struct{} {
	return acceptanceCollectionIdentitiesForPaths(run.Paths)
}

func acceptanceCollectionIdentitiesForPaths(paths runPaths) map[collectionIdentity]struct{} {
	names := make([]string, 0, len(acceptanceScenarioNames)*6+3)
	for _, scenarioName := range acceptanceScenarioNames {
		fixtureRoot := filepath.Join(paths.Cases, scenarioName, "fixture")
		for _, root := range []string{fixtureRoot, fixtureRoot + "-second"} {
			live := codeCollectionName(root)
			names = append(names, live, live+"_stg", live+"_swap_previous")
		}
	}
	collectionID := "restart-" + filepath.Base(paths.RunRoot)
	conversation := "conv_chunks_" + tshash.PathPrefix(collectionID)
	names = append(names, conversation, conversation+"_stg", conversation+"_swap_previous")
	identities := make(map[collectionIdentity]struct{}, len(names))
	for _, name := range names {
		identities[collectionIdentity{Database: "default", Collection: name}] = struct{}{}
	}
	return identities
}

func cleanupAcceptanceRun(ctx context.Context, run acceptanceRun) error {
	var cleanupErr error
	for _, path := range []string{run.Paths.Restore, run.Paths.LMSState, run.Paths.LMSContext, run.Paths.LMSLogs, filepath.Dir(run.Paths.ClydeConfig), run.Paths.ComposeFile} {
		if err := removeTree(ctx, path); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	return cleanupErr
}

func (driver *realAcceptanceDriver) runCase(ctx context.Context, run acceptanceRun, name string) (runErr error) {
	defer func() {
		runErr = errors.Join(runErr, driver.takeTeardownError())
	}()
	h := driver.newCloneHarness(run)
	return h.withCompose(ctx, name, func(caseContext context.Context) (scenarioErr error) {
		defer func() {
			if scenarioErr != nil {
				scenarioErr = errors.Join(scenarioErr, preserveCaseDiagnostics(run.Paths, name))
			}
		}()
		if err := resetIsolatedRuntime(run.Paths); err != nil {
			return err
		}
		proxies, err := startCaseProxies(caseContext)
		if err != nil {
			return err
		}
		defer func() {
			scenarioErr = errors.Join(scenarioErr, proxies.close())
		}()
		fixture, err := createAcceptanceFixture(filepath.Join(run.Paths.Cases, name, "fixture"), name)
		if err != nil {
			return err
		}
		recorder := newEvidenceRecorder(run.Paths, time.Now)
		return driver.runInstalledScenario(caseContext, run, h, proxies, fixture, name, recorder)
	})
}

func preserveCaseDiagnostics(paths runPaths, scenarioName string) error {
	if !caseNamePattern.MatchString(scenarioName) {
		return fmt.Errorf("scenario name %q is invalid", scenarioName)
	}
	body, err := os.ReadFile(installedLMSLogPath(paths))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read scenario %s LMS log: %w", scenarioName, err)
	}
	if err := os.MkdirAll(paths.Artifacts, 0o700); err != nil {
		return fmt.Errorf("create scenario diagnostics directory: %w", err)
	}
	destination := filepath.Join(paths.Artifacts, "scenario-"+scenarioName+"-lms.log")
	if err := os.WriteFile(destination, body, 0o600); err != nil {
		return fmt.Errorf("write scenario %s LMS log: %w", scenarioName, err)
	}
	return nil
}

func (driver *realAcceptanceDriver) recordTeardownError(err error) {
	if err == nil {
		return
	}
	driver.mutex.Lock()
	driver.teardownError = errors.Join(driver.teardownError, err)
	driver.mutex.Unlock()
}

func (driver *realAcceptanceDriver) takeTeardownError() error {
	driver.mutex.Lock()
	defer driver.mutex.Unlock()
	err := driver.teardownError
	driver.teardownError = nil
	return err
}

func (driver *realAcceptanceDriver) stopDaemonRuntime(runtime *daemonRuntime) {
	driver.recordTeardownError(stopDaemonRuntime(runtime))
}

func (driver *realAcceptanceDriver) stopInstalledProcess(process *exec.Cmd) {
	driver.recordTeardownError(stopInstalledProcess(process))
}

func (driver *realAcceptanceDriver) runInstalledScenario(
	ctx context.Context,
	run acceptanceRun,
	h *harness,
	proxies caseProxies,
	fixture acceptanceFixture,
	name string,
	recorder *evidenceRecorder,
) error {
	switch name {
	case "a":
		return driver.runInstalledScenarioA(ctx, run, proxies, fixture, recorder)
	case "b":
		return driver.runInstalledScenarioB(ctx, run, proxies, fixture, recorder)
	case "c":
		return driver.runInstalledScenarioC(ctx, run, proxies, fixture, recorder)
	case "d":
		return driver.runInstalledScenarioD(ctx, run, fixture, recorder)
	case "e":
		return driver.runInstalledScenarioE(ctx, run, fixture, recorder)
	case "f":
		return driver.runInstalledScenarioF(ctx, run, h, fixture, recorder)
	case "g":
		return driver.runInstalledScenarioG(ctx, run, proxies, fixture, recorder)
	case "h":
		return driver.runInstalledScenarioH(ctx, run, proxies, fixture, recorder)
	case "i":
		return driver.runInstalledScenarioI(ctx, run, h, proxies, fixture, recorder)
	default:
		return fmt.Errorf("unknown restart acceptance scenario %q", name)
	}
}

func (driver *realAcceptanceDriver) runInstalledScenarioA(
	ctx context.Context,
	run acceptanceRun,
	proxies caseProxies,
	fixture acceptanceFixture,
	recorder *evidenceRecorder,
) error {
	proxies.embedding.GateAfter(1)
	runtime, err := startDaemonRuntime(ctx, installedLMSProcess(run), run.Paths.LMSSocket)
	if err != nil {
		return err
	}
	defer driver.stopDaemonRuntime(runtime)
	milvusClient, err := newCloneMilvusClient(ctx)
	if err != nil {
		return err
	}
	defer closeMilvusClient(milvusClient)
	_, err = runScenarioA(ctx, scenarioAInput{
		Client:                runtime.client,
		Path:                  fixture.root,
		Proxy:                 proxies.embedding,
		ExpectedUnfinishedIDs: fixture.files[1:],
		ObserveRows:           rowObserver(milvusClient, codeCollectionName(fixture.root)),
		ObserveCheckpoint: checkpointObserver(
			runtime.client,
			fixture.root,
			installedLMSMerklePath(run.Paths),
		),
		ObserveEmbeddingCalls: proxies.embedding.Inputs,
		Recorder:              recorder,
	})
	return err
}

func (driver *realAcceptanceDriver) runInstalledScenarioB(
	ctx context.Context,
	run acceptanceRun,
	proxies caseProxies,
	fixture acceptanceFixture,
	recorder *evidenceRecorder,
) error {
	startDaemon := driver.startScenarioBDaemon
	if startDaemon == nil {
		startDaemon = startDaemonRuntime
	}
	runtime, err := startDaemon(ctx, installedLMSProcessForScenarioB(run), run.Paths.LMSSocket)
	if err != nil {
		return err
	}
	defer driver.stopDaemonRuntime(runtime)
	if _, err := waitForCompletedIndex(ctx, runtime.client, fixture.root); err != nil {
		return fmt.Errorf("seed scenario B live target: %w", err)
	}
	openObservers := driver.openScenarioBObservers
	if openObservers == nil {
		openObservers = openInstalledScenarioBObservers
	}
	observeRows, observeCheckpoint, closeObservers, err := openObservers(ctx, runtime, run, fixture)
	if err != nil {
		return err
	}
	defer closeObservers()
	proxies.embedding.GateAfter(len(proxies.embedding.Inputs()))
	addedHashes, err := prepareScenarioBRebuild(fixture)
	if err != nil {
		return fmt.Errorf("prepare scenario B rebuild: %w", err)
	}
	_, err = runScenarioB(ctx, scenarioBInput{
		Client: runtime.client,
		Path:   fixture.root,
		Proxy:  proxies.milvus,
		ReleaseEmbeddingGate: func() error {
			return releaseScenarioBEmbeddingGate(proxies.milvus, proxies.embedding.ClearGate)
		},
		EmbeddingGateReached: proxies.embedding.GateReached(),
		ExpectedAddedHashes:  addedHashes,
		ObserveRows:          observeRows,
		ObserveCheckpoint:    observeCheckpoint,
		Recorder:             recorder,
	})
	return err
}

func openInstalledScenarioBObservers(
	ctx context.Context,
	runtime *daemonRuntime,
	run acceptanceRun,
	fixture acceptanceFixture,
) (rowSnapshotObserver, checkpointSnapshotObserver, func(), error) {
	milvusClient, err := newCloneMilvusClient(ctx)
	if err != nil {
		return nil, nil, func() {}, err
	}
	return rowObserver(milvusClient, codeCollectionName(fixture.root)), checkpointObserver(
		runtime.client,
		fixture.root,
		installedLMSMerklePath(run.Paths),
	), func() { closeMilvusClient(milvusClient) }, nil
}

func (driver *realAcceptanceDriver) runInstalledScenarioC(
	ctx context.Context,
	run acceptanceRun,
	proxies caseProxies,
	fixture acceptanceFixture,
	recorder *evidenceRecorder,
) error {
	proxies.embedding.GateAfter(1)
	milvusClient, err := newCloneMilvusClient(ctx)
	if err != nil {
		return err
	}
	defer closeMilvusClient(milvusClient)
	_, err = runScenarioC(ctx, scenarioCInput{
		Process:               installedLMSProcess(run),
		SocketPath:            run.Paths.LMSSocket,
		LockPath:              filepath.Join(run.Paths.LMSContext, "mcp-sync.flock"),
		Path:                  fixture.root,
		ExpectedUnfinishedIDs: fixture.files[1:],
		ObserveRows:           rowObserver(milvusClient, codeCollectionName(fixture.root)),
		ObserveCheckpoint: checkpointObserverForSocket(
			run.Paths.LMSSocket,
			fixture.root,
			installedLMSMerklePath(run.Paths),
		),
		ReleaseHeldEmbedding: proxies.embedding.ClearGate,
		Recorder:             recorder,
	})
	return err
}

func (driver *realAcceptanceDriver) runInstalledScenarioD(
	ctx context.Context,
	run acceptanceRun,
	fixture acceptanceFixture,
	recorder *evidenceRecorder,
) error {
	owner, err := startInstalledProcess(installedProcess{Path: "/bin/sleep", Args: []string{"300"}})
	if err != nil {
		return fmt.Errorf("start unrelated owner process: %w", err)
	}
	defer driver.stopInstalledProcess(owner)
	_, err = runScenarioD(ctx, scenarioDInput{
		Process:    installedLMSProcess(run),
		SocketPath: run.Paths.LMSSocket,
		Paths:      run.Paths,
		OwnerPID:   owner.Process.Pid,
		Path:       fixture.root,
		Recorder:   recorder,
	})
	return err
}

func (driver *realAcceptanceDriver) runInstalledScenarioE(
	ctx context.Context,
	run acceptanceRun,
	fixture acceptanceFixture,
	recorder *evidenceRecorder,
) error {
	if err := writeClydeConversation(run.Paths, fixture.marker, "initial"); err != nil {
		return err
	}
	lms, err := startDaemonRuntime(ctx, installedLMSProcessWithoutPeriodicMaintenance(run), run.Paths.LMSSocket)
	if err != nil {
		return err
	}
	defer func() {
		if lms != nil {
			driver.stopDaemonRuntime(lms)
		}
	}()
	clyde, err := driver.startClydeRuntime(ctx, run)
	if err != nil {
		return err
	}
	defer driver.stopInstalledProcess(clyde.process)
	if _, err := waitForSeededClydeSearch(
		ctx,
		func(statusContext context.Context) (clydeStatusObservation, error) {
			return driver.clydeStatus(statusContext, run, clyde.process.Process.Pid)
		},
		func(searchContext context.Context) (semanticSearchObservation, error) {
			return driver.searchClyde(searchContext, run, fixture.marker)
		},
		maximumClydeFeederRecovery,
		defaultScenarioFailureTimeout,
		defaultScenarioPollInterval,
	); err != nil {
		return fmt.Errorf("wait for initial Clyde feeder pass: %w", err)
	}
	_, err = runScenarioE(ctx, scenarioEInput{
		StopLMS: func(stopContext context.Context) error {
			if lms == nil {
				return nil
			}
			err := lms.stop(stopContext)
			lms = nil
			return err
		},
		StartLMS: func(startContext context.Context) error {
			if lms != nil {
				return nil
			}
			started, startErr := startDaemonRuntime(startContext, installedLMSProcessWithoutPeriodicMaintenance(run), run.Paths.LMSSocket)
			if startErr == nil {
				lms = started
			}
			return startErr
		},
		SearchClyde: func(searchContext context.Context) (semanticSearchObservation, error) {
			return driver.searchClyde(searchContext, run, fixture.marker)
		},
		ClydeStatus: func(statusContext context.Context) (clydeStatusObservation, error) {
			return driver.clydeStatus(statusContext, run, clyde.process.Process.Pid)
		},
		TriggerFeederSync: func(context.Context) error {
			return writeClydeConversation(run.Paths, "feeder recovery sentinel", "recovered")
		},
		Recorder: recorder,
	})
	return err
}

func (driver *realAcceptanceDriver) runInstalledScenarioF(
	ctx context.Context,
	run acceptanceRun,
	h *harness,
	fixture acceptanceFixture,
	recorder *evidenceRecorder,
) error {
	seed, err := startDaemonRuntime(ctx, installedLMSProcessWithoutPeriodicMaintenance(run), run.Paths.LMSSocket)
	if err != nil {
		return err
	}
	if _, err := waitForCompletedIndex(ctx, seed.client, fixture.root); err != nil {
		driver.stopDaemonRuntime(seed)
		return fmt.Errorf("seed scenario F registry target: %w", err)
	}
	driver.stopDaemonRuntime(seed)

	var runtime *daemonRuntime
	caseRoot := filepath.Join(run.Paths.Cases, "f")
	preservedEtcd := filepath.Join(caseRoot, "etcd-preserved")
	defer func() {
		if runtime != nil {
			driver.stopDaemonRuntime(runtime)
		}
	}()
	_, err = runScenarioF(ctx, scenarioFInput{
		VerifyClonePayload: func(context.Context) error { return requireClonePayload(caseRoot) },
		PrepareEmptyEtcd: func(composeContext context.Context) error {
			return replaceCaseEtcd(composeContext, h, caseRoot, preservedEtcd, false)
		},
		StartLMS: func(startContext context.Context) error {
			if startErr := startCaseCompose(startContext, h); startErr != nil {
				return startErr
			}
			started, startErr := startDaemonRuntime(startContext, installedLMSProcessWithoutPeriodicMaintenance(run), run.Paths.LMSSocket)
			if startErr == nil {
				runtime = started
			}
			return startErr
		},
		StopLMS: func(stopContext context.Context) error {
			if runtime == nil {
				return nil
			}
			err := runtime.stop(stopContext)
			runtime = nil
			return err
		},
		ObserveBoot: func(context.Context) (bootObservation, error) {
			return observeSelfCheckExhaustion(installedLMSLogPath(run.Paths))
		},
		RestoreEtcd: func(composeContext context.Context) error {
			return replaceCaseEtcd(composeContext, h, caseRoot, preservedEtcd, true)
		},
		Search: func(searchContext context.Context) (semanticSearchObservation, error) {
			if runtime == nil {
				return semanticSearchObservation{}, fmt.Errorf("scenario F LMS is not running")
			}
			return searchCodeObservation(searchContext, runtime.client, fixture.root)
		},
		EtcdFingerprint: func() (string, error) {
			root := preservedEtcd
			if _, statErr := os.Stat(root); errors.Is(statErr, os.ErrNotExist) {
				root = filepath.Join(caseRoot, "etcd")
			} else if statErr != nil {
				return "", statErr
			}
			return fingerprintTree(root)
		},
		ConfigurationFingerprint: func() (string, error) {
			return fingerprintInstalledProcess(installedLMSProcessWithoutPeriodicMaintenance(run)), nil
		},
		Recorder: recorder,
	})
	return err
}

func (driver *realAcceptanceDriver) runInstalledScenarioG(
	ctx context.Context,
	run acceptanceRun,
	proxies caseProxies,
	fixture acceptanceFixture,
	recorder *evidenceRecorder,
) error {
	seedProcess := installedLMSProcessWithoutPeriodicMaintenance(run)
	runtime, err := startDaemonRuntime(ctx, seedProcess, run.Paths.LMSSocket)
	if err != nil {
		return err
	}
	defer func() { driver.stopDaemonRuntime(runtime) }()
	if _, err := waitForCompletedIndex(ctx, runtime.client, fixture.root); err != nil {
		return fmt.Errorf("seed scenario G target: %w", err)
	}
	targetCollection := codeCollectionName(fixture.root)
	if err := prepareScenarioGColdTarget(
		ctx,
		runtime,
		targetCollection,
		stopDaemonRuntime,
		func(releaseContext context.Context, collectionName string) error {
			client, clientErr := newCloneMilvusClient(releaseContext)
			if clientErr != nil {
				return clientErr
			}
			defer closeMilvusClient(client)
			return releaseCloneCollection(releaseContext, client, collectionName)
		},
	); err != nil {
		return err
	}
	process := installedLMSProcessForScenarioG(run)
	runtime, err = startDaemonRuntime(ctx, process, run.Paths.LMSSocket)
	if err != nil {
		return err
	}
	editedMarker := fixture.marker + " edited target"
	if err := appendFixtureChange(fixture.root, fixture.files[0], editedMarker); err != nil {
		return err
	}
	baselineLoads := proxies.milvus.CallCount("LoadCollection", cloneMilvusDatabase, targetCollection)
	loadCount := func() int {
		return proxies.milvus.CallCount("LoadCollection", cloneMilvusDatabase, targetCollection) - baselineLoads
	}
	type searchResult struct {
		observation semanticSearchObservation
		err         error
	}
	failureReady := make(chan struct{})
	var failureResult searchResult
	var startFailureCaller sync.Once
	startCaller := func() {
		startFailureCaller.Do(func() {
			go func() {
				failureContext, cancel := context.WithTimeout(ctx, defaultScenarioFailureTimeout)
				defer cancel()
				failureResult.observation, failureResult.err = searchCodeObservation(
					failureContext,
					runtime.client,
					fixture.root,
				)
				close(failureReady)
			}()
		})
	}
	var capacityWatchStarted time.Time
	var secondJobID string
	_, err = runScenarioG(ctx, scenarioGInput{
		SetLoading: func() {
			proxies.milvus.SetLoadState(cloneMilvusDatabase, targetCollection, commonpb.LoadState_LoadStateLoading)
		},
		ClearLoading: func() {
			proxies.milvus.ClearLoadFault(cloneMilvusDatabase, targetCollection)
		},
		StartTargetJob: func(startContext context.Context) (string, error) {
			return startScenarioGTargetJob(
				startContext,
				startCaller,
				loadCount,
				func(jobContext context.Context) (string, error) {
					capacityWatchStarted = time.Now()
					response, startErr := runtime.client.SyncIndex(
						jobContext,
						&pb.SyncIndexRequest{Path: fixture.root, Client: scenarioClientInfo()},
					)
					return response.GetJobId(), startErr
				},
				defaultScenarioReadyTimeout,
				defaultScenarioPollInterval,
			)
		},
		ObserveFailure: func(searchContext context.Context) (semanticSearchObservation, error) {
			select {
			case <-failureReady:
				return failureResult.observation, failureResult.err
			case <-searchContext.Done():
				return semanticSearchObservation{}, context.Cause(searchContext)
			}
		},
		Status: daemonStatusObserver(runtime.client),
		StartSecondJob: func(startContext context.Context) (jobObservation, error) {
			proxies.embedding.GateAfter(len(proxies.embedding.Inputs()))
			response, startErr := runtime.client.StartIndex(startContext, &pb.StartIndexRequest{Path: fixture.secondRoot, Client: scenarioClientInfo()})
			if startErr != nil {
				return jobObservation{}, startErr
			}
			secondJobID = response.GetJobId()
			return jobObservation{ID: secondJobID, State: "queued"}, nil
		},
		ObserveCapacityRelease: func(observeContext context.Context) (time.Duration, error) {
			logPath := installedLMSLogPath(run.Paths)
			if _, observeErr := observeCapacityReleaseEvent(observeContext, logPath, capacityWatchStarted); observeErr != nil {
				return 0, observeErr
			}
			return observeSecondJobStartEvent(observeContext, logPath, capacityWatchStarted, secondJobID)
		},
		ReleaseSecondJob: proxies.embedding.ClearGate,
		ObserveJob:       jobObserver(runtime.client),
		RestartTargetJob: func(startContext context.Context) (string, error) {
			response, startErr := runtime.client.SyncIndex(startContext, &pb.SyncIndexRequest{Path: fixture.root, Client: scenarioClientInfo()})
			return response.GetJobId(), startErr
		},
		LoadCount: loadCount,
		SearchEditedTarget: func(searchContext context.Context) (semanticSearchObservation, error) {
			return searchCodeObservationForQuery(searchContext, runtime.client, fixture.root, editedMarker)
		},
		ExpectedEditedID: fixture.files[0],
		Recorder:         recorder,
	})
	proxies.embedding.ClearGate()
	return err
}

func prepareScenarioGColdTarget(
	ctx context.Context,
	runtime *daemonRuntime,
	collectionName string,
	stop func(*daemonRuntime) error,
	release func(context.Context, string) error,
) error {
	if err := stop(runtime); err != nil {
		return fmt.Errorf("stop scenario G seed daemon: %w", err)
	}
	if err := release(ctx, collectionName); err != nil {
		return fmt.Errorf("release scenario G target collection: %w", err)
	}
	return nil
}

func startScenarioGTargetJob(
	ctx context.Context,
	startCaller func(),
	loadCount func() int,
	startJob func(context.Context) (string, error),
	timeout time.Duration,
	poll time.Duration,
) (string, error) {
	startCaller()
	if err := waitForLoadCount(ctx, loadCount, 1, timeout, poll); err != nil {
		return "", fmt.Errorf("wait for scenario G public caller load: %w", err)
	}
	return startJob(ctx)
}

func (driver *realAcceptanceDriver) runInstalledScenarioH(
	ctx context.Context,
	run acceptanceRun,
	proxies caseProxies,
	fixture acceptanceFixture,
	recorder *evidenceRecorder,
) error {
	if err := writeClydeConversation(run.Paths, fixture.marker, "initial"); err != nil {
		return err
	}
	lms, err := startDaemonRuntime(ctx, installedLMSProcessWithoutPeriodicMaintenance(run), run.Paths.LMSSocket)
	if err != nil {
		return err
	}
	defer driver.stopDaemonRuntime(lms)
	if _, err := waitForCompletedIndex(ctx, lms.client, fixture.root); err != nil {
		return fmt.Errorf("seed scenario H LMS target: %w", err)
	}
	clyde, err := driver.startClydeRuntime(ctx, run)
	if err != nil {
		return err
	}
	defer driver.stopInstalledProcess(clyde.process)
	milvusClient, err := newCloneMilvusClient(ctx)
	if err != nil {
		return err
	}
	defer closeMilvusClient(milvusClient)
	_, err = waitForSeededClydeSearch(
		ctx,
		func(statusContext context.Context) (clydeStatusObservation, error) {
			return driver.clydeStatus(statusContext, run, clyde.process.Process.Pid)
		},
		func(searchContext context.Context) (semanticSearchObservation, error) {
			return driver.searchClyde(searchContext, run, fixture.marker)
		},
		maximumClydeFeederRecovery,
		defaultScenarioFailureTimeout,
		defaultScenarioPollInterval,
	)
	if err != nil {
		return fmt.Errorf("seed scenario H Clyde target: %w", err)
	}
	_, err = runScenarioH(ctx, scenarioHInput{
		SetFault: func(fault dependencyFault) {
			if fault == dependencyEmbedding {
				proxies.embedding.SetFailure(503, embedderBusyCode)
				return
			}
			proxies.milvus.SetUnavailable(codes.Unavailable, milvusUnavailableCode)
		},
		ClearFault: func(fault dependencyFault) {
			if fault == dependencyEmbedding {
				proxies.embedding.ClearFailure()
				return
			}
			proxies.milvus.ClearUnavailable()
		},
		SearchLMS: func(searchContext context.Context) (semanticSearchObservation, error) {
			return searchCodeObservation(searchContext, lms.client, fixture.root)
		},
		SearchClyde: func(searchContext context.Context) (semanticSearchObservation, error) {
			return driver.searchClyde(searchContext, run, fixture.marker)
		},
		SnapshotState: func(snapshotContext context.Context) (stateObservation, error) {
			collectionID := "restart-" + filepath.Base(run.Paths.RunRoot)
			conversationCollection := "conv_chunks_" + tshash.PathPrefix(collectionID)
			return snapshotCloneState(
				snapshotContext,
				milvusClient,
				installedLMSMerklePath(run.Paths),
				[]string{codeCollectionName(fixture.root), conversationCollection},
			)
		},
		LMSStatus: daemonStatusObserver(lms.client),
		ClydeStatus: func(statusContext context.Context) (clydeStatusObservation, error) {
			return driver.clydeStatus(statusContext, run, clyde.process.Process.Pid)
		},
		Recorder: recorder,
	})
	proxies.embedding.ClearFailure()
	proxies.milvus.ClearUnavailable()
	return err
}

func (driver *realAcceptanceDriver) runInstalledScenarioI(
	ctx context.Context,
	run acceptanceRun,
	h *harness,
	proxies caseProxies,
	fixture acceptanceFixture,
	recorder *evidenceRecorder,
) (runErr error) {
	proxies.embedding.GateAfter(0)
	runtime, err := startDaemonRuntime(ctx, installedLMSProcess(run), run.Paths.LMSSocket)
	if err != nil {
		return err
	}
	defer func() {
		if runtime != nil {
			runErr = errors.Join(runErr, stopDaemonRuntime(runtime))
		}
	}()

	priorityLow := pb.SchedulingPriority_SCHEDULING_PRIORITY_LOW
	priorityHigh := pb.SchedulingPriority_SCHEDULING_PRIORITY_HIGH
	var stoppedAt time.Time
	_, err = runScenarioI(ctx, scenarioIInput{
		StartLow: func(startContext context.Context) (*pb.StartIndexResponse, error) {
			return runtime.client.StartIndex(startContext, &pb.StartIndexRequest{
				Path: fixture.root,
				Client: scenarioClientInfo(),
				SchedulingPolicy: &pb.SchedulingPolicyPatch{Priority: &priorityLow},
			})
		},
		WaitForLowBoundary: func(boundaryContext context.Context) error {
			select {
			case <-proxies.embedding.GateReached():
				return nil
			case <-boundaryContext.Done():
				return context.Cause(boundaryContext)
			}
		},
		StartHigh: func(startContext context.Context) (*pb.StartIndexResponse, error) {
			return runtime.client.StartIndex(startContext, &pb.StartIndexRequest{
				Path: fixture.secondRoot,
				Client: scenarioClientInfo(),
				SchedulingPolicy: &pb.SchedulingPolicyPatch{Priority: &priorityHigh},
			})
		},
		ReleaseLowBoundary: proxies.embedding.ClearGate,
		WaitForLowPaused: func(waitContext context.Context, jobID string) (*pb.Job, error) {
			return waitForJob(waitContext, runtime.client, jobID, defaultScenarioReadyTimeout, defaultScenarioPollInterval, func(job *pb.Job) bool {
				return job.GetState() == "paused"
			})
		},
		CaptureLowJobs: func(captureContext context.Context, codebaseID string) (map[string]struct{}, error) {
			return captureJobSet(captureContext, runtime.client, codebaseID)
		},
		StopDaemon: func(stopContext context.Context) error {
			stoppedAt = time.Now()
			err := runtime.stop(stopContext)
			runtime = nil
			return err
		},
		StartDaemon: func(startContext context.Context) error {
			started, startErr := startDaemonRuntime(startContext, installedLMSProcess(run), run.Paths.LMSSocket)
			if startErr == nil {
				runtime = started
			}
			return startErr
		},
		WaitForLowSuccessor: func(waitContext context.Context, codebaseID string, before map[string]struct{}) (*pb.Job, error) {
			successor, successorErr := waitForSuccessor(waitContext, runtime.client, codebaseID, before, stoppedAt, defaultScenarioRecoveryTimeout, defaultScenarioPollInterval)
			if successorErr != nil {
				return nil, successorErr
			}
			return waitForJob(waitContext, runtime.client, successor.GetId(), defaultScenarioRecoveryTimeout, defaultScenarioPollInterval, func(job *pb.Job) bool {
				return job.GetState() == "completed"
			})
		},
		VerifyCloneInventory: func(censusContext context.Context) error {
			census, censusErr := h.census(censusContext)
			if censusErr != nil {
				return censusErr
			}
			if len(census.Databases) != 1 || census.Databases[0] != cloneMilvusDatabase {
				return fmt.Errorf("isolated Milvus databases = %v, want [%s]", census.Databases, cloneMilvusDatabase)
			}
			return nil
		},
		Recorder: recorder,
	})
	return err
}

func waitForSeededClydeSearch(
	ctx context.Context,
	statusObserver func(context.Context) (clydeStatusObservation, error),
	search func(context.Context) (semanticSearchObservation, error),
	timeout time.Duration,
	searchAttemptTimeout time.Duration,
	poll time.Duration,
) (semanticSearchObservation, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastStatus clydeStatusObservation
	var lastErr error
	for {
		observation, err := statusObserver(deadlineContext)
		lastStatus = observation
		lastErr = err
		if err == nil && observation.Responding && observation.PID > 0 &&
			observation.Manifest > 0 && observation.Pending == 0 &&
			observation.Needed == 0 && observation.Embedded == observation.Manifest {
			boundedSearch := func(searchContext context.Context) (semanticSearchObservation, error) {
				attemptContext, cancelAttempt := context.WithTimeout(searchContext, searchAttemptTimeout)
				defer cancelAttempt()
				return search(attemptContext)
			}
			return waitForSemanticSuccess(deadlineContext, boundedSearch, timeout, poll)
		}
		select {
		case <-deadlineContext.Done():
			return semanticSearchObservation{}, fmt.Errorf(
				"wait for initial Clyde feeder convergence: %w; last status=%+v error=%v",
				context.Cause(deadlineContext),
				lastStatus,
				lastErr,
			)
		case <-time.After(poll):
		}
	}
}

type clydeRuntime struct {
	process *exec.Cmd
}

func (driver *realAcceptanceDriver) startClydeRuntime(ctx context.Context, run acceptanceRun) (*clydeRuntime, error) {
	environment := isolatedClydeEnvironment(run.Paths)
	environment["CODEX_HOME"] = filepath.Join(run.Paths.ClydeHome, "codex")
	environment["CODEX_SQLITE_HOME"] = filepath.Join(run.Paths.ClydeHome, "codex-sqlite")
	environment["CLYDE_CURSOR_DATA_DIRS"] = filepath.Join(run.Paths.ClydeHome, "cursor-data")
	environment["CLYDE_CURSOR_PROJECTS_DIRS"] = filepath.Join(run.Paths.ClydeHome, "cursor-projects")
	environment["CLYDE_ZED_DATA_DIRS"] = filepath.Join(run.Paths.ClydeHome, "zed-data")
	for _, path := range []string{
		environment["CODEX_HOME"],
		environment["CODEX_SQLITE_HOME"],
		environment["CLYDE_CURSOR_DATA_DIRS"],
		environment["CLYDE_CURSOR_PROJECTS_DIRS"],
		environment["CLYDE_ZED_DATA_DIRS"],
	} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return nil, err
		}
	}
	process, err := startInstalledProcess(installedProcess{
		Path: run.Binaries.Clyde, Args: []string{"daemon", "run"}, Environment: environment,
	})
	if err != nil {
		return nil, err
	}
	deadlineContext, cancel := context.WithTimeout(ctx, defaultScenarioReadyTimeout)
	defer cancel()
	for {
		observation, statusErr := driver.clydeStatus(deadlineContext, run, process.Process.Pid)
		if statusErr == nil && observation.Responding && observation.PID > 0 {
			return &clydeRuntime{process: process}, nil
		}
		select {
		case <-deadlineContext.Done():
			driver.stopInstalledProcess(process)
			return nil, fmt.Errorf("wait for installed Clyde worker: %w", context.Cause(deadlineContext))
		case <-time.After(defaultScenarioPollInterval):
		}
	}
}

func clydeEnvironment(paths runPaths) map[string]string {
	environment := isolatedClydeEnvironment(paths)
	environment["CODEX_HOME"] = filepath.Join(paths.ClydeHome, "codex")
	environment["CODEX_SQLITE_HOME"] = filepath.Join(paths.ClydeHome, "codex-sqlite")
	environment["CLYDE_CURSOR_DATA_DIRS"] = filepath.Join(paths.ClydeHome, "cursor-data")
	environment["CLYDE_CURSOR_PROJECTS_DIRS"] = filepath.Join(paths.ClydeHome, "cursor-projects")
	environment["CLYDE_ZED_DATA_DIRS"] = filepath.Join(paths.ClydeHome, "zed-data")
	return environment
}

func (driver *realAcceptanceDriver) clydeStatus(
	ctx context.Context,
	run acceptanceRun,
	pid int,
) (clydeStatusObservation, error) {
	output, err := driver.runner.Run(ctx, clydeEnvironment(run.Paths), run.Binaries.Clyde, "--output-format=json", "status", "--once")
	if err != nil {
		return clydeStatusObservation{}, err
	}
	var decoded struct {
		Daemon struct {
			Responding bool  `json:"responding"`
			WorkerPIDs []int `json:"worker_pids"`
		} `json:"daemon"`
		Freshness *struct {
			LastSyncUnix int64 `json:"last_sync_unix"`
			Manifest     int   `json:"manifest"`
			Needed       int   `json:"needed"`
			Embedded     int   `json:"embedded"`
			Pending      int   `json:"pending"`
		} `json:"freshness"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		return clydeStatusObservation{}, fmt.Errorf("decode Clyde status: %w", err)
	}
	observation := clydeStatusObservation{Responding: decoded.Daemon.Responding}
	if slices.Contains(decoded.Daemon.WorkerPIDs, pid) {
		observation.PID = pid
	} else if len(decoded.Daemon.WorkerPIDs) == 1 {
		observation.PID = decoded.Daemon.WorkerPIDs[0]
	}
	if decoded.Freshness != nil {
		observation.LastSyncUnix = decoded.Freshness.LastSyncUnix
		observation.Manifest = decoded.Freshness.Manifest
		observation.Needed = decoded.Freshness.Needed
		observation.Embedded = decoded.Freshness.Embedded
		observation.Pending = decoded.Freshness.Pending
	}
	return observation, nil
}

func (driver *realAcceptanceDriver) searchClyde(
	ctx context.Context,
	run acceptanceRun,
	query string,
) (semanticSearchObservation, error) {
	output, err := driver.runner.Run(
		ctx,
		clydeEnvironment(run.Paths),
		run.Binaries.Clyde,
		"--output-format=json", "conversation", "search", "--query", query, "--limit", "3",
	)
	if err != nil {
		return semanticSearchObservation{Code: classifySearchError(err)}, nil
	}
	var decoded struct {
		ReturnedCount int    `json:"returned_count"`
		Source        string `json:"source"`
		Matches       []struct {
			Conversation struct {
				ID string `json:"id"`
			} `json:"conversation"`
			MessageIndex int `json:"message_index"`
		} `json:"matches"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		return semanticSearchObservation{}, fmt.Errorf("decode Clyde conversation search: %w", err)
	}
	resultIDs := make([]string, 0, len(decoded.Matches))
	for _, match := range decoded.Matches {
		resultIDs = append(resultIDs, match.Conversation.ID+":"+strconv.Itoa(match.MessageIndex))
	}
	return semanticSearchObservation{Succeeded: true, Source: decoded.Source, Matches: decoded.ReturnedCount, ResultIDs: resultIDs}, nil
}

func writeClydeConversation(paths runPaths, marker string, suffix string) error {
	directory := filepath.Join(paths.ClydeHome, ".claude", "projects", "restart-acceptance")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create isolated Claude transcript directory: %w", err)
	}
	type transcriptMessage struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type transcriptRecord struct {
		Type       string            `json:"type"`
		Message    transcriptMessage `json:"message"`
		SessionID  string            `json:"sessionId"`
		CWD        string            `json:"cwd"`
		UUID       string            `json:"uuid"`
		ParentUUID string            `json:"parentUuid"`
		Timestamp  string            `json:"timestamp"`
	}
	sessionID := "11111111-1111-4111-8111-111111111111"
	uuid := "22222222-2222-4222-8222-222222222222"
	if suffix != "initial" {
		sessionID = "33333333-3333-4333-8333-333333333333"
		uuid = "44444444-4444-4444-8444-444444444444"
	}
	record := transcriptRecord{
		Type: "user", Message: transcriptMessage{Role: "user", Content: marker},
		SessionID: sessionID, CWD: paths.RunRoot,
		UUID: uuid, Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, suffix+".jsonl"), append(body, '\n'), 0o600)
}

func requireClonePayload(caseRoot string) error {
	for _, name := range []string{"milvus", "minio"} {
		regularFiles := 0
		err := filepath.WalkDir(filepath.Join(caseRoot, name), func(_ string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.Type().IsRegular() {
				regularFiles++
			}
			return nil
		})
		if err != nil {
			return err
		}
		if regularFiles == 0 {
			return fmt.Errorf("restored %s clone contains no regular payload files", name)
		}
	}
	return nil
}

func replaceCaseEtcd(ctx context.Context, h *harness, caseRoot string, preservedEtcd string, restore bool) error {
	environment, err := h.composeEnvironment()
	if err != nil {
		return err
	}
	composeArgs := []string{"compose", "-p", h.composeProject, "-f", h.paths.ComposeFile}
	if _, err := h.runner.Run(ctx, environment, "docker", append(composeArgs, "stop", "standalone", "etcd")...); err != nil {
		return err
	}
	etcdPath := filepath.Join(caseRoot, "etcd")
	if restore {
		if err := removeTree(ctx, etcdPath); err != nil {
			return err
		}
		if err := os.Rename(preservedEtcd, etcdPath); err != nil {
			return fmt.Errorf("restore preserved etcd clone: %w", err)
		}
	} else {
		if err := os.Rename(etcdPath, preservedEtcd); err != nil {
			return fmt.Errorf("preserve etcd clone: %w", err)
		}
		if err := os.MkdirAll(etcdPath, 0o700); err != nil {
			return err
		}
	}
	if restore {
		return nil
	}
	_, err = h.runner.Run(ctx, environment, "docker", append(composeArgs, "up", "-d", "--wait")...)
	return err
}

func startCaseCompose(ctx context.Context, h *harness) error {
	environment, err := h.composeEnvironment()
	if err != nil {
		return err
	}
	composeArgs := []string{"compose", "-p", h.composeProject, "-f", h.paths.ComposeFile, "up", "-d", "--wait"}
	_, err = h.runner.Run(ctx, environment, "docker", composeArgs...)
	return err
}

func observeSelfCheckExhaustion(path string) (bootObservation, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return bootObservation{}, nil
	}
	if err != nil {
		return bootObservation{}, err
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, "daemon.selfcheck.exhausted") {
			continue
		}
		return bootObservation{FailureCode: classifyBootFailure(line), SelfCheckExhausted: true}, nil
	}
	return bootObservation{}, scanner.Err()
}

func classifyBootFailure(message string) string {
	for _, code := range []string{collectionNotReadyCode, milvusUnavailableCode, "collection_missing"} {
		if strings.Contains(message, code) {
			return code
		}
	}
	return "selfcheck_exhausted"
}

func fingerprintTree(root string) (string, error) {
	digest := sha256.New()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		_, _ = io.WriteString(digest, relative+"\x00")
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(digest, file)
		closeErr := file.Close()
		return errors.Join(copyErr, closeErr)
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func fingerprintInstalledProcess(process installedProcess) string {
	body, _ := json.Marshal(process)
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func jobObserver(client pb.SemanticSearchDaemonServiceClient) func(context.Context, string) (jobObservation, error) {
	return func(ctx context.Context, jobID string) (jobObservation, error) {
		response, err := client.GetJob(ctx, &pb.GetJobRequest{JobId: jobID})
		if err != nil {
			return jobObservation{}, err
		}
		job := response.GetJob()
		return jobObservation{ID: job.GetId(), State: job.GetState(), FailureCode: job.GetError().GetCode()}, nil
	}
}

func daemonStatusObserver(client pb.SemanticSearchDaemonServiceClient) func(context.Context) (daemonStatusObservation, error) {
	return func(ctx context.Context) (daemonStatusObservation, error) {
		_, err := client.GetStatus(ctx, &pb.GetStatusRequest{})
		return daemonStatusObservation{Responding: err == nil}, err
	}
}

func observeCapacityReleaseEvent(ctx context.Context, logPath string, notBefore time.Time) (time.Duration, error) {
	for {
		grace, found, err := readCapacityReleaseEvent(logPath, notBefore)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		if found {
			return grace, nil
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("observe capacity release event: %w", context.Cause(ctx))
		case <-time.After(defaultScenarioPollInterval):
		}
	}
}

func readCapacityReleaseEvent(logPath string, notBefore time.Time) (time.Duration, bool, error) {
	file, err := os.Open(logPath)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = file.Close() }()
	type capacityLogEntry struct {
		Time      time.Time `json:"time"`
		Message   string    `json:"msg"`
		ElapsedMS int64     `json:"elapsed_ms"`
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry capacityLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		if entry.Message != "released indexing capacity for a stalled read" || entry.Time.Before(notBefore) {
			continue
		}
		if entry.ElapsedMS <= 0 {
			return 0, false, fmt.Errorf("capacity release log omitted elapsed_ms")
		}
		return time.Duration(entry.ElapsedMS) * time.Millisecond, true, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, false, fmt.Errorf("scan capacity release log: %w", err)
	}
	return 0, false, nil
}

func observeSecondJobStartEvent(
	ctx context.Context,
	logPath string,
	notBefore time.Time,
	jobID string,
) (time.Duration, error) {
	for {
		elapsed, found, err := readSecondJobStartEvent(logPath, notBefore, jobID)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		if found {
			return elapsed, nil
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("observe second job start event: %w", context.Cause(ctx))
		case <-time.After(defaultScenarioPollInterval):
		}
	}
}

func readSecondJobStartEvent(
	logPath string,
	notBefore time.Time,
	jobID string,
) (time.Duration, bool, error) {
	file, err := os.Open(logPath)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = file.Close() }()
	type jobLogEntry struct {
		Time    time.Time `json:"time"`
		Message string    `json:"msg"`
		Span    string    `json:"span"`
		JobID   string    `json:"job_id"`
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry jobLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		if entry.Message != "daemon.span.started" || entry.Span != "daemon.runJob" ||
			entry.JobID != jobID || entry.Time.Before(notBefore) {
			continue
		}
		return entry.Time.Sub(notBefore), true, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, false, fmt.Errorf("scan second job start log: %w", err)
	}
	return 0, false, nil
}

func appendFixtureChange(root string, relativePath string, marker string) error {
	path := filepath.Join(root, relativePath)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, writeErr := io.WriteString(file, "\n// "+marker+"\n")
	closeErr := file.Close()
	return errors.Join(writeErr, closeErr)
}

func prepareScenarioBRebuild(fixture acceptanceFixture) (map[string]string, error) {
	added := []string{"04.go", "05.go"}
	paths := make([]string, len(added))
	for index, name := range added {
		paths[index] = filepath.Join(fixture.root, name)
		if _, err := os.Lstat(paths[index]); err == nil {
			return nil, fmt.Errorf("scenario B fixture target %q already exists", name)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}
	written := make([]string, 0, len(paths))
	hashes := make(map[string]string, len(paths))
	for index, name := range added {
		body := fmt.Sprintf(
			"package fixture\n\n// restart_acceptance_id:%s\nfunc ScenarioB%d() string { return %q }\n",
			name,
			index+1,
			fixture.marker,
		)
		if err := os.WriteFile(paths[index], []byte(body), 0o600); err != nil {
			cleanupErrors := []error{err}
			for _, path := range written {
				cleanupErrors = append(cleanupErrors, os.Remove(path))
			}
			return nil, errors.Join(cleanupErrors...)
		}
		written = append(written, paths[index])
		digest := sha256.Sum256([]byte(body))
		hashes[name] = hex.EncodeToString(digest[:])
	}
	return hashes, nil
}

func snapshotCloneState(
	ctx context.Context,
	client *milvusclient.Client,
	checkpointRoot string,
	detailedCollectionNames []string,
) (stateObservation, error) {
	collectionNames, err := client.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		return stateObservation{}, err
	}
	slices.Sort(collectionNames)
	collections := make([]collectionStateObservation, 0, len(collectionNames))
	for _, collectionName := range collectionNames {
		stats, statsErr := client.GetCollectionStats(ctx, milvusclient.NewGetCollectionStatsOption(collectionName))
		if statsErr != nil {
			return stateObservation{}, statsErr
		}
		rowCount, parseErr := strconv.Atoi(stats["row_count"])
		if parseErr != nil {
			return stateObservation{}, fmt.Errorf("parse %s row count: %w", collectionName, parseErr)
		}
		var rows []rowStateObservation
		detailed := slices.Contains(detailedCollectionNames, collectionName)
		if detailed {
			rows, err = snapshotCollectionRows(ctx, client, collectionName)
			if err != nil {
				return stateObservation{}, err
			}
		}
		rowCount = snapshotRowCount(rowCount, rows, detailed)
		collections = append(collections, collectionStateObservation{Name: collectionName, RowCount: rowCount, Rows: rows})
	}
	checkpoints, err := snapshotCheckpointFiles(checkpointRoot)
	if err != nil {
		return stateObservation{}, err
	}
	return stateObservation{Collections: collections, Checkpoints: checkpoints}, nil
}

func snapshotRowCount(statisticsCount int, rows []rowStateObservation, detailed bool) int {
	if detailed {
		return len(rows)
	}
	return statisticsCount
}

func snapshotCollectionRows(ctx context.Context, client *milvusclient.Client, collectionName string) ([]rowStateObservation, error) {
	result, err := client.Query(ctx, milvusclient.NewQueryOption(collectionName).
		WithFilter(`id != ""`).
		WithOutputFields("id", "relativePath", "startLine", "endLine", "splitPart", "vector").
		WithConsistencyLevel(entity.ClStrong))
	if err != nil {
		return nil, fmt.Errorf("query state rows from %s: %w", collectionName, err)
	}
	idColumn := result.GetColumn("id")
	relativePathColumn := result.GetColumn("relativePath")
	startLineColumn := result.GetColumn("startLine")
	endLineColumn := result.GetColumn("endLine")
	vectorColumn := result.GetColumn("vector")
	if idColumn == nil || relativePathColumn == nil || startLineColumn == nil || endLineColumn == nil || vectorColumn == nil {
		return nil, fmt.Errorf("state row query from %s omitted required columns", collectionName)
	}
	rows := make([]rowStateObservation, 0, result.ResultCount)
	for index := range result.ResultCount {
		id, idErr := idColumn.GetAsString(index)
		relativePath, relativePathErr := relativePathColumn.GetAsString(index)
		startLine, startLineErr := startLineColumn.GetAsInt64(index)
		endLine, endLineErr := endLineColumn.GetAsInt64(index)
		if err := errors.Join(idErr, relativePathErr, startLineErr, endLineErr); err != nil {
			return nil, fmt.Errorf("read state identity from %s: %w", collectionName, err)
		}
		splitPart := int64(0)
		if splitPartColumn := result.GetColumn("splitPart"); splitPartColumn != nil {
			var splitPartErr error
			splitPart, splitPartErr = splitPartColumn.GetAsInt64(index)
			if splitPartErr != nil {
				return nil, fmt.Errorf("read state split part from %s: %w", collectionName, splitPartErr)
			}
		}
		vectorValue, vectorErr := vectorColumn.Get(index)
		if vectorErr != nil {
			return nil, fmt.Errorf("read state vector from %s: %w", collectionName, vectorErr)
		}
		vector, ok := vectorValue.(entity.FloatVector)
		if !ok {
			return nil, fmt.Errorf("state vector from %s has type %T", collectionName, vectorValue)
		}
		identity := fmt.Sprintf("%s|%s|%d|%d|%d", id, relativePath, startLine, endLine, splitPart)
		rows = append(rows, rowStateObservation{Identity: identity, DenseVectorHash: hashDenseVector(vector)})
	}
	slices.SortFunc(rows, func(left rowStateObservation, right rowStateObservation) int {
		return strings.Compare(left.Identity, right.Identity)
	})
	return rows, nil
}

func snapshotCheckpointFiles(root string) ([]string, error) {
	checkpoints := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.Type().IsRegular() || filepath.Ext(path) != ".json" {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(body)
		checkpoints = append(checkpoints, relative+"="+hex.EncodeToString(digest[:]))
		return nil
	})
	slices.Sort(checkpoints)
	return checkpoints, err
}

func checkpointObserverForSocket(socket string, path string, merkleRoot string) checkpointSnapshotObserver {
	return func(ctx context.Context) (checkpointSnapshot, error) {
		client, connection, err := waitForDaemon(ctx, socket, defaultScenarioReadyTimeout, defaultScenarioPollInterval)
		if err != nil {
			return checkpointSnapshot{}, err
		}
		defer func() { _ = connection.Close() }()
		return checkpointObserver(client, path, merkleRoot)(ctx)
	}
}

func stopDaemonRuntime(runtime *daemonRuntime) error {
	return stopDaemonRuntimeWithin(runtime, daemonProcessExitTimeout)
}

func stopDaemonRuntimeWithin(runtime *daemonRuntime, timeout time.Duration) error {
	if runtime == nil {
		return nil
	}
	stopContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := runtime.stop(stopContext); err != nil {
		killErr := runtime.kill()
		return errors.Join(fmt.Errorf("stop LMS daemon: %w", err), killErr)
	}
	return nil
}

func stopInstalledProcess(process *exec.Cmd) error {
	if process == nil || process.Process == nil {
		return nil
	}
	signalErr := process.Process.Signal(os.Interrupt)
	if errors.Is(signalErr, os.ErrProcessDone) {
		signalErr = nil
	}
	done := make(chan error, 1)
	go func() {
		done <- process.Wait()
	}()
	select {
	case waitErr := <-done:
		if signalErr == nil && exitedFromSignal(waitErr, os.Interrupt) {
			waitErr = nil
		}
		return errors.Join(signalErr, waitErr)
	case <-time.After(10 * time.Second):
		killErr := process.Process.Kill()
		var waitErr error
		select {
		case waitErr = <-done:
		case <-time.After(5 * time.Second):
			return errors.Join(signalErr, killErr, fmt.Errorf("installed process did not exit within 5s after kill"))
		}
		if killErr == nil && exitedFromSignal(waitErr, os.Kill) {
			waitErr = nil
		}
		return errors.Join(signalErr, killErr, waitErr, fmt.Errorf("installed process did not stop within 10s"))
	}
}

func waitForSignaledProcess(process *exec.Cmd, signal os.Signal, timeout time.Duration) (*os.ProcessState, error) {
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	select {
	case waitErr := <-done:
		if exitedFromSignal(waitErr, signal) {
			waitErr = nil
		}
		return process.ProcessState, waitErr
	case <-time.After(timeout):
		return nil, fmt.Errorf("process did not exit within %s after %s", timeout, signal)
	}
}

func exitedFromSignal(err error, signal os.Signal) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
	want, okSignal := signal.(syscall.Signal)
	return ok && okSignal && status.Signaled() && status.Signal() == want
}

func closeMilvusClient(client *milvusclient.Client) {
	closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = client.Close(closeContext)
}

func resetIsolatedRuntime(paths runPaths) error {
	for _, path := range []string{paths.LMSState, paths.LMSContext, filepath.Dir(paths.ClydeConfig)} {
		if err := removeTree(context.Background(), path); err != nil {
			return err
		}
	}
	return createIsolationLayout(paths)
}

type caseProxies struct {
	embedding  *embeddingProxy
	backend    *localEmbeddingBackend
	milvus     *milvusProxy
	serveError chan error
}

func startCaseProxies(ctx context.Context) (caseProxies, error) {
	readinessContext, cancel := context.WithTimeout(ctx, embeddingReadinessTimeout)
	defer cancel()
	backend, err := startLocalEmbeddingBackend()
	if err != nil {
		return caseProxies{}, err
	}
	if err := verifyEmbeddingReadiness(
		readinessContext,
		backend.URL(),
		cloneEmbeddingModel,
		cloneEmbeddingDimension,
	); err != nil {
		return caseProxies{}, errors.Join(err, backend.Close())
	}
	embeddingListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", embeddingProxyPort))
	if err != nil {
		return caseProxies{}, errors.Join(fmt.Errorf("listen embedding proxy: %w", err), backend.Close())
	}
	embedding, err := newEmbeddingProxy(embeddingListener, backend.URL())
	if err != nil {
		_ = embeddingListener.Close()
		return caseProxies{}, errors.Join(err, backend.Close())
	}
	milvusListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", milvusProxyPort))
	if err != nil {
		_ = embedding.Close()
		return caseProxies{}, errors.Join(fmt.Errorf("listen Milvus proxy: %w", err), backend.Close())
	}
	milvus, err := newMilvusProxy(milvusListener, cloneMilvusAddress)
	if err != nil {
		_ = embedding.Close()
		_ = milvusListener.Close()
		return caseProxies{}, errors.Join(err, backend.Close())
	}
	serveError := make(chan error, 2)
	go func() { serveError <- embedding.Serve() }()
	go func() { serveError <- milvus.Serve() }()
	return caseProxies{
		embedding:  embedding,
		backend:    backend,
		milvus:     milvus,
		serveError: serveError,
	}, nil
}

func verifyEmbeddingReadiness(
	ctx context.Context,
	baseURL string,
	model string,
	dimension int32,
) error {
	readinessContext, cancel := context.WithTimeout(ctx, embeddingReadinessTimeout)
	defer cancel()
	body, err := json.Marshal(struct {
		Model      string   `json:"model"`
		Input      []string `json:"input"`
		Dimensions int32    `json:"dimensions,omitempty"`
	}{
		Model:      model,
		Input:      []string{"restart acceptance embedding readiness"},
		Dimensions: dimension,
	})
	if err != nil {
		return fmt.Errorf("encode embedding readiness request: %w", err)
	}
	endpoint := strings.TrimRight(baseURL, "/") + "/embeddings"
	request, err := http.NewRequestWithContext(readinessContext, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create embedding readiness request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("verify embedding readiness: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, embeddingReadinessResponseLimit))
		return fmt.Errorf("verify embedding readiness: endpoint returned HTTP %d", response.StatusCode)
	}
	var result struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, embeddingReadinessResponseLimit+1))
	if err != nil {
		return fmt.Errorf("read embedding readiness response: %w", err)
	}
	if len(payload) > embeddingReadinessResponseLimit {
		return fmt.Errorf("verify embedding readiness: response exceeds %d bytes", embeddingReadinessResponseLimit)
	}
	if err := json.Unmarshal(payload, &result); err != nil {
		return fmt.Errorf("decode embedding readiness response: %w", err)
	}
	if len(result.Data) != 1 || len(result.Data[0].Embedding) == 0 {
		return fmt.Errorf("verify embedding readiness: endpoint returned an empty vector")
	}
	return nil
}

func (proxies caseProxies) close() error {
	closeErr := errors.Join(proxies.embedding.Close(), proxies.milvus.Close(), proxies.backend.Close())
	for range 2 {
		select {
		case serveErr := <-proxies.serveError:
			closeErr = errors.Join(closeErr, serveErr)
		case <-time.After(5 * time.Second):
			closeErr = errors.Join(closeErr, fmt.Errorf("proxy did not stop within 5s"))
		}
	}
	return closeErr
}

type acceptanceFixture struct {
	root       string
	secondRoot string
	files      []string
	marker     string
}

func createAcceptanceFixture(root string, name string) (acceptanceFixture, error) {
	secondRoot := root + "-second"
	for _, directory := range []string{root, secondRoot} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return acceptanceFixture{}, fmt.Errorf("create fixture: %w", err)
		}
	}
	marker := "restart_acceptance_" + name
	files := []string{"01.go", "02.go", "03.go"}
	for index, name := range files {
		body := fmt.Sprintf("package fixture\n\n// restart_acceptance_id:%s\nfunc Marker%d() string { return %q }\n", name, index+1, marker)
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o600); err != nil {
			return acceptanceFixture{}, err
		}
	}
	body := fmt.Sprintf("package second\n\n// restart_acceptance_id:second.go\nfunc Marker() string { return %q }\n", marker)
	if err := os.WriteFile(filepath.Join(secondRoot, "second.go"), []byte(body), 0o600); err != nil {
		return acceptanceFixture{}, err
	}
	return acceptanceFixture{root: root, secondRoot: secondRoot, files: files, marker: marker}, nil
}

func installedLMSProcess(run acceptanceRun) installedProcess {
	environment := isolatedLMSEnvironment(run.Paths)
	environment["EMBEDDING_BATCH_SIZE"] = "1"
	environment["CLAUDE_CONTEXT_SYNC_INTERVAL_MS"] = "1000"
	environment["CLAUDE_CONTEXT_MAX_CONCURRENT_INDEX_JOBS"] = "1"
	environment["CLAUDE_CONTEXT_MILVUS_METADATA_CALL_TIMEOUT_MS"] = "60000"
	environment["CLAUDE_CONTEXT_MILVUS_COLLECTION_LOAD_TIMEOUT_MS"] = "90000"
	environment["CLAUDE_CONTEXT_EMBEDDING_REQUEST_TIMEOUT_MS"] = "5000"
	environment["CLAUDE_CONTEXT_DEBUG_LISTENER"] = "false"
	environment["HYBRID_MODE"] = "true"
	return installedProcess{Path: run.Binaries.Daemon, Environment: environment}
}

func installedLMSProcessWithoutPeriodicMaintenance(run acceptanceRun) installedProcess {
	process := installedLMSProcess(run)
	process.Environment["CLAUDE_CONTEXT_BACKGROUND_SYNC"] = "false"
	return process
}

func installedLMSProcessForScenarioB(run acceptanceRun) installedProcess {
	process := installedLMSProcess(run)
	process.Environment["CLAUDE_CONTEXT_BACKGROUND_SYNC"] = "false"
	process.Environment["CLAUDE_CONTEXT_TRIGGER_WATCHER"] = "false"
	process.Environment["CLAUDE_CONTEXT_FILE_WATCHER"] = "false"
	process.Environment["CLAUDE_CONTEXT_MILVUS_METADATA_CALL_TIMEOUT_MS"] = strconv.Itoa(scenarioBMetadataTimeoutMilliseconds)
	return process
}

func installedLMSProcessForScenarioG(run acceptanceRun) installedProcess {
	process := installedLMSProcessWithoutPeriodicMaintenance(run)
	process.Environment["CLAUDE_CONTEXT_MILVUS_COLLECTION_IDLE_TIMEOUT_MS"] = strconv.Itoa(scenarioGIdleTimeoutMilliseconds)
	process.Environment["CLAUDE_CONTEXT_MILVUS_COLLECTION_LOAD_TIMEOUT_MS"] = strconv.Itoa(scenarioGLoadTimeoutMilliseconds)
	process.Environment["CLAUDE_CONTEXT_MILVUS_COLLECTION_LOAD_WAIT_TIMEOUT_MS"] = strconv.Itoa(scenarioGCallerWaitMilliseconds)
	return process
}

func installedLMSLogPath(paths runPaths) string {
	return filepath.Join(paths.LMSState, "lm-semantic-search", "logs", "lm-semantic-search-daemon.log")
}

func installedLMSMerklePath(paths runPaths) string {
	return filepath.Join(paths.LMSState, "lm-semantic-search", "merkle")
}

type daemonRuntime struct {
	process *exec.Cmd
	client  pb.SemanticSearchDaemonServiceClient
	close   interface{ Close() error }
	spec    installedProcess
	socket  string
}

func startDaemonRuntime(ctx context.Context, spec installedProcess, socket string) (*daemonRuntime, error) {
	process, err := startInstalledProcess(spec)
	if err != nil {
		return nil, err
	}
	client, connection, err := waitForDaemon(ctx, socket, defaultScenarioReadyTimeout, defaultScenarioPollInterval)
	if err != nil {
		return nil, errors.Join(err, stopInstalledProcess(process))
	}
	return &daemonRuntime{process: process, client: client, close: connection, spec: spec, socket: socket}, nil
}

func (runtime *daemonRuntime) stop(ctx context.Context) error {
	if runtime == nil || runtime.process == nil {
		return nil
	}
	_, shutdownErr := runtime.client.Shutdown(ctx, &pb.ShutdownRequest{})
	closeErr := runtime.close.Close()
	done := make(chan error, 1)
	go func() { done <- runtime.process.Wait() }()
	select {
	case waitErr := <-done:
		runtime.process = nil
		if shutdownErr != nil && status.Code(shutdownErr) != codes.Unavailable {
			return errors.Join(shutdownErr, closeErr, waitErr)
		}
		return errors.Join(closeErr, waitErr)
	case <-ctx.Done():
		killErr := runtime.process.Process.Kill()
		var waitErr error
		select {
		case waitErr = <-done:
		case <-time.After(5 * time.Second):
			runtime.process = nil
			return errors.Join(context.Cause(ctx), closeErr, killErr, fmt.Errorf("LMS daemon did not exit within 5s after kill"))
		}
		runtime.process = nil
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			waitErr = nil
		}
		return errors.Join(context.Cause(ctx), closeErr, killErr, waitErr)
	}
}

func (runtime *daemonRuntime) kill() error {
	if runtime != nil && runtime.process != nil {
		killErr := runtime.process.Process.Kill()
		waitErr := runtime.process.Wait()
		runtime.process = nil
		if errors.Is(killErr, os.ErrProcessDone) {
			killErr = nil
		}
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			waitErr = nil
		}
		return errors.Join(killErr, waitErr)
	}
	return nil
}

func waitForCompletedIndex(ctx context.Context, client pb.SemanticSearchDaemonServiceClient, path string) (*pb.StartIndexResponse, error) {
	started, err := client.StartIndex(ctx, &pb.StartIndexRequest{Path: path, Client: scenarioClientInfo()})
	if err != nil {
		return nil, err
	}
	_, err = waitForJob(ctx, client, started.GetJobId(), defaultScenarioRecoveryTimeout, defaultScenarioPollInterval, func(job *pb.Job) bool {
		return job.GetState() == "completed"
	})
	return started, err
}

func codeCollectionName(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	return "hybrid_code_chunks_" + tshash.PathPrefix(absolute)
}

func newCloneMilvusClient(ctx context.Context) (*milvusclient.Client, error) {
	return milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: cloneMilvusAddress, DBName: cloneMilvusDatabase,
		DialOptions: milvusgrpc.DialOptions(ctx, slog.Default(), milvusgrpc.DefaultCallTimeouts()),
	})
}

func rowObserver(client *milvusclient.Client, collectionName string) rowSnapshotObserver {
	return func(ctx context.Context) (rowSnapshot, error) {
		name := collectionName
		staging := collectionName + "_stg"
		hasStaging, err := client.HasCollection(ctx, milvusclient.NewHasCollectionOption(staging))
		if err == nil && hasStaging {
			name = staging
		}
		result, err := client.Query(ctx, milvusclient.NewQueryOption(name).
			WithFilter(`relativePath != ""`).
			WithOutputFields("id", "relativePath", "startLine", "endLine", "splitPart", "embeddingModel", "vector").
			WithConsistencyLevel(entity.ClStrong))
		if err != nil {
			return rowSnapshot{}, err
		}
		columns := []string{"id", "relativePath", "startLine", "endLine", "vector"}
		for _, column := range columns {
			if result.GetColumn(column) == nil {
				return rowSnapshot{}, fmt.Errorf("row observer missing %s", column)
			}
		}
		entries := make(map[string]rowSnapshotEntry, result.ResultCount)
		for index := range result.ResultCount {
			id, _ := result.GetColumn("id").GetAsString(index)
			relativePath, _ := result.GetColumn("relativePath").GetAsString(index)
			startLine, _ := result.GetColumn("startLine").GetAsInt64(index)
			endLine, _ := result.GetColumn("endLine").GetAsInt64(index)
			splitPosition := int64(0)
			if column := result.GetColumn("splitPart"); column != nil {
				splitPosition, _ = column.GetAsInt64(index)
			}
			embeddingModel := ""
			if column := result.GetColumn("embeddingModel"); column != nil {
				embeddingModel, _ = column.GetAsString(index)
			}
			vectorValue, vectorErr := result.GetColumn("vector").Get(index)
			if vectorErr != nil {
				return rowSnapshot{}, vectorErr
			}
			vector, ok := vectorValue.(entity.FloatVector)
			if !ok {
				return rowSnapshot{}, fmt.Errorf("row vector has type %T", vectorValue)
			}
			entries[relativePath] = rowSnapshotEntry{ID: id, RelativePath: relativePath, StartLine: int(startLine), EndLine: int(endLine), SplitPosition: int(splitPosition), EmbeddingModel: embeddingModel, DenseVectorHash: hashDenseVector(vector)}
		}
		return rowSnapshot{Entries: entries}, nil
	}
}

func hashDenseVector(vector entity.FloatVector) string {
	body := make([]byte, len(vector)*4)
	for index, value := range vector {
		binary.LittleEndian.PutUint32(body[index*4:], math.Float32bits(value))
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func checkpointObserver(
	client pb.SemanticSearchDaemonServiceClient,
	path string,
	merkleRoot string,
) checkpointSnapshotObserver {
	return func(ctx context.Context) (checkpointSnapshot, error) {
		response, err := client.GetIndex(ctx, &pb.GetIndexRequest{Path: path, Client: scenarioClientInfo()})
		if err != nil {
			return checkpointSnapshot{}, err
		}
		codebase := response.GetCodebase()
		if codebase == nil {
			return checkpointSnapshot{}, fmt.Errorf("index response omitted codebase")
		}
		checkpointPath := strings.TrimSpace(codebase.GetMerkleSnapshotPath())
		if checkpointPath == "" {
			if codebase.GetId() == "" || merkleRoot == "" {
				return checkpointSnapshot{}, fmt.Errorf("index response omitted checkpoint identity")
			}
			checkpointPath = filepath.Join(merkleRoot, codebase.GetId()+".json")
		}
		stagingPath := strings.TrimSuffix(checkpointPath, ".json") + ".staging.json"
		if _, err := os.Stat(stagingPath); err == nil {
			checkpointPath = stagingPath
		}
		body, err := os.ReadFile(checkpointPath)
		if err != nil {
			return checkpointSnapshot{}, err
		}
		var snapshot struct {
			Files map[string]string `json:"files"`
		}
		if err := json.Unmarshal(body, &snapshot); err != nil {
			return checkpointSnapshot{}, fmt.Errorf("decode checkpoint snapshot: %w", err)
		}
		return checkpointSnapshot{FileHashes: snapshot.Files, CompletedCount: len(snapshot.Files)}, nil
	}
}

func searchCodeObservation(ctx context.Context, client pb.SemanticSearchDaemonServiceClient, path string) (semanticSearchObservation, error) {
	return searchCodeObservationForQuery(ctx, client, path, fixtureQuery)
}

func searchCodeObservationForQuery(ctx context.Context, client pb.SemanticSearchDaemonServiceClient, path string, query string) (semanticSearchObservation, error) {
	response, err := client.SearchCode(ctx, &pb.SearchCodeRequest{Path: path, Query: query, Limit: 3, Client: scenarioClientInfo()})
	if err != nil {
		return semanticSearchObservation{Code: classifySearchError(err)}, nil
	}
	resultIDs := make([]string, 0, len(response.GetResults()))
	for _, result := range response.GetResults() {
		resultIDs = append(resultIDs, fmt.Sprintf("%s:%d:%d", result.GetRelativePath(), result.GetStartLine(), result.GetEndLine()))
	}
	return semanticSearchObservation{Succeeded: true, Source: "semantic", Matches: len(response.GetResults()), ResultIDs: resultIDs}, nil
}

func classifySearchError(err error) string {
	responded, ok := status.FromError(err)
	if ok {
		for _, detail := range responded.Details() {
			info, isErrorInfo := detail.(*errdetails.ErrorInfo)
			if isErrorInfo && info.GetDomain() == "goodkind.io/lm-semantic-search" && info.GetReason() != "" {
				return info.GetReason()
			}
		}
		return responded.Code().String()
	}
	message := err.Error()
	for _, code := range []string{collectionNotReadyCode, embedderBusyCode, milvusUnavailableCode, clydeSourceUnavailableCode, "conversation_search_source_refused", "conversation_search_source_failed"} {
		if strings.Contains(message, code) {
			return code
		}
	}
	return codes.Unknown.String()
}
