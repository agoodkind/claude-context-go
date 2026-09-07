//go:build restartacceptance

package restartacceptance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
)

const (
	clydeSourceUnavailableCode = "conversation_search_source_unavailable"
	collectionNotReadyCode     = "collection_not_ready"
	embedderBusyCode           = "embedder_busy"
	milvusUnavailableCode      = "milvus_unavailable"

	maximumClydeSearchRecovery        = 150 * time.Second
	maximumClydeFeederRecovery        = 225 * time.Second
	maximumEmptyEtcdBoot              = 70 * time.Second
	maximumJobCapacityRecovery        = 5 * time.Second
	maximumScenarioHDependencyFailure = 70 * time.Second
)

type semanticSearchObservation struct {
	Succeeded bool
	Source    string
	Matches   int
	Code      string
	ResultIDs []string
}

type clydeStatusObservation struct {
	PID          int
	Responding   bool
	LastSyncUnix int64
	Manifest     int
	Needed       int
	Embedded     int
	Pending      int
}

type daemonStatusObservation struct {
	Responding bool
}

type jobObservation struct {
	ID          string
	State       string
	FailureCode string
}

type bootObservation struct {
	Healthy            bool
	FailureCode        string
	SelfCheckExhausted bool
}

type rowStateObservation struct {
	Identity        string
	DenseVectorHash string
}

type collectionStateObservation struct {
	Name     string
	RowCount int
	Rows     []rowStateObservation
}

type stateObservation struct {
	Collections []collectionStateObservation
	Checkpoints []string
}

type scenarioETimeouts struct {
	Failure        time.Duration
	SearchRecovery time.Duration
	FeederRecovery time.Duration
	Poll           time.Duration
}

func (timeouts scenarioETimeouts) resolved() (scenarioETimeouts, error) {
	if timeouts.Failure <= 0 {
		timeouts.Failure = defaultScenarioFailureTimeout
	}
	if timeouts.SearchRecovery <= 0 {
		timeouts.SearchRecovery = maximumClydeSearchRecovery
	}
	if timeouts.FeederRecovery <= 0 {
		timeouts.FeederRecovery = maximumClydeFeederRecovery
	}
	if timeouts.Poll <= 0 {
		timeouts.Poll = defaultScenarioPollInterval
	}
	if timeouts.SearchRecovery > maximumClydeSearchRecovery {
		return scenarioETimeouts{}, fmt.Errorf("Clyde search recovery timeout %s exceeds %s", timeouts.SearchRecovery, maximumClydeSearchRecovery)
	}
	if timeouts.FeederRecovery > maximumClydeFeederRecovery {
		return scenarioETimeouts{}, fmt.Errorf("Clyde feeder recovery timeout %s exceeds %s", timeouts.FeederRecovery, maximumClydeFeederRecovery)
	}
	return timeouts, nil
}

type scenarioEInput struct {
	StopLMS           func(context.Context) error
	StartLMS          func(context.Context) error
	SearchClyde       func(context.Context) (semanticSearchObservation, error)
	ClydeStatus       func(context.Context) (clydeStatusObservation, error)
	TriggerFeederSync func(context.Context) error
	Recorder          *evidenceRecorder
	Timeouts          scenarioETimeouts
}

type scenarioEResult struct {
	ClydePIDBefore   int
	ClydePIDAfter    int
	OutageCode       string
	RecoveredMatches int
	SearchRecovery   time.Duration
	FeederRecovery   time.Duration
}

func runScenarioE(ctx context.Context, input scenarioEInput) (scenarioEResult, error) {
	if input.Recorder == nil {
		return scenarioEResult{}, fmt.Errorf("scenario E requires an evidence recorder")
	}
	if input.StopLMS == nil || input.StartLMS == nil || input.SearchClyde == nil || input.ClydeStatus == nil || input.TriggerFeederSync == nil {
		return scenarioEResult{}, fmt.Errorf("scenario E requires LMS controls and Clyde search and status observers")
	}
	timeouts, err := input.Timeouts.resolved()
	if err != nil {
		return scenarioEResult{}, err
	}
	before, err := input.ClydeStatus(ctx)
	if err != nil {
		return scenarioEResult{}, fmt.Errorf("scenario E read initial Clyde status: %w", err)
	}
	if !before.Responding || before.PID <= 0 {
		return scenarioEResult{}, fmt.Errorf("scenario E initial Clyde status is not responsive")
	}
	initialSearch, err := input.SearchClyde(ctx)
	if err != nil {
		return scenarioEResult{}, fmt.Errorf("scenario E initial Clyde search: %w", err)
	}
	if err := requireSemanticSuccess(initialSearch, "scenario E initial Clyde search"); err != nil {
		return scenarioEResult{}, err
	}
	if err := requireStableResultIDs(initialSearch, "scenario E initial Clyde search"); err != nil {
		return scenarioEResult{}, err
	}
	if err := input.StopLMS(ctx); err != nil {
		return scenarioEResult{}, fmt.Errorf("scenario E stop isolated LMS: %w", err)
	}
	lmsStopped := true
	defer func() {
		if lmsStopped {
			_ = input.StartLMS(context.Background())
		}
	}()
	outage, err := waitForSearchFailure(ctx, input.SearchClyde, clydeSourceUnavailableCode, timeouts.Failure, timeouts.Poll)
	if err != nil {
		return scenarioEResult{}, fmt.Errorf("scenario E require typed source unavailable: %w", err)
	}
	during, err := input.ClydeStatus(ctx)
	if err != nil {
		return scenarioEResult{}, fmt.Errorf("scenario E read Clyde status during outage: %w", err)
	}
	if !during.Responding || during.PID != before.PID {
		return scenarioEResult{}, fmt.Errorf("scenario E Clyde did not remain responsive with pid %d", before.PID)
	}
	searchRecoveryStarted := time.Now()
	if err := input.StartLMS(ctx); err != nil {
		return scenarioEResult{}, fmt.Errorf("scenario E restart isolated LMS: %w", err)
	}
	lmsStopped = false
	recovered, err := waitForSemanticSuccess(ctx, input.SearchClyde, timeouts.SearchRecovery, timeouts.Poll)
	if err != nil {
		return scenarioEResult{}, fmt.Errorf("scenario E wait for automatic Clyde search recovery: %w", err)
	}
	if err := requireStableResultIDs(recovered, "scenario E recovered Clyde search"); err != nil {
		return scenarioEResult{}, err
	}
	if !slices.Equal(initialSearch.ResultIDs, recovered.ResultIDs) {
		return scenarioEResult{}, fmt.Errorf("scenario E result identities changed from %v to %v", initialSearch.ResultIDs, recovered.ResultIDs)
	}
	searchRecovery := time.Since(searchRecoveryStarted)
	feederStarted := time.Now()
	if err := input.TriggerFeederSync(ctx); err != nil {
		return scenarioEResult{}, fmt.Errorf("scenario E trigger public feeder sync: %w", err)
	}
	after, err := waitForClydeFeeder(ctx, input.ClydeStatus, before.LastSyncUnix, before.Manifest, before.PID, timeouts.FeederRecovery, timeouts.Poll)
	if err != nil {
		return scenarioEResult{}, fmt.Errorf("scenario E wait for automatic feeder recovery: %w", err)
	}
	feederRecovery := time.Since(feederStarted)
	result := scenarioEResult{
		ClydePIDBefore:   before.PID,
		ClydePIDAfter:    after.PID,
		OutageCode:       outage.Code,
		RecoveredMatches: recovered.Matches,
		SearchRecovery:   searchRecovery,
		FeederRecovery:   feederRecovery,
	}
	if err := recordScenario(input.Recorder, "e", map[string]string{
		"clyde_pid":         strconv.Itoa(result.ClydePIDAfter),
		"outage_code":       result.OutageCode,
		"recovered_matches": strconv.Itoa(result.RecoveredMatches),
		"search_recovery":   result.SearchRecovery.String(),
		"feeder_recovery":   result.FeederRecovery.String(),
	}); err != nil {
		return scenarioEResult{}, err
	}
	return result, nil
}

type scenarioFTimeouts struct {
	BootFailure time.Duration
	Recovery    time.Duration
	Poll        time.Duration
}

func (timeouts scenarioFTimeouts) resolved() (scenarioFTimeouts, error) {
	if timeouts.BootFailure <= 0 {
		timeouts.BootFailure = maximumEmptyEtcdBoot
	}
	if timeouts.Recovery <= 0 {
		timeouts.Recovery = defaultScenarioRecoveryTimeout
	}
	if timeouts.Poll <= 0 {
		timeouts.Poll = defaultScenarioPollInterval
	}
	if timeouts.BootFailure > maximumEmptyEtcdBoot {
		return scenarioFTimeouts{}, fmt.Errorf("empty etcd boot timeout %s exceeds %s", timeouts.BootFailure, maximumEmptyEtcdBoot)
	}
	return timeouts, nil
}

type scenarioFInput struct {
	VerifyClonePayload       func(context.Context) error
	PrepareEmptyEtcd         func(context.Context) error
	StartLMS                 func(context.Context) error
	StopLMS                  func(context.Context) error
	ObserveBoot              func(context.Context) (bootObservation, error)
	RestoreEtcd              func(context.Context) error
	Search                   func(context.Context) (semanticSearchObservation, error)
	EtcdFingerprint          func() (string, error)
	ConfigurationFingerprint func() (string, error)
	Recorder                 *evidenceRecorder
	Timeouts                 scenarioFTimeouts
}

type scenarioFResult struct {
	BootFailureCode  string
	BootFailure      time.Duration
	RecoveredMatches int
}

func runScenarioF(ctx context.Context, input scenarioFInput) (scenarioFResult, error) {
	if input.Recorder == nil {
		return scenarioFResult{}, fmt.Errorf("scenario F requires an evidence recorder")
	}
	if input.VerifyClonePayload == nil || input.PrepareEmptyEtcd == nil || input.StartLMS == nil ||
		input.StopLMS == nil || input.ObserveBoot == nil || input.RestoreEtcd == nil || input.Search == nil ||
		input.EtcdFingerprint == nil || input.ConfigurationFingerprint == nil {
		return scenarioFResult{}, fmt.Errorf("scenario F requires clone, LMS, restore, search, and fingerprint controls")
	}
	timeouts, err := input.Timeouts.resolved()
	if err != nil {
		return scenarioFResult{}, err
	}
	configurationBefore, err := input.ConfigurationFingerprint()
	if err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F fingerprint LMS configuration: %w", err)
	}
	if err := input.VerifyClonePayload(ctx); err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F verify restored object files and MinIO payload: %w", err)
	}
	if err := input.PrepareEmptyEtcd(ctx); err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F prepare empty etcd: %w", err)
	}
	etcdBefore, err := input.EtcdFingerprint()
	if err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F fingerprint preserved etcd: %w", err)
	}
	bootStarted := time.Now()
	if err := input.StartLMS(ctx); err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F start LMS against empty etcd: %w", err)
	}
	boot, err := waitForLoudBootFailure(ctx, input.ObserveBoot, timeouts.BootFailure, timeouts.Poll)
	if err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F empty etcd boot: %w", err)
	}
	bootElapsed := time.Since(bootStarted)
	if err := input.StopLMS(ctx); err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F stop unhealthy LMS: %w", err)
	}
	if err := input.RestoreEtcd(ctx); err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F restore etcd: %w", err)
	}
	etcdAfter, err := input.EtcdFingerprint()
	if err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F re-fingerprint restored etcd: %w", err)
	}
	if etcdAfter != etcdBefore {
		return scenarioFResult{}, fmt.Errorf("scenario F restored etcd changed from %q to %q", etcdBefore, etcdAfter)
	}
	if err := input.StartLMS(ctx); err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F start LMS after etcd restore: %w", err)
	}
	configurationAfter, err := input.ConfigurationFingerprint()
	if err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F re-fingerprint LMS configuration: %w", err)
	}
	if configurationAfter != configurationBefore {
		return scenarioFResult{}, fmt.Errorf("scenario F LMS configuration changed from %q to %q", configurationBefore, configurationAfter)
	}
	recovered, err := waitForSemanticSuccess(ctx, input.Search, timeouts.Recovery, timeouts.Poll)
	if err != nil {
		return scenarioFResult{}, fmt.Errorf("scenario F search after untouched etcd restore: %w", err)
	}
	result := scenarioFResult{
		BootFailureCode:  boot.FailureCode,
		BootFailure:      bootElapsed,
		RecoveredMatches: recovered.Matches,
	}
	if err := recordScenario(input.Recorder, "f", map[string]string{
		"boot_failure_code": result.BootFailureCode,
		"boot_failure":      result.BootFailure.String(),
		"recovered_matches": strconv.Itoa(result.RecoveredMatches),
	}); err != nil {
		return scenarioFResult{}, err
	}
	return result, nil
}

type scenarioGTimeouts struct {
	InitialLoad time.Duration
	Capacity    time.Duration
	Observation time.Duration
	Failure     time.Duration
	NoThirdLoad time.Duration
	Recovery    time.Duration
	Poll        time.Duration
}

func (timeouts scenarioGTimeouts) resolved() (scenarioGTimeouts, error) {
	if timeouts.InitialLoad <= 0 {
		timeouts.InitialLoad = defaultScenarioReadyTimeout
	}
	if timeouts.Capacity <= 0 {
		timeouts.Capacity = maximumJobCapacityRecovery
	}
	if timeouts.Observation <= 0 {
		timeouts.Observation = 2 * time.Second
	}
	if timeouts.Failure <= 0 {
		timeouts.Failure = defaultScenarioFailureTimeout
	}
	if timeouts.NoThirdLoad <= 0 {
		timeouts.NoThirdLoad = time.Second
	}
	if timeouts.Recovery <= 0 {
		timeouts.Recovery = defaultScenarioRecoveryTimeout
	}
	if timeouts.Poll <= 0 {
		timeouts.Poll = defaultScenarioPollInterval
	}
	if timeouts.Capacity > maximumJobCapacityRecovery {
		return scenarioGTimeouts{}, fmt.Errorf("job capacity timeout %s exceeds %s", timeouts.Capacity, maximumJobCapacityRecovery)
	}
	return timeouts, nil
}

type scenarioGInput struct {
	SetLoading             func()
	ClearLoading           func()
	StartTargetJob         func(context.Context) (string, error)
	ObserveFailure         func(context.Context) (semanticSearchObservation, error)
	Status                 func(context.Context) (daemonStatusObservation, error)
	StartSecondJob         func(context.Context) (jobObservation, error)
	ObserveCapacityRelease func(context.Context) (time.Duration, error)
	ReleaseSecondJob       func()
	ObserveJob             func(context.Context, string) (jobObservation, error)
	RestartTargetJob       func(context.Context) (string, error)
	LoadCount              func() int
	SearchEditedTarget     func(context.Context) (semanticSearchObservation, error)
	ExpectedEditedID       string
	Recorder               *evidenceRecorder
	Timeouts               scenarioGTimeouts
}

type scenarioGResult struct {
	LoadCalls        int
	FailureCode      string
	SecondJobID      string
	TargetJobID      string
	RecoveredJobID   string
	CapacityRecovery time.Duration
	RecoveredMatches int
}

func runScenarioG(ctx context.Context, input scenarioGInput) (scenarioGResult, error) {
	if err := validateScenarioGInput(input); err != nil {
		return scenarioGResult{}, err
	}
	timeouts, err := input.Timeouts.resolved()
	if err != nil {
		return scenarioGResult{}, err
	}
	input.SetLoading()
	faultActive := true
	defer func() {
		if faultActive {
			input.ClearLoading()
		}
	}()
	targetJobID, err := input.StartTargetJob(ctx)
	if err != nil {
		return scenarioGResult{}, fmt.Errorf("scenario G start target job: %w", err)
	}
	if targetJobID == "" {
		return scenarioGResult{}, fmt.Errorf("scenario G target job returned no id")
	}
	if err := waitForLoadCount(ctx, input.LoadCount, 1, timeouts.InitialLoad, timeouts.Poll); err != nil {
		return scenarioGResult{}, fmt.Errorf("scenario G wait for initial load: %w", err)
	}
	statusObservation, err := callStatusWithin(ctx, input.Status, timeouts.Observation)
	if err != nil {
		return scenarioGResult{}, fmt.Errorf("scenario G responsive status: %w", err)
	}
	if !statusObservation.Responding {
		return scenarioGResult{}, fmt.Errorf("scenario G status did not report responsive")
	}
	secondJob, err := callSecondJobWithin(ctx, input.StartSecondJob, timeouts.Observation)
	if err != nil {
		return scenarioGResult{}, fmt.Errorf("scenario G second job capacity: %w", err)
	}
	capacityElapsed, err := callCapacityReleaseWithin(
		ctx,
		input.ObserveCapacityRelease,
		timeouts.Capacity+timeouts.Observation,
	)
	if err != nil {
		return scenarioGResult{}, fmt.Errorf("scenario G capacity release event: %w", err)
	}
	if capacityElapsed > timeouts.Capacity {
		return scenarioGResult{}, fmt.Errorf("scenario G capacity release took %s, exceeds %s", capacityElapsed, timeouts.Capacity)
	}
	if secondJob.ID == "" {
		return scenarioGResult{}, fmt.Errorf("scenario G second job returned no id")
	}
	observationContext, cancelObservation := context.WithTimeout(ctx, timeouts.Recovery)
	secondJob, err = waitForObservedJob(observationContext, input.ObserveJob, secondJob.ID, timeouts.Poll, func(job jobObservation) bool {
		return job.State == "running"
	})
	cancelObservation()
	if err != nil {
		return scenarioGResult{}, fmt.Errorf("scenario G observe second job after capacity release: %w", err)
	}
	failed, err := waitForSearchFailure(ctx, input.ObserveFailure, collectionNotReadyCode, timeouts.Failure, timeouts.Poll)
	if err != nil {
		return scenarioGResult{}, fmt.Errorf("scenario G target caller failure: %w", err)
	}
	if err := waitForLoadCount(ctx, input.LoadCount, 2, timeouts.Recovery, timeouts.Poll); err != nil {
		return scenarioGResult{}, fmt.Errorf("scenario G wait for recovery load: %w", err)
	}
	input.ClearLoading()
	faultActive = false
	input.ReleaseSecondJob()
	recoveredJobID, recovered, err := recoverScenarioG(ctx, input, timeouts, targetJobID, secondJob.ID)
	if err != nil {
		return scenarioGResult{}, err
	}
	if err := requireNoThirdLoad(ctx, input.LoadCount, timeouts.NoThirdLoad, timeouts.Poll); err != nil {
		return scenarioGResult{}, fmt.Errorf("scenario G final load quiet interval: %w", err)
	}
	loadCalls := input.LoadCount()
	if loadCalls != 2 {
		return scenarioGResult{}, fmt.Errorf("scenario G final load calls = %d, want exactly one initial and one recovery load", loadCalls)
	}
	result := scenarioGResult{
		LoadCalls:        loadCalls,
		FailureCode:      failed.Code,
		SecondJobID:      secondJob.ID,
		TargetJobID:      targetJobID,
		RecoveredJobID:   recoveredJobID,
		CapacityRecovery: capacityElapsed,
		RecoveredMatches: recovered.Matches,
	}
	if err := recordScenario(input.Recorder, "g", map[string]string{
		"load_calls":        strconv.Itoa(result.LoadCalls),
		"failure_code":      result.FailureCode,
		"second_job_id":     result.SecondJobID,
		"target_job_id":     result.TargetJobID,
		"recovered_job_id":  result.RecoveredJobID,
		"capacity_recovery": result.CapacityRecovery.String(),
		"recovered_matches": strconv.Itoa(result.RecoveredMatches),
	}); err != nil {
		return scenarioGResult{}, err
	}
	return result, nil
}

func callCapacityReleaseWithin(
	ctx context.Context,
	observe func(context.Context) (time.Duration, error),
	timeout time.Duration,
) (time.Duration, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return observe(deadlineContext)
}

func validateScenarioGInput(input scenarioGInput) error {
	if input.Recorder == nil {
		return fmt.Errorf("scenario G requires an evidence recorder")
	}
	if input.SetLoading == nil || input.ClearLoading == nil || input.StartTargetJob == nil ||
		input.ObserveFailure == nil || input.Status == nil || input.StartSecondJob == nil ||
		input.ObserveCapacityRelease == nil || input.ReleaseSecondJob == nil || input.ObserveJob == nil ||
		input.RestartTargetJob == nil ||
		input.LoadCount == nil || input.SearchEditedTarget == nil || input.ExpectedEditedID == "" {
		return fmt.Errorf("scenario G requires loading, job, status, count, and recovery controls")
	}
	return nil
}

func recoverScenarioG(
	ctx context.Context,
	input scenarioGInput,
	timeouts scenarioGTimeouts,
	targetJobID string,
	secondJobID string,
) (string, semanticSearchObservation, error) {
	recoveryContext, cancelRecovery := context.WithTimeout(ctx, timeouts.Recovery)
	defer cancelRecovery()
	targetJob, err := waitForObservedJob(recoveryContext, input.ObserveJob, targetJobID, timeouts.Poll, func(job jobObservation) bool {
		return slices.Contains([]string{"completed", "failed", "canceled"}, job.State)
	})
	if err != nil {
		return "", semanticSearchObservation{}, fmt.Errorf("scenario G wait for edited target job terminal state: %w", err)
	}
	recoveredJobID := targetJobID
	if targetJob.State != "completed" {
		recoveredJobID, err = input.RestartTargetJob(recoveryContext)
		if err != nil {
			return "", semanticSearchObservation{}, fmt.Errorf("scenario G restart edited target job: %w", err)
		}
		if recoveredJobID == "" {
			return "", semanticSearchObservation{}, fmt.Errorf("scenario G restarted target job returned no id")
		}
	}
	if _, err := waitForObservedJob(recoveryContext, input.ObserveJob, secondJobID, timeouts.Poll, func(job jobObservation) bool {
		return job.State == "completed"
	}); err != nil {
		return "", semanticSearchObservation{}, fmt.Errorf("scenario G wait for second job completion: %w", err)
	}
	if _, err := waitForObservedJob(recoveryContext, input.ObserveJob, recoveredJobID, timeouts.Poll, func(job jobObservation) bool {
		return job.State == "completed"
	}); err != nil {
		return "", semanticSearchObservation{}, fmt.Errorf("scenario G wait for recovered target job completion: %w", err)
	}
	recovered, err := waitForSemanticSuccess(recoveryContext, input.SearchEditedTarget, timeouts.Recovery, timeouts.Poll)
	if err != nil {
		return "", semanticSearchObservation{}, fmt.Errorf("scenario G recovery after clearing Loading fault: %w", err)
	}
	if !containsResultIdentity(recovered.ResultIDs, input.ExpectedEditedID) {
		return "", semanticSearchObservation{}, fmt.Errorf("scenario G edited identity %q missing from recovered results %v", input.ExpectedEditedID, recovered.ResultIDs)
	}
	return recoveredJobID, recovered, nil
}

type dependencyFault string

const (
	dependencyEmbedding dependencyFault = "embedding"
	dependencyMilvus    dependencyFault = "milvus"
)

type scenarioHTimeouts struct {
	Failure  time.Duration
	Recovery time.Duration
	Poll     time.Duration
}

func (timeouts scenarioHTimeouts) resolved() scenarioHTimeouts {
	if timeouts.Failure <= 0 {
		timeouts.Failure = maximumScenarioHDependencyFailure
	}
	if timeouts.Recovery <= 0 {
		timeouts.Recovery = defaultScenarioRecoveryTimeout
	}
	if timeouts.Poll <= 0 {
		timeouts.Poll = defaultScenarioPollInterval
	}
	return timeouts
}

type scenarioHInput struct {
	SetFault      func(dependencyFault)
	ClearFault    func(dependencyFault)
	SearchLMS     func(context.Context) (semanticSearchObservation, error)
	SearchClyde   func(context.Context) (semanticSearchObservation, error)
	SnapshotState func(context.Context) (stateObservation, error)
	LMSStatus     func(context.Context) (daemonStatusObservation, error)
	ClydeStatus   func(context.Context) (clydeStatusObservation, error)
	Recorder      *evidenceRecorder
	Timeouts      scenarioHTimeouts
}

type scenarioIInput struct {
	StartLow             func(context.Context) (*pb.StartIndexResponse, error)
	WaitForLowBoundary   func(context.Context) error
	StartHigh            func(context.Context) (*pb.StartIndexResponse, error)
	ReleaseLowBoundary   func()
	WaitForLowPaused     func(context.Context, string) (*pb.Job, error)
	CaptureLowJobs       func(context.Context, string) (map[string]struct{}, error)
	StopDaemon           func(context.Context) error
	StartDaemon          func(context.Context) error
	WaitForLowSuccessor  func(context.Context, string, map[string]struct{}) (*pb.Job, error)
	VerifyCloneInventory func(context.Context) error
	Recorder             *evidenceRecorder
	Timeouts             scenarioTimeouts
}

type scenarioIResult struct {
	PausedJobID    string
	SuccessorJobID string
}

func runScenarioI(ctx context.Context, input scenarioIInput) (result scenarioIResult, runErr error) {
	if err := validateScenarioIInput(input); err != nil {
		return scenarioIResult{}, err
	}
	timeouts := input.Timeouts.resolved()
	low, err := input.StartLow(ctx)
	if err != nil {
		return scenarioIResult{}, fmt.Errorf("scenario I start low-priority job: %w", err)
	}
	if low.GetJobId() == "" || low.GetCodebaseId() == "" {
		return scenarioIResult{}, fmt.Errorf("scenario I low-priority job response is incomplete")
	}
	if err := input.WaitForLowBoundary(ctx); err != nil {
		return scenarioIResult{}, fmt.Errorf("scenario I wait for low-priority file boundary: %w", err)
	}
	lowBoundaryReleased := false
	defer func() {
		if !lowBoundaryReleased {
			input.ReleaseLowBoundary()
		}
	}()
	high, err := input.StartHigh(ctx)
	if err != nil {
		return scenarioIResult{}, fmt.Errorf("scenario I start high-priority job: %w", err)
	}
	if high.GetJobId() == "" || high.GetCodebaseId() == "" || high.GetCodebaseId() == low.GetCodebaseId() {
		return scenarioIResult{}, fmt.Errorf("scenario I high-priority job response is incomplete")
	}
	input.ReleaseLowBoundary()
	lowBoundaryReleased = true
	paused, err := input.WaitForLowPaused(ctx, low.GetJobId())
	if err != nil {
		return scenarioIResult{}, fmt.Errorf("scenario I wait for low-priority pause: %w", err)
	}
	if paused.GetState() != "paused" {
		return scenarioIResult{}, fmt.Errorf("scenario I low-priority state = %q, want paused", paused.GetState())
	}
	if !sameSchedulingPolicy(paused.GetEffectiveSchedulingPolicy(), lowPolicy()) {
		return scenarioIResult{}, fmt.Errorf("scenario I paused policy = %+v, want low priority", paused.GetEffectiveSchedulingPolicy())
	}
	before, err := input.CaptureLowJobs(ctx, low.GetCodebaseId())
	if err != nil {
		return scenarioIResult{}, fmt.Errorf("scenario I capture paused job set: %w", err)
	}
	if err := input.StopDaemon(ctx); err != nil {
		return scenarioIResult{}, fmt.Errorf("scenario I stop installed daemon: %w", err)
	}
	daemonRestarted := false
	defer func() {
		if daemonRestarted {
			return
		}
		restartContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeouts.Recovery)
		defer cancel()
		if err := input.StartDaemon(restartContext); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("scenario I restore installed daemon after failure: %w", err))
		}
	}()
	if err := input.StartDaemon(ctx); err != nil {
		return scenarioIResult{}, fmt.Errorf("scenario I restart installed daemon: %w", err)
	}
	daemonRestarted = true
	resumeContext, cancel := context.WithTimeout(ctx, timeouts.Recovery)
	defer cancel()
	successor, err := input.WaitForLowSuccessor(resumeContext, low.GetCodebaseId(), before)
	if err != nil {
		return scenarioIResult{}, fmt.Errorf("scenario I wait for low-priority successor: %w", err)
	}
	if successor.GetState() != "completed" {
		return scenarioIResult{}, fmt.Errorf("scenario I successor state = %q, want completed", successor.GetState())
	}
	if !sameSchedulingPolicy(successor.GetEffectiveSchedulingPolicy(), lowPolicy()) {
		return scenarioIResult{}, fmt.Errorf("scenario I successor policy = %+v, want low priority", successor.GetEffectiveSchedulingPolicy())
	}
	if err := input.VerifyCloneInventory(ctx); err != nil {
		return scenarioIResult{}, fmt.Errorf("scenario I verify isolated Milvus inventory: %w", err)
	}
	result = scenarioIResult{PausedJobID: paused.GetId(), SuccessorJobID: successor.GetId()}
	if err := recordScenario(input.Recorder, "i", map[string]string{
		"paused_job_id":    result.PausedJobID,
		"successor_job_id": result.SuccessorJobID,
		"policy":           "low",
	}); err != nil {
		return scenarioIResult{}, err
	}
	return result, nil
}

func validateScenarioIInput(input scenarioIInput) error {
	if input.StartLow == nil || input.WaitForLowBoundary == nil || input.StartHigh == nil || input.ReleaseLowBoundary == nil || input.WaitForLowPaused == nil || input.CaptureLowJobs == nil || input.StopDaemon == nil || input.StartDaemon == nil || input.WaitForLowSuccessor == nil || input.VerifyCloneInventory == nil || input.Recorder == nil {
		return fmt.Errorf("scenario I requires low and high jobs, daemon restart, inventory, and evidence controls")
	}
	return nil
}

func lowPolicy() *pb.SchedulingPolicy {
	return &pb.SchedulingPolicy{
		Priority:         pb.SchedulingPriority_SCHEDULING_PRIORITY_LOW,
		Quiet:            false,
		IdleAfterSeconds: 300,
	}
}

func sameSchedulingPolicy(left *pb.SchedulingPolicy, right *pb.SchedulingPolicy) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.GetPriority() == right.GetPriority() &&
		left.GetQuiet() == right.GetQuiet() &&
		left.GetIdleAfterSeconds() == right.GetIdleAfterSeconds()
}

type scenarioHOrderResult struct {
	First         dependencyFault
	Second        dependencyFault
	FirstCode     string
	RemainingCode string
	Recovered     bool
}

type scenarioHResult struct {
	Orders []scenarioHOrderResult
}

func runScenarioH(ctx context.Context, input scenarioHInput) (scenarioHResult, error) {
	if input.Recorder == nil {
		return scenarioHResult{}, fmt.Errorf("scenario H requires an evidence recorder")
	}
	if input.SetFault == nil || input.ClearFault == nil || input.SearchLMS == nil || input.SearchClyde == nil ||
		input.SnapshotState == nil || input.LMSStatus == nil || input.ClydeStatus == nil {
		return scenarioHResult{}, fmt.Errorf("scenario H requires dependency controls, searches, state, and statuses")
	}
	timeouts := input.Timeouts.resolved()
	orders := [][2]dependencyFault{
		{dependencyEmbedding, dependencyMilvus},
		{dependencyMilvus, dependencyEmbedding},
	}
	result := scenarioHResult{Orders: make([]scenarioHOrderResult, 0, len(orders))}
	for _, order := range orders {
		orderResult, err := runScenarioHOrder(ctx, input, order[0], order[1], timeouts)
		if err != nil {
			return scenarioHResult{}, err
		}
		result.Orders = append(result.Orders, orderResult)
	}
	if err := recordScenario(input.Recorder, "h", map[string]string{
		"orders":                   strconv.Itoa(len(result.Orders)),
		"embedding_first_code":     result.Orders[0].FirstCode,
		"embedding_remaining_code": result.Orders[0].RemainingCode,
		"milvus_first_code":        result.Orders[1].FirstCode,
		"milvus_remaining_code":    result.Orders[1].RemainingCode,
	}); err != nil {
		return scenarioHResult{}, err
	}
	return result, nil
}

func runScenarioHOrder(
	ctx context.Context,
	input scenarioHInput,
	first dependencyFault,
	second dependencyFault,
	timeouts scenarioHTimeouts,
) (scenarioHOrderResult, error) {
	before, err := input.SnapshotState(ctx)
	if err != nil {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H %s then %s initial state: %w", first, second, err)
	}
	input.SetFault(first)
	firstActive := true
	input.SetFault(second)
	secondActive := true
	defer func() {
		if firstActive {
			input.ClearFault(first)
		}
		if secondActive {
			input.ClearFault(second)
		}
	}()
	input.ClearFault(second)
	secondActive = false
	firstCode := codeForFault(first)
	if err := requireBothSearchFailures(ctx, input.SearchLMS, input.SearchClyde, firstCode, timeouts.Failure, timeouts.Poll); err != nil {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H %s first active cause: %w", first, err)
	}
	input.SetFault(second)
	secondActive = true
	if err := requireBothSearchesTyped(ctx, input.SearchLMS, input.SearchClyde, firstCode, codeForFault(second)); err != nil {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H %s and %s overlap: %w", first, second, err)
	}
	input.ClearFault(first)
	firstActive = false
	remainingCode := codeForFault(second)
	if err := requireBothSearchFailures(ctx, input.SearchLMS, input.SearchClyde, remainingCode, timeouts.Failure, timeouts.Poll); err != nil {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H %s remaining cause: %w", second, err)
	}
	during, err := input.SnapshotState(ctx)
	if err != nil {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H %s then %s fault state: %w", first, second, err)
	}
	if !equalStateObservation(before, during) {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H %s then %s deleted or changed state", first, second)
	}
	input.ClearFault(second)
	secondActive = false
	if _, err := waitForSemanticSuccess(ctx, input.SearchLMS, timeouts.Recovery, timeouts.Poll); err != nil {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H LMS recovery after %s then %s: %w", first, second, err)
	}
	if _, err := waitForSemanticSuccess(ctx, input.SearchClyde, timeouts.Recovery, timeouts.Poll); err != nil {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H Clyde recovery after %s then %s: %w", first, second, err)
	}
	lmsStatus, err := input.LMSStatus(ctx)
	if err != nil || !lmsStatus.Responding {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H LMS did not recover after %s then %s: %w", first, second, err)
	}
	clydeStatus, err := input.ClydeStatus(ctx)
	if err != nil || !clydeStatus.Responding {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H Clyde did not recover after %s then %s: %w", first, second, err)
	}
	after, err := input.SnapshotState(ctx)
	if err != nil {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H %s then %s recovered state: %w", first, second, err)
	}
	if !equalStateObservation(before, after) {
		return scenarioHOrderResult{}, fmt.Errorf("scenario H %s then %s changed state after recovery", first, second)
	}
	return scenarioHOrderResult{First: first, Second: second, FirstCode: firstCode, RemainingCode: remainingCode, Recovered: true}, nil
}

func requireSemanticSuccess(observation semanticSearchObservation, label string) error {
	if !observation.Succeeded || observation.Source != "semantic" || observation.Matches <= 0 || observation.Code != "" {
		return fmt.Errorf("%s returned success=%t source=%q matches=%d code=%q", label, observation.Succeeded, observation.Source, observation.Matches, observation.Code)
	}
	return nil
}

func waitForSemanticSuccess(
	ctx context.Context,
	search func(context.Context) (semanticSearchObservation, error),
	timeout time.Duration,
	poll time.Duration,
) (semanticSearchObservation, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastObservation semanticSearchObservation
	var lastErr error
	for {
		observation, err := search(deadlineContext)
		if err == nil {
			if validationErr := requireSemanticSuccess(observation, "semantic search"); validationErr == nil {
				return observation, nil
			} else {
				lastErr = validationErr
			}
		} else {
			lastErr = err
		}
		lastObservation = observation
		select {
		case <-deadlineContext.Done():
			return semanticSearchObservation{}, fmt.Errorf("%w; last observation=%+v error=%v", deadlineContext.Err(), lastObservation, lastErr)
		case <-time.After(poll):
		}
	}
}

func waitForSearchFailure(
	ctx context.Context,
	search func(context.Context) (semanticSearchObservation, error),
	wantCode string,
	timeout time.Duration,
	poll time.Duration,
) (semanticSearchObservation, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last semanticSearchObservation
	var lastErr error
	for {
		observation, err := search(deadlineContext)
		last = observation
		lastErr = err
		if err == nil && !observation.Succeeded && observation.Code == wantCode && observation.Matches == 0 {
			return observation, nil
		}
		if err == nil && observation.Succeeded {
			return semanticSearchObservation{}, fmt.Errorf("empty success or fallback source=%q matches=%d", observation.Source, observation.Matches)
		}
		select {
		case <-deadlineContext.Done():
			return semanticSearchObservation{}, fmt.Errorf("%w; last observation=%+v error=%v", deadlineContext.Err(), last, lastErr)
		case <-time.After(poll):
		}
	}
}

func waitForClydeFeeder(
	ctx context.Context,
	statusObserver func(context.Context) (clydeStatusObservation, error),
	previousSync int64,
	previousManifest int,
	wantPID int,
	timeout time.Duration,
	poll time.Duration,
) (clydeStatusObservation, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last clydeStatusObservation
	var lastErr error
	for {
		observation, err := statusObserver(deadlineContext)
		last = observation
		lastErr = err
		if err == nil && observation.Responding && observation.PID == wantPID && observation.LastSyncUnix > previousSync &&
			observation.Manifest > previousManifest && observation.Pending == 0 && observation.Needed == 0 &&
			observation.Embedded == observation.Manifest {
			return observation, nil
		}
		select {
		case <-deadlineContext.Done():
			return clydeStatusObservation{}, fmt.Errorf("%w; last status=%+v error=%v", deadlineContext.Err(), last, lastErr)
		case <-time.After(poll):
		}
	}
}

func waitForObservedJob(
	ctx context.Context,
	observe func(context.Context, string) (jobObservation, error),
	jobID string,
	poll time.Duration,
	done func(jobObservation) bool,
) (jobObservation, error) {
	var last jobObservation
	var lastErr error
	for {
		observation, err := observe(ctx, jobID)
		last = observation
		lastErr = err
		if err == nil && done(observation) {
			return observation, nil
		}
		select {
		case <-ctx.Done():
			return jobObservation{}, fmt.Errorf("%w; last job=%+v error=%v", context.Cause(ctx), last, lastErr)
		case <-time.After(poll):
		}
	}
}

func waitForLoudBootFailure(
	ctx context.Context,
	observe func(context.Context) (bootObservation, error),
	timeout time.Duration,
	poll time.Duration,
) (bootObservation, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last bootObservation
	var lastErr error
	for {
		observation, err := observe(deadlineContext)
		last = observation
		lastErr = err
		if err == nil && observation.Healthy {
			return bootObservation{}, fmt.Errorf("LMS became healthy with empty etcd")
		}
		if err == nil && observation.SelfCheckExhausted && observation.FailureCode != "" {
			return observation, nil
		}
		select {
		case <-deadlineContext.Done():
			return bootObservation{}, fmt.Errorf("no loud bounded boot failure: %w; last=%+v error=%v", deadlineContext.Err(), last, lastErr)
		case <-time.After(poll):
		}
	}
}

func waitForLoadCount(ctx context.Context, loadCount func() int, want int, timeout time.Duration, poll time.Duration) error {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	last := 0
	for {
		last = loadCount()
		if last >= want {
			return nil
		}
		select {
		case <-deadlineContext.Done():
			return fmt.Errorf("%w; load count=%d, want at least %d", deadlineContext.Err(), last, want)
		case <-time.After(poll):
		}
	}
}

func callStatusWithin(
	ctx context.Context,
	statusObserver func(context.Context) (daemonStatusObservation, error),
	timeout time.Duration,
) (daemonStatusObservation, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return statusObserver(deadlineContext)
}

func callSecondJobWithin(
	ctx context.Context,
	start func(context.Context) (jobObservation, error),
	timeout time.Duration,
) (jobObservation, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return start(deadlineContext)
}

func waitForTargetJobFailure(
	ctx context.Context,
	observe func(context.Context, string) (jobObservation, error),
	jobID string,
	timeout time.Duration,
	poll time.Duration,
) (jobObservation, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last jobObservation
	var lastErr error
	for {
		observation, err := observe(deadlineContext, jobID)
		last = observation
		lastErr = err
		if err == nil && observation.State == "failed" && observation.FailureCode != "" {
			return observation, nil
		}
		select {
		case <-deadlineContext.Done():
			return jobObservation{}, fmt.Errorf("%w; last job=%+v error=%v", deadlineContext.Err(), last, lastErr)
		case <-time.After(poll):
		}
	}
}

func requireNoThirdLoad(ctx context.Context, loadCount func() int, duration time.Duration, poll time.Duration) error {
	deadlineContext, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	for {
		count := loadCount()
		if count != 2 {
			return fmt.Errorf("observed %d load calls, want 2", count)
		}
		select {
		case <-deadlineContext.Done():
			return nil
		case <-time.After(poll):
		}
	}
}

func codeForFault(fault dependencyFault) string {
	if fault == dependencyEmbedding {
		return embedderBusyCode
	}
	return milvusUnavailableCode
}

func requireBothSearchFailures(
	ctx context.Context,
	lmsSearch func(context.Context) (semanticSearchObservation, error),
	clydeSearch func(context.Context) (semanticSearchObservation, error),
	wantCode string,
	timeout time.Duration,
	poll time.Duration,
) error {
	if _, err := waitForSearchFailure(ctx, lmsSearch, wantCode, timeout, poll); err != nil {
		return fmt.Errorf("LMS: %w", err)
	}
	if _, err := waitForTypedClydeFailure(ctx, clydeSearch, wantCode, timeout, poll); err != nil {
		return fmt.Errorf("Clyde: %w", err)
	}
	return nil
}

func waitForTypedClydeFailure(
	ctx context.Context,
	search func(context.Context) (semanticSearchObservation, error),
	dependencyCode string,
	timeout time.Duration,
	poll time.Duration,
) (semanticSearchObservation, error) {
	accepted := []string{dependencyCode, clydeSourceUnavailableCode, "conversation_search_source_refused", "conversation_search_source_failed"}
	deadlineContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		observation, err := search(deadlineContext)
		if err == nil && !observation.Succeeded && observation.Matches == 0 && slices.Contains(accepted, observation.Code) {
			return observation, nil
		}
		if err == nil && observation.Succeeded {
			return semanticSearchObservation{}, fmt.Errorf("empty success or fallback source=%q matches=%d", observation.Source, observation.Matches)
		}
		select {
		case <-deadlineContext.Done():
			return semanticSearchObservation{}, fmt.Errorf("%w; last=%+v error=%v", deadlineContext.Err(), observation, err)
		case <-time.After(poll):
		}
	}
}

func requireBothSearchesTyped(
	ctx context.Context,
	lmsSearch func(context.Context) (semanticSearchObservation, error),
	clydeSearch func(context.Context) (semanticSearchObservation, error),
	allowedCodes ...string,
) error {
	for label, search := range map[string]func(context.Context) (semanticSearchObservation, error){"LMS": lmsSearch, "Clyde": clydeSearch} {
		observation, err := search(ctx)
		if err != nil {
			return fmt.Errorf("%s overlap search: %w", label, err)
		}
		accepted := slices.Clone(allowedCodes)
		if label == "Clyde" {
			accepted = append(accepted, clydeSourceUnavailableCode, "conversation_search_source_refused", "conversation_search_source_failed")
		}
		if observation.Succeeded || observation.Matches != 0 || !slices.Contains(accepted, observation.Code) {
			return fmt.Errorf("%s overlap returned success=%t matches=%d code=%q", label, observation.Succeeded, observation.Matches, observation.Code)
		}
	}
	return nil
}

func equalStateObservation(left stateObservation, right stateObservation) bool {
	left.Collections = cloneCollectionStateObservations(left.Collections)
	right.Collections = cloneCollectionStateObservations(right.Collections)
	left.Checkpoints = slices.Clone(left.Checkpoints)
	right.Checkpoints = slices.Clone(right.Checkpoints)
	sortCollectionStateObservations(left.Collections)
	sortCollectionStateObservations(right.Collections)
	slices.Sort(left.Checkpoints)
	slices.Sort(right.Checkpoints)
	return reflect.DeepEqual(left, right)
}

func cloneCollectionStateObservations(collections []collectionStateObservation) []collectionStateObservation {
	cloned := make([]collectionStateObservation, len(collections))
	for index, collection := range collections {
		cloned[index] = collectionStateObservation{Name: collection.Name, RowCount: collection.RowCount, Rows: slices.Clone(collection.Rows)}
	}
	return cloned
}

func sortCollectionStateObservations(collections []collectionStateObservation) {
	for index := range collections {
		slices.SortFunc(collections[index].Rows, func(left rowStateObservation, right rowStateObservation) int {
			if order := strings.Compare(left.Identity, right.Identity); order != 0 {
				return order
			}
			return strings.Compare(left.DenseVectorHash, right.DenseVectorHash)
		})
	}
	slices.SortFunc(collections, func(left collectionStateObservation, right collectionStateObservation) int {
		return strings.Compare(left.Name, right.Name)
	})
}

func requireStableResultIDs(observation semanticSearchObservation, label string) error {
	if len(observation.ResultIDs) != observation.Matches {
		return fmt.Errorf("%s returned %d stable result identities for %d matches", label, len(observation.ResultIDs), observation.Matches)
	}
	for _, identity := range observation.ResultIDs {
		if identity == "" {
			return fmt.Errorf("%s returned an empty stable result identity", label)
		}
	}
	return nil
}

func containsResultIdentity(resultIDs []string, expected string) bool {
	for _, identity := range resultIDs {
		if identity == expected || strings.HasPrefix(identity, expected+":") {
			return true
		}
	}
	return false
}
