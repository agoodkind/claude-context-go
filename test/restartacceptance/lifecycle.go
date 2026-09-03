//go:build restartacceptance

package restartacceptance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

const restartAcceptanceCleanupTimeout = 2 * time.Minute

var acceptanceScenarioNames = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}

type acceptanceLifecycleOperations struct {
	Prepare      func(context.Context) (acceptanceRun, error)
	RunCase      func(context.Context, acceptanceRun, string) error
	ConfirmClone func(context.Context, acceptanceRun) error
	Cleanup      func(context.Context, acceptanceRun) error
	Finish       func(acceptanceRun, acceptanceResult) error
}

func executeRestartAcceptance(ctx context.Context, operations acceptanceLifecycleOperations) (runErr error) {
	if err := validateLifecycleOperations(operations); err != nil {
		return err
	}
	run, err := operations.Prepare(ctx)
	if err != nil {
		return fmt.Errorf("prepare restart acceptance: %w", err)
	}
	defer func() {
		cleanupContext, cancelCleanup := context.WithTimeout(context.Background(), restartAcceptanceCleanupTimeout)
		cleanupErr := operations.Cleanup(cleanupContext, run)
		cancelCleanup()
		if cleanupErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("cleanup restart acceptance: %w", cleanupErr))
		}
		result := acceptanceResult{RunID: run.ID, Status: "passed"}
		if runErr != nil {
			result.Status = "failed"
			result.Error = runErr.Error()
		}
		if finishErr := operations.Finish(run, result); finishErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("write restart acceptance artifact: %w", finishErr))
		}
	}()

	for _, name := range acceptanceScenarioNames {
		if caseErr := operations.RunCase(ctx, run, name); caseErr != nil {
			runErr = fmt.Errorf("scenario %s: %w", name, caseErr)
			return runErr
		}
	}
	if err := operations.ConfirmClone(ctx, run); err != nil {
		runErr = fmt.Errorf("isolated clone confirmation: %w", err)
		return runErr
	}
	return nil
}

func validateLifecycleOperations(operations acceptanceLifecycleOperations) error {
	if operations.Prepare == nil || operations.RunCase == nil || operations.ConfirmClone == nil ||
		operations.Cleanup == nil || operations.Finish == nil {
		return fmt.Errorf("restart acceptance lifecycle operations are incomplete")
	}
	return nil
}

func validateRestartAcceptanceConfirmation(restartConfirmation string) error {
	return validateOptIn(restartConfirmation)
}

type execCommandRunner struct{}

func (execCommandRunner) Run(
	ctx context.Context,
	environment map[string]string,
	name string,
	arguments ...string,
) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...)
	command.Env = mergeEnvironment(os.Environ(), environment)
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			return nil, fmt.Errorf("run %s: %w", name, err)
		}
		return nil, fmt.Errorf("run %s: %w; output: %s", name, err, message)
	}
	return output, nil
}

func mergeEnvironment(base []string, overrides map[string]string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	for _, entry := range base {
		key, value, found := strings.Cut(entry, "=")
		if found {
			values[key] = value
		}
	}
	for key, value := range overrides {
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}
