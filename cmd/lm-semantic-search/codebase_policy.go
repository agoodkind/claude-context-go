package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/spf13/cobra"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/pbconv"
)

type schedulingFlagValues struct {
	priority  string
	quiet     bool
	idleAfter time.Duration
}

type schedulingPriorityName string

const (
	schedulingPriorityHigh   schedulingPriorityName = "high"
	schedulingPriorityNormal schedulingPriorityName = "normal"
	schedulingPriorityLow    schedulingPriorityName = "low"
)

type quietSetting string

const (
	quietSettingOn  quietSetting = "on"
	quietSettingOff quietSetting = "off"
)

func (values *schedulingFlagValues) addFlags(command *cobra.Command) {
	command.Flags().StringVar(
		&values.priority,
		"priority",
		"",
		"scheduling priority: high|normal|low",
	)
	command.Flags().BoolVar(
		&values.quiet,
		"quiet",
		false,
		"wait for host idle",
	)
	command.Flags().DurationVar(
		&values.idleAfter,
		"idle-after",
		0,
		"host idle threshold",
	)
}

func schedulingPolicyPatch(
	command *cobra.Command,
	values schedulingFlagValues,
) (*pb.SchedulingPolicyPatch, error) {
	patch := &pb.SchedulingPolicyPatch{}
	changed := false
	if command.Flags().Changed("priority") {
		priority := schedulingPriority(schedulingPriorityName(values.priority))
		patch.Priority = &priority
		changed = true
	}
	if command.Flags().Changed("quiet") {
		quiet := values.quiet
		patch.Quiet = &quiet
		changed = true
	}
	if command.Flags().Changed("idle-after") {
		idleAfterSeconds, err := durationToWholeSeconds(values.idleAfter)
		if err != nil {
			return nil, err
		}
		patch.IdleAfterSeconds = &idleAfterSeconds
		changed = true
	}
	if !changed {
		return nil, nil
	}
	if err := validateSchedulingPolicyPatch(command.Context(), patch); err != nil {
		return nil, err
	}
	return patch, nil
}

func durationToWholeSeconds(duration time.Duration) (int32, error) {
	if duration%time.Second != 0 {
		return 0, fmt.Errorf("idle after must resolve to whole seconds")
	}
	seconds := duration / time.Second
	if seconds < math.MinInt32 || seconds > math.MaxInt32 {
		return 0, fmt.Errorf("idle after seconds exceeds the supported range")
	}
	return int32(seconds), nil
}

func schedulingPriority(value schedulingPriorityName) pb.SchedulingPriority {
	switch value {
	case schedulingPriorityHigh:
		return pb.SchedulingPriority_SCHEDULING_PRIORITY_HIGH
	case schedulingPriorityNormal:
		return pb.SchedulingPriority_SCHEDULING_PRIORITY_NORMAL
	case schedulingPriorityLow:
		return pb.SchedulingPriority_SCHEDULING_PRIORITY_LOW
	default:
		return pb.SchedulingPriority_SCHEDULING_PRIORITY_UNSPECIFIED
	}
}

func newCodebasePriorityCmd(options *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "priority PATH|ID PRIORITY",
		Short: "Change one codebase's stored scheduling priority",
		Long: "Change one codebase's stored scheduling priority.\n\n" +
			"Arguments:\n" +
			"  PATH|ID     A codebase path, a symlink to it, or its codebase id\n" +
			"  PRIORITY    The stored priority: high, normal, or low",
		Args: requireExactArgs(
			"codebase priority requires PATH|ID and PRIORITY",
			2,
		),
		Example: "  lm-semantic-search codebase priority /abs/path/to/repo low",
		RunE: func(cmd *cobra.Command, args []string) error {
			priority := schedulingPriority(schedulingPriorityName(args[1]))
			patch := &pb.SchedulingPolicyPatch{Priority: &priority}
			if err := validateSchedulingPolicyPatch(cmd.Context(), patch); err != nil {
				return err
			}
			return updateCodebasePolicy(options, args[0], patch)
		},
	}
}

func newCodebaseQuietCmd(options *rootOptions) *cobra.Command {
	var idleAfter time.Duration
	command := &cobra.Command{
		Use:   "quiet PATH|ID on|off",
		Short: "Change one codebase's stored quiet scheduling policy",
		Long: "Change one codebase's stored quiet scheduling policy.\n\n" +
			"Arguments:\n" +
			"  PATH|ID    A codebase path, a symlink to it, or its codebase id\n" +
			"  on|off     Enable or disable quiet scheduling",
		Args:    requireExactArgs("codebase quiet requires PATH|ID and on|off", 2),
		Example: "  lm-semantic-search codebase quiet /abs/path/to/repo on --idle-after=5m",
		RunE: func(cmd *cobra.Command, args []string) error {
			quiet, err := quietArgument(quietSetting(args[1]))
			if err != nil {
				return err
			}
			patch := &pb.SchedulingPolicyPatch{Quiet: &quiet}
			if cmd.Flags().Changed("idle-after") {
				idleAfterSeconds, conversionErr := durationToWholeSeconds(idleAfter)
				if conversionErr != nil {
					return conversionErr
				}
				patch.IdleAfterSeconds = &idleAfterSeconds
			}
			if err := validateSchedulingPolicyPatch(cmd.Context(), patch); err != nil {
				return err
			}
			return updateCodebasePolicy(options, args[0], patch)
		},
	}
	command.Flags().DurationVar(
		&idleAfter,
		"idle-after",
		0,
		"stored host idle threshold",
	)
	return command
}

func quietArgument(value quietSetting) (bool, error) {
	switch value {
	case quietSettingOn:
		return true, nil
	case quietSettingOff:
		return false, nil
	default:
		return false, fmt.Errorf("quiet must be on or off")
	}
}

func validateSchedulingPolicyPatch(
	ctx context.Context,
	patch *pb.SchedulingPolicyPatch,
) error {
	if _, err := pbconv.FromSchedulingPolicyPatch(patch); err != nil {
		wrappedErr := fmt.Errorf("validate scheduling policy patch: %w", err)
		slog.WarnContext(ctx, "validate scheduling policy patch failed", "err", wrappedErr)
		return wrappedErr
	}
	return nil
}

func updateCodebasePolicy(
	options *rootOptions,
	path string,
	patch *pb.SchedulingPolicyPatch,
) error {
	clientInfo, err := resolveClientInfo()
	if err != nil {
		return err
	}
	return callAndPrint(
		options.cliOptions(),
		func(
			ctx context.Context,
			client pb.SemanticSearchDaemonServiceClient,
		) (protoMessage, error) {
			return client.UpdateCodebasePolicy(
				ctx,
				&pb.UpdateCodebasePolicyRequest{
					Path:   path,
					Patch:  patch,
					Client: clientInfo,
				},
			)
		},
	)
}
