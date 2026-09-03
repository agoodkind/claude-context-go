package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/response"
	"goodkind.io/lm-semantic-search/internal/wait"
)

func newCodebaseCmd(options *rootOptions) *cobra.Command {
	codebase := &cobra.Command{
		Use:   "codebase",
		Short: "Inspect and mutate tracked codebases",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	codebase.AddCommand(newCodebaseListCmd(options))
	codebase.AddCommand(newCodebaseStatusCmd(options))
	codebase.AddCommand(newCodebaseIndexCmd(options))
	codebase.AddCommand(newCodebaseSyncCmd(options))
	codebase.AddCommand(newCodebasePriorityCmd(options))
	codebase.AddCommand(newCodebaseQuietCmd(options))
	codebase.AddCommand(newCodebaseWaitCmd(options))
	codebase.AddCommand(newCodebaseSearchCmd(options))
	codebase.AddCommand(newCodebaseClearCmd(options))
	return codebase
}

func newCodebaseListCmd(options *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List tracked codebases",
		Args:  requireNoArgs("codebase list"),
		Example: strings.Join([]string{
			"  lm-semantic-search codebase list",
			"  lm-semantic-search --json codebase list",
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			cliOpts := options.cliOptions()
			if cliOpts.outputMode == response.ModeHuman && term.IsTerminal(int(os.Stdout.Fd())) {
				return runCodebaseListTUI(cliOpts)
			}
			return callAndPrint(cliOpts, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (protoMessage, error) {
				return client.ListIndexes(ctx, &pb.ListIndexesRequest{})
			})
		},
	}
}

func newCodebaseStatusCmd(options *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "status PATH|ID",
		Short: "Show indexing status for one codebase path",
		Long: strings.Join([]string{
			"Show indexing status for one codebase path.",
			"",
			"Arguments:",
			"  PATH|ID    A codebase path, a symlink to it, or its codebase id",
		}, "\n"),
		Args: requireExactArgs("codebase status requires PATH", 1),
		Example: strings.Join([]string{
			"  lm-semantic-search codebase status /abs/path/to/repo",
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			clientInfo, err := resolveClientInfo()
			if err != nil {
				return err
			}
			return callAndPrint(options.cliOptions(), func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (protoMessage, error) {
				return client.GetIndex(ctx, &pb.GetIndexRequest{Path: args[0], Client: clientInfo})
			})
		},
	}
}

func newCodebaseIndexCmd(options *rootOptions) *cobra.Command {
	var force bool
	var splitterType string
	var ignorePatterns []string
	var includeSubmodules []string
	var waitTimeout time.Duration
	var schedulingFlags schedulingFlagValues

	cmd := &cobra.Command{
		Use:   "index PATH|ID",
		Short: "Start background indexing for one codebase",
		Long: strings.Join([]string{
			"Start background indexing for one codebase.",
			"",
			"Arguments:",
			"  PATH|ID    A codebase path, a symlink to it, or its codebase id",
		}, "\n"),
		Args: requireExactArgs("codebase index requires PATH", 1),
		Example: strings.Join([]string{
			"  lm-semantic-search codebase index /abs/path/to/repo",
			"  lm-semantic-search codebase index /abs/path/to/repo --force",
			"  lm-semantic-search codebase index /abs/path/to/repo --splitter ast",
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			clientInfo, err := resolveClientInfo()
			if err != nil {
				return err
			}
			cliOpts := options.cliOptions()
			if waitTimeout > 0 && cliOpts.outputMode != response.ModeHuman {
				return errors.New("--wait requires human output mode")
			}
			schedulingPolicy, err := schedulingPolicyPatch(cmd, schedulingFlags)
			if err != nil {
				return err
			}
			request := &pb.StartIndexRequest{
				Path:              args[0],
				Force:             force,
				IgnorePatterns:    ignorePatterns,
				IncludeSubmodules: includeSubmodules,
				Client:            clientInfo,
				SchedulingPolicy:  schedulingPolicy,
			}
			if splitterType != "" {
				request.Splitter = &pb.SplitterConfig{Type: splitterType}
			}
			result, err := callDaemon(cliOpts, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (protoMessage, error) {
				return client.StartIndex(ctx, request)
			})
			if err != nil {
				return err
			}
			if err := printResponse(cliOpts, result); err != nil {
				return err
			}
			if waitTimeout > 0 {
				started, ok := result.(*pb.StartIndexResponse)
				if !ok {
					return nil
				}
				return watchJob(cliOpts, started.GetJobId(), waitTimeout)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "force reindex even if already indexed")
	cmd.Flags().StringVar(&splitterType, "splitter", "", "splitter type: ast|langchain")
	cmd.Flags().StringArrayVar(&ignorePatterns, "ignore", nil, "ignore pattern to exclude")
	cmd.Flags().StringArrayVar(&includeSubmodules, "include-submodule", nil, "submodule name or path to include")
	cmd.Flags().DurationVar(&waitTimeout, "wait", 0, "attach to the job and render progress; value needs the = form (--wait=30s), bare --wait uses 5m")
	cmd.Flags().Lookup("wait").NoOptDefVal = "5m"
	schedulingFlags.addFlags(cmd)
	return cmd
}

func newCodebaseSyncCmd(options *rootOptions) *cobra.Command {
	var waitTimeout time.Duration
	var schedulingFlags schedulingFlagValues

	cmd := &cobra.Command{
		Use:   "sync PATH|ID",
		Short: "Start an incremental sync for one tracked codebase",
		Long: strings.Join([]string{
			"Start an incremental sync for one tracked codebase.",
			"",
			"Arguments:",
			"  PATH|ID    A codebase path, a symlink to it, or its codebase id",
		}, "\n"),
		Args: requireExactArgs("codebase sync requires PATH", 1),
		Example: strings.Join([]string{
			"  lm-semantic-search codebase sync /abs/path/to/repo",
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			clientInfo, err := resolveClientInfo()
			if err != nil {
				return err
			}
			cliOpts := options.cliOptions()
			if waitTimeout > 0 && cliOpts.outputMode != response.ModeHuman {
				return errors.New("--wait requires human output mode")
			}
			schedulingPolicy, err := schedulingPolicyPatch(cmd, schedulingFlags)
			if err != nil {
				return err
			}
			result, err := callDaemon(cliOpts, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (protoMessage, error) {
				return client.SyncIndex(ctx, &pb.SyncIndexRequest{
					Path:             args[0],
					Client:           clientInfo,
					SchedulingPolicy: schedulingPolicy,
				})
			})
			if err != nil {
				return err
			}
			if err := printResponse(cliOpts, result); err != nil {
				return err
			}
			if waitTimeout > 0 {
				started, ok := result.(*pb.SyncIndexResponse)
				if !ok {
					return nil
				}
				return watchJob(cliOpts, started.GetJobId(), waitTimeout)
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&waitTimeout, "wait", 0, "attach to the job and render progress; value needs the = form (--wait=30s), bare --wait uses 5m")
	cmd.Flags().Lookup("wait").NoOptDefVal = "5m"
	schedulingFlags.addFlags(cmd)
	return cmd
}

func newCodebaseWaitCmd(options *rootOptions) *cobra.Command {
	var waitTimeout time.Duration

	cmd := &cobra.Command{
		Use:   "wait PATH|ID",
		Short: "Wait for one codebase to become searchable",
		Long: strings.Join([]string{
			"Wait for one codebase to become searchable.",
			"",
			"This command polls the indexing status until the codebase is ready for",
			"search, reaches a terminal nonready state, the timeout expires, or the",
			"command is interrupted. The operation is read-only and never starts",
			"or mutates an index.",
			"",
			"Arguments:",
			"  PATH|ID    A codebase path, a symlink to it, or its codebase id",
		}, "\n"),
		Args: requireExactArgs("codebase wait requires PATH|ID", 1),
		Example: strings.Join([]string{
			"  lm-semantic-search codebase wait /abs/path/to/repo",
			"  lm-semantic-search codebase wait /abs/path/to/repo --wait-timeout 1m",
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			cliOpts := options.cliOptions()
			if cliOpts.outputMode != response.ModeHuman {
				return errors.New("wait requires human output mode")
			}
			result, err := wait.ForIndexStatus(cmd.Context(), cliOpts.socketPath, args[0], waitTimeout)
			if err != nil {
				// The last status the daemon answered still tells the operator how
				// far the build got, so print it before surfacing the failure.
				if result != nil {
					if printErr := printResponse(cliOpts, result); printErr != nil {
						return printErr
					}
				}
				slog.ErrorContext(cmd.Context(), "wait for indexing failed", "path", args[0], "err", err)
				return fmt.Errorf("wait for indexing: %w", err)
			}
			return printResponse(cliOpts, result)
		},
	}
	cmd.Flags().DurationVar(&waitTimeout, "wait-timeout", 5*time.Minute, "timeout for waiting")
	return cmd
}

func newCodebaseSearchCmd(options *rootOptions) *cobra.Command {
	var limit int
	var extensions []string

	cmd := &cobra.Command{
		Use:   "search PATH|ID QUERY",
		Short: "Search one indexed codebase",
		Long: strings.Join([]string{
			"Search one indexed codebase.",
			"",
			"Arguments:",
			"  PATH|ID    A codebase path, a symlink to it, or its codebase id",
			"  QUERY      Natural-language search query",
		}, "\n"),
		Args: requireExactArgs("codebase search requires PATH and QUERY", 2),
		Example: strings.Join([]string{
			"  lm-semantic-search codebase search /abs/path/to/repo \"indexing flow\"",
			"  lm-semantic-search codebase search /abs/path/to/repo splitter --limit 5",
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			clientInfo, err := resolveClientInfo()
			if err != nil {
				return err
			}
			searchLimit, err := safeSearchLimit(limit)
			if err != nil {
				return err
			}
			return callAndPrint(options.cliOptions(), func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (protoMessage, error) {
				return client.SearchCode(ctx, &pb.SearchCodeRequest{
					Path:            args[0],
					Query:           args[1],
					Limit:           searchLimit,
					ExtensionFilter: extensions,
					Client:          clientInfo,
				})
			})
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 10, "maximum number of results")
	cmd.Flags().StringArrayVar(&extensions, "extension", nil, "file extension filter")
	return cmd
}

func newCodebaseClearCmd(options *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "clear PATH|ID",
		Short: "Clear one tracked codebase",
		Long: strings.Join([]string{
			"Clear one tracked codebase.",
			"",
			"Arguments:",
			"  PATH|ID    A codebase path, a symlink to it, or its codebase id",
		}, "\n"),
		Args: requireExactArgs("codebase clear requires PATH", 1),
		Example: strings.Join([]string{
			"  lm-semantic-search codebase clear /abs/path/to/repo",
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			clientInfo, err := resolveClientInfo()
			if err != nil {
				return err
			}
			return callAndPrint(options.cliOptions(), func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (protoMessage, error) {
				return client.ClearIndex(ctx, &pb.ClearIndexRequest{Path: args[0], Client: clientInfo})
			})
		},
	}
}
