package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"

	"github.com/mark3labs/mcp-go/mcp"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/pbconv"
)

type mcpSchedulingPriorityName string

const (
	mcpSchedulingPriorityHigh   mcpSchedulingPriorityName = "high"
	mcpSchedulingPriorityNormal mcpSchedulingPriorityName = "normal"
	mcpSchedulingPriorityLow    mcpSchedulingPriorityName = "low"
)

func indexSchedulingPolicyPatch(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*pb.SchedulingPolicyPatch, error) {
	arguments := request.GetArguments()
	patch := &pb.SchedulingPolicyPatch{}
	changed := false

	if rawPriority, found := arguments["priority"]; found {
		priorityText, ok := rawPriority.(string)
		if !ok {
			return nil, fmt.Errorf("priority must be a string")
		}
		priority := mcpSchedulingPriority(mcpSchedulingPriorityName(priorityText))
		patch.Priority = &priority
		changed = true
	}
	if rawQuiet, found := arguments["quiet"]; found {
		quiet, ok := rawQuiet.(bool)
		if !ok {
			return nil, fmt.Errorf("quiet must be a boolean")
		}
		patch.Quiet = &quiet
		changed = true
	}
	if _, found := arguments["idle_after_seconds"]; found {
		idleAfterSeconds, err := mcpInt32Argument(
			request,
			"idle_after_seconds",
		)
		if err != nil {
			return nil, err
		}
		patch.IdleAfterSeconds = &idleAfterSeconds
		changed = true
	}
	if !changed {
		return nil, nil
	}
	if _, err := pbconv.FromSchedulingPolicyPatch(patch); err != nil {
		wrappedErr := fmt.Errorf("validate scheduling policy patch: %w", err)
		slog.DebugContext(ctx, "validate scheduling policy patch failed", "err", wrappedErr)
		return nil, wrappedErr
	}
	return patch, nil
}

func mcpSchedulingPriority(value mcpSchedulingPriorityName) pb.SchedulingPriority {
	switch value {
	case mcpSchedulingPriorityHigh:
		return pb.SchedulingPriority_SCHEDULING_PRIORITY_HIGH
	case mcpSchedulingPriorityNormal:
		return pb.SchedulingPriority_SCHEDULING_PRIORITY_NORMAL
	case mcpSchedulingPriorityLow:
		return pb.SchedulingPriority_SCHEDULING_PRIORITY_LOW
	default:
		return pb.SchedulingPriority_SCHEDULING_PRIORITY_UNSPECIFIED
	}
}

func mcpInt32Argument(request mcp.CallToolRequest, name string) (int32, error) {
	rawValue, found := request.GetArguments()[name]
	if !found {
		return 0, fmt.Errorf("%s is required", name)
	}
	var value int64
	switch typedValue := rawValue.(type) {
	case int:
		value = int64(typedValue)
	case int32:
		value = int64(typedValue)
	case int64:
		value = typedValue
	case float64:
		if math.IsNaN(typedValue) || math.IsInf(typedValue, 0) ||
			math.Trunc(typedValue) != typedValue {
			return 0, fmt.Errorf("%s must be a whole number", name)
		}
		if typedValue < math.MinInt64 || typedValue > math.MaxInt64 {
			return 0, fmt.Errorf("%s exceeds the supported range", name)
		}
		value = int64(typedValue)
	case json.Number:
		converted, err := typedValue.Int64()
		if err != nil {
			return 0, fmt.Errorf("%s must be a whole number", name)
		}
		value = converted
	default:
		return 0, fmt.Errorf("%s must be a number", name)
	}
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, fmt.Errorf("%s exceeds the supported range", name)
	}
	return int32(value), nil
}
