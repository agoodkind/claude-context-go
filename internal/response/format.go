// Package response formats daemon RPC responses for CLI and MCP clients.
package response

import (
	"fmt"
	"log/slog"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Mode controls how RPC responses are formatted for human-facing clients.
type Mode string

const (
	// ModeHuman prints the daemon-provided display text.
	ModeHuman Mode = "human"
	// ModeText is an alias for human-readable text output.
	ModeText Mode = "text"
	// ModeJSON prints one compact JSON object on one line.
	ModeJSON Mode = "json"
	// ModeSingleLine prints only the first non-empty line of display text.
	ModeSingleLine Mode = "single-line"
	// ModeSingleLineAlias accepts singleline without punctuation.
	ModeSingleLineAlias Mode = "singleline"
)

type displayTextGetter interface {
	GetDisplayText() string
}

// ParseMode maps environment or flag values to the supported response modes.
func ParseMode(value string) Mode {
	normalized := Mode(strings.TrimSpace(strings.ToLower(value)))
	switch normalized {
	case "":
		return ModeHuman
	case ModeHuman, ModeText:
		return ModeHuman
	case ModeJSON:
		return ModeJSON
	case ModeSingleLine, ModeSingleLineAlias:
		return ModeSingleLine
	default:
		return ModeHuman
	}
}

// FormatProto formats one protobuf message using the selected mode.
func FormatProto(mode Mode, message proto.Message) (string, error) {
	switch mode {
	case ModeJSON:
		return MarshalCompactJSON(message)
	case ModeSingleLine, ModeSingleLineAlias:
		displayText := extractDisplayText(message)
		if displayText == "" {
			return MarshalCompactJSON(message)
		}
		return firstNonEmptyLine(displayText), nil
	case ModeHuman, ModeText:
		fallthrough
	default:
		displayText := extractDisplayText(message)
		if displayText != "" {
			return displayText, nil
		}
		return MarshalCompactJSON(message)
	}
}

func extractDisplayText(message proto.Message) string {
	getter, ok := message.(displayTextGetter)
	if !ok {
		return ""
	}
	return strings.TrimSpace(getter.GetDisplayText())
}

// MarshalCompactJSON marshals one protobuf message onto a single line.
func MarshalCompactJSON(message proto.Message) (string, error) {
	data, err := protojson.MarshalOptions{Multiline: false}.Marshal(message)
	if err != nil {
		slog.Error("marshal compact JSON failed", "err", err)
		return "", fmt.Errorf("marshal compact JSON: %w", err)
	}
	return string(data), nil
}

func firstNonEmptyLine(text string) string {
	for line := range strings.SplitSeq(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
