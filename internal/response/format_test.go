package response

import (
	"strings"
	"testing"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
)

func TestFormatProtoHumanUsesDisplayText(t *testing.T) {
	t.Parallel()

	message := &pb.GetIndexResponse{DisplayText: "line one\nline two"}
	formatted, err := FormatProto(ModeHuman, message)
	if err != nil {
		t.Fatalf("FormatProto returned error: %v", err)
	}
	if formatted != "line one\nline two" {
		t.Fatalf("FormatProto returned %q", formatted)
	}
}

func TestFormatProtoSingleLineUsesFirstLine(t *testing.T) {
	t.Parallel()

	message := &pb.GetIndexResponse{DisplayText: "\nline one\nline two"}
	formatted, err := FormatProto(ModeSingleLine, message)
	if err != nil {
		t.Fatalf("FormatProto returned error: %v", err)
	}
	if formatted != "line one" {
		t.Fatalf("FormatProto returned %q", formatted)
	}
}

func TestFormatProtoJSONUsesCompactJSON(t *testing.T) {
	t.Parallel()

	message := &pb.GetIndexResponse{DisplayText: "line one", Tracked: true}
	formatted, err := FormatProto(ModeJSON, message)
	if err != nil {
		t.Fatalf("FormatProto returned error: %v", err)
	}
	if strings.Contains(formatted, "\n") {
		t.Fatalf("FormatProto returned multiline JSON: %q", formatted)
	}
	if !strings.Contains(formatted, "\"displayText\":\"line one\"") {
		t.Fatalf("FormatProto returned unexpected JSON: %q", formatted)
	}
}
