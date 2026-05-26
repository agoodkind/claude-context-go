package splitter

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestAddOverlapPreservesUTF8AcrossMultiByteBoundary(t *testing.T) {
	t.Parallel()

	// Each emoji is 4 UTF-8 bytes. Repeating one many times produces a
	// chunk whose byte length lands inside a codepoint when sliced from
	// the tail at byte resolution.
	previous := strings.Repeat("\U0001F600", 50)
	chunks := []Chunk{
		{Content: previous, StartLine: 1, EndLine: 1},
		{Content: "next chunk", StartLine: 5, EndLine: 5},
	}
	const overlap = 17
	result := addOverlap(chunks, overlap)
	if len(result) != 2 {
		t.Fatalf("addOverlap returned %d chunks, want 2", len(result))
	}
	if !utf8.ValidString(result[1].Content) {
		t.Fatalf("second chunk content is not valid UTF-8: %q", result[1].Content)
	}
	if !strings.HasSuffix(result[1].Content, "next chunk") {
		t.Fatalf("second chunk lost its tail: %q", result[1].Content)
	}
}

func TestHardSplitProducesValidUTF8ChunksAcrossEmoji(t *testing.T) {
	t.Parallel()

	content := strings.Repeat("\U0001F600", 20) + "ASCII tail"
	const chunkSize = 7 // mid-emoji boundary
	out := hardSplit(content, chunkSize)
	if len(out) == 0 {
		t.Fatal("hardSplit returned empty slice")
	}
	for index, piece := range out {
		if !utf8.ValidString(piece) {
			t.Fatalf("chunk %d is not valid UTF-8: %q", index, piece)
		}
	}
	rejoined := strings.Join(out, "")
	if rejoined != content {
		t.Fatalf("hardSplit lost content; rejoined len=%d original len=%d", len(rejoined), len(content))
	}
}

func TestAlignToRuneStart(t *testing.T) {
	t.Parallel()

	s := "a\U0001F600b" // a (1 byte), emoji (4 bytes), b (1 byte) = 6 bytes
	cases := []struct {
		name   string
		offset int
		want   int
	}{
		{"zero", 0, 0},
		{"first byte already aligned", 1, 1},
		{"inside emoji continuation", 2, 5},
		{"third continuation byte", 4, 5},
		{"after emoji", 5, 5},
		{"past end", 100, len(s)},
	}
	for _, tc := range cases {
		got := alignToRuneStart(s, tc.offset)
		if got != tc.want {
			t.Errorf("%s: alignToRuneStart(%d) = %d, want %d", tc.name, tc.offset, got, tc.want)
		}
	}
}

func TestAlignDownToRuneStart(t *testing.T) {
	t.Parallel()

	s := "a\U0001F600b"
	cases := []struct {
		name   string
		offset int
		want   int
	}{
		{"zero", 0, 0},
		{"first byte aligned", 1, 1},
		{"first continuation", 2, 1},
		{"third continuation", 4, 1},
		{"after emoji aligned", 5, 5},
		{"past end", 100, len(s)},
	}
	for _, tc := range cases {
		got := alignDownToRuneStart(s, tc.offset)
		if got != tc.want {
			t.Errorf("%s: alignDownToRuneStart(%d) = %d, want %d", tc.name, tc.offset, got, tc.want)
		}
	}
}
