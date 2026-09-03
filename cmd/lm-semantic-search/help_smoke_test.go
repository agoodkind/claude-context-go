package main

import (
	"strings"
	"testing"
)

func TestRootListsAllTopLevelCommands(t *testing.T) {
	root, stdout, _ := testRoot()
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("root help: %v", err)
	}
	out := stdout.String()
	for _, name := range []string{"codebase", "job", "daemon", "version"} {
		if !strings.Contains(out, name) {
			t.Errorf("root help missing %q:\n%s", name, out)
		}
	}
}

func TestGroupNameListsSubcommands(t *testing.T) {
	root, stdout, _ := testRoot()
	root.SetArgs([]string{"codebase", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("codebase help: %v", err)
	}
	out := stdout.String()
	for _, name := range []string{
		"list",
		"status",
		"index",
		"sync",
		"priority",
		"quiet",
		"search",
		"clear",
	} {
		if !strings.Contains(out, name) {
			t.Errorf("codebase help missing subcommand %q:\n%s", name, out)
		}
	}
}

func TestJobGroupHelpListsSubcommands(t *testing.T) {
	root, stdout, _ := testRoot()
	root.SetArgs([]string{"job", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("job help: %v", err)
	}
	out := stdout.String()
	for _, name := range []string{"list", "get", "cancel"} {
		if !strings.Contains(out, name) {
			t.Errorf("job help missing subcommand %q:\n%s", name, out)
		}
	}
}

func TestDaemonGroupHelpListsSubcommands(t *testing.T) {
	root, stdout, _ := testRoot()
	root.SetArgs([]string{"daemon", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("daemon help: %v", err)
	}
	out := stdout.String()
	for _, name := range []string{"status", "stop", "doctor"} {
		if !strings.Contains(out, name) {
			t.Errorf("daemon help missing subcommand %q:\n%s", name, out)
		}
	}
}

func TestLeafHelpShowsArgumentsExamplesAndEnum(t *testing.T) {
	root, stdout, _ := testRoot()
	root.SetArgs([]string{"codebase", "index", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("codebase index help: %v", err)
	}
	out := stdout.String()
	for _, want := range []string{
		"Arguments:",
		"PATH",
		"Examples:",
		"lm-semantic-search codebase index",
		"ast|langchain",
		"priority",
		"quiet",
		"idle-after",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("codebase index help missing %q:\n%s", want, out)
		}
	}
}

func TestSearchLeafHelpShowsArgumentsAndExample(t *testing.T) {
	root, stdout, _ := testRoot()
	root.SetArgs([]string{"codebase", "search", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("codebase search help: %v", err)
	}
	out := stdout.String()
	for _, want := range []string{
		"Arguments:",
		"PATH",
		"QUERY",
		"Examples:",
		"lm-semantic-search codebase search",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("codebase search help missing %q:\n%s", want, out)
		}
	}
}

func TestDaemonLeafHelpShowsExample(t *testing.T) {
	root, stdout, _ := testRoot()
	root.SetArgs([]string{"daemon", "status", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("daemon status help: %v", err)
	}
	if out := stdout.String(); !strings.Contains(out, "lm-semantic-search daemon status") {
		t.Errorf("daemon status help missing example:\n%s", out)
	}
}
