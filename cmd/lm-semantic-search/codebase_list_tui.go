package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/pbconv"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/status"
)

const (
	refreshInterval  = 3 * time.Second
	defaultTermWidth = 120
	maxNameWidth     = 22
	statusWidth      = 13 // glyph + space + len("not indexed")
	columnGap        = "  "
)

// runCodebaseListTUI fetches the tracked codebases once and drives the
// interactive list. It runs only when stdout is a real terminal in human mode;
// the piped, JSON, and single-line paths stay on callAndPrint. An empty registry
// prints the daemon's plain text instead of launching an empty table.
func runCodebaseListTUI(options cliOptions) error {
	result, err := callDaemon(options, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (protoMessage, error) {
		return client.ListIndexes(ctx, &pb.ListIndexesRequest{})
	})
	if err != nil {
		return err
	}
	listResponse, ok := result.(*pb.ListIndexesResponse)
	if !ok {
		return errors.New("unexpected response type from ListIndexes")
	}
	codebases := listResponse.GetIndexes()
	if len(codebases) == 0 {
		_, writeErr := fmt.Fprintln(os.Stdout, strings.TrimSpace(listResponse.GetDisplayText()))
		if writeErr != nil {
			slog.Error("write empty codebase list failed", "err", writeErr)
			return fmt.Errorf("write empty list output: %w", writeErr)
		}
		return nil
	}

	program := tea.NewProgram(newListModel(options, codebases, listResponse.GetDependencyHealth()), tea.WithAltScreen())
	if _, runErr := program.Run(); runErr != nil {
		slog.Error("run codebase list TUI failed", "err", runErr)
		return fmt.Errorf("run codebase list: %w", runErr)
	}
	return nil
}

// listModel is the bubbletea state for the interactive codebase list. It lays
// out daemon-provided records and, on enter, asks the daemon (GetIndex) for the
// current-state detail. It makes no status decision of its own; the daemon owns
// every value, and the list re-fetches on a timer so it never goes stale.
type listModel struct {
	options       cliOptions
	codebases     []*pb.Codebase
	cursor        int
	offset        int
	width         int
	height        int
	showingDetail bool
	detail        string
	loading       bool
	refreshing    bool
	err           error
	quitting      bool
	health        *pb.DependencyHealth
}

type detailLoadedMsg struct {
	text string
	err  error
}

type refreshedMsg struct {
	codebases []*pb.Codebase
	health    *pb.DependencyHealth
	err       error
}

type tickMsg struct{}

func newListModel(options cliOptions, codebases []*pb.Codebase, health *pb.DependencyHealth) listModel {
	return listModel{
		options:       options,
		codebases:     codebases,
		cursor:        0,
		offset:        0,
		width:         0,
		height:        0,
		showingDetail: false,
		detail:        "",
		loading:       false,
		refreshing:    false,
		err:           nil,
		quitting:      false,
		health:        health,
	}
}

func (m listModel) Init() tea.Cmd {
	return tickEvery()
}

func (m listModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch typed := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = typed.Width
		m.height = typed.Height
		m = m.clampOffset()
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(typed)
	case detailLoadedMsg:
		m.loading = false
		if typed.err != nil {
			m.err = typed.err
			return m, nil
		}
		m.err = nil
		m.detail = typed.text
		m.showingDetail = true
		return m, nil
	case refreshedMsg:
		return m.applyRefresh(typed), nil
	case tickMsg:
		return m.handleTick()
	}
	return m, nil
}

// handleTick reschedules the timer and, when the list view is idle, kicks off a
// background re-fetch so the rows reflect the daemon's current state.
func (m listModel) handleTick() (tea.Model, tea.Cmd) {
	cmds := []tea.Cmd{tickEvery()}
	if !m.showingDetail && !m.refreshing {
		m.refreshing = true
		cmds = append(cmds, refreshCmd(m.options))
	}
	return m, tea.Batch(cmds...)
}

// applyRefresh swaps in freshly fetched records while keeping the cursor on the
// same codebase id, so a background refresh never makes the selection jump.
func (m listModel) applyRefresh(msg refreshedMsg) listModel {
	m.refreshing = false
	if msg.err != nil {
		m.err = msg.err
		return m
	}
	m.err = nil
	m.health = msg.health
	selectedID := ""
	if m.cursor >= 0 && m.cursor < len(m.codebases) {
		selectedID = m.codebases[m.cursor].GetId()
	}
	m.codebases = msg.codebases
	m.cursor = 0
	for index, codebase := range m.codebases {
		if codebase.GetId() == selectedID {
			m.cursor = index
			break
		}
	}
	return m.clampOffset()
}

func (m listModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if keyMatches(msg, "ctrl+c", "q") {
		m.quitting = true
		return m, tea.Quit
	}
	if m.showingDetail {
		return m.handleDetailKey(msg)
	}
	return m.handleListKey(msg)
}

func (m listModel) handleDetailKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if keyMatches(msg, "esc", "backspace", "left", "h") {
		m.showingDetail = false
		m.detail = ""
		m.err = nil
	}
	return m, nil
}

func (m listModel) handleListKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case keyMatches(msg, "esc"):
		m.quitting = true
		return m, tea.Quit
	case keyMatches(msg, "up", "k"):
		return m.moveCursor(-1), nil
	case keyMatches(msg, "down", "j"):
		return m.moveCursor(1), nil
	case keyMatches(msg, "r"):
		if !m.refreshing {
			m.refreshing = true
			return m, refreshCmd(m.options)
		}
		return m, nil
	case keyMatches(msg, "enter", "right", "l"):
		if len(m.codebases) == 0 {
			return m, nil
		}
		id := m.codebases[m.cursor].GetId()
		m.loading = true
		m.err = nil
		return m, loadDetailCmd(m.options, id)
	default:
		return m, nil
	}
}

func (m listModel) moveCursor(delta int) listModel {
	if len(m.codebases) == 0 {
		return m
	}
	m.cursor = clampInt(m.cursor+delta, len(m.codebases)-1)
	return m.clampOffset()
}

// clampOffset keeps the scroll window anchored so the cursor stays visible.
func (m listModel) clampOffset() listModel {
	visible := m.visibleRows()
	if m.cursor < m.offset {
		m.offset = m.cursor
	}
	if m.cursor >= m.offset+visible {
		m.offset = m.cursor - visible + 1
	}
	maxOffset := max(len(m.codebases)-visible, 0)
	m.offset = clampInt(m.offset, maxOffset)
	return m
}

// visibleRows is the number of data rows that fit between the header and footer.
func (m listModel) visibleRows() int {
	chromeRows := 4 // header, rule, selected policy, footer
	if m.bannerLine() != "" {
		chromeRows++
	}
	height := m.height
	if height <= 0 {
		height = len(m.codebases) + chromeRows
	}
	rows := height - chromeRows
	if rows < 1 {
		return 1
	}
	return rows
}

// keyMatches reports whether the pressed key equals any of the given names,
// keeping key handling as plain comparisons rather than a switch on a bare
// string.
func keyMatches(msg tea.KeyMsg, keys ...string) bool {
	return slices.Contains(keys, msg.String())
}

func (m listModel) View() string {
	if m.quitting {
		return ""
	}
	if m.showingDetail {
		return m.detailView()
	}
	return m.listView()
}

func (m listModel) detailView() string {
	body := m.detail
	if m.err != nil {
		body = errorStyle.Render(m.err.Error())
	}
	footer := faintStyle.Render("esc back · q quit")
	return strings.TrimRight(body, "\n") + "\n\n" + footer + "\n"
}

func (m listModel) listView() string {
	widths := m.columnWidths()
	lines := make([]string, 0, m.visibleRows()+4)
	if banner := m.bannerLine(); banner != "" {
		lines = append(lines, banner)
	}
	lines = append(lines, m.headerLine(widths))
	lines = append(lines, faintStyle.Render(strings.Repeat("─", lineWidth(widths))))

	visible := m.visibleRows()
	end := min(m.offset+visible, len(m.codebases))
	for index := m.offset; index < end; index++ {
		lines = append(lines, m.renderRow(m.codebases[index], index == m.cursor, widths))
	}
	lines = append(lines, m.policyLine())
	lines = append(lines, m.footerLine())
	return strings.Join(lines, "\n") + "\n"
}

// bannerLine renders the degraded-dependency banner above the table when the
// daemon reports a shared-dependency outage, so the interactive list carries the
// same warning as the piped surfaces. The headline comes from the single status
// vocabulary; an empty string means the dependencies are healthy.
func (m listModel) bannerLine() string {
	if m.health == nil || !m.health.GetDegraded() {
		return ""
	}
	headline := status.BannerHeadlineFor(status.DependencyMode(m.health.GetMode()))
	return bannerStyle.Render("🟥 " + headline)
}

func (m listModel) footerLine() string {
	keys := "↑/↓ move · enter open · r refresh · q quit"
	if m.refreshing {
		keys += " · updating"
	}
	position := fmt.Sprintf("  %d/%d", m.cursor+1, len(m.codebases))
	if m.err != nil {
		return errorStyle.Render(m.err.Error())
	}
	return faintStyle.Render(keys + position)
}

func (m listModel) headerLine(widths colWidths) string {
	header := strings.Join([]string{
		padTo("NAME", widths.name),
		padTo("STATUS", widths.status),
		padLeftTo("FILES", widths.files),
		padTo("ID", widths.id),
		padTo("PATH", widths.path),
	}, columnGap)
	return "  " + headerStyle.Render(header)
}

func (m listModel) renderRow(codebase *pb.Codebase, selected bool, widths colWidths) string {
	name := padTo(fitTail(filepath.Base(codebase.GetCanonicalPath()), widths.name), widths.name)
	files := padLeftTo(fileCountCell(codebase), widths.files)
	id := padTo(fitTail(codebase.GetId(), widths.id), widths.id)
	pathCell := padTo(fitHead(codebase.GetCanonicalPath(), widths.path), widths.path)
	status := codebase.GetDisplayStatus()
	glyph := codebase.GetGlyphToken()
	labelText := codebase.GetStatusLabel()
	label := strings.TrimSpace(glyph + " " + labelText)
	statusText := padTo(fitTail(label, widths.status), widths.status)

	var rowLine string
	if selected {
		rowLine = selectedStyle.Render("❯ " + strings.Join([]string{name, statusText, files, id, pathCell}, columnGap))
	} else {
		statusCell := lipgloss.NewStyle().Foreground(statusColor(status)).Render(statusText)
		rowLine = "  " + strings.Join([]string{name, statusCell, files, id, pathCell}, columnGap)
	}
	tree := activeBreakdownLines(codebase)
	if len(tree) == 0 {
		return rowLine
	}
	return rowLine + "\n" + strings.Join(tree, "\n")
}

// activeBreakdownLines renders the live breakdown tree for an actively-indexing
// codebase through the same render.BreakdownLines the daemon and status surface
// use, so the TUI projects from the one resolver rather than re-deriving. It
// returns nil when there is no active progress to show.
func activeBreakdownLines(codebase *pb.Codebase) []string {
	progress := codebase.GetActiveProgress()
	if progress == nil {
		return nil
	}
	treeLines := render.BreakdownLines(pbconv.BreakdownFromProto(progress.GetBreakdown()))
	if len(treeLines) == 0 {
		return nil
	}
	out := make([]string, 0, len(treeLines))
	for _, line := range treeLines {
		out = append(out, faintStyle.Render("      "+line))
	}
	return out
}

// colWidths holds the computed column widths for one render pass.
type colWidths struct {
	name   int
	status int
	files  int
	id     int
	path   int
}

func (m listModel) columnWidths() colWidths {
	nameW := len("NAME")
	filesW := len("FILES")
	idW := len("ID")
	for _, codebase := range m.codebases {
		nameW = max(nameW, len(filepath.Base(codebase.GetCanonicalPath())))
		filesW = max(filesW, len(fileCountCell(codebase)))
		idW = max(idW, len(codebase.GetId()))
	}
	nameW = min(nameW, maxNameWidth)

	termWidth := m.width
	if termWidth <= 0 {
		termWidth = defaultTermWidth
	}
	const marker = 2
	const gaps = 4 * len(columnGap)
	const minPath = 8
	fixed := marker + nameW + statusWidth + filesW + idW + gaps
	pathW := max(termWidth-fixed, minPath)
	return colWidths{name: nameW, status: statusWidth, files: filesW, id: idW, path: pathW}
}

func lineWidth(widths colWidths) int {
	return 2 + widths.name + widths.status + widths.files + widths.id + widths.path + 4*len(columnGap)
}

func (m listModel) policyLine() string {
	policy := "unavailable"
	if m.cursor >= 0 && m.cursor < len(m.codebases) {
		if resolved := schedulingPolicyCell(m.codebases[m.cursor]); resolved != "" {
			policy = resolved
		}
	}
	return faintStyle.Render("Stored policy: " + policy)
}

func schedulingPolicyCell(codebase *pb.Codebase) string {
	scheduling := pbconv.SchedulingFromProto(
		codebase.GetSchedulingPolicy(),
		"",
		pb.SchedulingReason_SCHEDULING_REASON_UNSPECIFIED,
	)
	return render.SchedulingPolicy(scheduling)
}

func refreshCmd(options cliOptions) tea.Cmd {
	return func() tea.Msg {
		result, err := callDaemon(options, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (protoMessage, error) {
			return client.ListIndexes(ctx, &pb.ListIndexesRequest{})
		})
		if err != nil {
			return refreshedMsg{codebases: nil, health: nil, err: err}
		}
		listResponse, ok := result.(*pb.ListIndexesResponse)
		if !ok {
			return refreshedMsg{codebases: nil, health: nil, err: errors.New("unexpected response type from ListIndexes")}
		}
		return refreshedMsg{codebases: listResponse.GetIndexes(), health: listResponse.GetDependencyHealth(), err: nil}
	}
}

func loadDetailCmd(options cliOptions, id string) tea.Cmd {
	return func() tea.Msg {
		result, err := callDaemon(options, func(ctx context.Context, client pb.SemanticSearchDaemonServiceClient) (protoMessage, error) {
			return client.GetIndex(ctx, &pb.GetIndexRequest{Path: id})
		})
		if err != nil {
			return detailLoadedMsg{text: "", err: err}
		}
		indexResponse, ok := result.(*pb.GetIndexResponse)
		if !ok {
			return detailLoadedMsg{text: "", err: errors.New("unexpected response type from GetIndex")}
		}
		return detailLoadedMsg{text: indexResponse.GetDisplayText(), err: nil}
	}
}

func tickEvery() tea.Cmd {
	return tea.Tick(refreshInterval, func(time.Time) tea.Msg {
		return tickMsg{}
	})
}

var (
	faintStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	errorStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	headerStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("250")).Bold(true)
	selectedStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("231")).Background(lipgloss.Color("57")).Bold(true)
	bannerStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true)
)

// statusColors maps each resolved display status to a distinct foreground
// color. An unknown status falls back to grayStatus.
var statusColors = map[string]lipgloss.Color{
	"preparing":  lipgloss.Color("12"),
	"indexed":    lipgloss.Color("10"),
	"indexing":   lipgloss.Color("11"),
	"waiting":    lipgloss.Color("214"),
	"stale":      lipgloss.Color("208"),
	"failed":     lipgloss.Color("9"),
	"missing":    lipgloss.Color("245"),
	"discovered": lipgloss.Color("44"),
}

const grayStatus = lipgloss.Color("245")

func statusColor(status string) lipgloss.Color {
	if color, ok := statusColors[status]; ok {
		return color
	}
	return grayStatus
}

// fileCountCell returns the indexed-file count from the last successful run, or
// a hyphen placeholder when the codebase has never completed a run. A discovered
// worktree has no run yet, so it shows its reuse forecast (♻N sibling
// collections) instead, signalling that its pending build is cheap.
func fileCountCell(codebase *pb.Codebase) string {
	if codebase.GetDisplayStatus() == "discovered" {
		if reuse := codebase.GetReuseSiblingCount(); reuse > 0 {
			return "♻" + strconv.Itoa(int(reuse))
		}
		return "-"
	}
	run := codebase.GetLastSuccessfulRun()
	if run == nil {
		return "-"
	}
	return strconv.Itoa(int(run.GetIndexedFiles()))
}

// padTo pads text with trailing spaces to a display width of width, measured in
// runes so a multibyte ellipsis counts as one column. It assumes text already
// fits; fit it first with fitTail or fitHead.
func padTo(text string, width int) string {
	gap := width - utf8.RuneCountInString(text)
	if gap <= 0 {
		return text
	}
	return text + strings.Repeat(" ", gap)
}

// padLeftTo right-aligns text within width by prepending spaces, measured in
// runes.
func padLeftTo(text string, width int) string {
	gap := width - utf8.RuneCountInString(text)
	if gap <= 0 {
		return text
	}
	return strings.Repeat(" ", gap) + text
}

// fitTail keeps the head of text and drops the tail with a trailing ellipsis
// when it overflows width. Width and slicing are rune-based so the ellipsis is
// not double-counted.
func fitTail(text string, width int) string {
	if width <= 0 {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= width {
		return text
	}
	if width == 1 {
		return "…"
	}
	return string(runes[:width-1]) + "…"
}

// fitHead keeps the tail of text (for a path, the repo end) and drops the head
// with a leading ellipsis when it overflows width.
func fitHead(text string, width int) string {
	if width <= 0 {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= width {
		return text
	}
	if width == 1 {
		return "…"
	}
	return "…" + string(runes[len(runes)-(width-1):])
}

// clampInt holds value inside the row range, which starts at zero for every
// list. A high below zero (an empty list) clamps to zero rather than to a
// negative index.
func clampInt(value, high int) int {
	if value < 0 {
		return 0
	}
	if value > high {
		return max(high, 0)
	}
	return value
}
