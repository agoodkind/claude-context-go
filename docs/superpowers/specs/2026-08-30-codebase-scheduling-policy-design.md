# Codebase scheduling policy design

Status: implemented. macOS installed-service validation passed. Linux
installed-service validation remains tracked separately.
Date: 2026-08-30

Scope: persistent and per-run scheduling policy for code indexing. The policy
orders daemon work, allows cooperative preemption, and optionally limits work to
periods without recent user input.

## Result

Every codebase has a stored scheduling policy. Each job has an effective
policy resolved from that stored policy and any one-run override.

The daemon admits every indexing source through one scheduler. High-priority
work can pause normal- and low-priority work. Normal work can pause low-priority
work. Free slots go to waiting high, then normal, then low work.

Quiet work requires five minutes without user input. The quiet decision never
uses graphics processor load, power draw, or ordinary thermal movement because
LMS itself changes those signals. Platform thermal state remains a safety
cutoff.

## Goals

- Store `high`, `normal`, or `low` priority per codebase.
- Default existing and new codebases to `normal` unless initial indexing says
  otherwise.
- Store quiet eligibility separately from priority.
- Accept one-run policy overrides on index and sync requests.
- Change stored policy, and current work, through codebase commands.
- Order user, watcher, periodic, adoption, and recovery work consistently.
- Pause safely after the current file and resume without discarding completed
  work.
- Explain every queued or paused decision through existing status surfaces.
- Support macOS and Linux service installations.

## Non-goals

- Provider-specific graphics processor accounting.
- Graphics processor, power-draw, or load-average thresholds.
- Changing embedding, chunking, collection, or Merkle identity.
- Priority for search requests or code-graph queries.
- Priority controls for conversation document codebases. Their jobs remain
  normal priority and still participate in global admission.
- A new MCP sync tool. Sync overrides use the CLI and existing gRPC method.
- Wayland, X11, or desktop-session helper processes.

## Scheduling policy

A full scheduling policy contains:

- `priority`: `high`, `normal`, or `low`.
- `quiet`: whether recent user input gates the work.
- `idle_after_seconds`: the required idle duration in seconds. The default is
  300 seconds.

Missing persisted fields decode as `normal`, quiet disabled, and five minutes.
This preserves old registries without a migration.

The codebase record also carries `policy_pending_initialization`. A missing or
false value means initialized, so every pre-upgrade registry entry remains an
existing codebase. Automatic discovery of an unbuilt worktree explicitly stores
true. Its deferred work uses defaults but does not clear the marker. The first
explicit index request or stored-policy command clears it. Adoption of an
existing collection stores false because that codebase already exists. This
lets an operator's first explicit index persist supplied values after a status
read discovers an unbuilt worktree without changing legacy behavior.

The effective policy follows these rules:

| Request | Stored policy | Effective run policy |
| --- | --- | --- |
| First explicit index for an uninitialized policy with no flags | Persist defaults and initialize | Defaults |
| First explicit index for an uninitialized policy with flags | Persist supplied values plus defaults for omissions and initialize | Stored result |
| Existing index or sync with no policy flags | Unchanged | Stored policy |
| Existing index or sync with policy flags | Unchanged | Stored policy with supplied one-run overrides |
| Codebase policy command | Update immediately | Reclassify active, paused, and queued work immediately |

An effective `high` priority bypasses the user-idle gate. It does not bypass the
thermal safety cutoff when quiet is enabled.

## Command behavior

`codebase index` and `codebase sync` accept `--priority`, `--quiet`, and
`--idle-after`. The flags use presence-aware parsing, so omission means "use the
stored value" rather than `normal`, false, or zero.

`codebase priority PATH high|normal|low` changes stored priority.

`codebase quiet PATH on|off` changes the stored quiet setting. Its optional
`--idle-after` flag changes the stored threshold. Changing either stored command
updates current work for that codebase immediately.

The update request is a field-level patch. It changes only supplied stored
fields and only those fields on active, paused, or queued work. For example,
`codebase priority` does not reset stored quiet settings or an unrelated
one-run quiet override on the active job.

The MCP `index_codebase` tool exposes the same optional index overrides. This
work adds no MCP sync tool. A protocol update method backs both stored-policy
commands so command-line and future clients share one mutation path.

## Scheduler

The scheduler replaces direct contention on the current indexing-slot channel.
User requests, pending successors, watcher converges, periodic syncs, adoption
refreshes, and boot recovery all request capacity from it.

Each admitted job receives a capacity lease. The lease covers the indexing slot
and the job's reference on the shared sync lock. A paused job releases both. A
resumed job reacquires both before writing.

The scheduler lease is the only owner allowed to acquire or release either
resource. The existing stalled-read watchdog yields through that lease instead
of touching the slot channel or sync lock directly. Its reacquisition joins the
scheduler with the same job priority and queue sequence. Scheduler-managed
waiting replaces the watchdog's terminal five-second reacquisition timeout,
because a visible paused job may correctly wait behind higher-priority work.
The lease makes yield and reacquire idempotent so a watchdog and a priority
change cannot release or acquire twice.

The scheduler keeps a first-in, first-out queue within each priority. A paused
job retains its original queue sequence, so newer work of the same priority
cannot overtake it.

Admission follows these rules:

1. Each free slot goes to the highest-priority waiting job.
2. A waiting high job revokes only enough lower-priority leases to obtain a
   slot.
3. A waiting normal job revokes only enough low-priority leases to obtain a
   slot.
4. A low job may use spare capacity after every waiting high and normal job has
   a slot.
5. Running lower-priority jobs continue beside higher-priority jobs until a
   waiting higher-priority job needs their capacity.
6. Equal-priority jobs never preempt each other.

Revocation is cooperative. The worker finishes its current file, records
progress, durably journals the paused transition, and releases its capacity
lease. The job keeps its identifier, staging collection, counters, and
cancellation context. Cancellation remains available while it waits.

A journal flush barrier must confirm the paused transition before the lease is
released as resumable work. If that write fails, the job ends with
`pause_journal_failed` through the existing terminal cleanup path and releases
its lease. It never reports paused state that restart recovery cannot prove.
Resume similarly records and flushes the running transition before the worker
writes again.

If the resume transition cannot reach durable storage, the job ends failed with
`resume_journal_failed`. It releases the newly reserved slot and sync-lock lease,
cleans staging through the terminal failure path, and does not retry or write
another file.

The scheduler reevaluates admission after a job arrives, finishes, pauses,
resumes, changes policy, or receives a new platform-activity sample.

## Quiet eligibility

One injected activity source reports:

- elapsed user-input idle time;
- whether thermal safety requires a pause;
- whether the input signal is available; and
- a stable reason when it is unavailable.

The daemon refreshes this snapshot every two seconds. Detection may occur
within two seconds, but work stops only after the current file reaches its safe
checkpoint.

A quiet job may run only when the input signal is available and every relevant
session has been idle for its configured threshold. New input pauses it. If the
signal disappears, the job stays queued or paused with
`waiting: activity unavailable`. It resumes automatically when the signal
returns and becomes eligible.

Thermal safety is independent from the idle decision. A quiet job pauses at the
platform's serious, hot, or critical condition even when LMS caused the heat.
Missing thermal data adds a status note but does not block work.

### macOS

A small Objective-C bridge, kept in its own `.m` file, reads combined-session
input idle time through [Core Graphics](https://developer.apple.com/documentation/coregraphics/cgeventsource/secondssincelasteventtype(_:eventtype:)).
It reads `serious` and `critical` thermal states through
[ProcessInfo](https://developer.apple.com/documentation/foundation/processinfo/thermalstate-swift.enum).
The Go adapter calls this bridge in process.

The installed launch agent must prove the input query from its real `gui/<uid>`
service context. A clean-machine acceptance test determines whether macOS Input
Monitoring authorization is required. An authorization failure is activity
unavailable, never idle.

### Linux

The Linux adapter reads active, local user sessions for the daemon user through
the [systemd login manager D-Bus interface](https://www.freedesktop.org/software/systemd/man/org.freedesktop.login1.html).
Every eligible session must set `IdleHint` and must keep it set for the
configured threshold according to `IdleSinceHintMonotonic`. No eligible session,
a lost system bus, or invalid session data is activity unavailable.

Linux thermal safety reads the current temperature and platform-provided `hot`
or `critical` trip points from the
[kernel thermal interface](https://docs.kernel.org/driver-api/thermal/sysfs-api.html).
The daemon does not invent a temperature percentage or fixed threshold.

## Persisted and wire state

The codebase record stores the full policy plus its pending-initialization
marker. The job record stores its effective policy, queue sequence, and pause or
waiting reason. These fields remain outside `IndexConfig`, so changing policy
does not change the ignore digest, invalidate a Merkle snapshot, or request new
embeddings.

The protocol defines a closed priority enum with an unspecified wire value. A
full policy message represents stored and effective values. A separate override
message uses optional fields so requests distinguish omission from explicit
defaults.

The codebase wire view carries stored policy. The job wire view carries
effective policy and its scheduling reason. One update request changes stored
policy by canonical path and returns the updated codebase.

Priority changes do not affect collection names, schema, chunk identifiers, or
TypeScript bookkeeping. Shared Milvus compatibility remains unchanged.

## Job state, recovery, and failures

Paused is a distinct active job state. Queued means the job has never acquired
capacity. Paused means it started and released capacity at a safe checkpoint.

The job journal records effective policy and every queued, running, paused, and
resumed transition. Boot reconciliation treats interrupted paused work like
interrupted running work. The existing recovery path creates a successor job
with the interrupted job's effective policy, including a one-run override.

Invalid policy values fail before registry or job mutation. Registry write
failure leaves both stored policy and current-job classification unchanged.
Activity-source failure does not fail a job. Scheduler shutdown and explicit
cancellation use the existing terminal cancellation path.

An older daemon can read a registry containing the new JSON fields because it
ignores unknown fields. A rollback can drop those fields on its next full
registry rewrite. Release notes must identify that reverse-compatibility limit.

## Manual downgrade

An older daemon does not understand a latest journal state of paused. Before a
manual downgrade, let every paused job resume and finish, or cancel it, so each
job's latest event is a state the older daemon understands. The automatic
updater only moves forward, so this work adds no downgrade mechanism.

## Operator visibility

Codebase list and status show stored priority, quiet state, and idle threshold.
Job list and get show effective priority plus `queued`, `running`, or `paused`.
Waiting text names the controlling reason, including:

- higher-priority work;
- user active;
- activity unavailable; and
- thermal safety.

Status metrics report queue depth and running count by priority. They also
report activity-source availability without exporting paths, input events, or
session identifiers.

## Verification

Tests enter through daemon, protocol, command, and installed-service boundaries
as far as each contract permits.

Scheduler tests use temporary codebases and controlled file boundaries to
prove:

- every free slot goes to waiting high, then normal, then low work;
- high and normal arrivals pause only enough lower jobs;
- the current file finishes before pause;
- pause releases capacity and resume continues the same job;
- equal-priority first-in, first-out order survives pause;
- a live policy mutation reclassifies current work;
- stalled-read yield and reacquisition use scheduler order without duplicate
  release;
- a pause-journal failure terminates honestly before resumable release;
- a resume-journal failure terminates and releases reacquired capacity before
  any further write;
- an automatically discovered codebase still persists policy on its first
  explicit index;
- a legacy codebase treats index flags as one-run overrides;
- field-level stored-policy patches preserve unrelated run overrides;
- watcher and periodic work cannot bypass the scheduler;
- cancellation works while paused;
- restart recovery preserves a one-run effective policy;
- completion or cancellation leaves no paused latest journal state.

Protocol and command tests prove initial persistence, existing-codebase
overrides, explicit policy mutation, validation, and consistent human and JSON
status.

Platform tests inject activity and thermal sources. Linux tests inject a D-Bus
client and thermal filesystem root. macOS tests inject bridge results. Installed
service smoke tests prove the real launch agent and systemd user service can
read their activity source. The final change runs concurrency tests, race tests,
and `make check`.
