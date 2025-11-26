# Life of a Backup

## Introduction

This document aims to explain the execution of a scheduled backup in CockroachDB,
following the code paths from the SQL `CREATE SCHEDULE FOR BACKUP` statement
through schedule creation, periodic execution, and the actual backup operation
itself. We'll trace through: SQL parsing, schedule storage, the jobs scheduler
daemon, backup planning, job execution, protected timestamp management, and the
distinction between full and incremental backups. The idea is to provide a
high-level unifying view of the backup scheduling system; none will be explored
in particular depth but pointers to other documentation will be provided where
such documentation exists. Code pointers will abound.

This document will generally not discuss design decisions, but rather focus on
tracing through the actual (current) code. In the interest of brevity, it only
covers the most significant and common parts of scheduled backup execution, and
omits a large amount of detail and special cases.

The intended audience is folks curious about a walk through CockroachDB's backup
architecture, particularly the interaction between SQL schedules, the jobs
system, and backup execution. It will hopefully also be helpful for open source
contributors and new Cockroach Labs engineers.

## Backup Schedules: An Overview

CockroachDB supports scheduled backups through the `CREATE SCHEDULE FOR BACKUP`
statement, which allows users to define recurring full and incremental backups.
A backup schedule consists of:

- A **full backup schedule** that runs periodically (e.g., daily)
- An optional **incremental backup schedule** that runs more frequently (e.g., hourly)
- **Protected timestamp records** that prevent garbage collection of data needed for the backup chain
- **Dependency links** between full and incremental schedules for coordination

Let's walk through a concrete example:

```sql
CREATE SCHEDULE hourly_backups
  FOR BACKUP INTO 's3://bucket/database-backups?AUTH=implicit'
  RECURRING '@hourly'
  FULL BACKUP '@daily'
  WITH SCHEDULE OPTIONS first_run = 'now';
```

This creates two schedules:
1. A full backup schedule running daily
2. An incremental backup schedule running hourly

We'll trace how this statement flows through the system and eventually produces
backups on the configured cadence.

## SQL Parsing and AST Construction

A backup schedule arrives at the server through the [PostgreSQL wire
protocol](https://www.postgresql.org/docs/current/protocol.html), just like any
other SQL statement. The parsing flow is identical to what's described in the
"Life of a Query" document up to the point where we have a parsed AST.

The `CREATE SCHEDULE FOR BACKUP` statement is represented by the
[`tree.ScheduledBackup`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/sql/sem/tree/schedule.go#L34-L45)
AST node, which contains:

- `ScheduleLabel`: the schedule name
- `Recurrence`: the incremental backup frequency (e.g., `@hourly`)
- `FullBackup`: the full backup frequency (e.g., `@daily`)
- `Targets`: what to back up (databases, tables, or full cluster)
- `To`: destination URIs
- `BackupOptions`: encryption, revision history, etc.
- `ScheduleOptions`: first run time, on execution/previous running behavior

The AST is constructed by the [yacc-generated parser](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/sql/parser/sql.y)
from the SQL grammar, producing a `tree.ScheduledBackup` that will be processed
by the plan hook system.

## Schedule Creation: Plan Hook Entry Point

CockroachDB uses [plan hooks](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/sql/plan_hook.go#L34-L35)
to extend SQL with enterprise features. The backup schedule plan hook is
registered in
[`create_scheduled_backup.go`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L830):

```go
sql.AddPlanHook("schedule backup", createBackupScheduleHook)
```

When the planner encounters a `tree.ScheduledBackup` node, it
[calls the hook](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L804-L828),
which:

1. [Extracts and validates](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L810-L818)
   the schedule parameters
2. [Calls `makeScheduledBackupSpec()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L819-L822)
   to prepare the schedule specification
3. [Returns a plan](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L824-L828)
   that will execute `doCreateBackupSchedules()` when run

The [`makeScheduledBackupSpec()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L584-L633)
function is responsible for evaluating all the schedule expressions and options:

- [Evaluates the incremental recurrence](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L598-L601)
  cron expression (`@hourly` in our example)
- [Evaluates the full backup recurrence](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L602-L609)
  (`@daily` in our example)
- [Extracts backup options](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L610-L626)
  like encryption settings, revision history, etc.
- [Extracts schedule options](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L627-L632)
  like first run time and execution behavior

## Creating Schedule Records

The heart of schedule creation is
[`doCreateBackupSchedules()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L166-L432),
which creates one or two schedule records depending on whether both full and
incremental backups are configured. Let's walk through this process step by step.

### Computing Full Backup Frequency

If the user specified only an incremental schedule without an explicit full
backup cadence, the system
[automatically picks a full backup frequency](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L138-L161):

```go
func getScheduledBackupRecurrence(incRecurrence, fullRecurrence *string) (
    incCron, fullCron string, err error) {
    if fullRecurrence == nil && incRecurrence != nil {
        incCron = *incRecurrence
        incDuration := schedulebase.ComputeScheduleRecurrence(..., incCron)
        if incDuration <= time.Hour {
            // Incrementals run hourly or more frequently → daily full backup
            fullCron = "@daily"
        } else if incDuration <= 24*time.Hour {
            // Incrementals run daily → weekly full backup
            fullCron = "@weekly"
        }
    }
    ...
}
```

This ensures that even if users don't explicitly configure full backups, they
still get them at a reasonable cadence. In our example with `@hourly`
incrementals, the system would automatically choose `@daily` for full backups if
not specified.

### Preparing the Backup Statement

Before creating the schedule records, the system
[constructs a `tree.Backup` node](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L217-L227)
representing the backup command that will be executed by the schedule:

```go
backupNode := &tree.Backup{
    Targets:        scheduleDetails.backupTargets,
    To:             scheduleDetails.destination,
    IncrementalStorage: scheduleDetails.incrementalStorage,
    Options:        scheduleDetails.backupOptions,
}
// All scheduled backups run as jobs, so they're detached
backupNode.Options.Detached = tree.DBoolTrue
```

The system then performs a
[dry-run backup](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L266-L295)
to validate that the backup configuration is correct before creating any
schedules. This catches configuration errors early.

### Creating the Incremental Schedule

If an incremental schedule was specified, the system
[creates the incremental schedule first](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L317-L365).
The key aspect is setting up the backup statement with `AppendToLatest`:

```go
backupNode.AppendToLatest = true  // Incremental: append to existing backup chain
inc, incArgs, err := makeBackupSchedule(env, p.User(), scheduleLabel,
    incRecurrence, incrementalScheduleDetails, unpauseOnSuccessID,
    updateMetricOnSuccess, backupNode, chainProtectedTimestampRecords)
```

The incremental schedule is
[created in a PAUSED state](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L355-L360)
because it cannot run until the first full backup completes:

```go
inc.Pause(pauseStatusPendingFullBackup)  // Wait for initial full backup
```

### Creating the Full Backup Schedule

The [full backup schedule](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L367-L412)
is created similarly, but with `AppendToLatest = false`:

```go
backupNode.AppendToLatest = false  // Full backup: new backup chain
full, fullArgs, err := makeBackupSchedule(env, p.User(), scheduleLabel,
    fullRecurrence, details, unpauseOnSuccessID, updateMetricOnSuccess,
    backupNode, chainProtectedTimestampRecords)
```

The full schedule is linked to the incremental schedule through the
[`DependentScheduleID` field](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L396-L408):

```go
if inc != nil {
    fullArgs.DependentScheduleID = inc.ScheduleID()
}
```

This linkage enables the full backup to unpause the incremental schedule upon
successful completion.

### Schedule Record Structure

Each schedule is stored as a row in the
[`system.scheduled_jobs`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/jobspb/schedule.proto)
table. The
[`makeBackupSchedule()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L468-L530)
function constructs a
[`jobs.ScheduledJob`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/scheduled_job.go#L50-L68)
with:

- **Executor type**: [`"scheduled-backup-executor"`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L510)
- **Execution arguments**: A serialized
  [`ScheduledBackupExecutionArgs`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backuppb/schedule.proto#L27-L60)
  protobuf containing:
  - `BackupType`: [`FULL` or `INCREMENTAL`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L491-L493)
  - `BackupStatement`: [Serialized backup SQL](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L504)
  - `UnpauseOnSuccess`: [Schedule ID to unpause](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L495)
    when this schedule completes
  - `DependentScheduleID`: [Linked schedule ID](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L496)
  - `ChainProtectedTimestampRecords`: [Whether to chain PTS records](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L497)

The schedules are then [persisted to the system table](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/create_scheduled_backup.go#L419-L430).

## The Job Scheduler Daemon

Now that we have schedule records in `system.scheduled_jobs`, we need something
to execute them. This is the role of the
[job scheduler daemon](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/job_scheduler.go#L58-L61),
which runs continuously on each node and periodically checks for schedules that
are ready to execute.

### The Scheduling Loop

The scheduler's main loop is implemented in
[`processSchedule()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/job_scheduler.go#L123-L199).
At a high level, it:

1. [Loads a schedule](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/job_scheduler.go#L74-L97)
   that is ready to run (`next_run <= now()`)
2. [Checks if jobs are already running](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/job_scheduler.go#L127-L145)
   for this schedule and handles them according to the schedule's "on previous
   running" policy (WAIT, SKIP, or NO_WAIT)
3. [Updates the next run time](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/job_scheduler.go#L149-L163)
   based on the cron expression
4. [Executes the schedule](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/job_scheduler.go#L165-L179)
   by delegating to the appropriate executor

The schedule loading query uses `FOR UPDATE` to ensure that only one scheduler
daemon can process a given schedule at a time:

```go
SELECT * FROM system.scheduled_jobs
WHERE schedule_id = $1 AND next_run <= $2
FOR UPDATE
```

### Executor Dispatch

The scheduler [looks up the executor](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/job_scheduler.go#L165-L173)
based on the schedule's `executor_type`:

```go
executor, err := GetScheduledJobExecutor(schedule.ExecutorType())
if err != nil {
    return err
}
err = executor.ExecuteJob(ctx, txn, cfg, env, schedule)
```

For backup schedules, this resolves to the
[`scheduledBackupExecutor`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L43-L55),
which is registered during [package initialization](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L33-L39).

## Backup Execution: The Scheduled Backup Executor

When the job scheduler calls
[`ExecuteJob()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L57-L69)
on the backup executor, it [delegates to](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L65)
[`executeBackup()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L72-L199),
which is responsible for:

1. Extracting the backup statement from the schedule
2. Setting the backup timestamp to the scheduled run time
3. Planning and executing the backup
4. Handling any errors

### Extracting the Backup Statement

The backup statement was stored as a string when the schedule was created. Now
we need to [parse it back into an AST](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L75-L80):

```go
backupStmt, err := extractBackupStatement(ctx, execCtx, sj)
```

The [`extractBackupStatement()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L233-L265)
function deserializes the execution arguments and
[parses the SQL string](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L255-L258):

```go
stmt, err := parser.ParseOne(args.BackupStatement)
backupStmt := stmt.AST.(*tree.Backup)
```

This `tree.Backup` node contains the `AppendToLatest` flag that determines
whether this is a full or incremental backup.

### Setting the Backup Timestamp

One of the key aspects of scheduled backups is that they execute at a
deterministic timestamp based on when they were supposed to run, not when they
actually run. This ensures backup consistency even if the scheduler is slightly
delayed.

The backup timestamp is [set to the schedule's run time](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L95-L101):

```go
endTime, err := tree.MakeDTimestampTZ(sj.ScheduledRunTime(), time.Microsecond)
if err != nil {
    return err
}
backupStmt.AsOf = tree.AsOfClause{Expr: endTime}
```

This sets the backup's `AS OF SYSTEM TIME` to the scheduled run time, ensuring
that the backup captures data as of that moment.

### Planning the Backup

The backup is then [planned using the standard backup plan hook](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L145-L149):

```go
backupFn, err := planBackup(ctx, planner, backupStmt)
if err != nil {
    return err
}
```

The [`planBackup()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L277-L290)
function calls the [backup plan hook](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_planning.go#L275-L355),
which we'll explore in detail in the next section.

### Invoking the Backup

Finally, the backup is [executed](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L151-L157):

```go
_, err = invokeBackup(ctx, backupFn, nil, nil)
if err != nil {
    return errors.Wrap(err, "failed to backup")
}
```

This creates a backup job and waits for it to complete (or at least start,
since scheduled backups run with `DETACHED`).

## Backup Planning and Job Creation

The backup planning process is where the system decides what data to back up and
how to organize it. This happens in
[`backupPlanHook()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_planning.go#L275-L355),
the main entry point for backup planning.

### Destination Resolution

One of the first steps is to figure out where the backup will be written. This
is handled by
[`resolveDest()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_planning.go#L638-L721),
which behaves differently for full vs. incremental backups:

**For full backups** (`AppendToLatest = false`):
```go
if backupStmt.AppendToLatest {
    // Not taken for full backups
} else if subdir != "" {
    // Explicit subdirectory specified
} else {
    // FULL BACKUP: Create timestamp-based subdirectory
    initialDetails.Destination.Subdir = endTime.GoTime().Format(
        "2006/01/02-150405.00")
}
```

**For incremental backups** (`AppendToLatest = true`):
```go
if backupStmt.AppendToLatest {
    // INCREMENTAL BACKUP: Use LATEST
    initialDetails.Destination.Subdir = backupdest.LatestFileName
    initialDetails.Destination.Exists = true
}
```

The constant [`backupdest.LatestFileName`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backupdest/backup_destination.go#L51)
is simply the string `"LATEST"`, which is a special marker.

### Finding the Backup Chain

The [`backupdest.ResolveDest()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backupdest/backup_destination.go#L99-L214)
function resolves the actual destination path and finds any previous backups in
the chain.

**For incremental backups**, it [reads the `LATEST` file](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backupdest/backup_destination.go#L120-L134)
to find the current backup chain:

```go
if chosenSuffix == backupdest.LatestFileName {
    latest, err := backupinfo.ReadLatestFile(ctx, defaultURI, ...)
    if err != nil {
        return err
    }
    chosenSuffix = latest  // e.g., "2024/01/15-120000.00"
}
```

It then [looks for previous backups](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backupdest/backup_destination.go#L145-L172)
in that directory to build the backup chain:

```go
prevBackups, err := backupinfo.FindPriorBackups(ctx, defaultStore, ...)
```

This returns a list of URIs pointing to the full backup and all incremental
backups in the chain. The new incremental backup will read metadata from these
to determine what data has already been backed up.

**For full backups**, `PrevBackupURIs` is empty, as they start new backup chains.

### Creating the Backup Job

Once the destination is resolved and the backup chain is determined, the system
[creates a backup job record](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_planning.go#L846-L886):

```go
jr := jobs.Record{
    Description: backupStmt.String(),
    Username: p.User(),
    DescriptorIDs: func() (sqlDescIDs []descpb.ID) { ... },
    Details: jobspb.BackupDetails{
        Destination: to,
        EndTime: endTime,
        URI: defaultURI,
        ...
    },
    Progress: jobspb.BackupProgress{},
}
```

The job is then [created and started](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_planning.go#L889-L896):

```go
plannerTxn := p.InternalSQLTxn()
job, err := p.ExecCfg().JobRegistry.CreateJobWithTxn(ctx, jr, jobID, plannerTxn)
if err != nil {
    return err
}
err = job.Start(ctx)
```

## Backup Job Execution

The backup job is executed by the
[`backupResumer`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L44-L48),
which implements the
[`jobs.Resumer`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/jobs/registry.go#L113-L144)
interface. When the job starts, its
[`Resume()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L51-L237)
method is called.

### Protected Timestamp Setup

For scheduled backups with protected timestamp chaining enabled, one of the
first steps is to [set up protected timestamp records](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L71-L78):

```go
if scheduledBackupDetails := details.ScheduleDetails; scheduledBackupDetails != nil {
    if err := planSchedulePTSChaining(ctx, execCtx, p, scheduledBackupDetails,
        &backupDetails.SchedulePTSChainingRecord); err != nil {
        return err
    }
}
```

The [`planSchedulePTSChaining()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L1041-L1159)
function determines what to do with protected timestamps based on whether this
is a full or incremental backup. We'll explore this in detail in the "Protected
Timestamp Chaining" section.

### Resolving the Destination

The backup job [resolves the destination](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L88-L90)
using the same logic we saw in the planning phase:

```go
defaultURI, mainBackupManifests, localityInfo, err :=
    backupdest.ResolveDest(ctx, p.User(), details.Destination, ...)
```

This finds the backup chain (for incrementals) or creates a new subdirectory
(for full backups).

### Executing the Backup

The actual backup execution happens in the
[`backup()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_processor_planning.go#L92-L432)
function, which:

1. [Determines the spans to back up](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_processor_planning.go#L127-L144)
   (tables, indexes, etc.)
2. [Filters out data already in previous backups](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_processor_planning.go#L189-L232)
   (for incrementals)
3. [Distributes the backup work across nodes](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_processor_planning.go#L261-L336)
   using DistSQL
4. [Collects backup metadata](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_processor_planning.go#L367-L400)
   (files written, statistics, etc.)
5. [Writes the backup manifest](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_processor_planning.go#L411-L429)

The key difference between full and incremental backups at this stage is in step
2: incremental backups read the manifests from previous backups and
[filter out overlapping data](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/datadriven_test.go#L1574-L1619),
only backing up data that has changed since the previous backup in the chain.

### Writing the LATEST File

For incremental backups, after the backup manifest is written, the system
[updates the `LATEST` file](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L218-L224):

```go
if err := backupinfo.WriteLatestFile(ctx, defaultStore,
    details.Destination.Subdir); err != nil {
    return err
}
```

This ensures that the next incremental backup will find and append to this
backup chain.

## Protected Timestamp Chaining

Protected timestamps prevent garbage collection of data that is needed by
backups. For scheduled backups, CockroachDB uses a sophisticated "chaining"
mechanism to maintain continuous GC protection across full and incremental
backups.

### The Problem

Consider a daily full backup with hourly incrementals:
- Monday 00:00: Full backup at timestamp T1
- Monday 01:00: Incremental at T2
- Monday 02:00: Incremental at T3
- ...
- Tuesday 00:00: New full backup at T25

We need to protect data from T1 to T25 to ensure all incrementals can be
restored. However, we don't know T25 when creating the Monday full backup.
Additionally, we want to clean up protection for the old chain when a new full
backup completes.

### The Solution: PTS Chaining

The solution is to maintain a single protected timestamp record that is
repeatedly updated as incrementals complete, and replaced when a new full backup
starts a new chain.

### Full Backup PTS Chaining

When a full backup runs, it [plans to release the old PTS](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L1075-L1100)
and create a new one:

```go
func planSchedulePTSChaining(...) error {
    if args.BackupType == jobspb.ScheduledBackupExecutionArgs_FULL {
        incArgs := &jobspb.ScheduledBackupExecutionArgs{}
        if err := pbtypes.UnmarshalAny(incSchedule.ExecutionArgs(), incArgs);
            err != nil {
            return err
        }

        // Plan to RELEASE the old PTS record from the incremental schedule
        backupDetails.SchedulePTSChainingRecord = &jobspb.SchedulePTSChainingRecord{
            ProtectedTimestampRecord: incArgs.ProtectedTimestampRecord,
            Action: jobspb.SchedulePTSChainingRecord_RELEASE,
        }
    }
    ...
}
```

When the full backup [completes successfully](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L288-L316),
the [`OnSuccess()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L288-L316)
callback is invoked, which calls
[`processScheduledBackupCompletion()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L319-L341).
This function [handles the PTS chaining](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_pts_chaining.go#L140-L198)
by calling
[`manageFullBackupPTSChaining()`](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_pts_chaining.go#L140-L198):

```go
func manageFullBackupPTSChaining(ctx context.Context, ...) error {
    // 1. Create new PTS record at full backup's EndTime
    ptsRecord, err := protectTimestampRecordForSchedule(ctx, pts, target,
        backupDetails.EndTime, incScheduleID)

    // 2. Release the old PTS record
    if oldRecord := chainedRecord.ProtectedTimestampRecord; oldRecord != nil {
        if err := pts.Release(ctx, txn, *oldRecord); err != nil {
            return err
        }
    }

    // 3. Store new PTS record on incremental schedule
    incSchedule.SetScheduleDetails(incArgs)
    incArgs.ProtectedTimestampRecord = &ptsRecord
    return incSchedule.Update(ctx, txn, updateFunc)
}
```

This ensures:
- The new backup chain is protected from its EndTime forward
- The old backup chain's protection is removed
- Future incrementals will update this new PTS record

### Incremental Backup PTS Chaining

When an incremental backup runs, it [plans to update the existing PTS](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L1128-L1141):

```go
func planSchedulePTSChaining(...) error {
    if args.BackupType == jobspb.ScheduledBackupExecutionArgs_INCREMENTAL {
        if args.ProtectedTimestampRecord == nil {
            return errors.New("expected PTS record on incremental schedule")
        }

        // Plan to UPDATE the existing PTS record
        backupDetails.SchedulePTSChainingRecord = &jobspb.SchedulePTSChainingRecord{
            ProtectedTimestampRecord: args.ProtectedTimestampRecord,
            Action: jobspb.SchedulePTSChainingRecord_UPDATE,
        }
    }
    ...
}
```

When the incremental completes, it [updates the PTS timestamp](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_pts_chaining.go#L264-L284):

```go
func manageIncrementalBackupPTSChaining(ctx context.Context, ...) error {
    ptsRecordID := *chainedRecord.ProtectedTimestampRecord

    // Update PTS timestamp to this incremental's EndTime
    if err := pts.UpdateTimestamp(ctx, txn, ptsRecordID,
        backupDetails.EndTime); err != nil {
        return err
    }

    return nil
}
```

This extends the protection window to include the new incremental backup without
creating additional PTS records.

### PTS Chaining Timeline

Here's how PTS records evolve over a backup schedule's lifecycle with daily
fulls and hourly incrementals:

```
Monday 00:00 (Full Backup):
  - Create PTS record P1 protecting from T1 (full backup EndTime)
  - Store P1 on incremental schedule

Monday 01:00 (Incremental):
  - Update P1 to protect from T1 to T2 (incremental EndTime)

Monday 02:00 (Incremental):
  - Update P1 to protect from T1 to T3

...

Monday 23:00 (Incremental):
  - Update P1 to protect from T1 to T24

Tuesday 00:00 (Full Backup):
  - Create new PTS record P2 protecting from T25 (new full backup EndTime)
  - Release P1 (old chain no longer needed)
  - Store P2 on incremental schedule

Tuesday 01:00 (Incremental):
  - Update P2 to protect from T25 to T26

...
```

This mechanism ensures:
- Continuous GC protection for active backup chains
- Automatic cleanup when chains are superseded
- Decoupling from GC TTL settings
- Efficient use of protected timestamps (one per backup chain, not per backup)

## Schedule Completion and Coordination

After a backup job completes, the system needs to update the schedule state,
unpause dependent schedules, and update metrics. This happens through the
[job notification system](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L308-L316).

### Job Notification

When the backup job finishes, it [notifies the scheduler](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/backup_job.go#L308-L316):

```go
env := scheduledjobs.ProdJobSchedulerEnv
if err := jobs.NotifyJobTermination(ctx, env, jobID, jobStatus,
    details, r.job.Payload().UsernameProto.Decode()); err != nil {
    log.Warningf(ctx, "failed to notify job termination: %v", err)
}
```

This calls back into the [backup executor's notification handler](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L201-L231):

```go
func (e *scheduledBackupExecutor) NotifyJobTermination(ctx context.Context,
    jobID jobspb.JobID, jobStatus Status, details jobspb.Details, ...) error {

    if jobStatus == jobs.StatusSucceeded {
        return e.backupSucceeded(ctx, scheduleID, details, env, txn)
    }

    // Handle failures...
    return e.backupFailed(ctx, scheduleID, jobID, details, env, txn)
}
```

### Full Backup Success: Unpausing Incrementals

When a full backup succeeds, it [unpauses the incremental schedule](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L391-L408):

```go
func (e *scheduledBackupExecutor) backupSucceeded(...) error {
    args := &ScheduledBackupExecutionArgs{}
    if err := pbtypes.UnmarshalAny(sj.ExecutionArgs(), args); err != nil {
        return err
    }

    // If this full backup should unpause a dependent schedule
    if args.UnpauseOnSuccess != 0 {
        incSchedule, err := env.Load(ctx, txn, args.UnpauseOnSuccess)
        if err != nil {
            return err
        }

        // Unpause the incremental schedule
        incSchedule.Unpause(unresolvedReason)
        if err := incSchedule.Update(ctx, txn); err != nil {
            return err
        }
    }
    ...
}
```

This allows incremental backups to start running now that there's a full backup
to append to.

### Incremental Backup Success: Metric Updates

When an incremental backup succeeds, it [updates the last backup metric](https://github.com/cockroachdb/cockroach/blob/dd1d13d9c69ef8fcb2d4e6fa5bc1abfbda2ac275/pkg/ccl/backupccl/schedule_exec.go#L377-L385):

```go
if args.UpdatesLastBackupMetric {
    // Update schedule with successful backup timestamp
    if err := e.metrics.updateLastBackupMetric(scheduleID, details); err != nil {
        log.Warningf(ctx, "failed to update last backup metric: %v", err)
    }
}
```

This metric can be monitored to ensure backups are running successfully.

## Summary: Full vs. Incremental Backup Flow

Let's recap the key differences between full and incremental backup execution:

### Full Backup
1. **Schedule Creation**: `AppendToLatest = false`, links to incremental via `DependentScheduleID`
2. **Destination**: New timestamped subdirectory (e.g., `2024/01/15-120000.00`)
3. **Backup Chain**: Empty `PrevBackupURIs` (starts new chain)
4. **Protected Timestamp**: RELEASE old PTS, create new PTS at full backup EndTime
5. **Completion**: Unpause incremental schedule, store new PTS on incremental

### Incremental Backup
1. **Schedule Creation**: `AppendToLatest = true`, initially PAUSED
2. **Destination**: Resolves `LATEST` to current backup chain subdirectory
3. **Backup Chain**: Reads full + previous incrementals from chain
4. **Protected Timestamp**: UPDATE existing PTS to incremental EndTime
5. **Completion**: Update metrics, keep schedule active

### The Complete Flow

```
User: CREATE SCHEDULE FOR BACKUP ... RECURRING '@hourly' FULL BACKUP '@daily'
  ↓
SQL Parser → tree.ScheduledBackup AST
  ↓
createBackupScheduleHook → doCreateBackupSchedules
  ↓
Create two schedules:
  - Full Backup (ACTIVE, runs daily, AppendToLatest=false)
  - Incremental (PAUSED, runs hourly, AppendToLatest=true)
  ↓
Store in system.scheduled_jobs table
  ↓
Job Scheduler Daemon (polls every minute)
  ↓
When next_run <= now(): Load schedule FOR UPDATE
  ↓
GetScheduledJobExecutor("scheduled-backup-executor")
  ↓
scheduledBackupExecutor.ExecuteJob
  ↓
extractBackupStatement + set AS OF SYSTEM TIME
  ↓
planBackup → backupPlanHook
  ↓
resolveDest:
  - Full: Create timestamp subdir
  - Incremental: Resolve LATEST, find backup chain
  ↓
Create backup job with BackupDetails
  ↓
backupResumer.Resume
  ↓
planSchedulePTSChaining:
  - Full: Plan RELEASE old + create new
  - Incremental: Plan UPDATE existing
  ↓
Execute backup (scan spans, filter data, write files)
  ↓
Write BACKUP_MANIFEST + LATEST file
  ↓
OnSuccess → processScheduledBackupCompletion
  ↓
managePTSChaining:
  - Full: Release old PTS, create new PTS, store on inc schedule
  - Incremental: Update PTS timestamp
  ↓
NotifyJobTermination → backupSucceeded
  ↓
If full backup: Unpause incremental schedule
If incremental: Update last backup metric
  ↓
Schedule next run based on cron expression
```

This comprehensive flow ensures that backups run reliably on schedule, maintain
proper GC protection, and coordinate between full and incremental backups to
create restorable backup chains.
