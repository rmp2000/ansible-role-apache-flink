# Native Apache Flink

Flink 1.20.5, Java 17, official Pulsar connector 4.2.0-1.20. One VM hosts an
application JobManager and TaskManager as unprivileged systemd services. Control
listeners bind loopback; access REST 8081 through an authenticated SSH tunnel.
The SHA512-verified distribution and Maven dependencies are pinned. Compilation
and unit tests run when source changes, not on ordinary convergence.

The connector binary was built against Pulsar client 3.0.5. Client 3.0.17 changed
an internal metadata lookup signature. The job compiles two Apache connector
source classes with the updated call and excludes their older binary versions
from the shaded dependency. See `files/job/NOTICE` for provenance and the exact
patch. Live Pulsar integration testing is required before promoting a revision;
local-file mode alone cannot prove connector compatibility.

Local mode watches `/srv/apache-flink/input` for complete JSONL files. Each line
must have `event_id` (1-128 alphanumeric/underscore/hyphen), `sensor` (1-64 of the
same characters), and finite numeric `value` with absolute value <= 1e9. Input
IDs are unique per sensor. JSON output gives cumulative count, sum and mean.
Equal ID/value pairs are ignored; conflicts and invalid records produce stable
rejection records with a payload hash. Payloads are not included in rejection
records. Local results commit on checkpoints under `/srv/apache-flink/output`.

Pulsar mode reads `platform/events/measurements` with durable subscription
`flink-measurements`, and writes to `platform/events/data-platform`. Output has a
stable `event_id` message property for the existing idempotent dCache consumer.
TLS hostname and certificate verification are required. Configure only a scoped
client token and public CA through the trusted IM SSH channel. No administrator
token or signing key belongs on the Flink VM. Configuration is root:flink 0640.

Stable operator UIDs and fixed job ID allow the JobManager launcher to restore
the newest completed retained checkpoint after service or VM restart. State,
checkpoints and input/output files live on `/srv/apache-flink`; the template
requires an additional data mount. This is a single-host prototype, not HA or a
recovery solution after volume deletion. Checkpoint compatibility across job
changes or mode changes is not guaranteed; migrate explicitly using a tested
savepoint plan. Do not change mode on an existing deployment.

The Pulsar sink is at least once. dCache deduplicates equal results by event ID;
there is no end-to-end transaction across Flink, Pulsar and WebDAV. The current
reference aggregation keeps all seen IDs indefinitely, so state grows with
unique input events. Cumulative summaries are a demonstration, not windowed or
event-time analytics. Define retention, capacity and backup policies before
long-running production use.

Normal verification checks running vertices and completed checkpoints. In local
mode it additionally proves aggregation and duplicate handling on the live job.
`apache_flink_test_restart: true` proves recovery of keyed state and source/sink
positions after restarting services. The wrapper also supports stage/check
around a VM reboot. Local verification input and committed output fixtures are
retained on test disks: deleting shared sink files could remove application data.

Service names: `apache-flink-jobmanager`, `apache-flink-taskmanager`.
Logs: `/var/log/apache-flink` and systemd journal. Build source and unit tests:
`files/job`; live verifier: `/usr/local/bin/apache-flink-verify`.
