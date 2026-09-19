# Durable event delivery demo

A small, runnable example of reliable order and CRM updates: accept an event,
detect a repeated delivery, retry a failed update, and retain pending work
across a process restart.

Everything runs locally using Python's standard library and one SQLite file.
The example records are synthetic. There are no network calls, real customer
accounts, or financial actions.

## Run it

From this folder, with Python 3.10 or newer:

```sh
python3 event_delivery.py --db demo.sqlite3 demo
python3 -m unittest -v
```

The demo requires an empty database. For another run, choose a fresh filename,
for example `--db demo-2.sqlite3`. No package installation is needed. The
recorded verification used Python 3.14.6.

## Observed sample results

The included `sample-results.json` is captured output from running the demo.

| Step | Observed result |
| --- | --- |
| First delivery | Accepted and committed to the inbox |
| Identical delivery with reordered JSON keys | Reported as a duplicate |
| Same event ID with different content | Rejected as a conflict |
| Failure injected after the destination upsert | Zero destination records; event remains pending |
| Worker launched in a fresh Python process | Pending event completes on attempt 2 |
| New CRM event and later order update | Both complete on attempt 1 |
| Final state | 3 completed inbox events; 2 destination records |

The order's final status is `shipped`; the CRM record's stage is `qualified`.
These are demonstration results, not client delivery history.

## Walk through the CLI

Use a separate database to inspect each stage:

```sh
python3 event_delivery.py --db walkthrough.sqlite3 init
python3 event_delivery.py --db walkthrough.sqlite3 receive example-event.json
python3 event_delivery.py --db walkthrough.sqlite3 receive example-event.json
python3 event_delivery.py --db walkthrough.sqlite3 work --fail-after-write
python3 event_delivery.py --db walkthrough.sqlite3 status
python3 event_delivery.py --db walkthrough.sqlite3 work
python3 event_delivery.py --db walkthrough.sqlite3 status
```

The injected-failure command intentionally exits with code **1**. Continue to
`status` and the next `work` command to inspect the rollback and perform the retry.
Normal success exits **0**. Conflicting IDs, invalid input and ordinary CLI errors
exit **2**. A worker processes at most 100 events by default; use `--limit N` to
change the batch size.

## Processing contract

- Each envelope has exactly `id`, `type`, `record_id`, and `data`. Supported types
  are `order.updated` and `crm.updated`. `data` is a JSON object.
- The event ID identifies one immutable event. Object-key order and insignificant
  JSON whitespace do not affect duplicate detection. Value differences do:
  for example, `1` and `1.0` are not normalized to the same representation.
- Acceptance commits the payload before acknowledging it. A replay of the same
  ID and normalized content returns `duplicate`; different content raises
  `EventConflict`. It never silently replaces the accepted payload.
- Workers take pending events in inbox acceptance order. `BEGIN IMMEDIATE`
  serializes writers, so concurrent workers cannot claim the same event.
- One transaction contains the destination upsert and the inbox completion
  marker. A savepoint rolls both back on the injected transient failure while
  retaining the attempt count and error for retry.
- Each successful event replaces the whole local record identified by
  `(record_type, record_id)`. This is an upsert, not a partial field merge.
- Completed events remain in the inbox for later duplicate checks. A failed
  event stays pending. A batch stops on a retryable failure; invoke `work` again
  to retry. There is no hidden background worker.

## What the tests exercise

The ten included tests use temporary on-disk databases, real CLI subprocesses,
and independent SQLite connections. They check duplicate handling before and
after completion, conflicts, first-write rollback, rollback of an existing
record's update, retry in a new process, and invalid input. A database trigger
forces a failure at the completion step to check that the destination write
also rolls back. Concurrent subprocess tests check repeated delivery and multiple
workers updating the same local destination in accepted order. One test abruptly
exits a worker after its destination write, bypassing Python cleanup, then checks
that a new process sees the original pending event and can retry it.

Run all checks with `python3 -m unittest -v`. This is functional verification;
it is not a load test or a simulation of machine power loss.

## Limits and integration decisions

The atomic guarantee covers **one destination in the same SQLite transaction**.
It does not provide exactly-once effects for an external CRM, email service,
payment system, or arbitrary HTTP API. Connecting those services requires a
separate delivery design, typically destination idempotency keys, an outbox,
and reconciliation of uncertain responses.

This demo has no HTTP receiver, scheduler, exponential backoff, dead-letter
queue, schema migrations, retention policy, metrics exporter, or distributed
worker coordination. SQLite allows one writer at a time; a connection waits
up to 30 seconds for a lock. Use a local filesystem with normal SQLite locking.
It is not intended for a shared network filesystem.

Records follow acceptance order, not event timestamps or upstream versions.
A late old event can replace newer business data. A permanently failing event
also blocks later pending work. Production rules for stale events, retry limits
and operator recovery must be agreed for the actual workflow.

Attempt counts describe attempts committed by the worker. An unexpected error
or interrupted transaction rolls back that count along with its changes.
Durability relies on SQLite, its filesystem, and the host storage honoring
commits; the tests do not prove behavior under hardware failure.

## Files

- `event_delivery.py`: reusable `EventStore` and command-line interface.
- `test_event_delivery.py`: focused functional and concurrency tests.
- `example-event.json`: synthetic order update for the CLI walkthrough.
- `sample-results.json`: actual output from the synthetic demo.
- `LICENSE`: MIT license under the pseudonym Parsifal-The-Searcher.
