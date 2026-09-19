"""Durable local event processing using only the Python standard library."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import subprocess
import sys


SCHEMA = """
CREATE TABLE IF NOT EXISTS inbox (
    event_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'done')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT
);
CREATE TABLE IF NOT EXISTS records (
    record_type TEXT NOT NULL,
    record_id TEXT NOT NULL,
    data_json TEXT NOT NULL,
    last_event_id TEXT NOT NULL REFERENCES inbox(event_id),
    PRIMARY KEY (record_type, record_id)
);
"""


class EventConflict(ValueError):
    """An accepted event ID was reused for different content."""


class TransientProcessingError(RuntimeError):
    """An injected, retryable processing failure used by the demonstration."""


def canonical_event(event: dict) -> str:
    """Validate an envelope and normalize JSON object-key order for comparison."""
    if not isinstance(event, dict) or set(event) != {
        'id', 'type', 'record_id', 'data'
    }:
        raise ValueError('Event requires exactly id, type, record_id, and data.')
    for field in ('id', 'type', 'record_id'):
        if not isinstance(event[field], str) or not event[field].strip():
            raise ValueError(f'{field} must be a nonempty string.')
    if event['type'] not in ('order.updated', 'crm.updated'):
        raise ValueError('Supported types: order.updated, crm.updated.')
    if not isinstance(event['data'], dict):
        raise ValueError('data must be a JSON object.')
    try:
        return json.dumps(event, sort_keys=True, separators=(',', ':'),
                          ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError('Event must contain valid JSON values.') from exc


class EventStore:
    """Persist an inbox and its local destination in the same SQLite file."""

    def __init__(self, database: str | Path):
        self.database = str(database)
        if self.database == ':memory:':
            raise ValueError('Use a file path: this demo requires persistence.')
        with self._connection() as connection:
            connection.executescript(SCHEMA)

    @contextmanager
    def _connection(self):
        connection = sqlite3.connect(self.database, timeout=30,
                                     isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA foreign_keys = ON')
        connection.execute('PRAGMA synchronous = FULL')
        try:
            yield connection
        finally:
            # An exception or process interruption must not commit partial work.
            if connection.in_transaction:
                connection.rollback()
            connection.close()

    def accept(self, event: dict) -> str:
        """Commit before acknowledging; compare duplicate IDs under one lock."""
        payload = canonical_event(event)
        with self._connection() as connection:
            connection.execute('BEGIN IMMEDIATE')
            existing = connection.execute(
                'SELECT payload_json FROM inbox WHERE event_id = ?',
                (event['id'],),
            ).fetchone()
            if existing:
                if existing['payload_json'] != payload:
                    raise EventConflict(f'Conflicting payload for {event["id"]}.')
                connection.commit()
                return 'duplicate'
            connection.execute(
                'INSERT INTO inbox (event_id, payload_json) VALUES (?, ?)',
                (event['id'], payload),
            )
            connection.commit()
            return 'accepted'

    def process_one(self, *, fail_after_write: bool = False) -> dict | None:
        """Process the oldest pending event; the flag injects a test failure.

        A writer lock prevents a second worker from claiming the same event.
        The savepoint separates durable attempt/error tracking from effects.
        Only the destination upsert and successful completion share that inner
        unit. No network or other external side effects are performed here.
        """
        with self._connection() as connection:
            connection.execute('BEGIN IMMEDIATE')
            row = connection.execute(
                "SELECT * FROM inbox WHERE state = 'pending' ORDER BY rowid LIMIT 1"
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            event = json.loads(row['payload_json'])
            attempts = row['attempts'] + 1
            connection.execute(
                'UPDATE inbox SET attempts = ? WHERE event_id = ?',
                (attempts, row['event_id']),
            )
            connection.execute('SAVEPOINT local_effect')
            try:
                connection.execute(
                    '''INSERT INTO records
                       (record_type, record_id, data_json, last_event_id)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT (record_type, record_id) DO UPDATE SET
                           data_json = excluded.data_json,
                           last_event_id = excluded.last_event_id''',
                    (event['type'].split('.')[0], event['record_id'],
                     json.dumps(event['data'], sort_keys=True), event['id']),
                )
                if fail_after_write:
                    raise TransientProcessingError('Injected failure after upsert')
                connection.execute(
                    "UPDATE inbox SET state = 'done', last_error = NULL "
                    'WHERE event_id = ?', (event['id'],),
                )
            except TransientProcessingError as exc:
                connection.execute('ROLLBACK TO local_effect')
                connection.execute('RELEASE local_effect')
                connection.execute(
                    'UPDATE inbox SET last_error = ? WHERE event_id = ?',
                    (str(exc), event['id']),
                )
                connection.commit()
                return {'event_id': event['id'], 'state': 'retryable',
                        'attempts': attempts, 'error': str(exc)}
            # Unexpected exceptions escape; _connection rolls back everything.
            connection.execute('RELEASE local_effect')
            connection.commit()
            return {'event_id': event['id'], 'state': 'done', 'attempts': attempts}

    def work(self, limit: int = 100, *, fail_after_write: bool = False) -> list:
        """Run a bounded batch, stopping at the first retryable failure."""
        if limit < 1:
            raise ValueError('limit must be positive.')
        results = []
        for _ in range(limit):
            result = self.process_one(fail_after_write=fail_after_write)
            if result is None:
                break
            results.append(result)
            if result['state'] == 'retryable':
                break
        return results

    def snapshot(self) -> dict:
        """Read inbox and destination from one consistent database snapshot."""
        with self._connection() as connection:
            connection.execute('BEGIN')
            inbox = [dict(row) for row in connection.execute(
                'SELECT event_id, state, attempts, last_error '
                'FROM inbox ORDER BY rowid'
            )]
            records = []
            for row in connection.execute(
                'SELECT * FROM records ORDER BY record_type, record_id'
            ):
                record = dict(row)
                record['data'] = json.loads(record.pop('data_json'))
                records.append(record)
            connection.commit()
            return {'inbox': inbox, 'records': records}


def demo(database: str) -> dict:
    """Run synthetic order/CRM events, including retry in a fresh process."""
    store = EventStore(database)
    if store.snapshot()['inbox']:
        raise ValueError('Demo requires an empty database; choose a new --db path.')
    order = {'id': 'evt-order-001', 'type': 'order.updated',
             'record_id': 'DEMO-1001', 'data': {'status': 'packed', 'units': 3}}
    accepted = store.accept(order)
    duplicate = store.accept(dict(reversed(list(order.items()))))
    try:
        store.accept({**order, 'data': {'status': 'shipped', 'units': 3}})
    except EventConflict:
        conflict = 'rejected'
    failed = store.process_one(fail_after_write=True)
    count_after_failure = len(store.snapshot()['records'])

    # This really launches a separate interpreter, rather than reopening only
    # an object. It consumes the durable pending event left by the first run.
    restarted = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), '--db', str(database),
         'work', '--limit', '1'], check=True, capture_output=True, text=True,
    )
    store.accept({'id': 'evt-crm-001', 'type': 'crm.updated',
                  'record_id': 'DEMO-CRM-01',
                  'data': {'organization': 'Synthetic Shop', 'stage': 'qualified'}})
    store.accept({'id': 'evt-order-002', 'type': 'order.updated',
                  'record_id': 'DEMO-1001',
                  'data': {'status': 'shipped', 'units': 3}})
    final_batch = store.work()
    return {'first_delivery': accepted, 'duplicate_delivery': duplicate,
            'conflicting_delivery': conflict, 'failed_attempt': failed,
            'records_after_failure': count_after_failure,
            'fresh_process_retry': json.loads(restarted.stdout),
            'remaining_work': final_batch, 'final': store.snapshot()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='events.sqlite3', help='SQLite file path')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('init', help='Create the local tables')
    receive = commands.add_parser('receive', help='Accept one JSON event from a file')
    receive.add_argument('file')
    worker = commands.add_parser('work', help='Process a bounded pending batch')
    worker.add_argument('--limit', type=int, default=100)
    worker.add_argument('--fail-after-write', action='store_true',
                        help='Inject a retryable failure after the local upsert')
    commands.add_parser('status', help='Show inbox and local records')
    commands.add_parser('demo', help='Run synthetic examples in an empty database')
    args = parser.parse_args()
    try:
        if args.command == 'demo':
            result = demo(args.db)
        else:
            store = EventStore(args.db)
            if args.command == 'init':
                result = {'initialized': True}
            elif args.command == 'receive':
                event = json.loads(Path(args.file).read_text(encoding='utf-8'))
                result = {'delivery': store.accept(event)}
            elif args.command == 'work':
                result = store.work(args.limit, fail_after_write=args.fail_after_write)
            else:
                result = store.snapshot()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        if args.command == 'work' and any(r['state'] == 'retryable' for r in result):
            return 1
        return 0
    except EventConflict as exc:
        print(json.dumps({'error': str(exc), 'kind': 'event_conflict'}), file=sys.stderr)
        return 2
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(json.dumps({'error': str(exc)}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
