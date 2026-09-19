"""Behavior tests against temporary on-disk databases and real subprocesses."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from event_delivery import EventConflict, EventStore


SCRIPT = Path(__file__).with_name('event_delivery.py')


def event(event_id='evt-1', status='packed'):
    return {'id': event_id, 'type': 'order.updated', 'record_id': 'DEMO-1',
            'data': {'status': status, 'units': 3}}


def crash_worker(database):
    """Exit after the upsert without running Python cleanup or rollback code."""
    store = EventStore(database)
    original_connection = store._connection

    @contextmanager
    def interrupted_connection():
        with original_connection() as connection:
            def interrupt(sql):
                if sql.startswith("UPDATE inbox SET state = 'done'"):
                    os._exit(23)
            connection.set_trace_callback(interrupt)
            yield connection

    store._connection = interrupted_connection
    store.process_one()
    raise RuntimeError('Expected interruption did not execute.')


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / 'events.sqlite3'
        self.store = EventStore(self.db)

    def cli(self, *args, check=True):
        return subprocess.run([sys.executable, str(SCRIPT), '--db', str(self.db),
                               *args], capture_output=True, text=True, check=check)

    def test_duplicate_before_and_after_completion_does_not_reprocess(self):
        original = event()
        reordered = {'data': {'units': 3, 'status': 'packed'},
                     'record_id': 'DEMO-1', 'type': 'order.updated', 'id': 'evt-1'}
        self.assertEqual(self.store.accept(original), 'accepted')
        self.assertEqual(self.store.accept(reordered), 'duplicate')
        self.store.work()
        completed = self.store.snapshot()
        self.assertEqual(self.store.accept(original), 'duplicate')
        self.assertEqual(self.store.work(), [])
        self.assertEqual(self.store.snapshot(), completed)
        self.assertEqual(completed['inbox'][0]['attempts'], 1)
        self.assertEqual(len(completed['records']), 1)

    def test_conflicting_id_rejected_without_changing_pending_or_done_event(self):
        self.store.accept(event())
        for process in (False, True):
            if process:
                self.store.work()
            before = self.store.snapshot()
            with self.assertRaises(EventConflict):
                self.store.accept(event(status='shipped'))
            self.assertEqual(self.store.snapshot(), before)
        self.assertEqual(self.store.snapshot()['records'][0]['data']['status'], 'packed')

    def test_failure_rolls_back_initial_write_and_retry_finishes_once(self):
        self.store.accept(event())
        failed = self.store.process_one(fail_after_write=True)
        self.assertEqual(failed['state'], 'retryable')
        after = EventStore(self.db).snapshot()
        self.assertEqual(after['records'], [])
        self.assertEqual(after['inbox'][0]['state'], 'pending')
        self.assertEqual(after['inbox'][0]['attempts'], 1)
        self.assertTrue(after['inbox'][0]['last_error'])
        EventStore(self.db).work()
        done = self.store.snapshot()
        self.assertEqual(done['inbox'][0]['attempts'], 2)
        self.assertEqual(done['inbox'][0]['state'], 'done')
        self.assertIsNone(done['inbox'][0]['last_error'])
        self.assertEqual(done['records'][0]['data']['status'], 'packed')
        self.assertIsNone(self.store.process_one())

    def test_failed_update_preserves_existing_destination_then_upserts(self):
        self.store.accept(event())
        self.store.work()
        first_record = self.store.snapshot()['records']
        self.store.accept(event('evt-2', 'shipped'))
        self.store.process_one(fail_after_write=True)
        self.assertEqual(self.store.snapshot()['records'], first_record)
        self.store.work()
        records = self.store.snapshot()['records']
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['data']['status'], 'shipped')
        self.assertEqual(records[0]['last_event_id'], 'evt-2')

    def test_pending_event_survives_restart_and_cli_retry(self):
        self.store.accept(event())
        failed = self.cli('work', '--fail-after-write', check=False)
        self.assertEqual(failed.returncode, 1)
        self.assertEqual(json.loads(failed.stdout)[0]['state'], 'retryable')
        retry = json.loads(self.cli('work').stdout)
        self.assertEqual(retry[0]['attempts'], 2)
        observed = json.loads(self.cli('status').stdout)
        self.assertEqual(observed['inbox'][0]['state'], 'done')
        self.assertEqual(observed['records'][0]['last_event_id'], 'evt-1')

    def test_unexpected_database_error_rolls_back_destination_and_completion(self):
        self.store.accept(event())
        with sqlite3.connect(self.db) as conn:
            conn.execute("""CREATE TRIGGER fail_completion BEFORE UPDATE OF state
                            ON inbox WHEN NEW.state = 'done'
                            BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.process_one()
        self.assertEqual(self.store.snapshot()['records'], [])
        self.assertEqual(self.store.snapshot()['inbox'][0]['state'], 'pending')
        with sqlite3.connect(self.db) as conn:
            conn.execute('DROP TRIGGER fail_completion')
        self.store.work()
        self.assertEqual(self.store.snapshot()['inbox'][0]['state'], 'done')

    def test_concurrent_duplicate_deliveries_and_workers_in_separate_processes(self):
        source = Path(self.temp.name) / 'event.json'
        source.write_text(json.dumps(event()), encoding='utf-8')
        with ThreadPoolExecutor(max_workers=6) as pool:
            delivered = list(pool.map(lambda _: self.cli('receive', str(source)), range(6)))
            outcomes = [json.loads(result.stdout)['delivery'] for result in delivered]
            self.assertEqual(outcomes.count('accepted'), 1)
            self.assertEqual(outcomes.count('duplicate'), 5)
            workers = list(pool.map(lambda _: self.cli('work'), range(6)))
        processed = [row for result in workers for row in json.loads(result.stdout)]
        self.assertEqual(len(processed), 1)
        self.assertEqual(self.store.snapshot()['inbox'][0]['attempts'], 1)

    def test_concurrent_workers_preserve_accepted_order_for_one_destination(self):
        for i in range(12):
            self.store.accept(event(f'evt-{i}', f'stage-{i}'))
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.cli('work', '--limit', '4'), range(4)))
        snapshot = self.store.snapshot()
        self.assertEqual(len(snapshot['records']), 1)
        self.assertEqual(snapshot['records'][0]['data']['status'], 'stage-11')
        self.assertTrue(all(row['state'] == 'done' and row['attempts'] == 1
                            for row in snapshot['inbox']))

    def test_invalid_envelope_creates_no_inbox_entry(self):
        with self.assertRaises(ValueError):
            self.store.accept({**event(), 'type': 'unsupported'})
        with self.assertRaises(ValueError):
            self.store.accept({**event(), 'data': {'units': float('nan')}})
        self.assertEqual(self.store.snapshot(), {'inbox': [], 'records': []})

    def test_abrupt_worker_exit_rolls_back_before_restart_retry(self):
        self.store.accept(event())
        before = self.store.snapshot()
        crashed = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), '--crash-worker', str(self.db)],
            capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(crashed.returncode, 23, crashed.stderr)
        restarted = EventStore(self.db)
        self.assertEqual(restarted.snapshot(), before)
        self.assertEqual(restarted.process_one()['state'], 'done')
        self.assertEqual(len(restarted.snapshot()['records']), 1)


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--crash-worker':
        crash_worker(sys.argv[2])
    else:
        unittest.main()
