"""Bounded streaming conversion and audits; uploads run independently."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import shutil
import signal
import threading
import time
import uuid
from .quality import Auditor, millis
from .storage import (HuggingFace, LocalMirror, WalError, atomic_json, cleanup,
                      connect, digest, frames, fsync_dir, register, upload_pending)
try:
    import orjson
    loads = orjson.loads
except ImportError:
    loads = json.loads
UPSTREAM = 'cb0f6631556bf460d03594fe20f9bbd020b47d19'
STOP = threading.Event()


def setting(name, default):
    value = int(os.environ.get(name, default))
    if value < 0:
        raise ValueError(f'{name} must be nonnegative')
    return value


def redact(value):
    return re.sub(r'hf_[A-Za-z0-9]+', '[REDACTED]', str(value))[:600]


def schema():
    import pyarrow as pa
    return pa.schema([
        ('source_session', pa.string()), ('local_seq', pa.uint64()),
        ('receive_ns', pa.int64()), ('exchange_ms', pa.int64()),
        ('event_type', pa.string()), ('asset_id', pa.string()), ('market', pa.string()),
        ('record_type', pa.string()), ('payload', pa.binary()),
    ], metadata={b'schema_version': b'1', b'upstream_commit': UPSTREAM.encode(),
                 b'ordering': b'local receiver order; NOT exchange sequence'})


def partition(ns):
    t = datetime.fromtimestamp(ns / 1e9, timezone.utc)
    return f'date={t:%Y-%m-%d}/hour={t:%H}'


def write_audit(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq
    path.parent.mkdir(parents=True, exist_ok=True)
    audit_schema = pa.schema([('time_ns', pa.int64()), ('reason', pa.string()),
                              ('status', pa.string()), ('asset_id', pa.string()),
                              ('gap_id', pa.string()), ('start_ns', pa.int64()), ('end_ns', pa.int64()),
                              ('needs_manual_backfill', pa.bool_()), ('details_json', pa.string())])
    tmp = path.with_suffix('.tmp')
    with pq.ParquetWriter(tmp, audit_schema, compression='zstd', compression_level=1) as writer:
        for offset in range(0, len(rows), 500):
            data = [{**{k: row.get(k) for k in audit_schema.names if k != 'details_json'},
                     'details_json': json.dumps(row, ensure_ascii=False, default=str)}
                    for row in rows[offset:offset+500]]
            writer.write_table(pa.Table.from_pylist(data, schema=audit_schema))
    with tmp.open('rb') as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fsync_dir(path.parent)


def save_reports(db, root, auditor, key, ns):
    rows = auditor.report()
    path = root / 'out/health/integrity_checks' / partition(ns) / f'{key}.parquet'
    write_audit(path, rows)
    register(db, root, path, 'integrity_checks')
    result = [path]
    gaps = [r for r in rows if r.get('needs_manual_backfill') or r.get('gap_id')]
    if gaps:
        path = root / 'out/health/collector_gaps' / partition(ns) / f'{key}.parquet'
        write_audit(path, gaps)
        register(db, root, path, 'collector_gaps')
        result.append(path)
    return result


def persist_auditor(db, auditor):
    db.execute("INSERT OR REPLACE INTO kv VALUES ('open_gaps',?)", (json.dumps(auditor.gaps),))
    db.execute("INSERT OR REPLACE INTO kv VALUES ('sequences',?)", (json.dumps(auditor.sequence),))


def convert(db, root, source, auditor):
    import pyarrow as pa
    import pyarrow.parquet as pq
    rel = source.relative_to(root).as_posix()
    if db.execute('SELECT 1 FROM segments WHERE path=?', (rel,)).fetchone():
        return 0
    stream = source.parent.name
    stem = source.name.rsplit('.', 1)[0]
    session = stem.split('-', 1)[1]
    start_ns = int(stem.split('-', 1)[0])
    out = root / 'out' / ('raw' if stream == 'raw' else 'upstream_logs') / partition(start_ns) / f'{stem}.parquet'
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix('.tmp')
    count = data_rows = size = 0
    first_seq = last_seq = None
    batch, seen = [], {}
    error = None
    payload_digest = hashlib.sha256()
    writer = pq.ParquetWriter(tmp, schema(), compression='zstd', compression_level=1,
                             use_dictionary=['asset_id', 'market', 'event_type', 'record_type', 'source_session'])
    try:
        try:
            for seq, recv_ns, payload in frames(source):
                count += 1
                first_seq = seq if first_seq is None else first_seq
                last_seq = seq
                payload_digest.update(payload)
                try:
                    event = loads(payload)
                    if not isinstance(event, dict):
                        raise ValueError('event is not a JSON object')
                except (ValueError, TypeError):
                    auditor.emit('json', 'invalid_preserved', source=rel, local_seq=seq)
                    event = {}
                control = event.get('_polydata_control') is True
                data_rows += not control
                if stream == 'raw':
                    auditor.record(session, seq, recv_ns, event)
                    if isinstance(event.get('asset_id'), str):
                        seen[event['asset_id']] = recv_ns
                else:
                    auditor.upstream(session, recv_ns, event)
                batch.append({'source_session': session, 'local_seq': seq, 'receive_ns': recv_ns,
                              'exchange_ms': millis(event.get('timestamp')),
                              **{k: event.get(k) if isinstance(event.get(k), str) else None for k in ('event_type', 'asset_id', 'market')},
                              'record_type': 'collector_control' if control else ('pmxt_event' if stream == 'raw' else 'upstream_log'),
                              'payload': payload})
                size += len(payload)
                if size >= setting('ARROW_BATCH_BYTES', 1 << 20) or len(batch) >= 2000:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema()), row_group_size=len(batch))
                    batch, size = [], 0
        except WalError as exc:
            error = str(exc)
            auditor.emit('wal', 'corrupt_prefix_salvaged', source=rel, valid_rows=count,
                         error=error, needs_manual_backfill=True)
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema()), row_group_size=len(batch))
    finally:
        writer.close()
    if pq.ParquetFile(tmp).metadata.num_rows != count:
        raise ValueError('Parquet row count mismatch')
    with tmp.open('rb') as f:
        os.fsync(f.fileno())
    os.replace(tmp, out)
    fsync_dir(out.parent)
    # Only the committed catalog makes files eligible for upload.
    with db:
        register(db, root, out, stream)
        audits = save_reports(db, root, auditor, stem, start_ns)
        manifest = root / 'out/manifests' / partition(start_ns) / f'{stem}.json'
        atomic_json(manifest, {'schema_version': 1, 'upstream_commit': UPSTREAM,
                              'source_file': rel, 'source_sha256': digest(source),
                              'source_session': session, 'rows': count, 'data_rows': data_rows,
                              'first_seq': first_seq, 'last_seq': last_seq,
                              'payload_concat_sha256': payload_digest.hexdigest(),
                              'recovered': source.suffix == '.recovered', 'wal_error': error,
                              'history_complete': False,
                              'artifacts': [{'path': p.relative_to(root / 'out').as_posix(),
                                             'sha256': digest(p), 'bytes': p.stat().st_size}
                                            for p in [out, *audits]]})
        register(db, root, manifest, 'manifest')
        artifact = out.relative_to(root / 'out').as_posix()
        db.executemany('INSERT OR IGNORE INTO dependencies VALUES (?,?)',
                       [(artifact, p.relative_to(root / 'out').as_posix()) for p in [*audits, manifest]])
        db.execute('INSERT INTO segments VALUES (?,?,?,?,?,?)',
                   (rel, artifact, count, int(source.suffix == '.recovered'), error, time.time()))
        db.executemany('INSERT INTO seen VALUES (?,?) ON CONFLICT(asset) DO UPDATE SET receive_ns=MAX(receive_ns,excluded.receive_ns)', seen.items())
        persist_auditor(db, auditor)
    return count


def upload_loop(root, sink, notices):
    db = connect(root)
    retry = 5
    try:
        while not STOP.is_set():
            try:
                pending = db.execute('SELECT COUNT(*),MIN(created),COALESCE(SUM(bytes),0) FROM artifacts WHERE verified IS NULL').fetchone()
                due = pending[0] and (time.time() - pending[1] >= setting('UPLOAD_INTERVAL_SECONDS', 600) or
                                      pending[2] >= setting('UPLOAD_BATCH_BYTES', 64 << 20))
                if due:
                    n = upload_pending(db, root, sink, limit=96)
                    notices.put({'reason': 'upload', 'status': 'verified', 'files': n})
                pressure = shutil.disk_usage(root).free < setting('DISK_RESERVE_BYTES', 2 << 30) + (512 << 20)
                cleanup(db, root, setting('LOCAL_RETENTION_SECONDS', 21600), pressure=pressure)
                retry = 5
                STOP.wait(5)
            except Exception as exc:
                notices.put({'reason': 'upload', 'status': 'failed_retrying', 'error': redact(exc)})
                STOP.wait(retry)
                retry = min(retry * 2, 600)
    finally:
        db.close()


def check_catalog(root):
    db = connect(root)
    results = {'pending_files': db.execute('SELECT COUNT(*) FROM artifacts WHERE verified IS NULL').fetchone()[0],
               'pending_bytes': db.execute('SELECT COALESCE(SUM(bytes),0) FROM artifacts WHERE verified IS NULL').fetchone()[0],
               'verified_files': db.execute('SELECT COUNT(*) FROM artifacts WHERE verified IS NOT NULL').fetchone()[0],
               'corrupt_segments': db.execute('SELECT COUNT(*) FROM segments WHERE error IS NOT NULL').fetchone()[0],
               'free_bytes': shutil.disk_usage(root).free}
    db.close()
    for name in ('raw', 'upstream', 'worker'):
        try:
            results[name] = json.loads((root / f'state/{name}.json').read_text())
        except (OSError, ValueError):
            results[name] = None
    return results


def receiver_audit(root, auditor):
    try:
        state = json.loads((root / 'state/raw.json').read_text())
        received, appended, synced = (state.get(k, 0) for k in ('received', 'appended', 'fsynced'))
        auditor.emit('receiver_accounting', 'pass' if received == appended and synced <= appended else 'suspect',
                     received=received, appended=appended, fsynced=synced,
                     pending_fsync=appended-synced, scope='downstream_only',
                     known_unsaved=received-appended, exchange_message_count_unknown=True)
        if time.time_ns() - state['time_ns'] > 30_000_000_000 or state.get('disk_pressure'):
            auditor.emit('receiver_health', 'possible_gap', start_ns=state.get('last_receive_ns'),
                         end_ns=None, needs_manual_backfill=True, receiver_status=state)
    except (OSError, ValueError, KeyError):
        auditor.emit('receiver_health', 'unavailable')


def coverage(db, root, auditor, counter):
    """Cache/observation check; a silent token is not necessarily unsubscribed."""
    try:
        import redis
        with redis.Redis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379'),
                                  socket_timeout=15, socket_connect_timeout=5) as client:
            raw = client.get('polymarket:active_markets')
            if not raw:
                raise ValueError('active market cache is empty')
            data = loads(raw)
            expected = {str(m[k]) for m in data['markets'] for k in ('yes_asset_id', 'no_asset_id')}
            cutoff = time.time_ns() - setting('OBSERVATION_WINDOW_SECONDS', 3600) * 1_000_000_000
            observed = {r[0] for r in db.execute('SELECT asset FROM seen WHERE receive_ns>=?', (cutoff,))}
            auditor.emit('token_observation', 'reported', expected_tokens=len(expected),
                         observed_tokens=len(expected & observed), unseen_tokens=len(expected - observed),
                         unseen_sample=sorted(expected - observed)[:100],
                         exact_subscription_set_verified=False, independent_gamma_scan=False,
                         silence_is_gap=False, cache_fetched_at=data.get('fetched_at'))
            if counter % 60 == 0:
                path = root / 'out/metadata' / partition(time.time_ns()) / f'markets-{time.time_ns()}.json'
                atomic_json(path, data)
                with db:
                    register(db, root, path, 'market_mapping')
                    db.execute('DELETE FROM seen WHERE receive_ns<?', (time.time_ns() - 7*86400*10**9,))
            # Trim only behind every consumer's delivered/pending boundary.
            if client.exists('polymarket:market_events'):
                boundaries = []
                key = lambda v: tuple(map(int, v.split(b'-')))
                for group in client.xinfo_groups('polymarket:market_events'):
                    gid = group['last-delivered-id']
                    pending = client.xpending('polymarket:market_events', group['name'])
                    if pending.get('min'):
                        gid = min(gid, pending['min'], key=key)
                    boundaries.append(gid)
                if boundaries:
                    boundary = min(*boundaries, f'{int((time.time()-86400)*1000)}-0'.encode(), key=key)
                    client.xtrim('polymarket:market_events', minid=boundary, approximate=False)
    except Exception as exc:
        auditor.emit('coverage_probe', 'unavailable', error=redact(exc))


def probe(root, asset, target):
    try:
        import httpx
        url = os.getenv('CLOB_HTTP_URL', 'https://clob.polymarket.com')
        with httpx.Client(timeout=10) as client:
            with client.stream('GET', url+'/book', params={'token_id': asset}) as result:
                result.raise_for_status()
                chunks, size = [], 0
                for chunk in result.iter_bytes():
                    size += len(chunk)
                    if size > 4 << 20:
                        raise ValueError('REST snapshot exceeds diagnostic memory budget')
                    chunks.append(chunk)
                book = loads(b''.join(chunks))
        target.put((asset, book, None))
    except Exception as exc:
        target.put((asset, None, redact(exc)))


def run(root, sink=None, once=False):
    import pyarrow as pa
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    db = connect(root)
    lock = (root / 'state/worker.lock').open('a+')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    auditor = Auditor(setting('CHECK_MAX_BOOKS', 64), setting('CHECK_MAX_LEVELS', 20000))
    for key, attr in [('open_gaps', 'gaps'), ('sequences', 'sequence')]:
        value = db.execute('SELECT value FROM kv WHERE key=?', (key,)).fetchone()
        if value:
            setattr(auditor, attr, json.loads(value[0]))
    notices, references = queue.Queue(maxsize=1000), queue.Queue(maxsize=4)
    if sink and not once:
        threading.Thread(target=upload_loop, args=(root, sink, notices), daemon=True).start()
    last_coverage = last_probe = last_audit = 0.0
    probe_thread, probe_cursor, coverage_counter, processed = None, 0, 0, 0
    try:
        while not STOP.is_set():
            paths = sorted([*root.glob('spool/*/*.ready'), *root.glob('spool/*/*.recovered')], key=lambda p: p.name)
            for path in paths:
                processed += convert(db, root, path, auditor)
                atomic_json(root / 'state/worker.json', {'time_ns': time.time_ns(),
                            'rows_processed_this_process': processed, 'cached_books': len(auditor.books),
                            'cached_levels': auditor.level_count, 'history_complete': False})
                if STOP.is_set():
                    break
            while not notices.empty():
                n = notices.get_nowait()
                auditor.emit(n.pop('reason'), n.pop('status'), **n)
            while not references.empty():
                asset, book, error = references.get_nowait()
                if error:
                    auditor.emit('full_book', 'probe_failed', asset, error=error)
                else:
                    try:
                        auditor.reference(asset, book)
                    except (ValueError, TypeError, KeyError) as exc:
                        auditor.emit('full_book', 'invalid_response', asset, error=redact(exc))
            now = time.monotonic()
            if not once and now - last_coverage >= 60:
                coverage(db, root, auditor, coverage_counter)
                receiver_audit(root, auditor)
                coverage_counter += 1
                last_coverage = now
            if not once and now - last_probe >= setting('PROBE_INTERVAL_SECONDS', 30) and auditor.books and (probe_thread is None or not probe_thread.is_alive()):
                assets = list(auditor.books)
                asset = assets[probe_cursor % len(assets)]
                probe_cursor += 1
                probe_thread = threading.Thread(target=probe, args=(root, asset, references), daemon=True)
                probe_thread.start()
                last_probe = now
            if now - last_audit > setting('REPORT_INTERVAL_SECONDS', 900) or once:
                with db:
                    save_reports(db, root, auditor, f'periodic-{uuid.uuid4().hex}', time.time_ns())
                    persist_auditor(db, auditor)
                db.execute('PRAGMA wal_checkpoint(PASSIVE)')
                last_audit = now
            remaining = [p for p in paths if not db.execute('SELECT 1 FROM segments WHERE path=?', (p.relative_to(root).as_posix(),)).fetchone()]
            atomic_json(root / 'state/worker.json', {'time_ns': time.time_ns(),
                        'rows_processed_this_process': processed, 'cached_books': len(auditor.books),
                        'cached_levels': auditor.level_count, 'unprocessed_segments': len(remaining),
                        'history_complete': False})
            if once:
                if sink:
                    while upload_pending(db, root, sink, limit=96):
                        pass
                break
            STOP.wait(2)
    finally:
        db.close()
        lock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['run', 'once', 'status', 'verify-local'])
    parser.add_argument('--root', type=Path, default=Path(os.getenv('DATA_DIR', '/data')))
    parser.add_argument('--mirror', type=Path, help='explicit offline integration-test destination')
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    if args.mode == 'status':
        print(json.dumps(check_catalog(args.root), indent=2))
        return
    if args.mode == 'verify-local':
        db = connect(args.root)
        errors = []
        for row in db.execute('SELECT * FROM artifacts'):
            p = args.root / 'out' / row['path']
            if p.exists() and digest(p) != row['sha256']:
                errors.append(row['path'])
            elif not p.exists() and row['verified'] is None:
                errors.append(row['path'])
        print(json.dumps({'bad_files': errors}))
        db.close()
        raise SystemExit(bool(errors))
    sink = None
    if args.mirror:
        sink = LocalMirror(args.mirror)
    elif os.getenv('HF_REPO_ID'):
        token = Path(os.getenv('HF_TOKEN_FILE', '/run/secrets/hf_token')).read_text().strip()
        sink = HuggingFace(os.environ['HF_REPO_ID'], token)
    elif args.mode == 'run':
        raise ValueError('HF_REPO_ID is required; use --mirror only for offline tests')
    run(args.root, sink, once=args.mode == 'once')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        import sys
        print(redact(exc), file=sys.stderr)
        raise SystemExit(1)
