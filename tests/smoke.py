"""TCP -> Go receiver -> WAL -> Parquet -> verified offline mirror.
Inject socket disconnect and SIGKILL, then verify every expected payload byte.
No live accounts or internet required.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from polydata.storage import frames, WalError, connect, cleanup, digest


def payload(i):
    kind = ['book', 'price_change', 'last_trade_price', 'tick_size_change'][i % 4]
    e = dict(event_type=kind, market='test-market', asset_id='test-asset',
             timestamp=str(1791331200000+i), fixture_id=i)
    if kind == 'book':
        e.update(bids=[['0.40', '10.000']], asks=[['0.60', '20']], hash=f'h{i}')
    elif kind == 'price_change':
        e.update(price='0.40', size='25', side='BUY', best_bid='0.40', best_ask='0.60', hash=f'h{i}')
    elif kind == 'last_trade_price':
        e.update(price='0.50', size='2', side='BUY', fee_rate_bps='0', transaction_hash=f'test{i}')
    else:
        e.update(old_tick_size='0.01', new_tick_size='0.001')
    return json.dumps(e, separators=(',', ':')).encode()


def encode(parts):
    return b'*'+str(len(parts)).encode()+b'\r\n'+b''.join(
        b'$'+str(len(p)).encode()+b'\r\n'+p+b'\r\n' for p in parts)


def read_command(f):
    line = f.readline()
    if not line:
        return []
    parts = []
    for _ in range(int(line[1:])):
        n = int(f.readline()[1:])
        parts.append(f.read(n))
        if f.read(2) != b'\r\n':
            raise ValueError('RESP terminator')
    return parts


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            parts = read_command(self.rfile)
            assert parts[0] == b'SUBSCRIBE'
            with self.server.lock:
                index = self.server.connections
                self.server.connections += 1
            self.wfile.write(b'*3\r\n$9\r\nsubscribe\r\n$17\r\npolymarket:events\r\n:1\r\n')
            ranges = [range(250), range(250, 1000), range(1000, 1010)]
            batch = ranges[index] if index < len(ranges) else []
            for i in batch:
                self.wfile.write(encode([b'message', b'polymarket:events', payload(i)]))
            self.wfile.flush()
            if index == 0:
                return
            while read_command(self.rfile):
                self.wfile.write(encode([b'pong', b'']))
                self.wfile.flush()
        except (OSError, ValueError):
            return


def wait_status(root, count, proc, timeout=20):
    end = time.monotonic()+timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            raise AssertionError(f'receiver exited early: {proc.communicate()}')
        try:
            s = json.loads((root/'state/raw.json').read_text())
            if s['received'] == count and s['fsynced'] == count:
                return
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(0.05)
    raise AssertionError(f'did not durably receive {count} events')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('binary', type=Path)
    parser.add_argument('--transport-only', action='store_true')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='polydata-smoke-') as td, Server(('127.0.0.1', 0), Handler) as server:
        root = Path(td)/'data'
        root.mkdir()
        server.lock = threading.Lock()
        server.connections = 0
        threading.Thread(target=server.serve_forever, daemon=True).start()
        env = {**os.environ, 'DATA_DIR': str(root), 'REDIS_ADDR': f'127.0.0.1:{server.server_address[1]}',
               'DISK_RESERVE_BYTES': '0', 'WAL_ROTATE_SECONDS': '3600', 'GOMEMLIMIT': '64MiB'}
        processes = []
        try:
            p = subprocess.Popen([str(args.binary.resolve()), 'receive'], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(p)
            wait_status(root, 1000, p)
            p.kill()
            p.wait(timeout=5)
            assert list(root.glob('spool/raw/*.open'))
            p = subprocess.Popen([str(args.binary.resolve()), 'receive'], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(p)
            wait_status(root, 10, p)
            p.send_signal(signal.SIGTERM)
            assert p.wait(timeout=10) == 0
            actual, controls, corrupt = [], [], 0
            for path in sorted(root.glob('spool/raw/*')):
                try:
                    for seq, ns, b in frames(path):
                        if json.loads(b).get('_polydata_control'):
                            controls.append(json.loads(b))
                        else:
                            actual.append(b)
                except WalError:
                    corrupt += 1
            assert actual == [payload(i) for i in range(1010)]
            assert corrupt == 1 and server.connections >= 3
            assert any(x['reason'] == 'subscriber_disconnected' for x in controls)
            assert any(x['reason'] == 'process_start' and x['details']['previous_status'] for x in controls)
            if not args.transport_only:
                import pyarrow.parquet as pq
                from polydata.worker import run
                from polydata.storage import LocalMirror
                mirror = Path(td)/'remote'
                run(root, LocalMirror(mirror), once=True)
                rows = []
                for path in sorted(mirror.glob('raw/**/*.parquet')):
                    rows.extend(pq.ParquetFile(path).read().to_pylist())
                assert [r['payload'] for r in rows if r['record_type'] == 'pmxt_event'] == actual
                assert list(mirror.glob('health/collector_gaps/**/*.parquet'))
                db = connect(root)
                assert db.execute('SELECT COUNT(*) FROM artifacts WHERE verified IS NULL').fetchone()[0] == 0
                for row in db.execute('SELECT * FROM artifacts'):
                    assert digest(mirror/row['path']) == row['sha256']
                cleanup(db, root, 0, pressure=True)
                assert list(root.glob('spool/raw/*.recovered'))
                db.close()
            print(json.dumps({'test': 'tcp_disconnect_sigkill_recovery', 'payloads_verified': 1010,
                              'connections': server.connections, 'corrupt_gzip_salvaged': corrupt,
                              'parquet_and_mirror_tested': not args.transport_only}))
        finally:
            for p in processes:
                if p.poll() is None:
                    p.kill()
                    p.wait(timeout=5)
            server.shutdown()


if __name__ == '__main__':
    main()
