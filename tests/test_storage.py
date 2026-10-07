import gzip
import json
import struct
import time
import zlib
from types import SimpleNamespace
import pytest
from polydata.storage import (HEADER, MAX_FRAME, WalError, frames, connect, register,
                              digest, atomic_json, cleanup, upload_pending, LocalMirror, HuggingFace)


def make_wal(path, payloads, bad_index=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, 'wb') as f:
        f.write(b'PDW1')
        for i, payload in enumerate(payloads, 1):
            tail = struct.pack('>Qq', i, 1791331200000000000+i)
            crc = zlib.crc32(payload, zlib.crc32(tail))
            f.write(struct.pack('>II', len(payload), crc ^ (1 if i == bad_index else 0)) + tail + payload)
    return path


def artifact(db, root, name='raw/a.parquet', data=b'example'):
    p = root/'out'/name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    with db:
        register(db, root, p, 'raw')
    return p


def test_wal_exact_bytes(tmp_path):
    data = [b'{"price":"0.1000"}', b'not json', '测试'.encode()]
    got = list(frames(make_wal(tmp_path/'x', data)))
    assert [x[2] for x in got] == data
    assert [x[0] for x in got] == [1, 2, 3]


def test_wal_corrupt_prefix(tmp_path):
    it = frames(make_wal(tmp_path/'x', [b'good', b'bad', b'untrusted'], bad_index=2))
    assert next(it)[2] == b'good'
    with pytest.raises(WalError, match='CRC'):
        next(it)


@pytest.mark.parametrize('data', [b'wrong', b'PDW1short', b'PDW1'+HEADER.pack(MAX_FRAME+1, 0, 1, 1)])
def test_invalid_wal(tmp_path, data):
    path = tmp_path/'x'
    with gzip.open(path, 'wb') as f:
        f.write(data)
    with pytest.raises(WalError):
        list(frames(path))


def test_truncated_gzip(tmp_path):
    path = make_wal(tmp_path/'x', [b'a', b'b'])
    path.write_bytes(path.read_bytes()[:-8])
    with pytest.raises(WalError):
        list(frames(path))


def test_failed_upload_never_deletes(tmp_path):
    db = connect(tmp_path)
    path = artifact(db, tmp_path)
    class Failed:
        def upload(self, *args):
            raise OSError('offline')
    with pytest.raises(OSError):
        upload_pending(db, tmp_path, Failed())
    assert cleanup(db, tmp_path, 0, pressure=True) == 0
    assert path.exists()


def test_failed_verification_never_deletes(tmp_path):
    db = connect(tmp_path)
    path = artifact(db, tmp_path)
    class Wrong(LocalMirror):
        def verify(self, *args):
            return False
    with pytest.raises(ValueError, match='remote checksum'):
        upload_pending(db, tmp_path, Wrong(tmp_path/'remote'))
    cleanup(db, tmp_path, 0, pressure=True)
    assert path.exists()


def test_retry_after_committed_response_lost(tmp_path):
    db = connect(tmp_path)
    path = artifact(db, tmp_path)
    class LostResponse(LocalMirror):
        def upload(self, root, rows):
            super().upload(root, rows)
            raise ConnectionError('response lost after commit')
    with pytest.raises(ConnectionError):
        upload_pending(db, tmp_path, LostResponse(tmp_path/'remote'))
    assert path.exists()
    assert upload_pending(db, tmp_path, LocalMirror(tmp_path/'remote')) == 1
    cleanup(db, tmp_path, 0, pressure=True)
    assert not path.exists()
    assert (tmp_path/'remote/raw/a.parquet').read_bytes() == b'example'


def test_source_waits_for_companion_audits(tmp_path):
    db = connect(tmp_path)
    raw = artifact(db, tmp_path)
    audit = artifact(db, tmp_path, 'health/check.parquet')
    src = tmp_path/'spool/raw/a.ready'
    src.parent.mkdir(parents=True)
    src.write_bytes(b'source')
    with db:
        db.execute('INSERT INTO segments VALUES (?,?,?,?,?,?)', ('spool/raw/a.ready', 'raw/a.parquet', 1, 0, None, time.time()))
        db.execute('INSERT INTO dependencies VALUES (?,?)', ('raw/a.parquet', 'health/check.parquet'))
        db.execute("UPDATE artifacts SET verified=0 WHERE path='raw/a.parquet'")
    cleanup(db, tmp_path, 0, pressure=True)
    assert src.exists() and audit.exists() and not raw.exists()
    with db:
        db.execute('UPDATE artifacts SET verified=0')
    cleanup(db, tmp_path, 0, pressure=True)
    assert not src.exists()


def test_corrupt_source_never_auto_deleted(tmp_path):
    db = connect(tmp_path)
    artifact(db, tmp_path)
    src = tmp_path/'broken.recovered'
    src.write_bytes(b'forensic evidence')
    with db:
        db.execute('INSERT INTO segments VALUES (?,?,?,?,?,?)', ('broken.recovered', 'raw/a.parquet', 0, 1, 'CRC', 0))
        db.execute('UPDATE artifacts SET verified=0')
    cleanup(db, tmp_path, 0, pressure=True)
    assert src.exists()


def test_local_corruption_prevents_upload(tmp_path):
    db = connect(tmp_path)
    path = artifact(db, tmp_path)
    path.write_bytes(b'changed')
    with pytest.raises(ValueError, match='local artifact'):
        upload_pending(db, tmp_path, LocalMirror(tmp_path/'remote'))
    assert path.exists()


def test_remote_collision_not_overwritten(tmp_path):
    db = connect(tmp_path)
    artifact(db, tmp_path)
    p = tmp_path/'remote/raw/a.parquet'
    p.parent.mkdir(parents=True)
    p.write_bytes(b'other author data')
    with pytest.raises(ValueError, match='collision'):
        upload_pending(db, tmp_path, LocalMirror(tmp_path/'remote'))
    assert p.read_bytes() == b'other author data'


def test_content_hash_not_git_oid():
    sink = HuggingFace.__new__(HuggingFace)
    row = {'bytes': 3, 'sha256': 'abc'}
    assert sink._matches(row, SimpleNamespace(size=3, lfs=SimpleNamespace(sha256='abc')), 'sha')
    assert not sink._matches(row, SimpleNamespace(size=3, lfs={'sha256': 'bad'}), 'sha')
    assert not sink._matches(row, SimpleNamespace(size=4, lfs={'sha256': 'abc'}), 'sha')


def test_atomic_json(tmp_path):
    p = tmp_path/'sub/state.json'
    atomic_json(p, {'k': 42})
    assert json.loads(p.read_text()) == {'k': 42}
    assert not p.with_name(p.name+'.tmp').exists()
