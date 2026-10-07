"""Durable catalog, bounded WAL decoding, and content-verified HF archiving."""
from __future__ import annotations
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import struct
import time
import zlib
from typing import Iterator

HEADER = struct.Struct('>IIQq')
MAX_FRAME = 32 * 1024 * 1024


def fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, ensure_ascii=False, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fsync_dir(path.parent)


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


class WalError(ValueError):
    """The yielded prefix is verified, but the remainder is not trustworthy."""


def frames(path: Path) -> Iterator[tuple[int, int, bytes]]:
    try:
        with gzip.open(path, 'rb') as f:
            if f.read(4) != b'PDW1':
                raise WalError('bad WAL magic')
            while True:
                h = f.read(HEADER.size)
                if not h:
                    return
                if len(h) != HEADER.size:
                    raise WalError('truncated WAL header')
                length, checksum, seq, recv = HEADER.unpack(h)
                if length > MAX_FRAME:
                    raise WalError('WAL frame exceeds size limit')
                payload = f.read(length)
                if len(payload) != length:
                    raise WalError('truncated WAL payload')
                if zlib.crc32(payload, zlib.crc32(h[8:])) != checksum:
                    raise WalError('WAL frame CRC mismatch')
                yield seq, recv, payload
    except (EOFError, gzip.BadGzipFile, zlib.error) as exc:
        raise WalError(f'incomplete or corrupt gzip: {type(exc).__name__}') from exc


def connect(root: Path) -> sqlite3.Connection:
    (root / 'state').mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / 'state/catalog.sqlite', timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.execute('PRAGMA cache_size=-2048')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS artifacts (
      path TEXT PRIMARY KEY, sha256 TEXT NOT NULL, bytes INTEGER NOT NULL,
      created REAL NOT NULL, verified REAL, commit_id TEXT, kind TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS segments (
      path TEXT PRIMARY KEY, artifact TEXT NOT NULL, rows INTEGER NOT NULL,
      recovered INTEGER NOT NULL, error TEXT, created REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS seen (asset TEXT PRIMARY KEY, receive_ns INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS dependencies (parent TEXT NOT NULL, child TEXT NOT NULL,
      PRIMARY KEY(parent,child));
    ''')
    return db


def register(db: sqlite3.Connection, root: Path, path: Path, kind: str) -> None:
    rel = path.relative_to(root / 'out').as_posix()
    sha = digest(path)
    old = db.execute('SELECT sha256 FROM artifacts WHERE path=?', (rel,)).fetchone()
    if old and old[0] != sha:
        raise ValueError(f'immutable artifact collision: {rel}')
    db.execute('INSERT OR IGNORE INTO artifacts VALUES (?,?,?,?,NULL,NULL,?)',
               (rel, sha, path.stat().st_size, time.time(), kind))


class LocalMirror:
    """Offline integration-test sink; never selected implicitly."""
    def __init__(self, path: Path):
        self.path = path

    def upload(self, root: Path, rows: list[sqlite3.Row]) -> str:
        for row in rows:
            target = self.path / row['path']
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() and digest(target) != row['sha256']:
                raise ValueError('remote immutable path collision')
            shutil.copyfile(root / 'out' / row['path'], target)
        return 'offline-mirror'

    def verify(self, row: sqlite3.Row, revision: str) -> bool:
        target = self.path / row['path']
        return target.exists() and target.stat().st_size == row['bytes'] and digest(target) == row['sha256']


class HuggingFace:
    def __init__(self, repo: str, token: str):
        from huggingface_hub import HfApi
        self.api = HfApi(token=token)
        self.repo, self.token = repo, token

    def upload(self, root: Path, rows: list[sqlite3.Row]) -> str:
        from huggingface_hub import CommitOperationAdd
        # Pin the comparison to one commit; parent_commit protects concurrent writes.
        head = self.api.repo_info(self.repo, repo_type='dataset').sha
        found = {r.path: r for r in self.api.get_paths_info(
            self.repo, [r['path'] for r in rows], repo_type='dataset', revision=head)}
        for row in rows:
            remote = found.get(row['path'])
            if remote is not None and not self._matches(row, remote, head):
                raise ValueError('remote immutable path collision; refusing overwrite')
        operations = [CommitOperationAdd(path_in_repo=r['path'], path_or_fileobj=str(root / 'out' / r['path']))
                      for r in rows if r['path'] not in found]
        if not operations:
            return head
        result = self.api.create_commit(self.repo, repo_type='dataset', operations=operations,
                                       commit_message=f'Archive {len(operations)} immutable artifacts',
                                       parent_commit=head, num_threads=1)
        return result.oid

    def _matches(self, row, remote, revision: str) -> bool:
        if getattr(remote, 'size', None) != row['bytes']:
            return False
        lfs = getattr(remote, 'lfs', None)
        sha = (lfs.get('sha256') if isinstance(lfs, dict) else getattr(lfs, 'sha256', None)) if lfs else None
        if sha:
            return sha == row['sha256']
        # Neither a Git blob ID nor a Xet identifier is the file SHA256.
        import httpx
        from huggingface_hub import hf_hub_url
        url = hf_hub_url(self.repo, row['path'], repo_type='dataset', revision=revision)
        h = hashlib.sha256()
        with httpx.Client(follow_redirects=True, timeout=120) as client:
            with client.stream('GET', url, headers={'Authorization': f'Bearer {self.token}'}) as response:
                response.raise_for_status()
                for data in response.iter_bytes(1024 * 1024):
                    h.update(data)
        return h.hexdigest() == row['sha256']

    def verify(self, row, revision: str) -> bool:
        result = self.api.get_paths_info(self.repo, [row['path']], repo_type='dataset', revision=revision)
        return len(result) == 1 and self._matches(row, result[0], revision)


def upload_pending(db, root: Path, sink, limit: int = 48) -> int:
    rows = db.execute('SELECT * FROM artifacts WHERE verified IS NULL ORDER BY created LIMIT ?', (limit,)).fetchall()
    if not rows:
        return 0
    for row in rows:
        if digest(root / 'out' / row['path']) != row['sha256']:
            raise ValueError('local artifact checksum mismatch')
    revision = sink.upload(root, rows)
    verified = 0
    for row in rows:
        if not sink.verify(row, revision):
            raise ValueError(f"remote checksum verification failed: {row['path']}")
        with db:
            db.execute('UPDATE artifacts SET verified=?,commit_id=? WHERE path=?',
                       (time.time(), revision, row['path']))
        verified += 1
    return verified


def cleanup(db, root: Path, retention_seconds: float, pressure: bool = False) -> int:
    """Never evict unverified artifacts. WAL also requires verified companion audits."""
    cutoff = time.time() if pressure else time.time() - retention_seconds
    rows = db.execute('SELECT path FROM artifacts WHERE verified IS NOT NULL AND verified<=?', (cutoff,)).fetchall()
    count = 0
    for row in rows:
        p = root / 'out' / row['path']
        if p.is_file():
            p.unlink()
            count += 1
    segments = db.execute('''SELECT s.path FROM segments s JOIN artifacts a ON s.artifact=a.path
       WHERE s.error IS NULL AND a.verified IS NOT NULL AND a.verified<=?
       AND NOT EXISTS (SELECT 1 FROM dependencies d LEFT JOIN artifacts c ON c.path=d.child
                       WHERE d.parent=s.artifact AND (c.verified IS NULL OR c.verified>?))''', (cutoff, cutoff)).fetchall()
    for row in segments:
        p = root / row[0]
        if p.is_file():
            p.unlink()
    return count
