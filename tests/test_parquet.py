import json
import pyarrow.parquet as pq
from polydata.storage import connect, upload_pending, LocalMirror, cleanup
from polydata.quality import Auditor
from polydata.worker import convert
from test_storage import make_wal
from smoke import payload


def test_all_four_events_exact_and_idempotent(tmp_path):
    p = tmp_path/'spool/raw/1791331200000000000-session.ready'
    make_wal(p, [payload(i) for i in range(4)])
    db = connect(tmp_path)
    assert convert(db, tmp_path, p, Auditor()) == 4
    assert convert(db, tmp_path, p, Auditor()) == 0
    files = list(tmp_path.glob('out/raw/**/*.parquet'))
    assert len(files) == 1
    rows = pq.ParquetFile(files[0]).read().to_pylist()
    assert [r['payload'] for r in rows] == [payload(i) for i in range(4)]
    assert {r['event_type'] for r in rows} == {'book', 'price_change', 'last_trade_price', 'tick_size_change'}
    manifest = json.loads(next(tmp_path.glob('out/manifests/**/*.json')).read_text())
    assert manifest['rows'] == 4 and manifest['history_complete'] is False


def test_corrupt_frame_is_salvaged_and_flagged(tmp_path):
    p = tmp_path/'spool/raw/1791331200000000000-session.recovered'
    make_wal(p, [payload(i) for i in range(4)], bad_index=3)
    db = connect(tmp_path)
    assert convert(db, tmp_path, p, Auditor()) == 2
    mirror = LocalMirror(tmp_path/'mirror')
    while upload_pending(db, tmp_path, mirror):
        pass
    cleanup(db, tmp_path, 0, pressure=True)
    assert p.exists()
    gaps = next((tmp_path/'mirror').glob('health/collector_gaps/**/*.parquet'))
    assert any(r['reason'] == 'wal' and r['needs_manual_backfill'] for r in pq.ParquetFile(gaps).read().to_pylist())


def test_invalid_json_is_kept(tmp_path):
    p = tmp_path/'spool/raw/1791331200000000000-session.ready'
    make_wal(p, [b'broken json', b'[]', b'{"event_type":[]}'])
    db = connect(tmp_path)
    assert convert(db, tmp_path, p, Auditor()) == 3
    rows = pq.ParquetFile(next(tmp_path.glob('out/raw/**/*.parquet'))).read().to_pylist()
    assert rows[0]['payload'] == b'broken json'
