from decimal import Decimal
import pytest
from polydata.quality import Auditor, levels, millis, number
from polydata.worker import redact


def book(asset='a', ts='1000', h='H'):
    return dict(event_type='book', asset_id=asset, market='m', timestamp=ts,
                bids=[['0.4', '10']], asks=[['0.6', '20']], hash=h)


def change(asset='a', ts='1001'):
    return dict(event_type='price_change', asset_id=asset, market='m', timestamp=ts,
                price='0.4', size='0', side='BUY', hash='H2', best_bid='0', best_ask='0.6')


def test_level_delete():
    a = Auditor()
    a.record('s', 1, 1, book())
    a.record('s', 2, 2, change())
    assert a.books['a'].bids == {}
    assert a.level_count == 1


def test_bbo_waits_for_next_timestamp():
    a = Auditor()
    a.record('s', 1, 1, book())
    a.record('s', 2, 2, change())
    assert a.counters['bbo_checked'] == 0
    a.record('s', 3, 3, change(ts='1002'))
    assert a.counters['bbo_matched'] == 1


def test_unanchored_not_claimed_verified():
    a = Auditor()
    a.record('s', 1, 1, change())
    assert a.counters['bbo_unchecked_unanchored'] == 1
    assert a.counters['bbo_checked'] == 0


def test_cache_budget_does_not_truncate_book():
    a = Auditor(max_books=1, max_levels=2)
    a.record('s', 1, 1, book('a'))
    a.record('s', 2, 2, book('b'))
    assert set(a.books) == {'b'} and a.level_count == 2
    b = book('c'); b['bids'].append(['0.3', '1'])
    a.record('s', 3, 3, b)
    assert 'c' not in a.books
    assert a.counters['validation_budget_skipped'] == 1


def test_rest_unaligned_does_not_reanchor():
    a = Auditor()
    a.record('s', 1, 1, book())
    reference = book(h='unrelated'); reference['bids'] = [['0.1', '50']]
    a.reference('a', reference)
    assert max(a.books['a'].bids) == Decimal('0.4')
    assert not any(x['reason'] == 'full_book' and x['status'] == 'suspect' for x in a.events)


def test_same_hash_full_book_is_state_only():
    a = Auditor()
    a.record('s', 1, 1, book())
    a.reference('a', book())
    match = [r for r in a.report() if r['reason'] == 'full_book'][0]
    assert match['status'] == 'state_match'
    assert match['history_complete'] is False


def test_same_hash_different_levels_is_suspect():
    a = Auditor()
    a.record('s', 1, 1, book())
    b = book(); b['bids'] = [['0.3', '10']]
    a.reference('a', b)
    assert any(r['status'] == 'suspect' for r in a.report())


def test_reference_budget():
    a = Auditor()
    for i in range(20):
        a.reference(str(i), book(str(i)))
    assert len(a.references) <= 4


def test_out_of_order_invalidates_anchor():
    a = Auditor()
    a.record('s', 1, 1, book())
    a.record('s', 2, 2, book(ts='999'))
    assert 'a' not in a.books
    assert any(r['reason'] == 'exchange_timestamp' for r in a.events)


@pytest.mark.parametrize('x', ['NaN', 'Infinity', '-Infinity', True, None, '1e99999'])
def test_invalid_decimal(x):
    with pytest.raises(ValueError):
        number(x)


def test_duplicate_even_zero_size_rejected():
    with pytest.raises(ValueError):
        levels([['0.1', '0'], ['0.1', '2']])


def test_millis_bounds():
    assert millis('2026-10-07T00:00:00Z') == 1791331200000
    assert millis(str(2**63)) is None
    assert millis(None) is None


def test_invalid_schema_preserved_diagnostic():
    a = Auditor()
    a.record('s', 1, 1, {'event_type': []})
    assert any(r['status'] == 'unknown_event_preserved' for r in a.report())


def test_redundancy_degraded_is_not_known_gap():
    a = Auditor()
    a.upstream('up', 100, {'fields': {'event': 'asset_degraded', 'asset': 'a'}})
    assert not a.gaps
    assert not any(r.get('needs_manual_backfill') for r in a.report())


def test_gap_retained_past_detail_cap():
    a = Auditor()
    for _ in range(600):
        a.emit('noise', 'info')
    a.upstream('up', 1791331200000000000, {'fields': {'event': 'asset_down', 'asset': 'a'}})
    rows = a.report()
    assert any(r.get('gap_id') and r.get('needs_manual_backfill') for r in rows)


def test_reconnect_is_not_history_repair():
    a = Auditor()
    a.upstream('up', 10**15, {'fields': {'event': 'asset_down', 'asset': 'a'}})
    a.upstream('up', 10**15+10**9, {'fields': {'event': 'asset_data_gap', 'asset': 'a'}})
    a.record('s', 1, 10**15+2*10**9, book())
    rows = [r for r in a.report() if r.get('gap_id')]
    assert rows[-1]['snapshot_recovered'] is True
    assert rows[-1]['l2_backfilled'] is False
    assert rows[-1]['needs_manual_backfill'] is True


def test_local_sequence_gap():
    a = Auditor()
    a.record('s', 1, 1, book())
    a.record('s', 3, 2, change())
    assert any(r['reason'] == 'local_sequence' for r in a.report())


def test_token_redaction():
    assert 'hf_secret123' not in redact('failure hf_secret123 request')
