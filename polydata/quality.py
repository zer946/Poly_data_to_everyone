"""Evidence-based, memory-bounded audits; no claim of gap-free history.
Raw recording is never sampled. Validation caches whole books.
"""
from __future__ import annotations
from collections import Counter, OrderedDict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import time
from typing import Any

KINDS = {'book', 'price_change', 'last_trade_price', 'tick_size_change'}

def number(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ValueError('boolean is not a price or quantity')
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError('invalid decimal') from exc
    if not result.is_finite() or abs(result.adjusted()) > 100:
        raise ValueError('non-finite or out-of-range decimal')
    return result

def levels(values: list) -> dict[Decimal, Decimal]:
    result, seen = {}, set()
    for value in values:
        p, s = (value['price'], value['size']) if isinstance(value, dict) else value
        p, s = number(p), number(s)
        if not 0 <= p <= 1 or s < 0 or p in seen:
            raise ValueError('invalid/duplicate price level')
        seen.add(p)
        if s:
            result[p] = s
    return result

def millis(value: Any) -> int | None:
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, str) and not value.isdigit():
            result = int(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() * 1000)
        else:
            result = int(value)
        return result if 0 <= result < 2**63 else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None

@dataclass
class Book:
    bids: dict
    asks: dict
    timestamp: int
    hash: str | None
    born_ns: int
    tick: Decimal | None = None
    pending: tuple | None = None

class Auditor:
    def __init__(self, max_books=64, max_levels=20000, ttl_seconds=300):
        self.max_books, self.max_levels = max_books, max_levels
        self.ttl_ns = ttl_seconds * 1_000_000_000
        self.books = OrderedDict()
        self.level_count = 0
        self.counters = Counter()
        self.events, self.critical, self.gaps = [], {}, {}
        self.references, self.sequence, self.last_pool = {}, {}, {}

    def emit(self, reason, status, asset=None, **details):
        self.counters[f'audit_{reason}_{status}'] += 1
        row = {'time_ns': time.time_ns(), 'reason': reason, 'status': status,
               'asset_id': asset, **details}
        if details.get('needs_manual_backfill') or details.get('gap_id'):
            key = details.get('gap_id') or f"{reason}:{asset}:{details.get('source', '')}:{details.get('session', '')}"
            self.critical[key] = row
        elif len(self.events) < 500:
            self.events.append(row)
        else:
            self.counters['detail_rows_suppressed'] += 1

    def drop(self, asset):
        old = self.books.pop(asset, None)
        if old:
            self.level_count -= len(old.bids) + len(old.asks)
            self.counters['book_cache_evictions'] += 1

    def _bbo(self, asset, book):
        if not book.pending:
            return
        expected_bid, expected_ask = book.pending
        bid, ask = max(book.bids, default=None), min(book.asks, default=None)
        match = ((expected_bid is None or expected_bid == bid or (bid is None and expected_bid == 0)) and
                 (expected_ask is None or expected_ask == ask or (ask is None and expected_ask == 1)))
        self.counters['bbo_checked'] += 1
        self.counters['bbo_matched' if match else 'bbo_suspect'] += 1
        if not match:
            self.emit('bbo', 'suspect', asset, timestamp_ms=book.timestamp,
                      explanation='Possible loss, reordering, or same-ms grouping; not proof of loss')
        book.pending = None

    def _compare(self, asset, reference, origin):
        book, rh = self.books.get(asset), reference.get('hash')
        if not book or not rh or not book.hash or rh != book.hash:
            return False
        bids, asks = levels(reference.get('bids', [])), levels(reference.get('asks', []))
        match = book.bids == bids and book.asks == asks
        self.emit('full_book', 'state_match' if match else 'suspect', asset,
                  origin=origin, alignment='same_upstream_hash', history_complete=False,
                  local_levels=len(book.bids) + len(book.asks), remote_levels=len(bids) + len(asks))
        return True

    def reference(self, asset, reference):
        if not isinstance(reference, dict) or str(reference.get('asset_id')) != asset:
            self.emit('full_book', 'invalid_asset_response', asset, origin='REST')
        elif not reference.get('hash'):
            self.emit('full_book', 'unverifiable_no_hash', asset, origin='REST')
        elif len(reference.get('bids', [])) + len(reference.get('asks', [])) > self.max_levels:
            self.emit('full_book', 'validation_budget_skipped', asset, origin='REST')
        elif not self._compare(asset, reference, 'REST'):
            if len(self.references) >= 4:
                old = next(iter(self.references))
                self.references.pop(old)
                self.emit('full_book', 'inconclusive', old, explanation='Reference cache budget')
            self.references[asset] = (reference, time.monotonic() + 660)

    def expire_references(self):
        for asset, (_, deadline) in list(self.references.items()):
            if deadline < time.monotonic():
                self.references.pop(asset)
                self.emit('full_book', 'inconclusive', asset, origin='REST',
                          explanation='No comparable anchor; concurrent changes or archive lag')

    def record(self, session, seq, recv_ns, payload):
        previous = self.sequence.get(session)
        if previous is not None and seq != previous + 1:
            self.emit('local_sequence', 'known_gap', previous=previous, current=seq, session=session,
                      needs_manual_backfill=True)
        self.sequence[session] = seq
        if payload.get('_polydata_control') is True:
            self.control(session, recv_ns, payload)
            return
        self.counters['events'] += 1
        asset, kind = payload.get('asset_id'), payload.get('event_type')
        if not isinstance(kind, str) or kind not in KINDS:
            self.emit('schema', 'unknown_event_preserved')
            return
        if not isinstance(asset, str) or not isinstance(payload.get('market'), str) or millis(payload.get('timestamp')) is None:
            self.emit('schema', 'invalid', explanation='missing identity or exchange timestamp')
            return
        ts = millis(payload['timestamp'])
        self.counters[f'event_{kind}'] += 1
        try:
            self._event(asset, kind, ts, recv_ns, payload)
        except (ValueError, TypeError, KeyError, InvalidOperation, OverflowError) as exc:
            self.emit('book_structure', 'invalid', asset, error=str(exc)[:160])
            self.drop(asset)

    def _event(self, asset, kind, ts, recv, event):
        book = self.books.get(asset)
        if book and recv - book.born_ns > self.ttl_ns:
            self.drop(asset)
            book = None
        if book and ts < book.timestamp:
            self.emit('exchange_timestamp', 'out_of_order', asset, previous=book.timestamp, current=ts)
            self.drop(asset)
            book = None
            if kind == 'book':
                return
        if book and ts != book.timestamp:
            self._bbo(asset, book)
        if kind == 'book':
            bids, asks = levels(event['bids']), levels(event['asks'])
            if book and not self._compare(asset, event, 'feed_book'):
                self.counters['snapshot_unaligned'] += 1
            self.drop(asset)
            count = len(bids) + len(asks)
            if count > self.max_levels or self.max_books == 0:
                self.counters['validation_budget_skipped'] += 1
                return
            while self.books and (len(self.books) >= self.max_books or self.level_count + count > self.max_levels):
                self.drop(next(iter(self.books)))
            book = Book(bids, asks, ts, event.get('hash'), recv)
            self.books[asset] = book
            self.level_count += count
            self.counters['snapshot_anchors'] += 1
            gap = self.gaps.get(asset)
            if gap and gap.get('end_ns') is not None and recv >= gap['end_ns']:
                self.emit('collector_gap', 'state_reanchored', asset,
                          **{**gap, 'snapshot_recovered': True, 'revision_ns': recv})
                self.gaps.pop(asset, None)
        elif kind in {'price_change', 'last_trade_price'}:
            p, s = number(event['price']), number(event['size'])
            if not 0 <= p <= 1 or s < 0 or event.get('side') not in {'BUY', 'SELL'}:
                raise ValueError('invalid price, quantity, or side')
            if kind == 'price_change':
                if book is None:
                    self.counters['bbo_unchecked_unanchored'] += 1
                    return
                target = book.bids if event['side'] == 'BUY' else book.asks
                old_len = len(target)
                if s:
                    target[p] = s
                else:
                    target.pop(p, None)
                self.level_count += len(target) - old_len
                book.timestamp, book.hash = ts, event.get('hash')
                refs = tuple(number(event[k]) if event.get(k) is not None else None for k in ('best_bid', 'best_ask'))
                book.pending = refs if any(v is not None for v in refs) else None
                if book.pending is None:
                    self.counters['bbo_unchecked_no_reference'] += 1
                if book.tick and p % book.tick:
                    self.emit('tick_alignment', 'suspect', asset)
        elif kind == 'tick_size_change':
            tick = number(event['new_tick_size'])
            if not 0 < tick <= 1:
                raise ValueError('invalid tick size')
            if book:
                book.tick = tick
        if book and book.bids and book.asks and max(book.bids) >= min(book.asks):
            self.counters['locked_or_crossed_observations'] += 1
        while self.level_count > self.max_levels and self.books:
            self.drop(next(iter(self.books)))
        if asset in self.references and self._compare(asset, self.references[asset][0], 'REST'):
            self.references.pop(asset)

    def control(self, session, recv, event):
        reason, details = event.get('reason'), event.get('details', {})
        previous = details.get('previous_status') or {}
        if reason == 'process_start' and previous:
            self.emit('collector_restart', 'possible_gap', start_ns=previous.get('last_receive_ns'),
                      end_ns=recv, session=session, needs_manual_backfill=True, l2_backfilled=False)
        elif reason in {'subscriber_disconnected', 'disk_pressure'}:
            key = '__redis_subscriber'
            gap = self.gaps.get(key) or {
                'gap_id': hashlib.sha256(f'redis:{session}:{recv}'.encode()).hexdigest()[:32],
                'start_ns': details.get('last_receive_ns') or recv,
                'needs_manual_backfill': True, 'l2_backfilled': False}
            gap = {**gap, 'end_ns': None, 'revision_ns': recv, 'snapshot_recovered': False}
            self.gaps[key] = gap
            self.emit(reason, 'possible_gap', **gap)
        elif reason == 'upstream_exit':
            self.emit(reason, 'possible_gap', start_ns=recv, end_ns=None,
                      needs_manual_backfill=True, l2_backfilled=False, details=details)
        elif reason == 'subscriber_connected':
            gap = self.gaps.pop('__redis_subscriber', None)
            if gap:
                self.emit('subscriber_gap', 'transport_restored', **{**gap, 'end_ns': recv, 'revision_ns': recv})
            else:
                self.emit(reason, 'transport_restored', snapshot_recovered=False, l2_backfilled=False)
        else:
            self.emit(reason or 'control', 'info', details=details)

    def upstream(self, session, recv, log):
        if log.get('_polydata_control') is True:
            self.control(session, recv, log)
            return
        f = log.get('fields', log)
        if not isinstance(f, dict):
            self.emit('upstream_log', 'invalid_preserved')
            return
        event, meta = f.get('event', ''), f.get('meta', {})
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except ValueError:
                meta = {}
        if not isinstance(meta, dict):
            meta = {}
        asset = f.get('asset')
        ts = (millis(log.get('timestamp')) or recv // 1_000_000) * 1_000_000
        if event == 'asset_down':
            gid = hashlib.sha256(f'{session}:{asset}:{ts}'.encode()).hexdigest()[:32]
            gap = {'gap_id': gid, 'start_ns': ts, 'end_ns': None, 'revision_ns': ts,
                   'snapshot_recovered': False, 'l2_backfilled': False, 'needs_manual_backfill': True}
            self.gaps[asset] = gap
            self.emit('collector_gap', 'open', asset, **gap)
        elif event == 'asset_data_gap':
            start = (millis(meta.get('down_since')) or ts // 1_000_000) * 1_000_000
            gap = self.gaps.get(asset) or {'gap_id': hashlib.sha256(f'{session}:{asset}:{start}'.encode()).hexdigest()[:32], 'start_ns': start}
            gap = {**gap, 'end_ns': ts, 'revision_ns': ts, 'snapshot_recovered': False,
                   'l2_backfilled': False, 'needs_manual_backfill': True}
            self.gaps[asset] = gap
            self.emit('collector_gap', 'transport_restored', asset, **gap)
        elif event == 'pool_stats':
            self.last_pool = {**meta, 'observed_ns': recv}
            a, b = meta.get('subscribed_markets'), meta.get('cache_active_markets')
            self.emit('subscription_coverage', 'count_match_only' if a == b and a is not None else 'suspect',
                      subscribed_markets=a, cached_markets=b, exact_set_verified=False,
                      assets_down=f.get('assets_down'), queue_size=meta.get('queue_size'), queue_max=meta.get('queue_max'))
        elif event in {'conn_down', 'asset_degraded', 'asset_healthy', 'conn_up', 'startup_complete', 'queue_pressure'}:
            self.emit(event, 'warning' if event.endswith('down') or event == 'queue_pressure' else 'info', asset, meta=meta)
        elif 'total_dropped' in f:
            self.emit('publisher_drop', 'known_gap', cumulative_dropped=f['total_dropped'],
                      start_ns=ts, end_ns=ts, exact_time_range=False, needs_manual_backfill=True)
        elif log.get('level') in {'ERROR', 'WARN'}:
            self.emit('upstream_log', 'warning', asset, message=str(f.get('message', ''))[:300])

    def report(self):
        self.expire_references()
        rows = self.events + list(self.critical.values())
        rows.append({'time_ns': time.time_ns(), 'reason': 'validation_summary', 'status': 'reported',
                     'counters': dict(self.counters), 'cached_books': len(self.books),
                     'cached_levels': self.level_count, 'validation_scope': 'bounded_whole_books',
                     'history_complete': False})
        self.events, self.critical = [], {}
        self.counters.clear()
        return rows
