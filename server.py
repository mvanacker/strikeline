#!/usr/bin/env python3
"""Strikeline paper desk server.

Streams live Coinbase mids and Deribit implied vols, prices binary contracts, keeps each
user's paper account in data/strikeline.db (SQLite) and settles contracts whether or not a
browser is open. Each account has its own Base Sepolia deposit address for test USDC (testnet only). The page at / is a thin client: it signs in, renders what /api/stream
pushes for that user, and posts orders.

Run:  .venv\\Scripts\\python server.py [--vol front|dvol|rv] [--iv-tenor-hours 24]      then open http://127.0.0.1:8765
"""
import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import math
import os
import re
import secrets
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, WSMsgType, web

ROOT = Path(__file__).resolve().parent
DATA = ROOT / 'data'
DB_FILE = DATA / 'strikeline.db'
LEGACY_FILE = DATA / 'state.json'      # single-book file from before accounts; migrated once
LOCK_FILE = DATA / 'state.lock'
HOST, PORT = '127.0.0.1', int(os.environ.get('STRIKELINE_PORT', 8765))

ASSETS = [
    dict(id='BTC', pid='BTC-USD', name='Bitcoin', dp=2, vol0=0.0007, dvol='BTC', opt_currency='BTC', opt_prefix='BTC-'),
    dict(id='ETH', pid='ETH-USD', name='Ether', dp=2, vol0=0.0009, dvol='ETH', opt_currency='ETH', opt_prefix='ETH-'),
    dict(id='XRP', pid='XRP-USD', name='XRP', dp=4, vol0=0.0012, dvol=None, opt_currency='USDC', opt_prefix='XRP_USDC-'),
]
BY_ID = {a['id']: a for a in ASSETS}
BY_PID = {a['pid']: a for a in ASSETS}

COINBASE_API = 'https://api.exchange.coinbase.com'
COINBASE_WS = 'wss://ws-feed.exchange.coinbase.com'
DERIBIT = 'https://www.deribit.com/api/v2/public'

MIN_PER_YEAR = 525600          # crypto trades 24/7, so vols annualize over calendar minutes
WIN_PAST = 15 * 60e3
KEEP = WIN_PAST + 2 * 60e3     # price history kept in memory (ms)
SAMPLE = 500                   # min spacing of stored points (ms)
STALE = 4000                   # no message for this long => that coin's feed is down
SETTLE_TOL = 3000              # settlement price must be at most this old at expiry
VOID_AFTER = 10000             # unresolved this long after expiry => void and refund
IV_STALE = 5 * 60e3            # an implied vol older than this is treated as unavailable
IV_TENOR = 24 * 3600e3         # the desk's constant-maturity IV horizon (set with --iv-tenor-hours)
MIN_LEG_LEFT = 60 * 60e3       # expiries closer than this are left off the term structure; their IV gets erratic
RATIO_SEED = 1.5               # IV/RV ratio assumed before any observation
EDGE = 0.025
START = 0.0                    # balances are test USDC; accounts start empty and fund themselves by deposit
MAX_STAKE = 100.0              # USDC per contract
SIGMAS = [-1, -0.5, 0, 0.5, 1]
EXPIRIES = [15, 30, 60, 120, 180, 300]   # seconds
VOL_SOURCES = ('front', 'dvol', 'rv')
DESK_VOL = 'front'             # the desk's pricing volatility; set with --vol, never by the trader
PRICE_TOL = 0.005              # an order fills only if the server price is within this of what the page showed
WATCHDOG = 15                  # seconds of Coinbase silence before the socket is recycled

CONFIG = dict(
    assets=[dict(id=a['id'], pid=a['pid'], name=a['name'], dp=a['dp'], dvol=bool(a['dvol'])) for a in ASSETS],
    sigmas=SIGMAS, expiries=EXPIRIES, edge=EDGE, deskVol=DESK_VOL, winPast=WIN_PAST, keep=KEEP, sample=SAMPLE,
)

log = logging.getLogger('strikeline')


def now_ms():
    return time.time() * 1000


def ncdf(x):
    return 0.5 * math.erfc(-x / math.sqrt(2))


def ask_of(f):
    return min(0.99, max(0.03, f + EDGE))


def bid_of(f):
    return max(0.0, f - EDGE)


def fair_p(s, K, direction, ms, v):
    T = ms / 60000
    if T <= 0:
        return float(s > K) if direction == 'H' else float(s < K)
    c = ncdf((math.log(s / K) - 0.5 * v * v * T) / (v * math.sqrt(T)))
    return c if direction == 'H' else 1 - c


# ---------------------------------------------------------------- market state

class Market:
    def __init__(self):
        self.series = {a['id']: [] for a in ASSETS}   # [t, p, is_backfill]
        self.series_gen = 0                            # bumped when history is rewritten (backfill)
        self.mid, self.beat = {}, {}
        self.rv = {a['id']: a['vol0'] for a in ASSETS}
        self.rv_real = {a['id']: False for a in ASSETS}
        self.front, self.front_at, self.front_info = {}, {}, {}
        self.dvol, self.dvol_at = {}, {}
        self.conn = 'connecting'

    def add_point(self, aid, t, p):
        arr = self.series[aid]
        if len(arr) >= 2 and t - arr[-2][0] < SAMPLE:
            arr[-1] = [t, p, False]
        else:
            arr.append([t, p, False])
        cut = t - KEEP
        if arr[0][0] < cut:
            i = 0
            while i < len(arr) - 1 and arr[i][0] < cut:
                i += 1
            del arr[:i]

    def live(self, aid):
        return now_ms() - self.beat.get(aid, 0) < STALE and self.mid.get(aid, 0) > 0


M = Market()



# ---------------------------------------------------------------- storage

SESSION_TTL = 30 * 86400            # seconds a login lasts
COOKIE = 'strikeline_session'
USERNAME_RE = re.compile(r'[A-Za-z0-9_.-]{3,32}')
PW_MIN, PW_MAX = 8, 200
SCRYPT = dict(n=2 ** 14, r=8, p=1)
LOGIN_WINDOW, LOGIN_MAX_FAILS = 300, 5   # per IP + username: 5 failures in 5 minutes locks out until they age out


def fresh_book():
    return dict(balance=START, positions=[], seq=1, edgePaid=0.0)    # settled contracts live in the settlements table


class Account:
    """One user's book, cached in memory and written through to SQLite on every change."""

    def __init__(self, uid, username, data):
        self.uid, self.username, self.s, self.version = uid, username, data, 0
        self.unsaved = []        # settlements recorded by finish(), written with the next book save

    def save(self):
        self.version += 1
        DB.save_book(self)

    def book(self):
        s = self.s
        return dict(balance=s['balance'], positions=s['positions'], edgePaid=s['edgePaid'], stats=DB.settlement_stats(self.uid),
                    version=self.version, funding=funding_view(self.uid))


class Database:
    def __init__(self, path):
        self.c = sqlite3.connect(path, isolation_level=None)   # autocommit; each statement is its own transaction
        self.c.execute('PRAGMA journal_mode=WAL')
        self.c.execute('PRAGMA foreign_keys=ON')
        self.c.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL UNIQUE COLLATE NOCASE,
                pw_hash TEXT NOT NULL,
                created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS sessions (
                token_hash TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created REAL NOT NULL,
                expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS books (
                user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS deposit_addresses (
                user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                address TEXT NOT NULL UNIQUE,          -- lowercase; m/44'/60'/0'/0/<user_id> under the deposit seed
                created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS sweeps (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                address TEXT NOT NULL,
                amount_units INTEGER NOT NULL,
                status TEXT NOT NULL,                   -- signed, broadcast, sent, failed
                nonce INTEGER,
                tx_hash TEXT,
                raw_tx TEXT,
                created REAL NOT NULL,
                updated REAL NOT NULL,
                error TEXT);
            CREATE TABLE IF NOT EXISTS deposits (
                tx_hash TEXT NOT NULL,
                log_index INTEGER NOT NULL,
                block INTEGER NOT NULL,
                from_addr TEXT NOT NULL,
                amount_units INTEGER NOT NULL,
                user_id INTEGER REFERENCES users(id),  -- NULL only for rows from the old shared-address design
                credited REAL,
                seen REAL NOT NULL,
                PRIMARY KEY (tx_hash, log_index));
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                to_addr TEXT NOT NULL,
                amount_units INTEGER NOT NULL,
                status TEXT NOT NULL,                   -- queued, signed, broadcast, sent, failed, cancelled, review
                created REAL NOT NULL,
                updated REAL NOT NULL,
                nonce INTEGER,
                tx_hash TEXT,
                raw_tx TEXT,
                error TEXT);
            CREATE TABLE IF NOT EXISTS ledger (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                kind TEXT NOT NULL,                     -- deposit, withdrawal, withdrawal_refund, migration
                amount REAL NOT NULL,
                ref TEXT NOT NULL,
                created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS settlements (
                id INTEGER PRIMARY KEY,                 -- ascending = chronological
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                contract INTEGER NOT NULL,              -- the contract's number within the account (#n)
                asset TEXT NOT NULL,
                dir TEXT NOT NULL,
                strike REAL NOT NULL,
                stake REAL NOT NULL,
                result TEXT NOT NULL,                   -- win, loss, closed (sold back), void
                final REAL,
                pnl REAL NOT NULL,
                vol_source TEXT,
                prob REAL,
                t REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS settlements_by_user ON settlements(user_id, id);
        ''')
        self.c.execute('DELETE FROM sessions WHERE expires < ?', (time.time(),))

    # meta: market-level state such as the IV/RV ratios
    def get_meta(self, key, default=None):
        row = self.c.execute('SELECT value FROM meta WHERE key = ?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        self.c.execute('INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value',
                       (key, json.dumps(value)))

    def del_meta(self, key):
        self.c.execute('DELETE FROM meta WHERE key = ?', (key,))

    # accounts
    def load_accounts(self):
        rows = self.c.execute('SELECT u.id, u.username, b.data FROM users u JOIN books b ON b.user_id = u.id').fetchall()
        return {uid: Account(uid, name, json.loads(data)) for uid, name, data in rows}

    def save_book(self, acct):
        self.c.execute('BEGIN IMMEDIATE')
        try:
            self._write_book(acct, acct.s)
            self.c.execute('COMMIT')
        except BaseException:
            self.c.execute('ROLLBACK')
            raise
        acct.unsaved.clear()

    def _write_book(self, acct, book):
        """Inside a transaction: the book row plus any settlements finish() recorded, so a settled contract
        and the balance it paid out always land together."""
        for h in acct.unsaved:
            self.insert_settlement(acct.uid, h)
        self.c.execute('UPDATE books SET data = ? WHERE user_id = ?', (json.dumps(book), acct.uid))

    def insert_settlement(self, uid, h):
        self.c.execute('INSERT INTO settlements(user_id, contract, asset, dir, strike, stake, result, final, pnl, vol_source, prob, t) '
                       'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                       (uid, h['id'], h['asset'], h['dir'], h['K'], h['stake'], h['res'], h.get('px'), h['pnl'], h.get('vs'),
                        h.get('f'), h.get('t') or 0))

    def settlements_page(self, uid, offset, limit):
        rows = self.c.execute('SELECT contract, asset, dir, strike, final, result, pnl, vol_source FROM settlements '
                              'WHERE user_id = ? ORDER BY id DESC LIMIT ? OFFSET ?', (uid, limit, offset)).fetchall()
        return [dict(id=c, asset=a, dir=d, K=k, px=f, res=r, pnl=p, vs=v) for c, a, d, k, f, r, p, v in rows]

    def settlement_stats(self, uid):
        total, realized, wins, settled = self.c.execute(
            "SELECT COUNT(*), COALESCE(SUM(pnl), 0), COALESCE(SUM(result = 'win'), 0), COALESCE(SUM(result IN ('win', 'loss')), 0) "
            "FROM settlements WHERE user_id = ?", (uid,)).fetchone()
        return dict(total=total, realized=realized, wins=wins, settled=settled)

    def create_user(self, username, pw_hash, book):
        """Returns the new user id, or None if the username is taken."""
        try:
            self.c.execute('BEGIN')
            uid = self.c.execute('INSERT INTO users(username, pw_hash, created) VALUES(?, ?, ?)',
                                 (username, pw_hash, time.time())).lastrowid
            self.c.execute('INSERT INTO books(user_id, data) VALUES(?, ?)', (uid, json.dumps(book)))
            self.c.execute('COMMIT')
            return uid
        except sqlite3.IntegrityError:
            self.c.execute('ROLLBACK')
            return None

    def find_user(self, username):
        return self.c.execute('SELECT id, username, pw_hash FROM users WHERE username = ?', (username,)).fetchone()

    # sessions: only a hash of the token is stored, so a copied database can't be used to log in
    def new_session(self, uid):
        token = secrets.token_urlsafe(32)
        now = time.time()
        self.c.execute('INSERT INTO sessions(token_hash, user_id, created, expires) VALUES(?, ?, ?, ?)',
                       (token_hash(token), uid, now, now + SESSION_TTL))
        return token

    def session_user(self, token):
        row = self.c.execute('SELECT user_id FROM sessions WHERE token_hash = ? AND expires > ?',
                             (token_hash(token), time.time())).fetchone()
        return row[0] if row else None

    def end_session(self, token):
        self.c.execute('DELETE FROM sessions WHERE token_hash = ?', (token_hash(token),))


    # funding
    def deposit_address(self, uid):
        row = self.c.execute('SELECT address FROM deposit_addresses WHERE user_id = ?', (uid,)).fetchone()
        return row[0] if row else None

    def is_internal_address(self, addr):
        return addr == DESK.address.lower() or bool(
            self.c.execute('SELECT 1 FROM deposit_addresses WHERE address = ?', (addr,)).fetchone())

    def net_deposit_units(self, uid):
        dep = self.c.execute('SELECT COALESCE(SUM(amount_units), 0) FROM deposits WHERE user_id = ?', (uid,)).fetchone()[0]
        out = self.c.execute("SELECT COALESCE(SUM(amount_units), 0) FROM withdrawals WHERE user_id = ? "
                             "AND status NOT IN ('failed', 'cancelled')", (uid,)).fetchone()[0]
        return dep - out

    def withdrawn_units_since(self, uid, since):
        return self.c.execute("SELECT COALESCE(SUM(amount_units), 0) FROM withdrawals WHERE user_id = ? AND created > ? "
                              "AND status NOT IN ('failed', 'cancelled')", (uid, since)).fetchone()[0]

    def move_funds(self, acct, delta, kind, record):
        """One transaction: record(cursor) writes the deposit/withdrawal row and returns its ref, then the
        balance and the ledger move together. Either all of it lands or none of it does."""
        self.c.execute('BEGIN IMMEDIATE')
        try:
            ref = record(self.c)
            self._write_book(acct, dict(acct.s, balance=acct.s['balance'] + delta))
            self.c.execute('INSERT INTO ledger(user_id, kind, amount, ref, created) VALUES(?, ?, ?, ?, ?)',
                           (acct.uid, kind, delta, ref, time.time()))
            self.c.execute('COMMIT')
        except BaseException:
            self.c.execute('ROLLBACK')
            raise
        acct.s['balance'] += delta
        acct.unsaved.clear()
        acct.version += 1
        return ref


def token_hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


def hash_password(pw):
    salt = os.urandom(16)
    h = hashlib.scrypt(pw.encode(), salt=salt, maxmem=64 * 1024 * 1024, dklen=32, **SCRYPT)
    b64 = lambda b: base64.b64encode(b).decode()
    return f"scrypt${SCRYPT['n']}${SCRYPT['r']}${SCRYPT['p']}${b64(salt)}${b64(h)}"


def check_password(pw, stored):
    try:
        _, n, r, p, salt, h = stored.split('$')
        got = hashlib.scrypt(pw.encode(), salt=base64.b64decode(salt), n=int(n), r=int(r), p=int(p),
                             maxmem=64 * 1024 * 1024, dklen=32)
        return hmac.compare_digest(got, base64.b64decode(h))
    except (ValueError, TypeError):
        return False


DB = None            # opened in main() once this process holds the data-directory lock
ACCOUNTS = {}        # uid -> Account
META = {}            # ratios, ratiosBasis
_lock_handle = None
_DUMMY_HASH = None   # checked against for unknown usernames so a login takes the same time either way


def acquire_lock():
    """Hold an OS-level lock on data/state.lock for the life of the process, so a second server
    can't load and overwrite the same accounts. The OS drops it if the process dies, so it can't go stale."""
    global _lock_handle
    DATA.mkdir(exist_ok=True)
    f = open(LOCK_FILE, 'a+b')
    try:
        if os.name == 'nt':
            import msvcrt
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        f.close()
        return False
    _lock_handle = f
    return True


def migrate_legacy():
    """The pre-accounts state.json held one book plus the IV/RV ratios. The ratios move to meta;
    the book is parked and handed to the first account that signs up."""
    if not LEGACY_FILE.exists():
        return
    try:
        old = json.loads(LEGACY_FILE.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        log.error('Could not read %s; leaving it in place and not migrating it', LEGACY_FILE.name)
        return
    if old.get('ratios') and not META.get('ratios'):
        META.update(ratios=old['ratios'], ratiosBasis=old.get('ratiosBasis'))
        DB.set_meta('ratios', META['ratios'])
        DB.set_meta('ratiosBasis', META['ratiosBasis'])
    book = {k: old[k] for k in ('balance', 'positions', 'history', 'seq', 'edgePaid') if k in old}
    if book and not ACCOUNTS:
        DB.set_meta('legacy_book', {**fresh_book(), **book})
        log.info('Moved the existing book from %s; the first account to sign up gets it', LEGACY_FILE.name)
    LEGACY_FILE.replace(LEGACY_FILE.with_name('state.migrated.json'))


EVENTS = []          # (seq, uid, message) toasts for that user's open pages
_event_seq = 0


def event(uid, msg):
    global _event_seq
    _event_seq += 1
    EVENTS.append((_event_seq, uid, msg))
    del EVENTS[:-200]
    log.info('[%s] %s', ACCOUNTS[uid].username if uid in ACCOUNTS else uid, msg)


# ---------------------------------------------------------------- volatility

def fresh(values, at, aid):
    return values.get(aid, 0) > 0 and now_ms() - at.get(aid, 0) < IV_STALE


def ratio_of(aid):
    r = META.get('ratios', {}).get(aid)
    return r['r'] if r and r.get('n') else RATIO_SEED


def observe_ratio(aid):
    """Running average of front-IV / realized per coin: plain mean for 50 obs, then EWMA."""
    if not M.rv_real[aid] or not M.front.get(aid):
        return
    obs = M.front[aid] / M.rv[aid]
    e = META.setdefault('ratios', {}).setdefault(aid, dict(r=0.0, n=0))
    e['n'] += 1
    e['r'] += max(1 / e['n'], 0.02) * (obs - e['r'])
    DB.set_meta('ratios', META['ratios'])


def vol_pick(aid, src):
    """Implied choices never drop to raw realized: DVOL -> front IV -> realized × average IV/RV."""
    if src == 'rv':
        return dict(v=M.rv[aid], used='rv') if M.rv_real[aid] else None
    if src == 'dvol' and fresh(M.dvol, M.dvol_at, aid):
        return dict(v=M.dvol[aid], used='dvol')
    if fresh(M.front, M.front_at, aid):
        return dict(v=M.front[aid], used='front')
    if M.rv_real[aid]:
        return dict(v=M.rv[aid] * ratio_of(aid), used='rvx')
    return None


# ---------------------------------------------------------------- orders

class OrderError(Exception):
    pass


def make_quote(acct, asset, direction, k, ei, stake):
    a = BY_ID.get(asset)
    if not a or direction not in ('H', 'L') \
            or not isinstance(k, int) or not 0 <= k < len(SIGMAS) or not isinstance(ei, int) or not 0 <= ei < len(EXPIRIES):
        raise OrderError('That order is not valid.')
    if not M.live(asset):
        raise OrderError(f"No live {a['pid']} price right now. Buying is paused until the feed is back.")
    pick = vol_pick(asset, DESK_VOL)
    if not pick:
        raise OrderError('Still loading volatility from Deribit and recent candles.')
    try:
        stake = float(stake)
    except (TypeError, ValueError):
        stake = float('nan')
    if not math.isfinite(stake) or stake <= 0:
        raise OrderError('Enter a stake above zero.')
    if stake > acct.s['balance'] + 1e-9:
        raise OrderError(f"Stake is more than your {acct.s['balance']:,.2f} cash.")
    s, T = M.mid[asset], EXPIRIES[ei] * 1000
    K = round(s * math.exp(SIGMAS[k] * pick['v'] * math.sqrt(T / 60000)), a['dp'])
    f = fair_p(s, K, direction, T, pick['v'])
    if f > 0.95:
        raise OrderError('This outcome is nearly certain, so the desk will not quote it.')
    if f < 0.004:
        raise OrderError('This strike is too far away to quote at this expiry.')
    ask = ask_of(f)
    n = stake / ask
    if stake > MAX_STAKE:
        raise OrderError(f'The largest stake per contract is {MAX_STAKE:,.0f} USDC.')
    headroom = desk_headroom_units()
    if headroom is None:
        raise OrderError("The desk can't confirm its USDC balance right now, so new contracts are paused.")
    if (n - stake) * UNITS > headroom:
        most = max(0.0, headroom / UNITS / (1 / ask - 1))
        raise OrderError(f"The desk can't cover that payout right now. At this price it can take a stake of up to {most:,.2f} USDC."
                         if most >= 0.01 else "The desk can't take on more contracts right now. Try again later.")
    return dict(s=s, K=K, T=T, f=f, ask=ask, n=n, stake=stake, pick=pick, edge=stake - n * f)


def finish(acct, pos, **r):
    """Close a contract in memory; the settlement row is written with the caller's next save."""
    acct.s['positions'] = [p for p in acct.s['positions'] if p is not pos]
    acct.unsaved.append(dict(id=pos['id'], asset=pos['asset'], dir=pos['dir'], K=pos['K'], stake=pos['stake'],
                             vs=pos.get('vs'), f=pos.get('f'), **r))


def mark(pos):
    aid = pos['asset']
    pick = vol_pick(aid, DESK_VOL)    # the desk marks every contract with its current model
    left = pos['exp'] - now_ms()
    if not (M.live(aid) and pick and left > 0):
        return None
    f = fair_p(M.mid[aid], pos['K'], pos['dir'], left, pick['v'])
    return dict(f=f, bid=bid_of(f), value=pos['n'] * bid_of(f))


def resolve():
    now = now_ms()
    for acct in ACCOUNTS.values():
        changed = False
        for pos in list(acct.s['positions']):
            if pos['exp'] > now:
                continue
            arr = M.series[pos['asset']]
            i = len(arr) - 1
            while i >= 0 and arr[i][0] > pos['exp']:
                i -= 1
            at = arr[i] if i >= 0 else None
            has_after = bool(arr) and arr[-1][0] > pos['exp'] and not arr[-1][2]
            tag = f"#{pos['id']} {pos['asset']} {'Higher' if pos['dir'] == 'H' else 'Lower'}"
            if at and not at[2] and pos['exp'] - at[0] <= SETTLE_TOL and has_after:
                px = at[1]
                win = px > pos['K'] if pos['dir'] == 'H' else px < pos['K']
                pay = pos['n'] if win else 0.0
                acct.s['balance'] += pay
                finish(acct, pos, res='win' if win else 'loss', px=px, pnl=pay - pos['stake'], t=pos['exp'])
                event(acct.uid, f"{tag} finished in the money: +{pay - pos['stake']:,.2f}" if win else f"{tag} expired worthless: −{pos['stake']:,.2f}")
                changed = True
            elif now - pos['exp'] > VOID_AFTER:
                acct.s['balance'] += pos['stake']
                finish(acct, pos, res='void', px=None, pnl=0.0, t=pos['exp'])
                event(acct.uid, f"#{pos['id']} voided: no live price at expiry, stake refunded")
                changed = True
        if changed:
            acct.save()


# ---------------------------------------------------------------- data feeds

async def coinbase_feed(session):
    retry = 0
    while True:
        try:
            M.conn = 'reconnecting' if retry else 'connecting'
            async with session.ws_connect(COINBASE_WS, heartbeat=30) as ws:
                await ws.send_json(dict(type='subscribe', product_ids=[a['pid'] for a in ASSETS], channels=['ticker', 'heartbeat']))
                log.info('Coinbase feed connected')
                retry = 0
                while True:
                    msg = await ws.receive(timeout=WATCHDOG)
                    if msg.type != WSMsgType.TEXT:
                        break
                    on_coinbase(json.loads(msg.data))
        except asyncio.CancelledError:
            raise
        except Exception as e:   # timeouts, DNS, TLS, server closes: all mean reconnect
            log.warning('Coinbase feed dropped: %r', e)
        M.conn = 'reconnecting'
        await asyncio.sleep(min(30, 2 ** retry))
        retry += 1


def on_coinbase(m):
    a = BY_PID.get(m.get('product_id'))
    if not a:
        return
    aid, now = a['id'], now_ms()
    if m.get('type') == 'ticker':
        try:
            bb, ba = float(m.get('best_bid') or 0), float(m.get('best_ask') or 0)
            mid = (bb + ba) / 2 if bb > 0 and ba >= bb else float(m['price'])
        except (TypeError, ValueError, KeyError):
            return
        if mid <= 0:
            return
        M.mid[aid], M.beat[aid], M.conn = mid, now, 'live'
        M.add_point(aid, now, mid)
    elif m.get('type') == 'heartbeat':
        M.beat[aid], M.conn = now, 'live'
        if M.mid.get(aid):
            M.add_point(aid, now, M.mid[aid])


async def get_json(session, url):
    async with session.get(url) as r:
        r.raise_for_status()
        return await r.json(content_type=None)


async def load_rv(session, a):
    c = sorted(await get_json(session, f"{COINBASE_API}/products/{a['pid']}/candles?granularity=60"), key=lambda k: k[0])
    rets = [math.log(c[i][4] / c[i - 1][4]) for i in range(1, len(c)) if c[i][0] - c[i - 1][0] == 60]
    if len(rets) > 60:
        mu = sum(rets) / len(rets)
        sd = math.sqrt(sum((x - mu) ** 2 for x in rets) / (len(rets) - 1))
        M.rv[a['id']], M.rv_real[a['id']] = max(sd, 0.0001), True
    return c


async def backfill(session, a):
    """Seed the chart with recent history. Marked as backfill so settlement never trusts it."""
    pts, now = [], now_ms()
    try:
        for k in await load_rv(session, a):
            t = k[0] * 1000 + 60e3
            if now - KEEP <= t <= now:
                pts.append([t, k[4], True])
    except Exception as e:
        log.warning('%s candles failed: %r', a['id'], e)
    try:
        trades = await get_json(session, f"{COINBASE_API}/products/{a['pid']}/trades?limit=1000")
        after = pts[-1][0] if pts else 0
        for x in sorted(([datetime.fromisoformat(x['time'].replace('Z', '+00:00')).timestamp() * 1000, float(x['price'])] for x in trades)):
            if x[0] <= after:
                continue
            if pts and x[0] - pts[-1][0] < SAMPLE:
                pts[-1] = [x[0], x[1], True]
            else:
                pts.append([x[0], x[1], True])
    except Exception as e:
        log.warning('%s trades failed: %r', a['id'], e)
    live = M.series[a['id']]
    first = live[0][0] if live else math.inf
    M.series[a['id']] = [p for p in pts if p[0] < first] + live
    M.series_gen += 1


async def load_rv_all(session):
    for a in ASSETS:
        try:
            await load_rv(session, a)
        except Exception as e:
            log.warning('%s realized vol refresh failed: %r', a['id'], e)


async def load_dvol_all(session):
    end = int(now_ms())
    for a in ASSETS:
        if not a['dvol']:
            continue
        try:
            d = (await get_json(session, f"{DERIBIT}/get_volatility_index_data?currency={a['dvol']}"
                                         f"&start_timestamp={end - 15 * 60000}&end_timestamp={end}&resolution=60"))['result']['data']
            if d and d[-1][4] > 0:
                M.dvol[a['id']], M.dvol_at[a['id']] = d[-1][4] / 100 / math.sqrt(MIN_PER_YEAR), now_ms()
        except Exception as e:
            log.warning('%s DVOL failed: %r', a['id'], e)


MONTHS = dict(JAN=1, FEB=2, MAR=3, APR=4, MAY=5, JUN=6, JUL=7, AUG=8, SEP=9, OCT=10, NOV=11, DEC=12)


def parse_expiry(code):
    """Deribit expiries settle 08:00 UTC; codes look like 6OCT26."""
    m = re.fullmatch(r'(\d{1,2})([A-Z]{3})(\d{2})', code)
    if not m or m[2] not in MONTHS:
        return None
    return datetime(2000 + int(m[3]), MONTHS[m[2]], int(m[1]), 8, tzinfo=timezone.utc).timestamp() * 1000


def atm_term(rows, prefix):
    """ATM mark IV per expiry, as [(exp_ms, iv_pct)] sorted by expiry. Each expiry's ATM IV is
    interpolated in strike around its own forward (underlying_price), averaging call and put marks."""
    now, by_exp = now_ms(), {}
    for r in rows:
        name = r.get('instrument_name', '')
        if not name.startswith(prefix) or not (r.get('mark_iv') or 0) > 0 or not (r.get('underlying_price') or 0) > 0:
            continue
        parts = name.split('-')
        exp = parse_expiry(parts[1]) if len(parts) == 4 else None
        if exp is None or exp - now <= MIN_LEG_LEFT:
            continue
        by_exp.setdefault(exp, []).append((float(parts[2].replace('d', '.')), r['mark_iv'], r['underlying_price']))
    term = []
    for exp in sorted(by_exp):
        xs = by_exp[exp]
        u = xs[0][2]
        strikes = {}
        for K, iv, _ in xs:
            strikes.setdefault(K, []).append(iv)
        ks = sorted(strikes)
        iv_at = lambda k: sum(strikes[k]) / len(strikes[k])
        lo = max((k for k in ks if k <= u), default=None)
        hi = min((k for k in ks if k >= u), default=None)
        if lo is None or hi is None:
            continue
        term.append((exp, iv_at(lo) if hi == lo else iv_at(lo) + (iv_at(hi) - iv_at(lo)) * (u - lo) / (hi - lo)))
    return term


def constant_maturity(term, tenor_ms):
    """IV at a fixed horizon, linear in total variance (iv² × T) between the expiries either side.
    Outside the listed range it holds the nearest expiry's IV flat. Returns iv_pct and the legs used."""
    if not term:
        return None
    now = now_ms()
    pts = [(exp - now, iv, exp) for exp, iv in term]
    if tenor_ms <= pts[0][0]:
        return dict(iv=pts[0][1], legs=[dict(exp=pts[0][2], iv=pts[0][1])])
    if tenor_ms >= pts[-1][0]:
        return dict(iv=pts[-1][1], legs=[dict(exp=pts[-1][2], iv=pts[-1][1])])
    for (t1, iv1, e1), (t2, iv2, e2) in zip(pts, pts[1:]):
        if t1 <= tenor_ms <= t2:
            w1, w2 = iv1 ** 2 * t1, iv2 ** 2 * t2
            w = w1 + (w2 - w1) * (tenor_ms - t1) / (t2 - t1)
            return dict(iv=math.sqrt(max(w, 0) / tenor_ms), legs=[dict(exp=e1, iv=iv1), dict(exp=e2, iv=iv2)])


async def load_front(session):
    for cur in sorted({a['opt_currency'] for a in ASSETS}):
        try:
            rows = (await get_json(session, f"{DERIBIT}/get_book_summary_by_currency?currency={cur}&kind=option"))['result']
        except Exception as e:
            log.warning('Deribit %s options failed: %r', cur, e)
            continue
        for a in ASSETS:
            if a['opt_currency'] != cur:
                continue
            f = constant_maturity(atm_term(rows, a['opt_prefix']), IV_TENOR)
            if f:
                M.front[a['id']], M.front_at[a['id']], M.front_info[a['id']] = f['iv'] / 100 / math.sqrt(MIN_PER_YEAR), now_ms(), f
                observe_ratio(a['id'])


async def every(seconds, fn, delay=0):
    await asyncio.sleep(delay)
    while True:
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('periodic task failed')
        await asyncio.sleep(seconds)


async def resolver():
    while True:
        try:
            resolve()
        except Exception:
            log.exception('settlement pass failed')
        await asyncio.sleep(0.25)



# ---------------------------------------------------------------- testnet USDC funding (Base Sepolia)
#
# Every account gets its own deposit address, derived from a seed only the server holds. USDC sent
# there (from any wallet, exchange or faucet) is credited 1:1 once confirmed. A sweep then moves it to
# the desk's hot wallet using USDC's transferWithAuthorization (EIP-3009): the deposit address signs
# off-chain and the hot wallet submits and pays the gas, so deposit addresses never need ETH.
# Withdrawals are paid from the hot wallet to whatever address the user enters.

CHAIN_ID = 84532
NETWORK = 'Base Sepolia'
RPC_URL = os.environ.get('STRIKELINE_RPC', 'https://sepolia.base.org')
USDC = '0x036cbd53842c5426634e7929541ec2318f3dcf7e'          # Circle's test USDC on Base Sepolia
USDC_DOMAIN = dict(name='USDC', version='2', chainId=CHAIN_ID, verifyingContract='0x036CbD53842c5426634e7929541eC2318f3dCF7e')
UNITS = 10 ** 6                                               # USDC has 6 decimals
TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
CONFIRMATIONS = 10                 # Base makes a block about every 2 s
LOG_RANGE = 500                    # the public RPC caps eth_getLogs at 500 blocks
TOPIC_BATCH = 100                  # deposit addresses per eth_getLogs call
WITHDRAW_MIN, WITHDRAW_MAX, WITHDRAW_DAILY = 1.0, 1000.0, 2000.0
DEPOSIT_MIN_UNITS = 10_000          # 0.01 USDC; zero-value spam transfers (address poisoning) are common on testnets
SWEEP_MIN_UNITS = 1_000_000         # don't spend gas sweeping less than 1 USDC
SWEEP_VALID = 3600                  # seconds a signed sweep authorization stays usable
DESK_KEY_FILE = DATA / 'desk_key.txt'
DEPOSIT_SEED_FILE = DATA / 'deposit_seed.txt'
EXPLORER = 'https://sepolia.basescan.org'
ADDR_RE = re.compile(r'0x[0-9a-fA-F]{40}')

DESK = None                        # hot wallet (eth_account LocalAccount), loaded in main()
DEPOSIT_SEED = None                # mnemonic the per-user deposit addresses derive from
WAKE = None                        # asyncio.Event that nudges the chain worker
_deposit_keys = {}                 # uid -> LocalAccount, derived on demand for sweeps


class RpcError(Exception):
    pass


def pad32(addr_or_int):
    return f'{addr_or_int:064x}' if isinstance(addr_or_int, int) else addr_or_int.lower()[2:].rjust(64, '0')


def to_hex(b):
    h = b.hex()
    return h if h.startswith('0x') else '0x' + h


def load_desk_wallet():
    """Testnet only: the hot-wallet key lives in an env var or a plaintext file. See TODO.md before real funds."""
    from eth_account import Account
    key = os.environ.get('STRIKELINE_DESK_KEY')
    if not key and DESK_KEY_FILE.exists():
        key = DESK_KEY_FILE.read_text().strip()
    if not key:
        acct = Account.create()
        key = to_hex(acct.key)
        DESK_KEY_FILE.write_text(key + '\n')
        log.warning('Created a new testnet desk wallet %s (key in %s). Fund it with Base Sepolia ETH for gas '
                    'and test USDC for payouts.', acct.address, DESK_KEY_FILE.name)
    return Account.from_key(key)


def load_deposit_seed():
    """Testnet only: plaintext mnemonic. Losing it strands every unswept deposit; see TODO.md."""
    from eth_account import Account
    Account.enable_unaudited_hdwallet_features()
    seed = os.environ.get('STRIKELINE_DEPOSIT_SEED')
    if not seed and DEPOSIT_SEED_FILE.exists():
        seed = DEPOSIT_SEED_FILE.read_text().strip()
    if not seed:
        _, seed = Account.create_with_mnemonic()
        DEPOSIT_SEED_FILE.write_text(seed + '\n')
        log.warning('Created a new deposit-address seed in %s. Back it up: without it, unswept deposits are stranded.',
                    DEPOSIT_SEED_FILE.name)
    return seed


def deposit_key(uid):
    """The deposit address for account uid is m/44'/60'/0'/0/uid under the deposit seed."""
    from eth_account import Account
    if uid not in _deposit_keys:
        _deposit_keys[uid] = Account.from_mnemonic(DEPOSIT_SEED, account_path=f"m/44'/60'/0'/0/{uid}")
    return _deposit_keys[uid]


def ensure_deposit_address(uid):
    if not DB.deposit_address(uid):
        DB.c.execute('INSERT INTO deposit_addresses(user_id, address, created) VALUES(?, ?, ?)',
                     (uid, deposit_key(uid).address.lower(), time.time()))


async def rpc(session, method, params):
    async with session.post(RPC_URL, json=dict(jsonrpc='2.0', id=1, method=method, params=params)) as r:
        j = await r.json(content_type=None)
    if 'error' in j:
        raise RpcError(j['error'].get('message', 'RPC error'))
    return j['result']


async def usdc_balance(session, addr, block='latest'):
    return int(await rpc(session, 'eth_call', [dict(to=USDC, data='0x70a08231' + pad32(addr)), block]), 16)


def funding_view(uid):
    acct = ACCOUNTS[uid]
    deps = DB.c.execute('SELECT tx_hash, amount_units, credited FROM deposits WHERE user_id = ? ORDER BY credited DESC LIMIT 20',
                        (uid,)).fetchall()
    wds = DB.c.execute('SELECT id, to_addr, amount_units, status, tx_hash, created, error FROM withdrawals WHERE user_id = ? '
                       'ORDER BY id DESC LIMIT 20', (uid,)).fetchall()
    return dict(
        depositAddress=DB.deposit_address(uid),
        withdrawable=max(0.0, acct.s['balance']),
        dailyLeft=max(0.0, WITHDRAW_DAILY - DB.withdrawn_units_since(uid, time.time() - 86400) / UNITS),
        deposits=[dict(tx=t, amount=u / UNITS, t=c * 1000) for t, u, c in deps],
        withdrawals=[dict(id=i, to=a, amount=u / UNITS, status=s, tx=t, t=c * 1000, error=e) for i, a, u, s, t, c, e in wds],
    )


# ---- deposits

def credit_deposit(lg, uid_by_addr):
    tx, li = lg['transactionHash'].lower(), int(lg['logIndex'], 16)
    if DB.c.execute('SELECT 1 FROM deposits WHERE tx_hash = ? AND log_index = ?', (tx, li)).fetchone():
        return
    frm, to = '0x' + lg['topics'][1][-40:].lower(), '0x' + lg['topics'][2][-40:].lower()
    units, block = int(lg['data'], 16), int(lg['blockNumber'], 16)
    acct = ACCOUNTS.get(uid_by_addr.get(to))
    if units < DEPOSIT_MIN_UNITS or not acct:
        return

    def record(c):
        c.execute('INSERT INTO deposits(tx_hash, log_index, block, from_addr, amount_units, user_id, credited, seen) '
                  'VALUES(?, ?, ?, ?, ?, ?, ?, ?)', (tx, li, block, frm, units, acct.uid, time.time(), time.time()))
        return f'deposit:{tx}:{li}'
    DB.move_funds(acct, units / UNITS, 'deposit', record)
    event(acct.uid, f'Deposit of {units / UNITS:,.2f} USDC credited')
    if WAKE:
        WAKE.set()                      # sweep it into the hot wallet


async def scan_deposits(session):
    head = int(await rpc(session, 'eth_blockNumber', []), 16)
    safe = head - CONFIRMATIONS
    start = DB.get_meta('scan_block')
    if start is None:                   # first run: only watch from now on
        DB.set_meta('scan_block', safe)
        return
    uid_by_addr = {a: u for u, a in DB.c.execute('SELECT user_id, address FROM deposit_addresses')}
    topics = ['0x' + pad32(a) for a in uid_by_addr]
    while start < safe:
        end = min(start + LOG_RANGE, safe)
        for i in range(0, len(topics), TOPIC_BATCH):
            logs = await rpc(session, 'eth_getLogs', [dict(address=USDC, topics=[TRANSFER_TOPIC, None, topics[i:i + TOPIC_BATCH]],
                                                           fromBlock=hex(start + 1), toBlock=hex(end))])
            for lg in logs:
                if not lg.get('removed'):
                    credit_deposit(lg, uid_by_addr)
        DB.set_meta('scan_block', end)  # deposits are keyed by (tx, log index), so a rescan never double-credits
        start = end
    if DESK_FUNDS['refresh']:
        DESK_FUNDS['refresh'].set()     # re-read desk balances at the newly scanned block


# ---- hot-wallet transactions (withdrawals and sweeps), one at a time so nonces stay simple

async def sign_from_desk(session, data):
    """Sign a call to the USDC contract from the hot wallet. Returns (nonce, tx_hash, raw)."""
    from eth_utils import to_checksum_address
    nonce = int(await rpc(session, 'eth_getTransactionCount', [DESK.address, 'pending']), 16)
    gas = int(await rpc(session, 'eth_estimateGas', [dict(to=USDC, data=data, **{'from': DESK.address})]), 16)
    tip = int(await rpc(session, 'eth_maxPriorityFeePerGas', []), 16)
    base = int((await rpc(session, 'eth_getBlockByNumber', ['latest', False]))['baseFeePerGas'], 16)
    signed = DESK.sign_transaction(dict(type=2, chainId=CHAIN_ID, nonce=nonce, to=to_checksum_address(USDC), value=0,
                                        data=data, gas=gas * 6 // 5, maxPriorityFeePerGas=tip, maxFeePerGas=2 * base + tip))
    return nonce, to_hex(signed.hash), to_hex(signed.raw_transaction)


async def push_tx(session, tx_hash, raw):
    """Broadcast if not mined yet. Returns ('pending' | 'mined' | 'reverted' | 'nonce_used' | 'no_gas' | 'retry', receipt)."""
    receipt = await rpc(session, 'eth_getTransactionReceipt', [tx_hash])
    if receipt is not None:
        return ('mined' if int(receipt['status'], 16) == 1 else 'reverted'), receipt
    try:
        await rpc(session, 'eth_sendRawTransaction', [raw])
        return 'pending', None
    except RpcError as e:
        msg = str(e).lower()
        if 'already known' in msg or 'known transaction' in msg:
            return 'pending', None
        if 'nonce too low' in msg:
            return 'nonce_used', None
        if 'insufficient funds' in msg:
            return 'no_gas', None
        log.warning('Broadcast of %s failed: %s', tx_hash, e)
        return 'retry', None


async def confirmed(session, receipt):
    head = int(await rpc(session, 'eth_blockNumber', []), 16)
    return head - int(receipt['blockNumber'], 16) + 1 >= CONFIRMATIONS


def set_row(table, rid, **fields):
    fields['updated'] = time.time()
    cols = ', '.join(f'{k} = ?' for k in fields)
    DB.c.execute(f'UPDATE {table} SET {cols} WHERE id = ?', (*fields.values(), rid))
    if table == 'withdrawals':
        uid = DB.c.execute('SELECT user_id FROM withdrawals WHERE id = ?', (rid,)).fetchone()[0]
        if uid in ACCOUNTS:
            ACCOUNTS[uid].version += 1  # pages pick up the new status


def refund_withdrawal(wid, status, reason):
    uid, units = DB.c.execute('SELECT user_id, amount_units FROM withdrawals WHERE id = ?', (wid,)).fetchone()
    acct = ACCOUNTS[uid]

    def record(c):
        if c.execute("UPDATE withdrawals SET status = ?, error = ?, updated = ? WHERE id = ? AND status IN ('queued', 'signed', 'broadcast')",
                     (status, reason, time.time(), wid)).rowcount != 1:
            raise RuntimeError('withdrawal already final')
        return f'withdrawal:{wid}'
    DB.move_funds(acct, units / UNITS, 'withdrawal_refund', record)
    return acct, units


async def sign_withdrawal(session, wid, to, units):
    """Returns True once signed. Leaves it queued (and returns False) while the hot wallet is short of USDC."""
    if await usdc_balance(session, DESK.address) < units:
        set_row('withdrawals', wid, error='Waiting for funds to reach the payout wallet')
        return False
    nonce, tx_hash, raw = await sign_from_desk(session, '0xa9059cbb' + pad32(to) + pad32(units))
    # Persist the signed transaction before broadcasting. After a crash the same bytes (same nonce, same
    # hash) are re-sent, so a withdrawal can never be paid twice.
    set_row('withdrawals', wid, status='signed', nonce=nonce, tx_hash=tx_hash, raw_tx=raw, error=None)
    return True


async def advance_withdrawal(session, wid, units, tx_hash, raw):
    state, receipt = await push_tx(session, tx_hash, raw)
    if state == 'pending':
        set_row('withdrawals', wid, status='broadcast', error=None)
    elif state == 'nonce_used':
        # Something else used this nonce. Our transaction can't mine any more, but refunding blindly could
        # double-pay if it mined in the last moment, so a person has to look at it.
        set_row('withdrawals', wid, status='review', error='Held for review')
        log.error('Withdrawal %s: nonce already used and no receipt for %s; needs manual review', wid, tx_hash)
    elif state == 'no_gas':
        set_row('withdrawals', wid, error='Waiting for the desk to be topped up with test ETH for gas')
    elif state == 'retry':
        set_row('withdrawals', wid, error='Retrying')
    elif state == 'reverted':
        acct, _ = refund_withdrawal(wid, 'failed', 'The transfer reverted on chain; the amount was returned')
        event(acct.uid, f'Withdrawal of {units / UNITS:,.2f} USDC failed on chain and was returned to your balance')
    elif await confirmed(session, receipt):
        set_row('withdrawals', wid, status='sent', error=None)
        uid = DB.c.execute('SELECT user_id FROM withdrawals WHERE id = ?', (wid,)).fetchone()[0]
        event(uid, f'Withdrawal of {units / UNITS:,.2f} USDC sent')


def sweep_authorization(uid, units):
    """EIP-3009 transferWithAuthorization calldata: the deposit key authorizes moving `units` to the hot wallet."""
    from eth_account import Account
    from eth_account.messages import encode_typed_data
    key = deposit_key(uid)
    nonce = '0x' + os.urandom(32).hex()
    valid_before = int(time.time()) + SWEEP_VALID
    msg = {'from': key.address, 'to': DESK.address, 'value': units, 'validAfter': 0, 'validBefore': valid_before, 'nonce': nonce}
    typed = dict(primaryType='TransferWithAuthorization', domain=USDC_DOMAIN, message=msg, types={
        'EIP712Domain': [dict(name='name', type='string'), dict(name='version', type='string'),
                         dict(name='chainId', type='uint256'), dict(name='verifyingContract', type='address')],
        'TransferWithAuthorization': [dict(name='from', type='address'), dict(name='to', type='address'),
                                      dict(name='value', type='uint256'), dict(name='validAfter', type='uint256'),
                                      dict(name='validBefore', type='uint256'), dict(name='nonce', type='bytes32')]})
    sig = Account.sign_message(encode_typed_data(full_message=typed), key.key)
    return ('0xe3ee160e' + pad32(key.address) + pad32(DESK.address) + pad32(units) + pad32(0) + pad32(valid_before)
            + nonce[2:] + pad32(sig.v) + pad32(sig.r) + pad32(sig.s))


async def start_sweep(session):
    """Sweep one deposit address that received credited USDC since its last completed sweep."""
    rows = DB.c.execute('''
        SELECT d.user_id, a.address FROM deposits d JOIN deposit_addresses a ON a.user_id = d.user_id
        GROUP BY d.user_id
        HAVING MAX(d.credited) > COALESCE((SELECT MAX(s.created) FROM sweeps s WHERE s.user_id = d.user_id AND s.status = 'sent'), 0)
    ''').fetchall()
    for uid, addr in rows:
        units = await usdc_balance(session, addr)
        if units < SWEEP_MIN_UNITS:
            continue
        nonce, tx_hash, raw = await sign_from_desk(session, sweep_authorization(uid, units))
        now = time.time()                # saved before broadcast, like withdrawals
        sid = DB.c.execute("INSERT INTO sweeps(user_id, address, amount_units, status, nonce, tx_hash, raw_tx, created, updated) "
                           "VALUES(?, ?, ?, 'signed', ?, ?, ?, ?, ?)", (uid, addr, units, nonce, tx_hash, raw, now, now)).lastrowid
        await advance_sweep(session, sid, tx_hash, raw)
        return


async def advance_sweep(session, sid, tx_hash, raw):
    # A sweep can't run twice: its authorization nonce is single-use on the USDC contract. So unlike a
    # withdrawal, any failure is safe to mark failed; the next pass re-reads the balance and sweeps again.
    state, receipt = await push_tx(session, tx_hash, raw)
    if state == 'pending':
        set_row('sweeps', sid, status='broadcast', error=None)
    elif state in ('nonce_used', 'reverted'):
        set_row('sweeps', sid, status='failed', error=state)
    elif state in ('no_gas', 'retry'):
        set_row('sweeps', sid, error=state)
    elif await confirmed(session, receipt):
        set_row('sweeps', sid, status='sent', error=None)
        log.info('Swept %s into the hot wallet (%s)', DB.c.execute('SELECT address FROM sweeps WHERE id = ?', (sid,)).fetchone()[0], tx_hash)


async def process_chain(session):
    row = DB.c.execute("SELECT id, amount_units, tx_hash, raw_tx FROM withdrawals WHERE status IN ('signed', 'broadcast') "
                       "ORDER BY id LIMIT 1").fetchone()
    if row:
        await advance_withdrawal(session, *row)
        return
    row = DB.c.execute("SELECT id, tx_hash, raw_tx FROM sweeps WHERE status IN ('signed', 'broadcast') ORDER BY id LIMIT 1").fetchone()
    if row:
        await advance_sweep(session, *row)
        return
    row = DB.c.execute("SELECT id, to_addr, amount_units FROM withdrawals WHERE status = 'queued' ORDER BY id LIMIT 1").fetchone()
    if row and await sign_withdrawal(session, *row):
        nxt = DB.c.execute("SELECT id, amount_units, tx_hash, raw_tx FROM withdrawals WHERE id = ?", (row[0],)).fetchone()
        await advance_withdrawal(session, *nxt)
        return
    if DESK_FUNDS['eth'] == 0:          # nothing can be swept without gas in the hot wallet
        return
    await start_sweep(session)          # also frees up USDC for a withdrawal that's waiting on it


async def chain_worker(session):
    while True:
        try:
            await process_chain(session)
        except asyncio.CancelledError:
            raise
        except Exception as e:          # RPC down, timeouts: nothing changes state until a call succeeds
            log.warning('Chain pass failed: %r', e)
        try:
            await asyncio.wait_for(WAKE.wait(), 10)
        except asyncio.TimeoutError:
            pass
        WAKE.clear()


async def deposit_watcher(session):
    while True:
        try:
            await scan_deposits(session)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning('Deposit scan failed: %r', e)
        await asyncio.sleep(10)


# ---- desk solvency
# The desk only takes a contract if the USDC it controls (hot wallet plus every deposit address) would
# still cover everything it could owe at once: every user's balance, the full payout of every open
# contract (as if all of them win), and every withdrawal not yet confirmed sent.
# Balances are read at the block the deposit scanner last finished, so every deposit in the reading is
# already credited and a sweep in progress can't be counted in both places.
DESK_FUNDS = dict(usdc=None, eth=None, at=0.0, block=None, refresh=None)
DESK_FUNDS_STALE = 300            # seconds; past this, new contracts pause until the balance is read again
IN_FLIGHT = ('queued', 'signed', 'broadcast', 'review')


def liabilities_units():
    owed = sum(a.s['balance'] for a in ACCOUNTS.values())
    payouts = sum(p['n'] for a in ACCOUNTS.values() for p in a.s['positions'])
    inflight = DB.c.execute(f"SELECT COALESCE(SUM(amount_units), 0) FROM withdrawals WHERE status IN {IN_FLIGHT}").fetchone()[0]
    return int(math.ceil((owed + payouts) * UNITS)) + inflight


def desk_headroom_units():
    """USDC the desk could still pay out beyond what it already owes, or None if its balance is unknown."""
    if DESK_FUNDS['usdc'] is None or time.time() - DESK_FUNDS['at'] > DESK_FUNDS_STALE:
        return None
    return DESK_FUNDS['usdc'] - liabilities_units()


async def desk_balance_loop(session):
    last = None
    while True:
        try:
            block = DB.get_meta('scan_block')
            if block is not None:
                addrs = [DESK.address] + [a for (a,) in DB.c.execute(
                    'SELECT DISTINCT a.address FROM deposit_addresses a JOIN deposits d ON d.user_id = a.user_id')]
                total = 0
                for a in addrs:
                    total += await usdc_balance(session, a, hex(block))
                eth = int(await rpc(session, 'eth_getBalance', [DESK.address, 'latest']), 16)
                DESK_FUNDS.update(usdc=total, eth=eth, at=time.time(), block=block)
                if (total, eth) != last:
                    log.info('Desk controls %.2f test USDC (hot wallet %s plus deposit addresses) and %.5f ETH for gas; '
                             '%.2f USDC owed to users and open payouts', total / UNITS, DESK.address, eth / 1e18,
                             liabilities_units() / UNITS)
                    last = (total, eth)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning('Could not read desk balances: %r', e)
        try:
            await asyncio.wait_for(DESK_FUNDS['refresh'].wait(), 30)
        except asyncio.TimeoutError:
            pass
        DESK_FUNDS['refresh'].clear()


def migrate_history_to_table():
    """Once: settled contracts used to be a list (newest first, capped at 100) inside each book."""
    for acct in ACCOUNTS.values():
        hist = acct.s.pop('history', None)
        if hist is None:
            continue
        DB.c.execute('BEGIN IMMEDIATE')
        try:
            for h in reversed(hist):                    # oldest first, so ids stay chronological
                DB.insert_settlement(acct.uid, h)
            DB.c.execute('UPDATE books SET data = ? WHERE user_id = ?', (json.dumps(acct.s), acct.uid))
            DB.c.execute('COMMIT')
        except BaseException:
            DB.c.execute('ROLLBACK')
            raise
        log.info('[%s] moved %d settled contracts into the settlements table', acct.username, len(hist))


def migrate_to_usdc_only():
    """Once: balances used to start at 10,000 play credits. They become test USDC, so each account keeps
    only what it deposited (less withdrawals) and open play contracts are voided. History stays."""
    if DB.get_meta('usdc_only'):
        return
    for acct in ACCOUNTS.values():
        net = DB.net_deposit_units(acct.uid) / UNITS
        for pos in list(acct.s['positions']):
            finish(acct, pos, res='void', px=None, pnl=0.0, t=now_ms())

        def record(c, uid=acct.uid):
            return 'migration:usdc-only'
        delta = max(0.0, net) - acct.s['balance']
        DB.move_funds(acct, delta, 'migration', record)
        log.info('[%s] play balance replaced by test USDC: %.2f (was %.2f)', acct.username, acct.s['balance'], acct.s['balance'] - delta)
    DB.del_meta('legacy_book')          # an unclaimed pre-accounts book is play credits too
    DB.set_meta('usdc_only', True)


# ---------------------------------------------------------------- HTTP

def asset_view(aid):
    a = BY_ID[aid]
    r = META.get('ratios', {}).get(aid) or {}
    fi = M.front_info.get(aid)
    return dict(
        mid=M.mid.get(aid), live=M.live(aid),
        rv=M.rv[aid] if M.rv_real[aid] else None,
        front=M.front[aid] if fresh(M.front, M.front_at, aid) else None,
        frontLegs=fi['legs'] if fi else None,
        dvol=M.dvol[aid] if a['dvol'] and fresh(M.dvol, M.dvol_at, aid) else None,
        ratio=ratio_of(aid), ratioN=r.get('n', 0),
        pick=vol_pick(aid, DESK_VOL),
    )


def fail(msg, status=400):
    return web.json_response(dict(ok=False, error=msg), status=status)


PUBLIC_API = {'/api/signup', '/api/login', '/api/me'}


@web.middleware
async def guard(request, handler):
    """Same-origin JSON for every POST (a cross-site form can't send that without a CORS preflight,
    which this server never grants), and a valid session for everything under /api/ but sign-in."""
    if request.method == 'POST':
        origin = request.headers.get('Origin')
        if request.content_type != 'application/json' or (origin and origin != f'{request.scheme}://{request.host}'):
            return fail('That request is not allowed.', 403)
    request['acct'] = None
    token = request.cookies.get(COOKIE)
    if token:
        uid = DB.session_user(token)
        request['acct'] = ACCOUNTS.get(uid)
    if request.path.startswith('/api/') and request.path not in PUBLIC_API and not request['acct']:
        return fail('Log in to continue.', 401)
    resp = await handler(request)
    resp.headers.update({
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'Referrer-Policy': 'no-referrer',
        'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline'; "
                                   "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
                                   "font-src https://fonts.gstatic.com; connect-src 'self'; img-src 'self' data:; "
                                   "frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
    })
    return resp


async def body(request):
    try:
        b = await request.json()
        return b if isinstance(b, dict) else {}
    except ValueError:
        return {}


def with_session(resp, uid):
    token = DB.new_session(uid)
    # HttpOnly keeps it from page scripts; Strict keeps other sites from sending it. Not Secure-flagged
    # because the desk is served over plain http on 127.0.0.1; set secure=True if it ever moves behind https.
    resp.set_cookie(COOKIE, token, max_age=SESSION_TTL, httponly=True, samesite='Strict', path='/')
    return resp


_login_fails = {}    # (ip, username) -> [timestamps]


def too_many_fails(key):
    now = time.time()
    recent = [t for t in _login_fails.get(key, []) if now - t < LOGIN_WINDOW]
    _login_fails[key] = recent
    return len(recent) >= LOGIN_MAX_FAILS


async def h_signup(request):
    b = await body(request)
    username, pw = b.get('username'), b.get('password')
    if not isinstance(username, str) or not USERNAME_RE.fullmatch(username):
        return fail('Usernames are 3 to 32 characters: letters, numbers, dots, dashes and underscores.')
    if not isinstance(pw, str) or not PW_MIN <= len(pw) <= PW_MAX:
        return fail(f'Passwords need at least {PW_MIN} characters.')
    pw_hash = await asyncio.to_thread(hash_password, pw)
    legacy = DB.get_meta('legacy_book')
    uid = DB.create_user(username, pw_hash, legacy or fresh_book())
    if uid is None:
        return fail('That username is taken. Pick another one.', 409)
    if legacy:
        DB.del_meta('legacy_book')
        log.info('Gave the migrated book to new account %s', username)
    ACCOUNTS[uid] = Account(uid, username, legacy or fresh_book())
    ensure_deposit_address(uid)
    log.info('New account %s', username)
    return with_session(web.json_response(dict(ok=True, username=username)), uid)


async def h_login(request):
    b = await body(request)
    username, pw = b.get('username'), b.get('password')
    if not isinstance(username, str) or not isinstance(pw, str) or len(pw) > PW_MAX:
        return fail('Wrong username or password.', 401)
    key = (request.remote, username.lower())
    if too_many_fails(key):
        return fail('Too many attempts. Wait a few minutes and try again.', 429)
    row = DB.find_user(username)
    ok = await asyncio.to_thread(check_password, pw, row[2] if row else _DUMMY_HASH)
    if not (row and ok):
        _login_fails.setdefault(key, []).append(time.time())
        return fail('Wrong username or password.', 401)
    _login_fails.pop(key, None)
    return with_session(web.json_response(dict(ok=True, username=row[1])), row[0])


async def h_logout(request):
    token = request.cookies.get(COOKIE)
    if token:
        DB.end_session(token)
    resp = web.json_response(dict(ok=True))
    resp.del_cookie(COOKIE, path='/')
    return resp


async def h_me(request):
    acct = request['acct']
    return web.json_response(dict(ok=True, username=acct.username)) if acct else fail('Log in to continue.', 401)


async def h_stream(request):
    acct, token = request['acct'], request.cookies.get(COOKIE)
    resp = web.StreamResponse(headers={'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache'})
    await resp.prepare(request)
    sent_gen, sent_ver, sent_ev, sent_t, ticks = -1, -1, _event_seq, {}, 0
    try:
        while True:
            ticks += 1
            if ticks % 10 == 0 and DB.session_user(token) != acct.uid:    # logged out or expired: end the stream
                break
            msg = dict(now=now_ms(), conn=M.conn, assets={a['id']: asset_view(a['id']) for a in ASSETS},
                       marks={p['id']: mark(p) for p in acct.s['positions']})
            full = sent_gen != M.series_gen
            if full:
                msg['config'], msg['full'], sent_gen = CONFIG, True, M.series_gen
            pts = {}
            for aid, arr in M.series.items():
                since = -1 if full else sent_t.get(aid, -1)
                new = []
                for p in reversed(arr):
                    if p[0] <= since:
                        break
                    new.append([p[0], p[1]])
                pts[aid] = new[::-1]
                if arr:
                    sent_t[aid] = arr[-1][0]
            msg['points'] = pts
            if sent_ver != acct.version:
                msg['book'], sent_ver = acct.book(), acct.version
            ev = [m for i, uid, m in EVENTS if i > sent_ev and uid == acct.uid]
            sent_ev = _event_seq
            if ev:
                msg['events'] = ev
            await resp.write(f'data: {json.dumps(msg, separators=(",", ":"))}\n\n'.encode())
            await asyncio.sleep(0.5)
    except (ConnectionError, RuntimeError, asyncio.CancelledError):
        pass
    return resp


async def h_buy(request):
    acct = request['acct']
    b = await body(request)
    try:
        q = make_quote(acct, b.get('asset'), b.get('dir'), b.get('k'), b.get('exp'), b.get('stake'))
    except OrderError as e:
        return fail(str(e))
    try:
        seen = float(b.get('maxAsk'))
    except (TypeError, ValueError):
        return fail('That order is not valid.')
    if q['ask'] > seen + PRICE_TOL:
        return fail(f"The price moved to {q['ask']:.3f} (you saw {seen:.3f}). Check the new quote and buy again.")
    st, now = acct.s, now_ms()
    pos = dict(id=st['seq'], asset=b['asset'], dir=b['dir'], K=q['K'], stake=q['stake'], n=q['n'], ask=q['ask'], f=q['f'],
               entry=q['s'], open=now, exp=now + q['T'], vs=q['pick']['used'], vol=q['pick']['v'])
    st['seq'] += 1
    st['balance'] -= q['stake']
    st['edgePaid'] += q['edge']
    st['positions'].append(pos)
    acct.save()
    log.info('[%s] bought #%s %s %s K=%s stake=%.2f ask=%.3f', acct.username, pos['id'], pos['asset'], pos['dir'], pos['K'], pos['stake'], pos['ask'])
    return web.json_response(dict(ok=True, position=pos, book=acct.book()))


async def h_sell(request):
    acct = request['acct']
    b = await body(request)
    pos = next((p for p in acct.s['positions'] if p['id'] == b.get('id')), None)
    if not pos:
        return fail('That contract is no longer open.')
    m = mark(pos)
    if not m:
        return fail('No live bid for this contract right now.')
    try:
        seen = float(b.get('minBid'))
    except (TypeError, ValueError):
        return fail('That order is not valid.')
    if m['bid'] < seen - PRICE_TOL:
        return fail(f"The bid moved to {m['bid']:.3f} (you saw {seen:.3f}). Check it and sell again.")
    acct.s['balance'] += m['value']
    acct.s['edgePaid'] += pos['n'] * (m['f'] - m['bid'])
    finish(acct, pos, res='closed', px=M.mid[pos['asset']], pnl=m['value'] - pos['stake'], t=now_ms())
    acct.save()
    log.info('[%s] sold #%s for %.2f', acct.username, pos['id'], m['value'])
    return web.json_response(dict(ok=True, value=m['value'], pnl=m['value'] - pos['stake'], book=acct.book()))


async def h_withdraw(request):
    from eth_utils import is_checksum_address
    acct = request['acct']
    b = await body(request)
    to = b.get('address').strip() if isinstance(b.get('address'), str) else ''
    if not ADDR_RE.fullmatch(to):
        return fail('Enter the address to send to: 0x followed by 40 letters and numbers.')
    if to != to.lower() and to != '0x' + to[2:].upper() and not is_checksum_address(to):
        return fail("That address looks mistyped: its capital letters don't match its checksum. Copy it again from your wallet.")
    to = to.lower()
    if int(to, 16) == 0:
        return fail("That's the zero address; anything sent there is lost.")
    if DB.is_internal_address(to):
        return fail("That's a Strikeline deposit address. Withdraw to a wallet you control.")
    try:
        amount = float(b.get('amount'))
    except (TypeError, ValueError):
        amount = float('nan')
    if not math.isfinite(amount) or amount < WITHDRAW_MIN:
        return fail(f'The smallest withdrawal is {WITHDRAW_MIN:,.2f} USDC.')
    if amount > WITHDRAW_MAX:
        return fail(f'The largest single withdrawal is {WITHDRAW_MAX:,.2f} USDC.')
    units = int(round(amount * UNITS))
    if DB.withdrawn_units_since(acct.uid, time.time() - 86400) + units > WITHDRAW_DAILY * UNITS:
        return fail(f'That would go over the {WITHDRAW_DAILY:,.0f} USDC limit for 24 hours.')
    if units / UNITS > acct.s['balance'] + 1e-9:
        return fail(f"You can withdraw up to your balance of {max(0, acct.s['balance']):,.2f} USDC.")
    now = time.time()

    def record(c):
        wid = c.execute("INSERT INTO withdrawals(user_id, to_addr, amount_units, status, created, updated) "
                        "VALUES(?, ?, ?, 'queued', ?, ?)", (acct.uid, to, units, now, now)).lastrowid
        return f'withdrawal:{wid}'
    ref = DB.move_funds(acct, -units / UNITS, 'withdrawal', record)
    WAKE.set()
    log.info('[%s] withdrawal %s of %.2f USDC to %s queued', acct.username, ref, units / UNITS, to)
    return web.json_response(dict(ok=True, book=acct.book()))


async def h_history(request):
    acct = request['acct']
    try:
        offset = max(0, int(request.query.get('offset', 0)))
        limit = min(50, max(1, int(request.query.get('limit', 10))))
    except ValueError:
        return fail('That page is not valid.')
    return web.json_response(dict(ok=True, total=DB.settlement_stats(acct.uid)['total'], offset=offset,
                                  rows=DB.settlements_page(acct.uid, offset, limit)))


async def h_withdraw_cancel(request):
    acct = request['acct']
    b = await body(request)
    row = DB.c.execute('SELECT status FROM withdrawals WHERE id = ? AND user_id = ?', (b.get('id'), acct.uid)).fetchone()
    if not row:
        return fail('That withdrawal was not found.')
    if row[0] != 'queued':
        return fail("This withdrawal is already being sent and can't be cancelled.")
    try:
        refund_withdrawal(b['id'], 'cancelled', None)
    except RuntimeError:
        return fail("This withdrawal is already being sent and can't be cancelled.")
    return web.json_response(dict(ok=True, book=acct.book()))


async def h_index(request):
    return web.FileResponse(ROOT / 'static' / 'index.html', headers={'Cache-Control': 'no-store'})


async def on_startup(app):
    s = ClientSession(timeout=ClientTimeout(total=20), headers={'User-Agent': 'strikeline-paper-desk'})
    app['session'] = s

    async def backfill_all():
        await asyncio.gather(*(backfill(s, a) for a in ASSETS))

    app['tasks'] = [asyncio.create_task(c) for c in (
        coinbase_feed(s),
        backfill_all(),
        every(300, lambda: load_rv_all(s), delay=300),
        every(120, lambda: load_front(s), delay=5),    # after backfill so the first IV/RV ratio has a realized vol
        every(60, lambda: load_dvol_all(s)),
        resolver(),
        deposit_watcher(s),
        chain_worker(s),
        desk_balance_loop(s),
    )]


async def on_cleanup(app):
    for t in app['tasks']:
        t.cancel()
    await asyncio.gather(*app['tasks'], return_exceptions=True)
    await app['session'].close()
    DB.c.close()


def main():
    global DESK_VOL, IV_TENOR, DB, ACCOUNTS, _DUMMY_HASH
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    ap = argparse.ArgumentParser(description='Strikeline paper desk server')
    ap.add_argument('--vol', choices=VOL_SOURCES, default=DESK_VOL,
                    help='pricing volatility: front = Deribit constant-maturity ATM IV (default), dvol = Deribit DVOL '
                         '(XRP falls back to front), rv = 5h realized')
    ap.add_argument('--iv-tenor-hours', type=float, default=IV_TENOR / 3600e3,
                    help='horizon of the constant-maturity ATM IV the desk prices with (default 24)')
    args = ap.parse_args()
    if not args.iv_tenor_hours > 0:
        ap.error('--iv-tenor-hours must be above zero')
    DESK_VOL, IV_TENOR = args.vol, args.iv_tenor_hours * 3600e3
    CONFIG.update(deskVol=DESK_VOL, ivTenorHours=args.iv_tenor_hours)
    if not acquire_lock():
        log.error("Another Strikeline server is already using %s. Stop it first; two servers would overwrite each other's accounts.", DATA)
        sys.exit(1)
    DB = Database(DB_FILE)
    ACCOUNTS = DB.load_accounts()
    META.update(ratios=DB.get_meta('ratios', {}), ratiosBasis=DB.get_meta('ratiosBasis'))
    migrate_legacy()
    _DUMMY_HASH = hash_password(secrets.token_urlsafe(16))
    global DESK, WAKE, DEPOSIT_SEED
    DESK, WAKE, DEPOSIT_SEED = load_desk_wallet(), asyncio.Event(), load_deposit_seed()
    for uid in ACCOUNTS:
        ensure_deposit_address(uid)
    DESK_FUNDS['refresh'] = asyncio.Event()
    migrate_history_to_table()      # first, so voids from the USDC migration come after the old history
    migrate_to_usdc_only()
    CONFIG['funding'] = dict(network=NETWORK, chainId=CHAIN_ID, token=USDC, explorer=EXPLORER, confirmations=CONFIRMATIONS,
                             maxStake=MAX_STAKE, depositMin=DEPOSIT_MIN_UNITS / UNITS, min=WITHDRAW_MIN, max=WITHDRAW_MAX,
                             daily=WITHDRAW_DAILY)
    # the fallback IV/RV ratio only means something for the IV definition it was measured against
    basis = f'cm{args.iv_tenor_hours:g}h'
    if META.get('ratiosBasis') != basis:
        if META.get('ratios'):
            log.info('IV definition changed (%s -> %s); starting the IV/RV ratio average over', META.get('ratiosBasis'), basis)
        META.update(ratios={}, ratiosBasis=basis)
        DB.set_meta('ratios', {})
        DB.set_meta('ratiosBasis', basis)
    log.info('%d account(s); desk prices with %s volatility; implied IV is %gh constant-maturity ATM', len(ACCOUNTS), DESK_VOL, args.iv_tenor_hours)
    app = web.Application(middlewares=[guard], client_max_size=64 * 1024)
    app.router.add_get('/', h_index)
    app.router.add_post('/api/signup', h_signup)
    app.router.add_post('/api/login', h_login)
    app.router.add_post('/api/logout', h_logout)
    app.router.add_get('/api/me', h_me)
    app.router.add_get('/api/stream', h_stream)
    app.router.add_get('/api/history', h_history)
    app.router.add_post('/api/buy', h_buy)
    app.router.add_post('/api/sell', h_sell)
    app.router.add_post('/api/withdraw', h_withdraw)
    app.router.add_post('/api/withdraw/cancel', h_withdraw_cancel)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    log.info('Strikeline on http://%s:%s', HOST, PORT)
    web.run_app(app, host=HOST, port=PORT, print=None)


if __name__ == '__main__':
    main()
