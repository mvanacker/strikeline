/*
  Strikeline demo mode: a browser-side stand-in for server.py, so the page can run as a
  static site with no server, no wallet and no network access. Prices are simulated random
  walks, the desk's pricing and settlement rules are re-implemented from server.py, and each
  paper account is kept in this browser's localStorage. Nothing here touches real markets
  or real funds. It is loaded before the page's own script (see make_demo.py).
*/
(function () {
  'use strict';

  // ---------- constants (mirrors server.py) ----------
  const EDGE = 0.025, MAX_STAKE = 100, SIGMAS = [-1, -0.5, 0, 0.5, 1];
  const EXPIRIES = [15, 30, 60, 120, 180, 300];
  const WIN_PAST = 15 * 60e3, KEEP = WIN_PAST + 2 * 60e3, SAMPLE = 500, PRICE_TOL = 0.005;
  const WITHDRAW_MIN = 1, WITHDRAW_MAX = 1000, WITHDRAW_DAILY = 2000, DEMO_GRANT = 1000;
  const ASSETS = [
    { id: 'BTC', pid: 'BTC-USD', name: 'Bitcoin', dp: 2, vol0: 0.0007, dvol: true, start: 89400 },
    { id: 'ETH', pid: 'ETH-USD', name: 'Ether', dp: 2, vol0: 0.0009, dvol: true, start: 3120 },
    { id: 'XRP', pid: 'XRP-USD', name: 'XRP', dp: 4, vol0: 0.0012, dvol: false, start: 2.35 },
  ];
  const BY_ID = Object.fromEntries(ASSETS.map(a => [a.id, a]));
  const CONFIG = {
    assets: ASSETS.map(a => ({ id: a.id, pid: a.pid, name: a.name, dp: a.dp, dvol: a.dvol })),
    sigmas: SIGMAS, expiries: EXPIRIES, edge: EDGE, deskVol: 'front', winPast: WIN_PAST, keep: KEEP, sample: SAMPLE,
    funding: {
      network: 'Base Sepolia', chainId: 84532, token: '0x036cbd53842c5426634e7929541ec2318f3dcf7e',
      explorer: 'https://sepolia.basescan.org', confirmations: 10, maxStake: MAX_STAKE,
      depositMin: 0.01, min: WITHDRAW_MIN, max: WITHDRAW_MAX, daily: WITHDRAW_DAILY,
    },
  };

  // ---------- pricing ----------
  const erf = x => {
    const s = x < 0 ? -1 : 1, ax = Math.abs(x), t = 1 / (1 + 0.3275911 * ax);
    const poly = ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t;
    return s * (1 - poly * Math.exp(-ax * ax));
  };
  const ncdf = x => 0.5 * (1 + erf(x / Math.SQRT2));
  const askOf = f => Math.min(0.99, Math.max(0.03, f + EDGE));
  const bidOf = f => Math.max(0, f - EDGE);
  function fairP(s, K, dir, ms, v) {
    const T = ms / 60000;
    if (T <= 0) return dir === 'H' ? +(s > K) : +(s < K);
    const c = ncdf((Math.log(s / K) - 0.5 * v * v * T) / (v * Math.sqrt(T)));
    return dir === 'H' ? c : 1 - c;
  }
  const roundTo = (v, dp) => Math.round(v * 10 ** dp) / 10 ** dp;
  const gauss = () => Math.sqrt(-2 * Math.log(1 - Math.random())) * Math.cos(2 * Math.PI * Math.random());

  // ---------- simulated market ----------
  const M = { series: {}, mid: {} };
  function initMarket() {
    const now = Date.now(), n = Math.floor(KEEP / SAMPLE);
    for (const a of ASSETS) {
      const step = a.vol0 * Math.sqrt(SAMPLE / 60000);
      const pts = [];
      let p = a.start;
      for (let i = 0; i < n; i++) {
        pts.unshift([now - i * SAMPLE, p]);
        p /= Math.exp(gauss() * step);
      }
      M.series[a.id] = pts;
      M.mid[a.id] = pts[pts.length - 1][1];
    }
  }
  function tickMarket(now) {
    for (const a of ASSETS) {
      const step = a.vol0 * Math.sqrt(SAMPLE / 60000);
      const p = M.mid[a.id] * Math.exp(gauss() * step);
      M.mid[a.id] = p;
      const arr = M.series[a.id];
      arr.push([now, p]);
      const cut = now - KEEP;
      while (arr.length > 1 && arr[0][0] < cut) arr.shift();
    }
  }
  // the desk's pricing vol per sqrt-minute: drifts a little so the quotes breathe
  const volOf = a => a.vol0 * (1 + 0.12 * Math.sin(Date.now() / 45000 + a.vol0 * 1e4));
  function assetView(a) {
    const v = volOf(a);
    return {
      mid: M.mid[a.id], live: true, rv: a.vol0, front: v, frontLegs: 3,
      dvol: a.dvol ? v * 0.96 : null, ratio: 1.5, ratioN: 50, pick: { v, used: 'front' },
    };
  }

  // ---------- accounts (localStorage) ----------
  const STORE = 'strikeline-demo-accounts', SESSION = 'strikeline-demo-session';
  const load = () => { try { return JSON.parse(localStorage.getItem(STORE)) || {}; } catch (e) { return {}; } };
  const saveAll = all => { try { localStorage.setItem(STORE, JSON.stringify(all)); } catch (e) { /* private mode */ } };
  let ACCTS = load(), current = null;
  try { current = sessionStorage.getItem(SESSION); } catch (e) { current = null; }
  const events = [];   // pending toasts for the open page

  function fakeAddress(name) {
    let h = 2166136261, out = '';
    for (let i = 0; out.length < 40; i++) {
      h = Math.imul(h ^ (name.charCodeAt(i % name.length) + i), 16777619) >>> 0;
      out += h.toString(16).padStart(8, '0');
    }
    return '0x' + out.slice(0, 40);
  }
  function newAccount(name) {
    return {
      name, balance: DEMO_GRANT, positions: [], seq: 1, edgePaid: 0, settlements: [],
      deposits: [{ amount: DEMO_GRANT, t: Date.now() }], withdrawals: [], wdSeq: 1, version: 1,
      addr: fakeAddress(name),
    };
  }
  const acct = () => (current && ACCTS[current]) || null;
  const persist = a => { a.version++; ACCTS[a.name] = a; saveAll(ACCTS); };

  function stats(a) {
    const s = a.settlements;
    return {
      total: s.length, realized: s.reduce((x, r) => x + r.pnl, 0),
      wins: s.filter(r => r.res === 'win').length, settled: s.filter(r => r.res === 'win' || r.res === 'loss').length,
    };
  }
  function funding(a) {
    const day = Date.now() - 86400e3;
    const used = a.withdrawals.filter(w => w.t > day && w.status !== 'cancelled').reduce((x, w) => x + w.amount, 0);
    return {
      depositAddress: a.addr, withdrawable: Math.max(0, a.balance), dailyLeft: Math.max(0, WITHDRAW_DAILY - used),
      deposits: a.deposits.map(d => ({ tx: null, amount: d.amount, t: d.t })),
      withdrawals: a.withdrawals.slice().reverse().slice(0, 20).map(w => ({
        id: w.id, to: w.to, amount: w.amount, status: w.status, tx: null, t: w.t, error: null,
      })),
    };
  }
  const bookOf = a => ({
    balance: a.balance, positions: a.positions, edgePaid: a.edgePaid, stats: stats(a), version: a.version, funding: funding(a),
  });

  // on load, any contract left open from an earlier visit is refunded: the market restarted
  function voidStale(a) {
    if (!a.positions.length) return;
    for (const p of a.positions) {
      a.balance += p.stake;
      a.settlements.push({ id: p.id, asset: p.asset, dir: p.dir, K: p.K, px: null, res: 'void', pnl: 0, vs: p.vs });
    }
    events.push(`${a.positions.length} contract(s) from your last visit were voided and refunded: the demo market restarted`);
    a.positions = [];
    persist(a);
  }

  // ---------- desk logic ----------
  class OrderError extends Error {}
  function quote(a, b) {
    const asset = BY_ID[b.asset];
    if (!asset || !['H', 'L'].includes(b.dir) || !Number.isInteger(b.k) || b.k < 0 || b.k >= SIGMAS.length
        || !Number.isInteger(b.exp) || b.exp < 0 || b.exp >= EXPIRIES.length) throw new OrderError('That order is not valid.');
    const v = volOf(asset), stake = Number(b.stake);
    if (!isFinite(stake) || stake <= 0) throw new OrderError('Enter a stake above zero.');
    if (stake > a.balance + 1e-9) throw new OrderError(`Stake is more than your ${a.balance.toFixed(2)} cash.`);
    const s = M.mid[asset.id], T = EXPIRIES[b.exp] * 1000;
    const K = roundTo(s * Math.exp(SIGMAS[b.k] * v * Math.sqrt(T / 60000)), asset.dp);
    const f = fairP(s, K, b.dir, T, v);
    if (f > 0.95) throw new OrderError('This outcome is nearly certain, so the desk will not quote it.');
    if (f < 0.004) throw new OrderError('This strike is too far away to quote at this expiry.');
    const ask = askOf(f);
    if (stake > MAX_STAKE) throw new OrderError(`The largest stake per contract is ${MAX_STAKE.toLocaleString()} USDC.`);
    return { s, K, T, f, ask, n: stake / ask, stake, v, edge: stake - (stake / ask) * f };
  }
  function mark(pos) {
    const asset = BY_ID[pos.asset], left = pos.exp - Date.now();
    if (left <= 0) return null;
    const f = fairP(M.mid[pos.asset], pos.K, pos.dir, left, volOf(asset));
    return { f, bid: bidOf(f), value: pos.n * bidOf(f) };
  }
  function finish(a, pos, r) {
    a.positions = a.positions.filter(p => p !== pos);
    a.settlements.push({ id: pos.id, asset: pos.asset, dir: pos.dir, K: pos.K, px: r.px, res: r.res, pnl: r.pnl, vs: pos.vs });
  }
  function resolve(a) {
    const now = Date.now();
    let changed = false;
    for (const pos of a.positions.slice()) {
      if (pos.exp > now) continue;
      const arr = M.series[pos.asset];
      let i = arr.length - 1;
      while (i > 0 && arr[i][0] > pos.exp) i--;
      const px = arr[i][1], win = pos.dir === 'H' ? px > pos.K : px < pos.K, pay = win ? pos.n : 0;
      a.balance += pay;
      finish(a, pos, { res: win ? 'win' : 'loss', px, pnl: pay - pos.stake });
      const tag = `#${pos.id} ${pos.asset} ${pos.dir === 'H' ? 'Higher' : 'Lower'}`;
      events.push(win ? `${tag} finished in the money: +${(pay - pos.stake).toFixed(2)}` : `${tag} expired worthless: −${pos.stake.toFixed(2)}`);
      changed = true;
    }
    for (const w of a.withdrawals) {
      if (w.status === 'queued' && Date.now() - w.t > 6000) { w.status = 'sent'; changed = true; }
    }
    if (changed) persist(a);
  }

  // ---------- API ----------
  const json = (obj, status = 200) => new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json' } });
  const fail = (msg, status = 400) => json({ ok: false, error: msg }, status);
  const USERNAME_RE = /^[A-Za-z0-9_.-]{3,32}$/;

  function api(path, method, body, query) {
    if (path === '/api/signup' || path === '/api/login') {
      const username = body.username, pw = body.password;
      if (typeof username !== 'string' || !USERNAME_RE.test(username)) {
        return fail('Usernames are 3 to 32 characters: letters, numbers, dots, dashes and underscores.');
      }
      if (typeof pw !== 'string' || pw.length < 1) return fail('Enter a password. Any value works in the demo.');
      if (!ACCTS[username]) ACCTS[username] = newAccount(username);
      current = username;
      try { sessionStorage.setItem(SESSION, username); } catch (e) { /* ignore */ }
      voidStale(ACCTS[username]);
      saveAll(ACCTS);
      return json({ ok: true, username });
    }
    if (path === '/api/logout') {
      current = null;
      try { sessionStorage.removeItem(SESSION); } catch (e) { /* ignore */ }
      return json({ ok: true });
    }
    const a = acct();
    if (!a) return fail('Log in to continue.', 401);
    if (path === '/api/me') return json({ ok: true, username: a.name });
    if (path === '/api/history') {
      const offset = Math.max(0, parseInt(query.get('offset') || '0', 10) || 0);
      const limit = Math.min(50, Math.max(1, parseInt(query.get('limit') || '10', 10) || 10));
      const all = a.settlements.slice().reverse();
      return json({ ok: true, total: all.length, offset, rows: all.slice(offset, offset + limit) });
    }
    if (path === '/api/buy') {
      let q;
      try { q = quote(a, body); } catch (e) { return fail(e instanceof OrderError ? e.message : 'That order is not valid.'); }
      const seen = parseFloat(body.maxAsk);
      if (!isFinite(seen)) return fail('That order is not valid.');
      if (q.ask > seen + PRICE_TOL) return fail(`The price moved to ${q.ask.toFixed(3)} (you saw ${seen.toFixed(3)}). Check the new quote and buy again.`);
      const now = Date.now();
      const pos = { id: a.seq, asset: body.asset, dir: body.dir, K: q.K, stake: q.stake, n: q.n, ask: q.ask, f: q.f, entry: q.s, open: now, exp: now + q.T, vs: 'front', vol: q.v };
      a.seq++; a.balance -= q.stake; a.edgePaid += q.edge; a.positions.push(pos);
      persist(a);
      return json({ ok: true, position: pos, book: bookOf(a) });
    }
    if (path === '/api/sell') {
      const pos = a.positions.find(p => p.id === body.id);
      if (!pos) return fail('That contract is no longer open.');
      const m = mark(pos);
      if (!m) return fail('No live bid for this contract right now.');
      const seen = parseFloat(body.minBid);
      if (!isFinite(seen)) return fail('That order is not valid.');
      if (m.bid < seen - PRICE_TOL) return fail(`The bid moved to ${m.bid.toFixed(3)} (you saw ${seen.toFixed(3)}). Check it and sell again.`);
      a.balance += m.value;
      a.edgePaid += pos.n * (m.f - m.bid);
      finish(a, pos, { res: 'closed', px: M.mid[pos.asset], pnl: m.value - pos.stake });
      persist(a);
      return json({ ok: true, value: m.value, pnl: m.value - pos.stake, book: bookOf(a) });
    }
    if (path === '/api/withdraw') {
      const to = typeof body.address === 'string' ? body.address.trim() : '';
      if (!/^0x[0-9a-fA-F]{40}$/.test(to)) return fail('Enter the address to send to: 0x followed by 40 letters and numbers.');
      if (/^0x0{40}$/.test(to)) return fail("That's the zero address; anything sent there is lost.");
      const amount = Number(body.amount);
      if (!isFinite(amount) || amount < WITHDRAW_MIN) return fail(`The smallest withdrawal is ${WITHDRAW_MIN.toFixed(2)} USDC.`);
      if (amount > WITHDRAW_MAX) return fail(`The largest single withdrawal is ${WITHDRAW_MAX.toLocaleString()}.00 USDC.`);
      if (amount > funding(a).dailyLeft + 1e-9) return fail(`That would go over the ${WITHDRAW_DAILY.toLocaleString()} USDC limit for 24 hours.`);
      if (amount > a.balance + 1e-9) return fail(`You can withdraw up to your balance of ${Math.max(0, a.balance).toFixed(2)} USDC.`);
      a.balance -= amount;
      a.withdrawals.push({ id: a.wdSeq++, to: to.toLowerCase(), amount, status: 'queued', t: Date.now() });
      persist(a);
      return json({ ok: true, book: bookOf(a) });
    }
    if (path === '/api/withdraw/cancel') {
      const w = a.withdrawals.find(x => x.id === body.id);
      if (!w) return fail('That withdrawal was not found.');
      if (w.status !== 'queued') return fail("This withdrawal is already being sent and can't be cancelled.");
      w.status = 'cancelled'; a.balance += w.amount;
      persist(a);
      return json({ ok: true, book: bookOf(a) });
    }
    return fail('Not available in the demo.', 404);
  }

  // ---------- fetch shim ----------
  const realFetch = window.fetch.bind(window);
  window.fetch = (input, init) => {
    const url = new URL(typeof input === 'string' ? input : input.url, location.href);
    if (!url.pathname.startsWith('/api/') || url.pathname === '/api/stream') return realFetch(input, init);
    let body = {};
    try { body = init && init.body ? JSON.parse(init.body) : {}; } catch (e) { body = {}; }
    return new Promise(resolve => setTimeout(() => resolve(api(url.pathname, (init && init.method) || 'GET', body, url.searchParams)), 120));
  };

  // ---------- EventSource shim: the server-sent price and book stream ----------
  const RealES = window.EventSource;
  class DemoEventSource {
    constructor(url) {
      if (!String(url).includes('/api/stream')) return new RealES(url);
      this.onmessage = null; this.onerror = null; this.readyState = 1;
      this.sentT = {}; this.sentVer = -1; this.first = true;
      this.timer = setInterval(() => this.push(), 500);
      setTimeout(() => this.push(), 30);
    }
    push() {
      const a = acct();
      if (!a) { this.close(); if (this.onerror) this.onerror(new Event('error')); return; }
      const now = Date.now();
      tickMarket(now);
      resolve(a);
      const msg = {
        now, conn: 'live',
        assets: Object.fromEntries(ASSETS.map(x => [x.id, assetView(x)])),
        marks: Object.fromEntries(a.positions.map(p => [p.id, mark(p)])),
      };
      if (this.first) { msg.config = CONFIG; msg.full = true; }
      const points = {};
      for (const x of ASSETS) {
        const since = this.first ? -1 : (this.sentT[x.id] || -1), arr = M.series[x.id];
        points[x.id] = arr.filter(p => p[0] > since).map(p => [p[0], p[1]]);
        this.sentT[x.id] = arr[arr.length - 1][0];
      }
      msg.points = points;
      if (this.sentVer !== a.version) { msg.book = bookOf(a); this.sentVer = a.version; }
      if (events.length) { msg.events = events.splice(0); }
      this.first = false;
      if (this.onmessage) this.onmessage({ data: JSON.stringify(msg) });
    }
    close() { clearInterval(this.timer); this.readyState = 2; }
    addEventListener() { /* unused by the page */ }
  }
  window.EventSource = DemoEventSource;

  initMarket();
  for (const n of Object.keys(ACCTS)) { /* normalise older saves */ ACCTS[n].version = ACCTS[n].version || 1; }
  if (current && !ACCTS[current]) current = null;
  if (current) voidStale(ACCTS[current]);

  // ---------- demo labelling ----------
  document.addEventListener('DOMContentLoaded', () => {
    const chip = document.createElement('div');
    chip.textContent = 'Demo: simulated prices and a paper account stored in your browser. No real money, no server.';
    chip.style.cssText = 'position:fixed;left:50%;bottom:10px;transform:translateX(-50%);z-index:99999;max-width:92vw;'
      + 'padding:6px 14px;border-radius:999px;background:rgba(245,185,93,.12);border:1px solid rgba(245,185,93,.35);'
      + 'color:#f5b95d;font:500 12px system-ui,sans-serif;text-align:center;pointer-events:none;backdrop-filter:blur(6px)';
    document.body.appendChild(chip);
    const auth = document.getElementById('auth');
    if (auth) {
      const note = document.createElement('p');
      note.textContent = 'Demo: pick any username and any password. A new account starts with 1,000 test USDC.';
      note.style.cssText = 'margin:12px 0 0;font:13px system-ui,sans-serif;color:#f5b95d;text-align:center';
      (auth.querySelector('form') || auth).appendChild(note);
    }
  });
})();
