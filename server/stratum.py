#!/usr/bin/env python3
"""
SoloBCH Forge - Stratum V1 server (Phase 2 wiring).

Protocol handling is the implementation validated against a real NerdQaxe++.
Jobs come from JobManager: MOCK during IBD, REAL per-miner templates once the
node is synced (each miner's coinbase pays its own CashAddr payout). on_submit
reconstructs the header from the referenced source, validates share difficulty,
and -- for real jobs -- detects a network-target block and submits it via
submitblock, logging share / potential-block / accepted / rejected distinctly.

Standard library only. RPC credentials come from the environment (bch_rpc).
"""

import asyncio
import json
import logging
import os
import re
import secrets
import socket
import time
from collections import deque

import cashaddr
import config
from bch_rpc import BitcoinCashRPC
from block import block_hash
from diagnostics import Diagnostics, DIAG_WORKER
from job_manager import (JobManager, EXTRANONCE1_SIZE, EXTRANONCE2_SIZE,
                         COINBASE_TAG, DIFF1_TARGET, target_from_difficulty)
from history import History
from stats import Stats, stats_path
from status import StatusServer, human_hashrate
from template import BlockTemplate

BIND_HOST = "0.0.0.0"
BIND_PORT = 3334
VERSION_ROLLING_MASK = "1fffe000"
SHARE_DIFFICULTY = 1000
POLL_INTERVAL = 5.0
MAX_LINE_BYTES = 8192
MAX_CONNECTIONS = 64
STATUS_HOST = "0.0.0.0"
STATUS_PORT = 3335
SHARE_LOG_WINDOW = 3660.0            # keep ~1h of share history for hashrate windows
VARDIFF_INTERVAL = 45.0             # min seconds between retargets per worker
VARDIFF_WINDOW = 120.0              # look back this many seconds at share rate
VARDIFF_MIN_WINDOW = 30.0           # need at least this much data before adjusting
VARDIFF_TICK = 15.0                 # how often the vardiff loop scans workers
OFFLINE_TICK = 15.0                # how often the offline-miner monitor scans
OFFLINE_GRACE = 120.0              # a miner must be gone this long before we alert
                                    # (rides out brief internet blips / reconnects)
HISTORY_TICK = 120.0               # how often to append a point to the hashrate chart
AUTH_TIMEOUT = 60.0                # drop a connection that never authorizes (slot hogging)
NTIME_FUTURE_SLACK = 7200          # node rule: block time <= network time + 2h
MAX_SHARES_PER_JOB = 200_000       # per-connection dedupe cap per job (flood guard)
DIFF_CHANGE_GRACE = 15.0           # s a share may still meet the pre-change difficulty
MAX_PAYOUTS_PER_JOB = 4            # distinct payouts remembered per job per connection
# A block candidate whose submitblock fails in transport (node restarting,
# timeout, network blip) is retried in the background after each of these
# delays (~4 min in total) before it is given up as "submit failed". It stays
# saved on disk either way.
SUBMIT_RETRY_DELAYS = (2, 5, 10, 20, 30, 60, 60, 60)
BLOCK_DIR = "blocks"               # candidates saved under <data dir>/blocks/
MAX_WORKER_LEN = 32
MAX_MODEL_LEN = 48
# Worker names and miner model strings come from the unauthenticated Stratum
# port and are rendered on the dashboard, so they are reduced to a safe charset.
_LABEL_BAD = re.compile(r"[^A-Za-z0-9._\-]")
_HEX8 = re.compile(r"[0-9a-fA-F]{8}")   # ntime / nonce: exactly 8 hex digits

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("solobch")


def _sanitize_label(value, max_len):
    """Reduce a miner-supplied label to [A-Za-z0-9._-], bounded in length."""
    if not isinstance(value, str):
        return ""
    return _LABEL_BAD.sub("", value.strip())[:max_len]


def save_block_candidate(record):
    """Write a block candidate, full block hex included, to <data dir>/blocks/
    BEFORE it is submitted, so a block whose submit fails can still be replayed
    by hand (`bitcoin-cli submitblock <block_hex>`). Returns the file path, or
    None when there is no persistent data dir or the write failed."""
    base = os.path.dirname(stats_path())
    if not base:
        return None
    try:
        folder = os.path.join(base, BLOCK_DIR)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"{record['height']}-{record['hash']}.json")
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(record, f, indent=2)
        os.replace(tmp, path)
        return path
    except Exception as e:
        log.error("could not save block candidate %s: %s", record.get("hash"), e)
        return None


def _enable_keepalive(sock):
    """Detect silently-dead peers (miner power-cut, Wi-Fi drop) in about two
    minutes instead of the kernel default of two-plus hours, so a ghost
    connection is reaped even if the miner never reconnects."""
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for name, val in (("TCP_KEEPIDLE", 60), ("TCP_KEEPINTVL", 10),
                          ("TCP_KEEPCNT", 6)):
            opt = getattr(socket, name, None)
            if opt is not None:
                sock.setsockopt(socket.IPPROTO_TCP, opt, val)
    except OSError:
        pass


class StratumServer:
    def __init__(self, jobs: JobManager):
        self.jobs = jobs
        self.clients = set()
        self.stratum_port = BIND_PORT
        self.start_time_wall = time.time()
        # Identity (address.worker) -> {worker, payout, last_seen, alerted}. Tracks
        # miners that have connected so the offline monitor can alert when one that
        # was mining drops off and stays gone. RAM-only; not persisted.
        self.seen_miners = {}
        # Background submitblock retries. Held here so the tasks are not
        # garbage-collected, and outlive the connection that found the block.
        self.submit_tasks = set()

    async def handle(self, reader, writer):
        if len(self.clients) >= MAX_CONNECTIONS:
            writer.close()
            return
        conn = ClientConn(self, reader, writer)
        self.clients.add(conn)
        try:
            await conn.run()
        finally:
            self.clients.discard(conn)
            self.jobs.unregister(conn)

    def note_miner(self, conn):
        """Record/refresh a connected miner so the offline monitor can track it."""
        ru = conn.raw_user
        if not ru:
            return
        e = self.seen_miners.get(ru)
        now = time.monotonic()
        if e is None:
            self.seen_miners[ru] = {"worker": conn.worker,
                                    "payout": conn.payout_address,
                                    "last_seen": now, "alerted": False}
        else:
            e["worker"] = conn.worker
            e["payout"] = conn.payout_address
            e["last_seen"] = now
            e["alerted"] = False

    def scan_offline_miners(self):
        """Return entries for miners that have been gone longer than the grace
        period and haven't been alerted yet (marking them alerted). A miner that
        is currently connected refreshes its timer and clears any prior alert."""
        now = time.monotonic()
        connected = {c.raw_user for c in self.clients if c.authorized and c.raw_user}
        alerts = []
        for ru, e in self.seen_miners.items():
            if ru in connected:
                if e["alerted"]:
                    e["alerted"] = False
                    log.info("[%s] miner back online", e.get("worker") or ru)
                e["last_seen"] = now
                continue
            if not e["alerted"] and (now - e["last_seen"]) >= OFFLINE_GRACE:
                e["alerted"] = True
                gone = now - e["last_seen"]
                log.warning("[%s] miner offline for %.0fs — alerting",
                            e.get("worker") or ru, gone)
                alerts.append({"worker": e.get("worker"), "payout": e.get("payout"),
                               "offline_seconds": gone, "time": time.time()})
        return alerts

    def evict_stale_duplicates(self, keep):
        """Drop the ghost left behind when a miner reconnects.

        On an internet blip the miner's old socket dies silently — the server
        stays blocked reading it until the OS TCP timeout (minutes), so the dead
        connection lingers in ``clients`` and shows up as a duplicate row (0 H/s,
        stale "last share"). When the same miner reconnects and re-authorizes,
        we treat any other authorized connection presenting the *same* identity
        (``address.worker``) *from the same IP* as that ghost and remove it
        immediately, so only the fresh connection remains on the dashboard.

        The IP check matters: two rigs that share a username (no ``.worker``
        suffix set) live at different LAN addresses, and without it they would
        evict each other on every reconnect. TCP keepalive reaps the rare ghost
        the IP check leaves behind.
        """
        if keep.internal:
            return
        for c in list(self.clients):
            if c is keep or not c.authorized or c.internal:
                continue
            same_ip = bool(c.peer and keep.peer and c.peer[0] == keep.peer[0])
            if c.raw_user and c.raw_user == keep.raw_user and same_ip:
                log.info("%s superseded by reconnect — dropping stale duplicate",
                         c._tag())
                self.clients.discard(c)
                self.jobs.unregister(c)
                try:
                    c.writer.close()
                except Exception:
                    pass


class ClientConn:
    def __init__(self, server: StratumServer, reader, writer):
        self.server = server
        self.jobs = server.jobs
        self.reader = reader
        self.writer = writer
        self.peer = writer.get_extra_info("peername")
        sock = writer.get_extra_info("socket")
        if sock is not None:
            _enable_keepalive(sock)
        # Unique among live connections, so two miners paying the same address
        # are never handed identical coinbases (identical, wasted work).
        in_use = {c.extranonce1 for c in server.clients}
        self.extranonce1 = secrets.token_hex(EXTRANONCE1_SIZE)
        while self.extranonce1 in in_use:
            self.extranonce1 = secrets.token_hex(EXTRANONCE1_SIZE)
        self.subscribed = False
        self.authorized = False
        self.internal = False                # loopback self-test client (diagnostics)
        self.last_reject = None              # reason of the most recent rejected share
        self.worker = None
        self.model = None
        self.raw_user = None
        self.payout_address = None
        self.payout_script = None
        self.version_mask = None
        # job_id -> set of (en2, ntime, nonce, version) already submitted. Kept
        # per job so the sets of jobs that leave the manager's history can be
        # dropped, bounding memory over a months-long session.
        self.seen_shares = {}
        # job_id -> [(payout_script, payout_address), ...] this connection was
        # actually sent that job with. A share is rebuilt from these, not from
        # whatever address the connection holds now, so a re-authorize with a
        # different address cannot change the coinbase of work already out.
        self.job_payouts = {}
        self.connected_wall = time.time()
        self.connected_mono = time.monotonic()
        self.accepted = 0
        self.rejected = 0
        self.last_share_wall = None
        self.best_diff = 0.0                 # highest achieved share difficulty seen
        self._shares = deque()               # (monotonic_ts, assigned_share_difficulty)
        self.difficulty = None               # this worker's current share difficulty
        self.target = None                   # matching target (set on authorize)
        # The difficulty before the last change, honoured for DIFF_CHANGE_GRACE
        # seconds so shares already in flight are not rejected by a raise.
        self._prev_difficulty = None
        self._prev_target = None
        self._diff_changed = 0.0
        self._last_vardiff = 0.0            # monotonic ts of last vardiff retarget
        self.suggested_diff = None           # miner-requested starting difficulty

    def _tag(self):
        who = self.worker or f"{self.peer[0]}:{self.peer[1]}"
        return f"[{who}]"

    def _record_accept(self, achieved_diff, assigned_diff):
        # `achieved_diff` is the true difficulty of the hash found (heavy-tailed,
        # used only for the best-diff bragging stat). `assigned_diff` is the
        # threshold the share had to beat — the correct work unit for hashrate,
        # since (share count x threshold x 2**32) is what estimates hashes done.
        self.accepted += 1
        now = time.monotonic()
        self.last_share_wall = time.time()
        self.best_diff = max(self.best_diff, achieved_diff)
        self._shares.append((now, assigned_diff))
        cutoff = now - SHARE_LOG_WINDOW
        while self._shares and self._shares[0][0] < cutoff:
            self._shares.popleft()
        # Persist lifetime totals + all-time best diff (survives reboots).
        # A new all-time record (with a prior baseline) fires the best-share alert.
        if self.jobs.stats.record_accept(achieved_diff, assigned_diff, self.worker):
            entry = {"diff": achieved_diff, "worker": self.worker,
                     "payout": self.payout_address, "time": time.time()}
            asyncio.create_task(self.jobs.notify_best_diff(entry))

    def _record_reject(self, reason=None):
        self.rejected += 1
        if reason:
            self.last_reject = reason
        self.jobs.stats.record_reject()

    def window_hashrate(self, window_s):
        """Average hashrate over the last `window_s` seconds (H/s)."""
        cutoff = time.monotonic() - window_s
        work = sum(d for t, d in self._shares if t >= cutoff)
        return work * (2 ** 32) / window_s if work else 0.0

    def shares_in(self, window_s):
        cutoff = time.monotonic() - window_s
        return sum(1 for t, _ in self._shares if t >= cutoff)

    async def set_difficulty(self, diff):
        """Set this worker's share difficulty and notify the miner."""
        diff = max(1, int(round(diff)))
        if self.difficulty is not None and diff != self.difficulty:
            self._prev_difficulty, self._prev_target = self.difficulty, self.target
            self._diff_changed = time.monotonic()
        self.difficulty = diff
        self.target = target_from_difficulty(diff)
        await self.send({"id": None, "method": "mining.set_difficulty",
                         "params": [diff]})

    def _clamp_diff(self, diff):
        """Bound a difficulty to the configured vardiff floor/ceiling."""
        v = self.jobs.vardiff
        lo = max(1, v.get("min", 1))
        hi = max(lo, v.get("max", 4000000))
        return min(hi, max(lo, diff))

    async def on_suggest_difficulty(self, mid, params):
        """Honor a miner's mining.suggest_difficulty request (AxeOS sends this).

        With vardiff on, the value is clamped to the vardiff floor/ceiling and
        used as this worker's starting difficulty; vardiff re-tunes from there.
        With vardiff off the fixed share difficulty is authoritative (as the
        Settings page promises), so the suggestion is acknowledged but ignored."""
        d = None
        if params:
            try:
                d = float(params[0])
            except (TypeError, ValueError):
                d = None
        if d and d > 0:
            if not self.jobs.vardiff.get("enabled"):
                log.info("%s suggested difficulty %.0f ignored (vardiff off, fixed %s)",
                         self._tag(), d, self.jobs.share_difficulty)
            else:
                self.suggested_diff = self._clamp_diff(d)
                log.info("%s suggested difficulty %.0f -> using %d",
                         self._tag(), d, self.suggested_diff)
                # If the miner suggests after it's already authorized, apply now
                # and hold off vardiff briefly so its choice gets a fair trial.
                if self.authorized:
                    await self.set_difficulty(self.suggested_diff)
                    self._last_vardiff = time.monotonic()
        if mid is not None:
            await self.send_result(mid, True)

    async def maybe_retarget(self):
        """Vardiff: nudge this worker's difficulty toward the target share rate."""
        v = self.jobs.vardiff
        if not v.get("enabled") or not self.authorized or self.difficulty is None:
            return
        now = time.monotonic()
        if now - self._last_vardiff < VARDIFF_INTERVAL:
            return
        window = min(VARDIFF_WINDOW, now - self.connected_mono)
        if window < VARDIFF_MIN_WINDOW:
            return                            # too little data yet to judge
        observed_spm = self.shares_in(window) / (window / 60.0)
        target_spm = max(1, v.get("target_spm", 20))
        lo, hi = v.get("min", 128), v.get("max", 4000000)
        if observed_spm <= 0:
            new_diff = max(lo, self.difficulty / 4)   # silent worker: ease off hard
        else:
            new_diff = self.difficulty * (observed_spm / target_spm)
        new_diff = min(hi, max(lo, new_diff))
        # only retarget on a meaningful (>25%) change to avoid churn
        if abs(new_diff - self.difficulty) / self.difficulty >= 0.25:
            old = self.difficulty
            await self.set_difficulty(new_diff)
            self._last_vardiff = now
            log.info("%s vardiff %.0f -> %d (%.1f shares/min, target %d)",
                     self._tag(), old, self.difficulty, observed_spm, target_spm)

    def stats_snapshot(self, now_wall):
        hr = self.window_hashrate(300)       # 5-minute average for the table
        return {
            "worker": self.worker,
            "ip": self.peer[0] if self.peer else "?",
            "model": self.model,
            "payout": self.payout_address,
            "authorized": self.authorized,
            "subscribed": self.subscribed,
            "difficulty": self.difficulty,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "best_diff": self.best_diff,
            "blocks": self.jobs.stats.blocks_for_worker(self.worker),
            "last_share_ago": (now_wall - self.last_share_wall)
                              if self.last_share_wall else None,
            "connected_ago": now_wall - self.connected_wall,
            "hashrate_hps": hr,
            "hashrate": human_hashrate(hr),
        }

    async def send(self, obj):
        self.writer.write((json.dumps(obj) + "\n").encode())
        await self.writer.drain()

    async def send_result(self, mid, result, error=None):
        await self.send({"id": mid, "result": result, "error": error})

    def _remember_payout(self, job_id):
        """Record that `job_id` went out paying the current payout script."""
        for old in [j for j in self.job_payouts if self.jobs.get_source(j) is None]:
            del self.job_payouts[old]
        payouts = self.job_payouts.setdefault(job_id, [])
        pair = (self.payout_script, self.payout_address)
        if pair not in payouts:
            payouts.append(pair)
            del payouts[:-MAX_PAYOUTS_PER_JOB]

    async def send_current_job(self):
        jm = self.jobs
        src = jm.current_source
        jid = jm.current_job_id
        if src is None:
            return
        if isinstance(src, BlockTemplate):
            if self.payout_script is None:
                return
            c1, c2 = src.coinb1_coinb2(self.payout_script, EXTRANONCE1_SIZE,
                                       EXTRANONCE2_SIZE, COINBASE_TAG)
            params = src.notify_params(jid, c1, c2, jm.current_clean)
            self._remember_payout(jid)
        else:
            params = src.notify_params(jid, jm.current_clean)
        await self.send({"id": None, "method": "mining.notify", "params": params})

    async def run(self):
        log.info("%s connected", self._tag())
        try:
            while True:
                try:
                    if self.authorized:
                        raw = await self.reader.readline()
                    else:
                        # An unauthorized peer gets a bounded time to speak, so
                        # idle sockets cannot hold the connection slots forever.
                        raw = await asyncio.wait_for(self.reader.readline(),
                                                     AUTH_TIMEOUT)
                except asyncio.TimeoutError:
                    log.info("%s no authorize within %.0fs — dropping",
                             self._tag(), AUTH_TIMEOUT)
                    break
                except ValueError:
                    # StreamReader limit exceeded (line beyond 64 KiB).
                    log.warning("%s line too long — dropping", self._tag())
                    break
                if not raw:
                    break
                if len(raw) > MAX_LINE_BYTES:
                    log.warning("%s oversized line dropped", self._tag())
                    continue
                line = raw.decode(errors="replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    log.warning("%s bad JSON: %r", self._tag(), line[:120])
                    continue
                await self.dispatch(msg)
        except (OSError, asyncio.IncompleteReadError):
            pass                      # peer went away (reset, broken pipe, ...)
        finally:
            log.info("%s disconnected", self._tag())
            self.jobs.unregister(self)
            self.writer.close()

    async def dispatch(self, msg):
        if not isinstance(msg, dict):
            log.warning("%s non-object message dropped", self._tag())
            return
        method = msg.get("method")
        mid = msg.get("id")
        params = msg.get("params")
        if not isinstance(params, list):
            params = []
        if method == "mining.submit":
            # Every share is already summarized by the ACCEPT/REJECT line; the
            # raw echo would double the log volume on a long-running Pi.
            log.debug("%s -> %s %s", self._tag(), method, params)
        else:
            log.info("%s -> %s %s", self._tag(), method, params if params else "")

        try:
            if method == "mining.configure":
                await self.on_configure(mid, params)
            elif method == "mining.subscribe":
                await self.on_subscribe(mid, params)
            elif method == "mining.authorize":
                await self.on_authorize(mid, params)
            elif method == "mining.submit":
                await self.on_submit(mid, params)
            elif method == "mining.suggest_difficulty":
                await self.on_suggest_difficulty(mid, params)
            elif method in ("mining.extranonce.subscribe", "mining.suggest_target"):
                if mid is not None:
                    await self.send_result(mid, True)
            else:
                log.warning("%s unknown method %s", self._tag(), method)
                if mid is not None:
                    await self.send_result(mid, None, [20, "Unknown method", None])
        except OSError:
            raise                     # socket died: let run() end the connection
        except Exception as e:
            # A malformed request (wrong types, bad hex, ...) must only fail
            # that one request, never tear the connection down with a traceback.
            log.warning("%s %s rejected (%s: %s)", self._tag(), method,
                        e.__class__.__name__, e)
            if mid is not None:
                await self.send_result(mid, None, [20, "Invalid request", None])

    async def on_configure(self, mid, params):
        result = {}
        extensions = params[0] if params else []
        if isinstance(extensions, list) and "version-rolling" in extensions:
            self.version_mask = VERSION_ROLLING_MASK
            result["version-rolling"] = True
            result["version-rolling.mask"] = VERSION_ROLLING_MASK
        await self.send_result(mid, result)

    async def on_subscribe(self, mid, params):
        self.subscribed = True
        if params and isinstance(params[0], str):
            self.model = _sanitize_label(params[0], MAX_MODEL_LEN) or None
        sub = [["mining.set_difficulty", secrets.token_hex(4)],
               ["mining.notify", secrets.token_hex(4)]]
        await self.send_result(mid, [sub, self.extranonce1, EXTRANONCE2_SIZE])

    async def on_authorize(self, mid, params):
        raw = params[0] if params else ""
        if not isinstance(raw, str):
            await self.send_result(mid, False, [24, "Username must be a string", None])
            return
        raw = raw.strip()[:200]
        addr_part, _, worker_label = raw.partition(".")
        # The worker label is rendered on the dashboard and in alerts: keep it
        # to a safe charset (it arrives over the unauthenticated Stratum port).
        worker = _sanitize_label(worker_label, MAX_WORKER_LEN) or "default"
        try:
            script = cashaddr.to_script(addr_part)
        except cashaddr.CashAddrError as e:
            log.warning("[%s] authorize REJECTED — invalid BCH payout address %r: %s",
                        raw[:80] or "?", addr_part[:80], e)
            await self.send_result(mid, False,
                                   [24, f"Invalid BCH payout address: {e}", None])
            return

        self.payout_script = script
        self.payout_address = addr_part
        self.worker = worker
        self.raw_user = f"{addr_part}.{worker}"     # normalized identity
        # The diagnostics self-test connects from loopback with a fixed worker
        # label; it must not show up as a miner or trigger offline alerts.
        self.internal = (worker == DIAG_WORKER and bool(self.peer)
                         and self.peer[0] in ("127.0.0.1", "::1"))
        self.authorized = True
        await self.send_result(mid, True)
        if self.internal:
            log.info("%s diagnostics self-test client authorized", self._tag())
        else:
            log.info("%s authorized — payout %s", self._tag(), self.payout_address)
            # A reconnect (e.g. after an internet drop) leaves the old, now-dead
            # connection lingering until its socket times out. Now that this
            # fresh one is authorized, evict that stale twin so it can't
            # duplicate the row.
            self.server.evict_stale_duplicates(self)
            self.server.note_miner(self)  # track for the offline monitor
        self.jobs.register(self)
        # Start from the configured share difficulty. With vardiff on, a
        # difficulty the miner suggested (clamped) is used instead and vardiff
        # re-tunes from there; with vardiff off the fixed value is authoritative.
        start = self.jobs.share_difficulty
        if self.suggested_diff and self.jobs.vardiff.get("enabled"):
            start = self.suggested_diff
        await self.set_difficulty(start)
        await self.send_current_job()

    async def on_submit(self, mid, params):
        if not self.authorized:
            self._record_reject()
            await self.send_result(mid, None, [24, "Unauthorized worker", None])
            return
        if len(params) < 5:
            self._record_reject()
            await self.send_result(mid, None, [20, "Malformed submit", None])
            return

        worker, job_id, en2, ntime, nonce = params[:5]
        if not all(isinstance(x, str) for x in (job_id, en2, ntime, nonce)):
            self._record_reject()
            await self.send_result(mid, None, [20, "Malformed submit", None])
            return
        src = self.jobs.get_source(job_id)
        if src is None:
            self._record_reject("Job not found (stale)")
            log.info("%s REJECT stale/unknown job %s", self._tag(), job_id[:16])
            await self.send_result(mid, None, [21, "Job not found (stale)", None])
            return
        if len(en2) != EXTRANONCE2_SIZE * 2:
            self._record_reject()
            await self.send_result(mid, None, [20, "Bad extranonce2 size", None])
            return
        if not (_HEX8.fullmatch(ntime) and _HEX8.fullmatch(nonce)):
            self._record_reject()
            await self.send_result(mid, None, [20, "Bad ntime/nonce", None])
            return
        ntime_int, nonce_int = int(ntime, 16), int(nonce, 16)
        if isinstance(src, BlockTemplate):
            # The node rejects a block whose time is at or below the template's
            # mintime (median of the last 11 blocks) or more than 2h ahead of
            # its clock. Accepting such a share would credit work whose block
            # could never be valid, so enforce the same window here.
            if (ntime_int < src.mintime
                    or ntime_int > int(time.time()) + NTIME_FUTURE_SLACK):
                self._record_reject("ntime out of range")
                log.info("%s REJECT ntime %d outside [%d, now+%ds]", self._tag(),
                         ntime_int, src.mintime, NTIME_FUTURE_SLACK)
                await self.send_result(mid, None, [20, "ntime out of range", None])
                return

        payouts = None
        if isinstance(src, BlockTemplate):
            # Rebuild from the payout(s) this job was actually sent with.
            payouts = self.job_payouts.get(job_id)
            if not payouts:
                self._record_reject("Job not found (stale)")
                log.info("%s REJECT job %s was never sent to this connection",
                         self._tag(), job_id[:16])
                await self.send_result(mid, None, [21, "Job not found (stale)", None])
                return

        # Version rolling (BIP310), rolled bits in the 6th param. The spec
        # combines them as (base & ~mask) | (bits & mask); ESP-Miner firmware
        # sends base ^ rolled instead. The two agree whenever the base version
        # has no bits inside the mask (BCHN's 0x20000000), so both are only
        # tried when they differ.
        base_ver = src.version if isinstance(src.version, int) else int(src.version, 16)
        rolled = params[5] if len(params) > 5 else None
        versions, rolled_bits = [base_ver], 0
        if rolled is not None and self.version_mask:
            mask = int(self.version_mask, 16)
            try:
                rolled_bits = int(rolled, 16) & mask
            except (TypeError, ValueError):
                self._record_reject()
                await self.send_result(mid, None, [20, "Bad version bits", None])
                return
            versions = [(base_ver & ~mask) | rolled_bits]
            if base_ver ^ rolled_bits != versions[0]:
                versions.append(base_ver ^ rolled_bits)

        # Duplicate detection, keyed per job. Rolled version bits are part of the
        # identity: the same nonce under different version bits is distinct work.
        key = (en2, ntime, nonce, rolled_bits)
        seen = self.seen_shares.get(job_id)
        if seen is None:
            # First share on a new job: drop the sets of jobs the manager has
            # already forgotten, so memory is bounded by JOB_HISTORY.
            for old in [j for j in self.seen_shares if self.jobs.get_source(j) is None]:
                del self.seen_shares[old]
            seen = self.seen_shares[job_id] = set()
        if key in seen:
            self._record_reject("Duplicate share")
            await self.send_result(mid, None, [22, "Duplicate share", None])
            return
        if len(seen) >= MAX_SHARES_PER_JOB:
            log.warning("%s share flood on job %s — resetting dedupe set", self._tag(), job_id)
            seen.clear()
        seen.add(key)

        # Rebuild every candidate header (normally exactly one: one payout, one
        # version). Only the hashing can fail on bad input, so it is the only
        # part inside the try: nothing after the block check can skip a submit.
        cands = []                 # (hash, header, coinbase_raw, payout_address, version)
        try:
            if payouts is not None:
                for script, address in reversed(payouts):          # newest first
                    c1, c2 = src.coinb1_coinb2(script, EXTRANONCE1_SIZE,
                                               EXTRANONCE2_SIZE, COINBASE_TAG)
                    coinbase_raw, cb_hash = src.coinbase(c1, self.extranonce1, en2, c2)
                    for v in versions:
                        header = src.header(cb_hash, v, ntime_int, nonce_int)
                        cands.append((block_hash(header), header, coinbase_raw,
                                      address, v))
            else:
                for v in versions:
                    cands.append((src.build_header_hash(self.extranonce1, en2, ntime,
                                                        nonce, f"{v:08x}"),
                                  None, None, None, v))
        except Exception as e:
            self._record_reject()
            log.warning("%s REJECT unparseable submit: %s", self._tag(), e)
            await self.send_result(mid, None, [20, "Bad share encoding", None])
            return

        # The reconstruction that matches the miner's work is the one whose hash
        # is low; a mismatched one hashes to a random value.
        hh, header, coinbase_raw, payout, ver_int = min(
            cands, key=lambda c: int.from_bytes(c[0], "little"))
        hval = int.from_bytes(hh, "little")
        is_block = payouts is not None and hval <= src.target
        share_diff = DIFF1_TARGET / hval if hval else 0.0
        hash_be = hh[::-1].hex()

        if is_block:
            await self._submit_block(src, coinbase_raw, header, hash_be, job_id,
                                     payout, {
                "extranonce1": self.extranonce1, "extranonce2": en2,
                "ntime": ntime, "nonce": nonce, "version": f"{ver_int:08x}",
                "hash_diff": share_diff, "share_diff": self.difficulty})

        # A difficulty change reaches the miner while shares found at the old
        # one are still in flight: honour the previous target briefly.
        assigned = self.difficulty
        meets_share = hval <= self.target
        if (not meets_share and self._prev_target is not None
                and time.monotonic() - self._diff_changed < DIFF_CHANGE_GRACE
                and hval <= self._prev_target):
            meets_share, assigned = True, self._prev_difficulty

        if is_block:
            self._record_accept(share_diff, assigned)
            await self.send_result(mid, True)
            log.warning("%s BLOCK SHARE job=%s hash=%s", self._tag(), job_id, hash_be)
        elif self.jobs.is_stale(src):
            # Work on a chain tip that has been replaced is worthless.
            self._record_reject("Stale share")
            log.info("%s REJECT stale share on job %s (tip changed)", self._tag(),
                     job_id)
            await self.send_result(mid, None, [21, "Stale share", None])
        elif meets_share:
            self._record_accept(share_diff, assigned)
            await self.send_result(mid, True)
            log.info("%s ACCEPT share job=%s diff=%.0f hash=%s… (%s)",
                     self._tag(), job_id, share_diff, hash_be[:24],
                     human_hashrate(self.window_hashrate(300)))
        else:
            self._record_reject("Low difficulty share")
            await self.send_result(mid, None, [23, "Low difficulty share", None])

    async def _submit_block(self, src, coinbase_raw, header, hash_be, job_id,
                            payout, work):
        """Submit a network-target solution. The block is saved to disk first,
        tried once inline, and on a transport failure handed to a background
        retry task, so a node hiccup can never silently discard it. The status
        only says "rejected" when the node itself answered with a reason."""
        entry = {"height": src.height, "hash": hash_be, "worker": self.worker,
                 "payout": payout, "time": time.time(), "status": "submitting"}
        try:
            full = src.full_block(coinbase_raw, header)
        except Exception as e:
            # Cannot happen with node-supplied template data, but if it ever
            # does the candidate must still be on record, with its header.
            entry["status"] = f"block assembly failed: {e}"
            log.error("%s POTENTIAL BCH BLOCK %s could not be assembled (%s); "
                      "header=%s coinbase=%s job=%s", self._tag(), hash_be, e,
                      header.hex(), coinbase_raw.hex(), job_id)
            self.jobs.record_block(entry)
            return
        record = dict(entry, job_id=job_id,
                      prevhash=src.prevhash_internal[::-1].hex(),
                      target=f"{src.target:064x}", **work, block_hex=full)
        saved = save_block_candidate(record)
        log.warning("%s POTENTIAL BCH BLOCK height=%s job=%s hash=%s prev=%s "
                    "en1=%s en2=%s ntime=%s nonce=%s version=%s payout=%s — "
                    "submitting (%d B)", self._tag(), src.height, job_id, hash_be,
                    record["prevhash"], work["extranonce1"], work["extranonce2"],
                    work["ntime"], work["nonce"], work["version"],
                    payout, len(full) // 2)
        if saved:
            log.warning("%s block candidate saved to %s", self._tag(), saved)
        else:
            # No persistent data dir (or the write failed): the log is the only
            # copy, so put the full block in it.
            log.error("%s block candidate NOT saved to disk; block hex: %s",
                      self._tag(), full)
        self.jobs.record_block(entry)

        status, err = await self._attempt_submit(full, hash_be)
        if status:
            self._finish_block(entry, status)
            return
        log.error("%s submitblock failed for %s (%s) — retrying in the background",
                  self._tag(), hash_be, err)
        task = asyncio.create_task(self._retry_submit(entry, full, hash_be, err))
        self.server.submit_tasks.add(task)
        task.add_done_callback(self.server.submit_tasks.discard)

    async def _attempt_submit(self, full, hash_be):
        """One submitblock. Returns (final status, None), or (None, error text)
        when the outcome is unknown and the submit should be retried."""
        loop = asyncio.get_running_loop()
        rpc = self.jobs.rpc          # re-read: Settings may have fixed the creds
        try:
            result = await loop.run_in_executor(None, rpc.submitblock, full)
        except Exception as e:
            return None, str(e)
        if result in (None, ""):
            return "accepted", None
        if result in ("duplicate", "duplicate-inconclusive", "inconclusive"):
            # The node has the block and found it valid -- a duplicate possibly
            # from an earlier attempt of ours that timed out after it landed;
            # "inconclusive" when it is not (yet) on the active chain, e.g. it
            # lost a race with a competing block. Report where it really is.
            return (await self._chain_status(hash_be)
                    or f"inconclusive: node replied {result}"), None
        return f"rejected: {result}", None

    async def _chain_status(self, hash_be):
        """Ask the node about a block: "accepted" if it is in the active chain,
        "inconclusive" if known but not on it, None if unknown/unreachable."""
        loop = asyncio.get_running_loop()
        try:
            hdr = await loop.run_in_executor(None, self.jobs.rpc.getblockheader,
                                             hash_be)
        except Exception:
            return None
        if not isinstance(hdr, dict):
            return None
        if (hdr.get("confirmations") or -1) >= 1:
            return "accepted"
        return "inconclusive: node has the block but it is not in the active chain"

    async def _retry_submit(self, entry, full, hash_be, err):
        status = None
        for delay in SUBMIT_RETRY_DELAYS:
            await asyncio.sleep(delay)
            # A submit that timed out may still have landed: check first.
            status = await self._chain_status(hash_be)
            if status:
                break
            status, e = await self._attempt_submit(full, hash_be)
            if status:
                break
            err = e
            log.error("%s submitblock retry failed for %s: %s", self._tag(),
                      hash_be, err)
        if not status:
            status = (await self._chain_status(hash_be)
                      or f"submit failed: {err} — block saved in "
                         f"{BLOCK_DIR}/, resubmit it with submitblock")
        self._finish_block(entry, status)

    def _finish_block(self, entry, status):
        entry["status"] = status
        self.jobs.update_block()
        if status == "accepted":
            log.warning("%s 🎉 BCH BLOCK ACCEPTED! height=%s hash=%s payout=%s",
                        self._tag(), entry["height"], entry["hash"], entry["payout"])
        else:
            log.error("%s BLOCK NOT ACCEPTED height=%s hash=%s status=%s",
                      self._tag(), entry["height"], entry["hash"], status)
        # Fire-and-forget: the miner's share ack must not wait on a webhook
        # that can take up to 10 s to time out.
        asyncio.create_task(self.jobs.notify_block(entry))


async def main():
    cfg = config.load()
    rpc = BitcoinCashRPC(cfg["bchn_rpc_host"], cfg["bchn_rpc_port"],
                         cfg["bchn_rpc_user"], cfg["bchn_rpc_password"])
    log.info("SoloBCH Forge Phase 2 starting — node %s", rpc)
    if not cfg["bchn_rpc_password"]:
        log.warning("no BCHN RPC password configured (config.json / BCHN_RPC_PASSWORD) "
                    "— node calls will fail auth until it is set")
    vardiff = {"enabled": cfg["vardiff_enabled"],
               "target_spm": cfg["vardiff_target_spm"],
               "min": cfg["vardiff_min"], "max": cfg["vardiff_max"]}
    stats = Stats()
    # A block still "submitting" was cut off by a restart mid-retry; its
    # outcome is unknown here, but the candidate is on disk.
    for b in stats.blocks:
        if b.get("status") == "submitting":
            b["status"] = (f"submit interrupted by restart — check the node, or "
                           f"resubmit the block saved in {BLOCK_DIR}/")
            log.error("block %s was still submitting at the last shutdown — %s",
                      b.get("hash"), b["status"])
            stats.update_block()
    history = History()
    jobs = JobManager(rpc, cfg["share_difficulty"], POLL_INTERVAL,
                      vardiff=vardiff, webhook_url=cfg["webhook_url"], stats=stats,
                      notify_block=cfg["notify_block"],
                      notify_best_share=cfg["notify_best_share"],
                      notify_miner_offline=cfg["notify_miner_offline"],
                      zmq_endpoint=(cfg["zmq_block_endpoint"]
                                    if cfg["zmq_enabled"] else ""))

    server = StratumServer(jobs)
    server.stratum_port = cfg["stratum_port"]
    srv = await asyncio.start_server(server.handle, BIND_HOST, cfg["stratum_port"])
    addr = ", ".join(str(s.getsockname()) for s in srv.sockets)
    log.info("Stratum listening on %s", addr)
    log.info("Point a NerdQaxe++ at stratum+tcp://<umbrel-ip>:%d  user=<BCH address>.worker",
             cfg["stratum_port"])

    diag = Diagnostics(server, jobs, cfg["stratum_port"])
    status = StatusServer(server, jobs, STATUS_HOST, cfg["status_port"], history,
                          diag=diag)
    await status.start()
    log.info("Status dashboard on http://<umbrel-ip>:%d/  (JSON at /status)",
             cfg["status_port"])

    async def vardiff_loop():
        while True:
            await asyncio.sleep(VARDIFF_TICK)
            for c in list(server.clients):
                try:
                    await c.maybe_retarget()
                except Exception as e:
                    log.debug("vardiff retarget error: %s", e)

    async def offline_loop():
        while True:
            await asyncio.sleep(OFFLINE_TICK)
            try:
                for entry in server.scan_offline_miners():
                    await jobs.notify_miner_offline(entry)
            except Exception as e:
                log.debug("offline monitor error: %s", e)

    async def history_loop():
        while True:
            await asyncio.sleep(HISTORY_TICK)
            try:
                clients = [c for c in server.clients if not c.internal]
                hr = sum(c.window_hashrate(300) for c in clients)
                sps = sum(c.shares_in(60) for c in clients) / 60.0
                history.add(hr, sps, len(clients))
            except Exception as e:
                log.debug("history sample error: %s", e)

    # Flush lifetime stats on a clean shutdown (Umbrel sends SIGTERM with a 1m
    # grace period) so throttled counters land on disk instead of being lost.
    # The handler also trips a stop event so the container exits promptly
    # instead of waiting out the grace period.
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown():
        if server.submit_tasks:
            log.error("shutting down with %d block submit(s) still retrying — "
                      "the candidates are saved in %s/", len(server.submit_tasks),
                      BLOCK_DIR)
        stats.flush()
        history.flush()
        stop_event.set()

    try:
        import signal
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _shutdown)
    except (NotImplementedError, AttributeError, RuntimeError):
        pass  # signal handlers unavailable (e.g. Windows) — Ctrl+C path handles it

    asyncio.create_task(jobs.run())
    asyncio.create_task(vardiff_loop())
    asyncio.create_task(offline_loop())
    asyncio.create_task(history_loop())
    try:
        async with srv:
            serve_task = asyncio.create_task(srv.serve_forever())
            await stop_event.wait()
            serve_task.cancel()
            try:
                await serve_task
            except asyncio.CancelledError:
                pass
            # Since Python 3.12, leaving `async with srv` waits for every client
            # connection to finish. With a miner attached that is never, so the
            # container would sit out Umbrel's grace period and get SIGKILLed.
            # Stop listening, then close the miners' sockets so exit is prompt.
            srv.close()
            if hasattr(srv, "close_clients"):        # Python 3.13+
                srv.close_clients()
            for c in list(server.clients):
                try:
                    c.writer.close()
                except Exception:
                    pass
    finally:
        stats.flush()
        history.flush()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("shutting down")
