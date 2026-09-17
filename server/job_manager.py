#!/usr/bin/env python3
"""
SoloBCH Forge - job management + node/tip tracking + real templates (Phase 2).

While the node is in IBD we serve a structurally-valid MOCK job (miners keep
hashing, submit path exercised). Once the node is synced we call getblocktemplate
and serve REAL per-miner jobs (each miner's coinbase pays its own payout). New
templates are driven by chain-tip changes.

Standard library only. RPC calls run in a thread executor so they never block the
event loop.
"""

import asyncio
import hashlib
import json
import logging
import secrets
import time

from bch_rpc import RPCError, BitcoinCashRPC
from template import BlockTemplate

log = logging.getLogger("solobch.jobs")

DIFF1_TARGET = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
JOB_HISTORY = 12
MIN_BROADCAST_GAP = 8.0           # throttle mock re-pushes during rapid IBD churn
TEMPLATE_REFRESH = 30.0           # re-fetch the template this often on an unchanged
                                  # tip (picks up new fee-paying txs + fresh curtime)
EXTRANONCE1_SIZE = 4              # bytes; server-assigned per connection
EXTRANONCE2_SIZE = 4             # bytes; advertised in mining.subscribe
COINBASE_TAG = b"/SoloBCH Forge/"


def sha256d(b):
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def word_swap(b):
    return b"".join(b[i:i + 4][::-1] for i in range(0, len(b), 4))


def le_hex_to_be_bytes(h):
    return bytes.fromhex(h)[::-1]


def target_from_difficulty(diff):
    # Integer arithmetic: DIFF1_TARGET is a 224-bit number, and a float divide
    # would silently round it to 53 bits of precision.
    return DIFF1_TARGET // max(1, int(diff))


class MockJob:
    """Structurally-valid placeholder job used during IBD (no real payout)."""

    def __init__(self, job_id, clean_jobs=True):
        self.job_id = job_id
        self._prevhash_internal = secrets.token_bytes(32)
        self.prevhash_notify = word_swap(self._prevhash_internal).hex()
        # version | 1 input | null prevout | 0xffffffff | scriptSig len 8
        # (= extranonce1 + extranonce2, 4 bytes each) ... coinb2 = sequence |
        # 1 output | 50 BCH to an all-zero P2PKH (unspendable) | locktime 0.
        # Never submitted, but every length field is consistent so a miner
        # that parses the coinbase sees a well-formed transaction.
        self.coinb1 = ("01000000"                    # tx version 1
                       "01"                          # 1 input
                       + "00" * 32 +                 # null prevout hash (32 bytes)
                       "ffffffff"                    # prevout index
                       "08")                         # scriptSig length = en1 + en2
        self.coinb2 = ("ffffffff"                    # sequence
                       "01"                          # 1 output
                       "00f2052a01000000"            # 50 BCH in satoshis (LE)
                       "19" "76a914"                 # 25-byte P2PKH: OP_DUP OP_HASH160 <20>
                       + "00" * 20 +                 # all-zero hash160 (unspendable)
                       "88ac"                        # OP_EQUALVERIFY OP_CHECKSIG
                       "00000000")                   # locktime
        self.merkle_branch = []
        self.version = "20000000"
        # Advertise a realistic, HARD network target (~BCH mainnet difficulty) so
        # miners/monitors (AxeOS, Hashwatcher) don't mistake ordinary shares for
        # blocks. "1d00ffff" (difficulty 1) made every share look like a block.
        self.nbits = "1803ffff"
        self.ntime = f"{int(time.time()):08x}"
        self.clean_jobs = clean_jobs

    def notify_params(self, job_id, clean):
        return [job_id, self.prevhash_notify, self.coinb1, self.coinb2,
                self.merkle_branch, self.version, self.nbits, self.ntime, clean]

    def build_header_hash(self, en1, en2, ntime, nonce, version):
        coinbase = bytes.fromhex(self.coinb1 + en1 + en2 + self.coinb2)
        root = sha256d(coinbase)
        for br in self.merkle_branch:
            root = sha256d(root + bytes.fromhex(br))
        header = (le_hex_to_be_bytes(version) + self._prevhash_internal + root
                  + le_hex_to_be_bytes(ntime) + le_hex_to_be_bytes(self.nbits)
                  + le_hex_to_be_bytes(nonce))
        return sha256d(header)


class JobManager:
    def __init__(self, rpc, share_difficulty, poll_interval=5.0,
                 vardiff=None, webhook_url="", stats=None,
                 notify_block=True, notify_best_share=True,
                 notify_miner_offline=True, zmq_endpoint=""):
        self.rpc = rpc
        self.stats = stats
        # `notify_on_*` flags gate each alert type; named to avoid clashing with
        # the notify_block()/notify_miner_offline() coroutine methods below.
        self.notify_on_block = notify_block
        self.notify_best_share = notify_best_share
        self.notify_on_miner_offline = notify_miner_offline
        self.share_difficulty = share_difficulty
        self.share_target = target_from_difficulty(share_difficulty)
        self.poll_interval = poll_interval
        self.vardiff = vardiff or {"enabled": False, "target_spm": 20,
                                   "min": 128, "max": 4000000}
        self.webhook_url = webhook_url
        self.zmq_endpoint = zmq_endpoint
        # Set by the ZMQ hashblock watcher (if any) to wake the poll loop early
        # when a new block lands, instead of waiting out poll_interval.
        self._wake = asyncio.Event()
        self.subscribers = set()
        self._seq = 0
        self.sources = {}                 # job_id -> MockJob | BlockTemplate
        self.current_job_id = None
        self.current_source = None
        self.current_clean = True
        self.mode = "mock"                # "mock" (IBD) | "real" (synced)
        self.node_ready = False
        self.node_reachable = False
        self.last_poll_ok = None
        self.node_status = {}
        self._last_tip = None
        self._last_broadcast = 0.0
        self._template_at = 0.0           # monotonic time the current real template was fetched
        self._new_mock()                  # seed an initial job

    # --- job lifecycle --------------------------------------------------- #
    def _next_id(self):
        self._seq += 1
        return f"{self._seq:08x}"

    def _install(self, source, clean):
        self.sources[source.job_id] = source
        while len(self.sources) > JOB_HISTORY:
            self.sources.pop(sorted(self.sources)[0], None)
        self.current_job_id = source.job_id
        self.current_source = source
        self.current_clean = clean
        return source

    def _new_mock(self, clean=True):
        return self._install(MockJob(self._next_id(), clean), clean)

    def _new_real(self, gbt, clean=True):
        return self._install(BlockTemplate(gbt, self._next_id()), clean)

    def get_source(self, job_id):
        return self.sources.get(job_id)

    def register(self, conn):
        self.subscribers.add(conn)

    def unregister(self, conn):
        self.subscribers.discard(conn)

    async def _broadcast(self):
        for c in list(self.subscribers):
            try:
                await c.send_current_job()
            except Exception:
                self.subscribers.discard(c)
        self._last_broadcast = time.monotonic()

    @property
    def blocks_found(self):
        """Block-found history, persisted across reboots by the stats store."""
        return self.stats.blocks if self.stats else []

    def record_block(self, entry):
        if self.stats:
            self.stats.record_block(entry)

    async def notify_block(self, entry):
        """Fire the configured webhook (fire-and-forget) when a block is found."""
        url = self.webhook_url
        if not url or not self.notify_on_block:
            return
        payload = {
            "event": "block_found",
            "status": entry.get("status"),
            "height": entry.get("height"),
            "hash": entry.get("hash"),
            "worker": entry.get("worker"),
            "payout": entry.get("payout"),
            "time": entry.get("time"),
            "server": "SoloBCH Forge",
        }
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._post_webhook, url, payload)
            log.info("block webhook delivered to %s", url)
        except Exception as e:
            log.warning("block webhook failed (%s): %s", url, e)

    async def notify_best_diff(self, entry):
        """Fire the webhook when a new all-time best share is set (opt-in)."""
        url = self.webhook_url
        if not url or not self.notify_best_share:
            return
        payload = {
            "event": "best_share",
            "diff": entry.get("diff"),
            "worker": entry.get("worker"),
            "payout": entry.get("payout"),
            "time": entry.get("time"),
            "server": "SoloBCH Forge",
        }
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._post_webhook, url, payload)
            log.info("best-share webhook delivered to %s (diff %.0f)",
                     url, entry.get("diff") or 0)
        except Exception as e:
            log.warning("best-share webhook failed (%s): %s", url, e)

    async def notify_miner_offline(self, entry):
        """Fire the webhook when a connected miner drops offline (opt-in)."""
        url = self.webhook_url
        if not url or not self.notify_on_miner_offline:
            return
        payload = {
            "event": "miner_offline",
            "worker": entry.get("worker"),
            "payout": entry.get("payout"),
            "offline_seconds": entry.get("offline_seconds"),
            "time": entry.get("time"),
            "server": "SoloBCH Forge",
        }
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._post_webhook, url, payload)
            log.info("miner-offline webhook delivered to %s (worker %s)",
                     url, entry.get("worker"))
        except Exception as e:
            log.warning("miner-offline webhook failed (%s): %s", url, e)

    @staticmethod
    def _discord_message(p):
        """Human-readable one-liner for Discord's {content} format."""
        event = p.get("event")
        worker = p.get("worker") or "?"
        payout = p.get("payout") or "?"
        if event == "block_found":
            status = str(p.get("status") or "")
            emoji = "🎉" if status.startswith("accepted") else "⚠️"
            return (f"{emoji} **BCH block {status}** — height {p.get('height')}\n"
                    f"`{p.get('hash')}`\nworker `{worker}` → `{payout}`")
        if event == "best_share":
            diff = p.get("diff") or 0
            return (f"⛏️ **New best share!** difficulty {diff:,.0f}\n"
                    f"worker `{worker}` → `{payout}`")
        if event == "miner_offline":
            secs = p.get("offline_seconds")
            since = f" (no shares for {secs/60:.0f} min)" if secs else ""
            return (f"🔌 **Miner offline** — `{worker}`{since}\n"
                    f"payout `{payout}`")
        if event == "test":
            return "✅ SoloBCH Forge test webhook — notifications are working."
        return "SoloBCH Forge: " + json.dumps(p)

    @staticmethod
    def _post_webhook(url, payload):
        import urllib.request
        low = url.lower()
        # Defence in depth (the Settings page validates too): never let urllib
        # follow a file:// or other non-HTTP scheme that slipped into config.
        if not low.startswith(("http://", "https://")):
            raise ValueError("webhook URL must start with http:// or https://")
        # Discord webhooks reject arbitrary JSON — they require {content|embeds}.
        # Any other (custom) endpoint gets the raw structured payload.
        if "discord.com/api/webhooks" in low or "discordapp.com/api/webhooks" in low:
            body = {"content": JobManager._discord_message(payload)}
        else:
            body = payload
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={"Content-Type": "application/json",
                     "User-Agent": "SoloBCH-Forge"})
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read(1024)

    def apply_config(self, cfg):
        """Apply updated settings live (RPC creds + share difficulty). Port
        changes still require an app restart."""
        self.rpc = BitcoinCashRPC(cfg["bchn_rpc_host"], cfg["bchn_rpc_port"],
                                  cfg["bchn_rpc_user"], cfg["bchn_rpc_password"])
        self.share_difficulty = cfg["share_difficulty"]
        self.share_target = target_from_difficulty(cfg["share_difficulty"])
        self.vardiff = {"enabled": cfg["vardiff_enabled"],
                        "target_spm": cfg["vardiff_target_spm"],
                        "min": cfg["vardiff_min"], "max": cfg["vardiff_max"]}
        self.webhook_url = cfg["webhook_url"]
        self.notify_on_block = cfg["notify_block"]
        self.notify_best_share = cfg["notify_best_share"]
        self.notify_on_miner_offline = cfg["notify_miner_offline"]
        self.node_reachable = False       # force a fresh check on next poll
        log.info("config applied — node %s, share diff %s, vardiff %s",
                 self.rpc, self.share_difficulty,
                 "on" if self.vardiff["enabled"] else "off")
        try:
            asyncio.get_running_loop().create_task(self._push_difficulty())
        except RuntimeError:
            pass

    async def _push_difficulty(self):
        # Reset every worker to the anchor difficulty through the client's own
        # setter so the server-side validation target stays in sync; vardiff
        # (if enabled) re-tunes each worker from there.
        for c in list(self.subscribers):
            try:
                await c.set_difficulty(self.share_difficulty)
            except Exception:
                self.subscribers.discard(c)

    def poke(self):
        """Wake the poll loop now (called by the ZMQ hashblock watcher)."""
        self._wake.set()

    async def _wait_next(self):
        """Sleep until the next poll, woken early by a ZMQ new-block poke."""
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
        except asyncio.TimeoutError:
            pass
        finally:
            self._wake.clear()

    # --- polling loop ---------------------------------------------------- #
    async def run(self):
        loop = asyncio.get_running_loop()
        watcher = None
        if self.zmq_endpoint:
            from zmq_sub import watch_hashblock
            watcher = asyncio.create_task(
                watch_hashblock(self.zmq_endpoint, self.poke))
            log.info("job manager: ZMQ hashblock push on %s (poll every %.1fs "
                     "as fallback, share diff %s)",
                     self.zmq_endpoint, self.poll_interval, self.share_difficulty)
        else:
            log.info("job manager: polling node every %.1fs (share diff %s)",
                     self.poll_interval, self.share_difficulty)
        try:
            while True:
                try:
                    await self._poll(loop)
                except RPCError as e:
                    if self.node_reachable or self._last_tip is None:
                        log.warning("node RPC error: %s", e)
                    self.node_reachable = False
                except Exception as e:
                    log.warning("poll failure: %s", e)
                    self.node_reachable = False
                await self._wait_next()
        finally:
            if watcher:
                watcher.cancel()

    async def _poll(self, loop):
        info = await loop.run_in_executor(None, self.rpc.getblockchaininfo)
        ibd = bool(info.get("initialblockdownload", True))
        tip = info.get("bestblockhash")
        self.node_ready = not ibd
        self.node_reachable = True
        self.last_poll_ok = time.time()
        self.node_status = {
            "blocks": info.get("blocks"),
            "headers": info.get("headers"),
            "progress": info.get("verificationprogress"),
            "ibd": ibd,
            "difficulty": info.get("difficulty"),
        }
        if not tip:
            return

        first = self._last_tip is None
        tip_changed = tip != self._last_tip
        pct = (info.get("verificationprogress") or 0) * 100

        if self.node_ready:
            # `_last_tip` is only advanced once a job for that tip is actually
            # installed. Recording it up front (as an earlier version did) meant
            # a failed fetch left miners parked on the previous tip's template
            # with nothing to trigger a retry until the *next* block.
            stale = (self.mode == "real"
                     and time.monotonic() - self._template_at >= TEMPLATE_REFRESH)
            if tip_changed or self.mode != "real" or stale:
                # A clean job is required when the previous block changed (old
                # work is worthless). A periodic refresh on the same tip is sent
                # with clean_jobs=False so miners finish their current job and
                # shares on the previous job id stay valid.
                clean = tip_changed or self.mode != "real"
                try:
                    gbt = await loop.run_in_executor(None, self.rpc.getblocktemplate)
                    tmpl = self._new_real(gbt, clean=clean)
                    self.mode = "real"
                    self._last_tip = tip
                    self._template_at = time.monotonic()
                    if clean:
                        log.info("REAL job %s — height=%s tip=%s… value=%s sat, %d tx "
                                 "-> %d miner(s)", tmpl.job_id, tmpl.height, tip[:16],
                                 tmpl.coinbasevalue, len(tmpl.tx_data),
                                 len(self.subscribers))
                    else:
                        log.debug("template refresh %s — height=%s value=%s sat, %d tx",
                                  tmpl.job_id, tmpl.height, tmpl.coinbasevalue,
                                  len(tmpl.tx_data))
                    await self._broadcast()
                except Exception as e:
                    # Every failure mode lands here (RPC error, transport timeout,
                    # malformed template), never just RPCError: whatever went
                    # wrong, miners must not be left on a template for a tip that
                    # no longer exists.
                    if not clean:
                        # Same tip, refresh only: the current job is still valid,
                        # keep it and try again after the refresh interval.
                        log.warning("template refresh failed (%s); keeping job %s",
                                    e, self.current_job_id)
                        self._template_at = time.monotonic()
                        return
                    if self.mode != "mock":
                        log.warning("getblocktemplate unavailable (%s); mock fallback", e)
                    if self.mode != "mock" or tip_changed:
                        self._new_mock(clean=True)
                        self.mode = "mock"
                        await self._broadcast()
                    # Mock mode retries getblocktemplate on every poll regardless
                    # of the tip, so recording it here cannot suppress a retry.
                    self._last_tip = tip
            return

        # IBD -> mock, tip-driven with throttle
        if tip_changed:
            self._last_tip = tip
            if not first and (time.monotonic() - self._last_broadcast) < MIN_BROADCAST_GAP:
                log.info("tip -> %s… height=%s (%.2f%%) [job hold: within %.0fs gap]",
                         tip[:16], info.get("blocks"), pct, MIN_BROADCAST_GAP)
                return
            self._new_mock(clean=True)
            self.mode = "mock"
            tag = "node up" if first else "TIP CHANGE"
            log.info("%s -> tip %s… height=%s (%.2f%%) ibd=True — mock job %s to %d miner(s)",
                     tag, tip[:16], info.get("blocks"), pct, self.current_job_id,
                     len(self.subscribers))
            await self._broadcast()

    # --- status ---------------------------------------------------------- #
    def status_snapshot(self):
        src = self.current_source
        job = {"job_id": self.current_job_id, "clean": self.current_clean}
        if src is not None:
            job["prevhash"] = src.prevhash_notify[:24] + "…"
            job["height"] = getattr(src, "height", None)
        return {
            "mode": self.mode,
            "node_reachable": self.node_reachable,
            "node_ready": self.node_ready,
            "last_poll_ok": self.last_poll_ok,
            "blocks": self.node_status.get("blocks"),
            "headers": self.node_status.get("headers"),
            "progress": self.node_status.get("progress"),
            "ibd": self.node_status.get("ibd"),
            "difficulty": self.node_status.get("difficulty"),
            "coinbase_value": getattr(self.current_source, "coinbasevalue", None),
            "share_difficulty": self.share_difficulty,
            "vardiff": self.vardiff,
            "current_job": job,
            "blocks_found": len(self.blocks_found),
            "recent_blocks": self.blocks_found[-5:][::-1],
        }
