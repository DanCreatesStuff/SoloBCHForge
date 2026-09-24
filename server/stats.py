#!/usr/bin/env python3
"""
SoloBCH Forge - persistent lifetime stats.

The dashboard's live numbers (hashrate, connected workers, shares/sec) are
inherently per-session. But a few numbers are *achievements* a solo miner never
wants to lose to a reboot, an app update, or a miner briefly disconnecting:

    * all-time best share difficulty (+ when, + which worker) -- the headline
    * blocks found (full history)
    * lifetime accepted / rejected share counts
    * when this rig first started mining

These live in stats.json under APP_DATA_DIR (or SOLOBCH_STATS), the same
persistent volume as config.json. On the Umbrel app store that directory
survives reboots and updates for as long as the app stays installed, so the
webpage keeps showing these the moment it loads -- even with no miner connected.

Writes are atomic (tmp + os.replace) and throttled so the common path (an
accepted share) never hammers the disk; block finds and shutdown flush
immediately. Everything runs on the single asyncio thread, so no locking.

Standard library only.
"""

import json
import logging
import os
import time

log = logging.getLogger("solobch.stats")

SAVE_MIN_INTERVAL = 10.0     # seconds; throttle for high-frequency updates
MAX_BLOCKS = 100             # keep this many block records (bounded file size)


def stats_path():
    explicit = os.environ.get("SOLOBCH_STATS")
    if explicit:
        return explicit
    data = os.environ.get("APP_DATA_DIR")
    return os.path.join(data, "stats.json") if data else ""


class Stats:
    def __init__(self):
        self.best_diff = 0.0
        self.best_diff_at = None          # wall-clock time the best was achieved
        self.best_diff_worker = None      # worker label that found it
        self.lifetime_accepted = 0
        self.lifetime_rejected = 0
        self.lifetime_work = 0.0          # sum of assigned share difficulty (work units)
        self.mining_seconds = 0.0         # accumulated seconds the server has run
        self.blocks = []                  # persisted block-found history
        self.first_started = None         # wall-clock time first ever started
        self._dirty = False
        self._last_save = 0.0
        self._session_start = time.monotonic()   # for accumulating mining_seconds
        self._load()
        # Stamp the first-ever start once, and record that we came up.
        now = time.time()
        if not self.first_started:
            self.first_started = now
            self._dirty = True
        self._save(force=True)

    # --- persistence ----------------------------------------------------- #
    def _load(self):
        path = stats_path()
        if not path or not os.path.exists(path):
            return
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            log.warning("could not read %s (%s) — starting fresh", path, e)
            return
        if not isinstance(data, dict):
            return
        self.best_diff = float(data.get("best_diff") or 0.0)
        self.best_diff_at = data.get("best_diff_at")
        self.best_diff_worker = data.get("best_diff_worker")
        self.lifetime_accepted = int(data.get("lifetime_accepted") or 0)
        self.lifetime_rejected = int(data.get("lifetime_rejected") or 0)
        self.lifetime_work = float(data.get("lifetime_work") or 0.0)
        self.mining_seconds = float(data.get("mining_seconds") or 0.0)
        blocks = data.get("blocks")
        self.blocks = list(blocks) if isinstance(blocks, list) else []
        self.first_started = data.get("first_started")
        log.info("loaded stats: best_diff=%.0f, %d block(s), %d/%d shares",
                 self.best_diff, len(self.blocks),
                 self.lifetime_accepted, self.lifetime_rejected)

    def _serialize(self):
        return {
            "best_diff": self.best_diff,
            "best_diff_at": self.best_diff_at,
            "best_diff_worker": self.best_diff_worker,
            "lifetime_accepted": self.lifetime_accepted,
            "lifetime_rejected": self.lifetime_rejected,
            "lifetime_work": self.lifetime_work,
            "mining_seconds": self.mining_seconds,
            "blocks": self.blocks,
            "first_started": self.first_started,
        }

    def _accumulate_time(self):
        """Fold this session's elapsed time into the persisted mining total."""
        now = time.monotonic()
        self.mining_seconds += now - self._session_start
        self._session_start = now

    def _save(self, force=False):
        """Atomically write stats.json. Throttled unless force=True."""
        if not self._dirty and not force:
            return
        now = time.monotonic()
        if not force and (now - self._last_save) < SAVE_MIN_INTERVAL:
            return
        self._accumulate_time()           # checkpoint elapsed mining time
        path = stats_path()
        if not path:
            self._dirty = False           # no persistent dir (dev) — keep in RAM
            return
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self._serialize(), f, indent=2)
            os.replace(tmp, path)
            self._dirty = False
            self._last_save = now
        except Exception as e:
            log.warning("could not write %s: %s", path, e)

    def flush(self):
        """Force an immediate write (call on shutdown)."""
        self._save(force=True)

    # --- updates --------------------------------------------------------- #
    def record_accept(self, achieved_diff, assigned_diff, worker=None):
        """An accepted share. Bumps lifetime count/work and the all-time best diff.

        Returns True when this share sets a new all-time best AND there was a
        prior baseline (so callers can fire a "new best" alert without spamming
        the natural burst of records on a brand-new install)."""
        self.lifetime_accepted += 1
        self.lifetime_work += max(0.0, float(assigned_diff or 0))
        self._dirty = True
        if achieved_diff > self.best_diff:
            prev = self.best_diff
            self.best_diff = achieved_diff
            self.best_diff_at = time.time()
            self.best_diff_worker = worker
            log.info("new all-time best difficulty: %.0f (worker %s)",
                     achieved_diff, worker)
            self._save(force=True)        # a new record is worth an instant write
            return prev > 0
        self._save()                      # throttled
        return False

    def record_reject(self):
        self.lifetime_rejected += 1
        self._dirty = True
        self._save()                      # throttled

    def record_block(self, entry):
        """A block was found. Always flushed immediately — it's rare and precious."""
        self.blocks.append(entry)
        del self.blocks[:-MAX_BLOCKS]
        self._dirty = True
        self._save(force=True)

    def update_block(self):
        """A recorded block entry was changed in place (its submit status
        resolved). Flushed immediately, like record_block."""
        self._dirty = True
        self._save(force=True)

    def blocks_for_worker(self, worker):
        return sum(1 for b in self.blocks if b.get("worker") == worker)

    def reset(self):
        """Zero the lifetime counters and best diff. Found blocks are KEPT —
        they're historical facts, not a stat to clear."""
        self.best_diff = 0.0
        self.best_diff_at = None
        self.best_diff_worker = None
        self.lifetime_accepted = 0
        self.lifetime_rejected = 0
        self.lifetime_work = 0.0
        self.mining_seconds = 0.0
        self._session_start = time.monotonic()
        self.first_started = time.time()     # a fresh "mining since"
        self._dirty = True
        self._save(force=True)
        log.info("lifetime stats reset (blocks kept: %d)", len(self.blocks))

    # --- read ------------------------------------------------------------ #
    def snapshot(self):
        mining_seconds = self.mining_seconds + (time.monotonic() - self._session_start)
        return {
            "best_diff": self.best_diff,
            "best_diff_at": self.best_diff_at,
            "best_diff_worker": self.best_diff_worker,
            "lifetime_accepted": self.lifetime_accepted,
            "lifetime_rejected": self.lifetime_rejected,
            "lifetime_work": self.lifetime_work,
            "mining_seconds": mining_seconds,
            "blocks_found": len(self.blocks),
            "first_started": self.first_started,
        }
