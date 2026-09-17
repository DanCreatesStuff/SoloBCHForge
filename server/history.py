#!/usr/bin/env python3
"""
SoloBCH Forge - rolling hashrate/share time-series for the dashboard chart.

The live sparkline is a client-side ring buffer that resets on every page load.
This store keeps a longer, persisted series so the dashboard can draw a real
"hashrate over time" chart that survives refreshes, reboots and app updates.

A background loop samples the pool every couple of minutes and appends a point;
the series is bounded so the file stays small. It lives in history.json under
APP_DATA_DIR (or SOLOBCH_HISTORY), the same persistent volume as stats.json.
Writes are atomic (tmp + os.replace) and throttled. Standard library only.
"""

import json
import logging
import os
import time

log = logging.getLogger("solobch.history")

SAVE_MIN_INTERVAL = 20.0        # seconds; throttle disk writes
DEFAULT_MAX_POINTS = 2160       # e.g. 3 days at a 2-minute sampling interval


def history_path():
    explicit = os.environ.get("SOLOBCH_HISTORY")
    if explicit:
        return explicit
    data = os.environ.get("APP_DATA_DIR")
    return os.path.join(data, "history.json") if data else ""


class History:
    def __init__(self, max_points=DEFAULT_MAX_POINTS):
        self.max_points = max_points
        # each point: [epoch_seconds, hashrate_hps, shares_per_sec, workers]
        self.points = []
        self._dirty = False
        self._last_save = 0.0
        self._load()

    def _load(self):
        path = history_path()
        if not path or not os.path.exists(path):
            return
        try:
            with open(path) as f:
                data = json.load(f)
            pts = data.get("points") if isinstance(data, dict) else data
            if isinstance(pts, list):
                self.points = pts[-self.max_points:]
                log.info("loaded %d history point(s)", len(self.points))
        except Exception as e:
            log.warning("could not read %s (%s) - starting fresh", path, e)

    def add(self, hashrate, shares_per_sec, workers):
        self.points.append([int(time.time()), round(float(hashrate), 2),
                            round(float(shares_per_sec), 3), int(workers)])
        if len(self.points) > self.max_points:
            del self.points[:-self.max_points]
        self._dirty = True
        self._save()

    def _save(self, force=False):
        if not self._dirty and not force:
            return
        now = time.monotonic()
        if not force and (now - self._last_save) < SAVE_MIN_INTERVAL:
            return
        path = history_path()
        if not path:
            self._dirty = False           # no persistent dir (dev) - keep in RAM
            return
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"points": self.points}, f)
            os.replace(tmp, path)
            self._dirty = False
            self._last_save = now
        except Exception as e:
            log.warning("could not write %s: %s", path, e)

    def flush(self):
        self._save(force=True)

    def snapshot(self):
        return self.points
