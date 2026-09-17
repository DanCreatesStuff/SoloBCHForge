#!/usr/bin/env python3
"""
SoloBCH Forge - configuration loading.

Precedence (low -> high): built-in defaults < config.json < environment variables.
  * config.json lives under APP_DATA_DIR (Umbrel app) or SOLOBCH_CONFIG; it is the
    persistent, user-editable settings file (written by the Settings page).
  * environment variables always win. The Umbrel app injects the node's RPC
    host/port/user/password this way, so a stale or mistyped value in
    config.json can never break the node connection; the dev container uses the
    same BCHN_RPC_* variables. env_locked() reports which keys are pinned so the
    Settings page can grey them out instead of silently ignoring edits.

Secrets (the RPC password) are never logged, and config.json is written 0600.
"""

import json
import os

DEFAULTS = {
    "bchn_rpc_host": "bitcoind",
    "bchn_rpc_port": 8332,
    "bchn_rpc_user": "umbrel",
    "bchn_rpc_password": "",
    "stratum_port": 3334,
    "status_port": 3335,
    "share_difficulty": 1000,          # fixed diff, and the vardiff starting point
    "vardiff_enabled": True,           # auto-tune each worker's share difficulty
    "vardiff_target_spm": 20,          # target accepted shares per minute per worker
    "vardiff_min": 128,                # vardiff difficulty floor
    "vardiff_max": 4000000,            # vardiff difficulty ceiling
    "webhook_url": "",                 # POST JSON here for enabled alerts (blank = off)
    "notify_block": True,              # POST when a block is found
    "notify_best_share": True,         # POST when a new all-time best share is set
    "notify_miner_offline": True,      # POST when a connected miner drops offline
    "zmq_enabled": True,               # subscribe to BCHN ZMQ hashblock for instant jobs
    "zmq_block_endpoint": "tcp://bitcoind:28332",  # BCHN ZMQ hashblock endpoint
    "hidden_cards": [],                # dashboard card ids the user has hidden (UI pref)
}

_ENV = {
    "bchn_rpc_host": "BCHN_RPC_HOST",
    "bchn_rpc_port": "BCHN_RPC_PORT",
    "bchn_rpc_user": "BCHN_RPC_USER",
    "bchn_rpc_password": "BCHN_RPC_PASSWORD",
    "stratum_port": "SOLOBCH_STRATUM_PORT",
    "status_port": "SOLOBCH_STATUS_PORT",
    "share_difficulty": "SOLOBCH_SHARE_DIFFICULTY",
    "vardiff_enabled": "SOLOBCH_VARDIFF",
    "vardiff_target_spm": "SOLOBCH_VARDIFF_TARGET_SPM",
    "vardiff_min": "SOLOBCH_VARDIFF_MIN",
    "vardiff_max": "SOLOBCH_VARDIFF_MAX",
    "webhook_url": "SOLOBCH_WEBHOOK_URL",
    "notify_block": "SOLOBCH_NOTIFY_BLOCK",
    "notify_best_share": "SOLOBCH_NOTIFY_BEST_SHARE",
    "notify_miner_offline": "SOLOBCH_NOTIFY_MINER_OFFLINE",
    "zmq_enabled": "SOLOBCH_ZMQ",
    "zmq_block_endpoint": "SOLOBCH_ZMQ_ENDPOINT",
}
_INT_KEYS = {"bchn_rpc_port", "stratum_port", "status_port", "share_difficulty",
             "vardiff_target_spm", "vardiff_min", "vardiff_max"}
_BOOL_KEYS = {"vardiff_enabled", "notify_block", "notify_best_share",
              "notify_miner_offline", "zmq_enabled"}


def config_path():
    explicit = os.environ.get("SOLOBCH_CONFIG")
    if explicit:
        return explicit
    data = os.environ.get("APP_DATA_DIR")
    return os.path.join(data, "config.json") if data else ""


def load():
    cfg = dict(DEFAULTS)

    path = config_path()
    if path and os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, dict):
                cfg.update({k: v for k, v in data.items() if k in DEFAULTS})
        except Exception:
            pass

    for key, env in _ENV.items():
        val = os.environ.get(env)
        if val not in (None, ""):
            cfg[key] = val

    for key in _INT_KEYS:
        try:
            cfg[key] = int(cfg[key])
        except (TypeError, ValueError):
            cfg[key] = DEFAULTS[key]

    for key in _BOOL_KEYS:
        v = cfg[key]
        cfg[key] = (v.strip().lower() in ("1", "true", "yes", "on")
                    if isinstance(v, str) else bool(v))

    return cfg


def save(updates):
    """Persist selected settings to config.json (used by the config page)."""
    path = config_path()
    if not path:
        raise RuntimeError("no config path — set APP_DATA_DIR or SOLOBCH_CONFIG")
    current = {}
    if os.path.exists(path):
        try:
            with open(path) as f:
                current = json.load(f)
        except Exception:
            current = {}
    current.update({k: v for k, v in updates.items() if k in DEFAULTS})
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    # The file holds the RPC password: create it owner-read/write only. The
    # mode set at creation survives os.replace, so the final file is 0600 too.
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(current, f, indent=2)
    os.replace(tmp, path)
    return current


def env_locked():
    """Keys whose value is pinned by an environment variable. The Settings page
    may still save them to config.json, but the env value wins on every load."""
    return [k for k, env in _ENV.items()
            if os.environ.get(env) not in (None, "")]


def redacted(cfg):
    """A copy safe to log/display (password removed)."""
    c = dict(cfg)
    if c.get("bchn_rpc_password"):
        c["bchn_rpc_password"] = "***set***"
    else:
        c["bchn_rpc_password"] = "(unset)"
    return c
