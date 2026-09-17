#!/usr/bin/env python3
"""
SoloBCH Forge - "Is this working?" self-diagnostics.

Runs a sequence of checks against the live server, in the order a solo miner's
setup has to work: settings -> data dir -> node RPC -> sync -> block template ->
block assembly (getblocktemplate proposal mode, nothing is broadcast) -> job
manager -> ZMQ feed -> Stratum handshake (a loopback client) -> webhook -> a
miner connects -> shares arrive -> reject rate.

Each check ends pass / warn / fail / skip. The run stops at the first *fail*
and reports what went wrong plus a potential fix; warnings are collected and
shown at the end. The timed checks (miner, shares, rejects) wait with a visible
countdown, so a full run takes from a few seconds to about five minutes.

Everything runs on the asyncio loop; node calls go through the executor. Only
one run at a time; the last result stays available until the next run.
"""

import asyncio
import json
import logging
import os
import time
import urllib.parse

import cashaddr
import config
from job_manager import EXTRANONCE1_SIZE, EXTRANONCE2_SIZE, COINBASE_TAG
from template import BlockTemplate

log = logging.getLogger("solobch.diag")

# The loopback self-test client authorizes with this identity. stratum.py
# treats a worker with this label *from 127.0.0.1* as internal: it is not
# tracked by the offline monitor and does not appear as a miner.
DIAG_WORKER = "diag-selftest"
DIAG_ADDRESS = "bitcoincash:qr95sy3j9xwd2ap32xkykttr4cvcu7as4y0qverfuy"   # cashaddr vector

NET_TIMEOUT = 20.0        # node RPC calls
PROPOSAL_TIMEOUT = 60.0   # validating a full block can take a moment
JOB_WAIT = 15.0           # a real job must appear within this (3 poll cycles)
MINER_WAIT = 90.0         # wait for a miner to authorize
SHARE_WAIT = 120.0        # wait for the first accepted share
REJECT_WINDOW = 60.0      # then sample shares this long for the reject rate
REJECT_SAMPLE = 20        # ...or until this many shares were seen
REJECT_WARN_PCT = 5.0

CHECKS = [
    ("config",   "Settings are valid"),
    ("datadir",  "Data directory is writable"),
    ("rpc",      "Node RPC is reachable"),
    ("sync",     "Node is synced"),
    ("template", "Node provides block templates"),
    ("proposal", "Block assembly accepted by the node"),
    ("jobs",     "Job manager is serving jobs"),
    ("zmq",      "Instant block feed (ZMQ)"),
    ("stratum",  "Stratum port answers"),
    ("webhook",  "Webhook delivers"),
    ("miner",    "A miner is connected"),
    ("shares",   "Miner is submitting shares"),
    ("rejects",  "Reject rate is healthy"),
]

REPORT_FIX = ("This looks like a bug in SoloBCH Forge rather than in your setup. "
              "Please open an issue with the app log attached.")


class Diagnostics:
    def __init__(self, server, jobs, stratum_port):
        self.server = server
        self.jobs = jobs
        self.stratum_port = stratum_port
        self._task = None
        self._reset()

    def _reset(self):
        self.checks = [{"id": i, "name": n, "status": "pending", "detail": "", "fix": ""}
                       for i, n in CHECKS]
        self.started = None
        self.finished = None
        self.result = None            # None | "pass" | "fail" | "stopped"
        self.error = None             # {"check", "detail", "fix"} on fail

    @property
    def running(self):
        return self._task is not None and not self._task.done()

    def start(self):
        if self.running:
            return False
        self._reset()
        self._task = asyncio.get_running_loop().create_task(self._run())
        return True

    def stop(self):
        if not self.running:
            return False
        self._task.cancel()
        return True

    def snapshot(self):
        now = self.finished or time.time()
        return {
            "running": self.running,
            "started": self.started,
            "finished": self.finished,
            "elapsed": (now - self.started) if self.started else 0,
            "result": self.result,
            "error": self.error,
            "checks": self.checks,
        }

    # --- runner ---------------------------------------------------------- #
    async def _run(self):
        self.started = time.time()
        ctx = {"synced": False}
        log.info("diagnostics started")
        try:
            for i, chk in enumerate(self.checks):
                chk["status"] = "running"
                fn = getattr(self, "_check_" + chk["id"])
                try:
                    status, detail, fix = await fn(ctx, chk)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.exception("diagnostic %s crashed", chk["id"])
                    status = "fail"
                    detail = f"internal error: {e.__class__.__name__}: {e}"
                    fix = REPORT_FIX
                chk.update(status=status, detail=detail, fix=fix)
                log.info("diagnostic %-9s %s — %s", chk["id"], status, detail)
                if status == "fail":
                    self.result = "fail"
                    self.error = {"check": chk["name"], "detail": detail, "fix": fix}
                    for later in self.checks[i + 1:]:
                        later.update(status="skip", detail="not run")
                    break
            else:
                self.result = "pass"
        except asyncio.CancelledError:
            for c in self.checks:
                if c["status"] in ("running", "pending"):
                    c.update(status="skip", detail="stopped")
            self.result = "stopped"
        finally:
            self.finished = time.time()
            log.info("diagnostics finished: %s", self.result)

    async def _wait(self, chk, pred, timeout, label):
        """Poll pred() once a second until truthy or timeout, showing a countdown."""
        start = time.monotonic()
        while True:
            v = pred()
            if v:
                return v
            elapsed = time.monotonic() - start
            if elapsed >= timeout:
                return None
            chk["detail"] = f"{label}… {int(elapsed)}s / {int(timeout)}s"
            await asyncio.sleep(1.0)

    async def _rpc(self, fn, timeout=NET_TIMEOUT):
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(loop.run_in_executor(None, fn), timeout)

    def _miners(self):
        return [c for c in list(self.server.clients)
                if c.authorized and not getattr(c, "internal", False)]

    # --- checks ---------------------------------------------------------- #
    async def _check_config(self, ctx, chk):
        from status import validate_updates
        cfg = config.load()
        ctx["cfg"] = cfg
        err = validate_updates(dict(cfg), cfg)
        if err:
            return "fail", err, "Correct that value in the settings on this page and save."
        if not cfg["bchn_rpc_password"]:
            return ("fail", "no node RPC password is configured",
                    "On Umbrel the password comes from the Bitcoin Cash Node app "
                    "automatically: make sure that app is installed. Elsewhere, copy "
                    "it from the node's 'Node RPC' panel into the Node RPC section above.")
        return ("pass",
                f"stratum {cfg['stratum_port']}, status {cfg['status_port']}, "
                f"share difficulty {cfg['share_difficulty']}, "
                f"vardiff {'on' if cfg['vardiff_enabled'] else 'off'}", "")

    async def _check_datadir(self, ctx, chk):
        path = config.config_path()
        if not path:
            return ("warn", "no data directory configured — settings and stats live "
                    "in memory only and are lost on restart",
                    "Set APP_DATA_DIR (or SOLOBCH_CONFIG) to a persistent directory. "
                    "The Umbrel app does this automatically.")
        d = os.path.dirname(path) or "."
        probe = os.path.join(d, ".diag-write-test")
        try:
            os.makedirs(d, exist_ok=True)
            with open(probe, "w") as f:
                f.write("ok")
            os.remove(probe)
        except OSError as e:
            return ("fail", f"cannot write to {d}: {e}",
                    "The data volume is not writable by the app's user, so settings "
                    "and lifetime stats cannot be saved. On Umbrel run "
                    "`sudo chown -R 1000:1000 <umbrel>/app-data/solobch-forge/data` "
                    "and restart the app.")
        return "pass", d, ""

    async def _check_rpc(self, ctx, chk):
        rpc = self.jobs.rpc
        try:
            info = await self._rpc(rpc.getblockchaininfo)
        except asyncio.TimeoutError:
            return ("fail", f"no answer from {rpc.host}:{rpc.port} within {NET_TIMEOUT:.0f}s",
                    "The node accepted nothing in time. Check the Bitcoin Cash Node app "
                    "is running and not overloaded, and that the host/port match it.")
        except Exception as e:
            msg = str(e)
            low = msg.lower()
            if "401" in msg or "unauthorized" in low or "403" in msg:
                fix = ("The RPC user or password is wrong. On Umbrel both come from the "
                       "Bitcoin Cash Node app automatically, so reinstalling or restarting "
                       "that app usually fixes it. Elsewhere, re-enter the password from "
                       "the node's 'Node RPC' panel.")
            else:
                fix = (f"Could not reach the node at {rpc.host}:{rpc.port}. Check that the "
                       "Bitcoin Cash Node app is installed and running, and that the RPC "
                       "host and port in the Node RPC section match it.")
            return "fail", msg, fix
        if not isinstance(info, dict) or "blocks" not in info:
            return ("fail", "getblockchaininfo returned an unexpected result",
                    "The endpoint answered but does not look like a Bitcoin Cash Node. "
                    "Check the RPC host/port point at the BCH node, not another service.")
        ctx["info"] = info
        return ("pass", f"{info.get('chain')} chain at height {info.get('blocks')} "
                f"({rpc.host}:{rpc.port})", "")

    async def _check_sync(self, ctx, chk):
        info = ctx["info"]
        ibd = bool(info.get("initialblockdownload", True))
        ctx["synced"] = not ibd
        if ibd:
            pct = (info.get("verificationprogress") or 0) * 100
            return ("warn",
                    f"still syncing — {pct:.2f}% (block {info.get('blocks')} of "
                    f"{info.get('headers')})",
                    "Nothing to fix, just wait: until the Bitcoin Cash Node finishes "
                    "syncing, miners get placeholder (mock) work and no real block can be "
                    "found. The checks that need a synced node are skipped below.")
        if info.get("chain") not in (None, "main"):
            return ("warn", f"node is on the '{info.get('chain')}' chain",
                    "Blocks found here are only valid on that network. For real BCH "
                    "mining the node must run mainnet.")
        return "pass", f"synced at height {info.get('blocks')}", ""

    async def _check_template(self, ctx, chk):
        if not ctx["synced"]:
            return "skip", "node not synced", ""
        try:
            gbt = await self._rpc(self.jobs.rpc.getblocktemplate)
        except Exception as e:
            return ("fail", f"getblocktemplate failed: {e}",
                    "The node is synced but refused to build a block template. Check "
                    "the node's own log; if it was just restarted, wait a minute and run "
                    "the checks again.")
        try:
            tmpl = BlockTemplate(gbt, "diag")
        except Exception as e:
            return ("fail", f"template could not be parsed: {e.__class__.__name__}: {e}",
                    "The node returned a template in a format this version does not "
                    "understand. " + REPORT_FIX + " Include the node version.")
        ctx["tmpl"] = tmpl
        return ("pass", f"height {tmpl.height}, {len(tmpl.tx_data)} tx, "
                f"reward {tmpl.coinbasevalue / 1e8:.4f} BCH", "")

    async def _check_proposal(self, ctx, chk):
        tmpl = ctx.get("tmpl")
        if tmpl is None:
            return "skip", "no template (node not synced)", ""
        script, addr = None, None
        for c in self._miners():
            if c.payout_script:
                script, addr = c.payout_script, c.payout_address
                break
        if script is None:
            script, addr = cashaddr.to_script(DIAG_ADDRESS), DIAG_ADDRESS
        en1, en2 = "00" * EXTRANONCE1_SIZE, "00" * EXTRANONCE2_SIZE
        c1, c2 = tmpl.coinb1_coinb2(script, EXTRANONCE1_SIZE, EXTRANONCE2_SIZE,
                                    COINBASE_TAG)
        raw, cb_hash = tmpl.coinbase(c1, en1, en2, c2)
        header = tmpl.header(cb_hash, tmpl.version, tmpl.curtime, 0)
        blockhex = tmpl.full_block(raw, header)
        chk["detail"] = f"asking the node to validate a {len(blockhex) // 2}-byte block…"
        rpc = self.jobs.rpc
        try:
            res = await self._rpc(
                lambda: rpc.call("getblocktemplate",
                                 [{"mode": "proposal", "data": blockhex}]),
                PROPOSAL_TIMEOUT)
        except Exception as e:
            return ("fail", f"proposal RPC failed: {e}",
                    "The node could not be asked to validate a block. If this keeps "
                    "happening while the RPC check above passes, " + REPORT_FIX.lower())
        if res in (None, "", "high-hash", "inconclusive"):
            return ("pass", f"node validated a fully assembled {len(blockhex) // 2}-byte "
                    f"block paying {addr}", "")
        res_s = str(res)
        if "mrklroot" in res_s or "merkle" in res_s:
            hint = "The merkle root does not match: transaction id byte order is wrong. "
        elif "cb" in res_s or "coinbase" in res_s or "height" in res_s:
            hint = "The coinbase is malformed (BIP34 height, value or payout script). "
        else:
            hint = ""
        return ("fail", f"node rejected the assembled block: {res_s}",
                "A block found by your miner would be rejected the same way, so this "
                "must be fixed before mining is worthwhile. " + hint + REPORT_FIX
                + " Include the rejection reason and the node version.")

    async def _check_jobs(self, ctx, chk):
        jm = self.jobs
        if not ctx["synced"]:
            if jm.mode == "mock":
                return "pass", f"mock job {jm.current_job_id} (node syncing)", ""
            return ("warn", f"{jm.mode} job {jm.current_job_id} while the node is syncing",
                    "Expected mock mode during sync; this should settle on the next poll.")

        def ok():
            src = jm.current_source
            h = getattr(src, "height", None)
            blocks = jm.node_status.get("blocks")
            return (jm.mode == "real" and h is not None
                    and (blocks is None or h == blocks + 1))
        if not await self._wait(chk, ok, JOB_WAIT, "waiting for a real job on the node's tip"):
            if jm.mode != "real":
                return ("fail", f"job manager is in {jm.mode} mode although the node is synced",
                        "The poll loop could not turn the node's template into a job. "
                        "Look in the app log for 'getblocktemplate unavailable' or 'poll "
                        "failure' and fix what it reports; restarting the app forces a "
                        "fresh attempt.")
            return ("fail", f"current job is for height "
                    f"{getattr(jm.current_source, 'height', '?')} but the node tip is "
                    f"{jm.node_status.get('blocks')}",
                    "The job is not built on the node's latest block, so shares on it "
                    "cannot become a valid block. This normally self-heals within one "
                    "poll; if it persists, check the app log for template fetch errors "
                    "and restart the app.")
        age = time.time() - (jm.last_poll_ok or 0)
        if age > 60:
            return ("fail", f"last successful node poll was {age:.0f}s ago",
                    "The poll loop has stalled. Restart the app and, if it recurs, "
                    "check the app log around that time.")
        return ("pass", f"real job {jm.current_job_id} for height "
                f"{jm.current_source.height} (last poll {age:.0f}s ago)", "")

    async def _check_zmq(self, ctx, chk):
        jm = self.jobs
        if not jm.zmq_endpoint:
            return "skip", "disabled in settings (polling only)", ""
        st = getattr(jm, "zmq_state", {}) or {}
        if st.get("connected"):
            last = st.get("last_block")
            since = f", last block {time.time() - last:.0f}s ago" if last else ""
            return "pass", f"connected to {jm.zmq_endpoint}{since}", ""
        return ("warn",
                f"not connected to {jm.zmq_endpoint} "
                f"({st.get('error') or 'no connection yet'}); polling every "
                f"{jm.poll_interval:.0f}s instead",
                "Not fatal: jobs still update by polling, just a few seconds later after "
                "each new block. To enable it, the node must run with zmqpubhashblock "
                "(the Umbrel Bitcoin Cash Node app does, on port 28332) and "
                "SOLOBCH_ZMQ_ENDPOINT must point at it. The Umbrel app sets "
                "tcp://<node ip>:28332 automatically from 1.1.1 on.")

    async def _check_stratum(self, ctx, chk):
        port = self.stratum_port
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port), 5)
        except Exception as e:
            return ("fail", f"cannot connect to 127.0.0.1:{port}: {e}",
                    f"The Stratum listener is not accepting connections on port {port}. "
                    "Restart the app; if its log says the port is already in use, choose "
                    "another Stratum port in the Network section and restart.")
        got = {"sub": False, "auth": None, "notify": None, "diff": None}
        try:
            async def send(mid, method, params):
                writer.write((json.dumps({"id": mid, "method": method,
                                          "params": params}) + "\n").encode())
                await writer.drain()

            await send(1, "mining.subscribe", ["SoloBCH-diag"])
            await send(2, "mining.authorize", [f"{DIAG_ADDRESS}.{DIAG_WORKER}", "x"])
            deadline = time.monotonic() + 10
            while (time.monotonic() < deadline
                   and not (got["sub"] and got["auth"] is not None and got["notify"])):
                line = await asyncio.wait_for(reader.readline(),
                                              max(0.1, deadline - time.monotonic()))
                if not line:
                    break
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(msg, dict):
                    continue
                if msg.get("id") == 1:
                    got["sub"] = bool(msg.get("result"))
                elif msg.get("id") == 2:
                    got["auth"] = msg.get("result")
                    if msg.get("error"):
                        got["auth_err"] = msg["error"]
                elif msg.get("method") == "mining.notify":
                    got["notify"] = (msg.get("params") or [None])[0]
                elif msg.get("method") == "mining.set_difficulty":
                    got["diff"] = (msg.get("params") or [None])[0]
        except (asyncio.TimeoutError, OSError):
            pass
        finally:
            writer.close()
        if not got["sub"]:
            return ("fail", "connected, but no answer to mining.subscribe",
                    "The server accepted the TCP connection but did not complete the "
                    "Stratum handshake. Restart the app and check its log for tracebacks. "
                    + REPORT_FIX)
        if got["auth"] is not True:
            return ("fail", f"authorize with a known-good address was refused: "
                    f"{got.get('auth_err') or got['auth']}", REPORT_FIX)
        if not got["notify"]:
            return ("fail", "authorized, but no job was delivered within 10s",
                    "The server accepts miners but is not handing out work. Check the "
                    "'Job manager' result above and the app log.")
        return ("pass", f"handshake OK on port {port} — difficulty {got['diff']}, "
                f"job {got['notify']} delivered", "")

    async def _check_webhook(self, ctx, chk):
        url = self.jobs.webhook_url
        if not url:
            return "skip", "no webhook URL configured", ""
        host = urllib.parse.urlsplit(url).hostname or "?"
        try:
            await self._rpc(lambda: self.jobs._post_webhook(url, {
                "event": "test", "server": "SoloBCH Forge",
                "message": "SoloBCH Forge diagnostics: webhook check"}))
        except Exception as e:
            return ("fail", f"{host}: {e}",
                    "The webhook endpoint rejected or did not answer a test message. "
                    "Check the URL in the Notifications section (for Discord paste the "
                    "complete webhook URL), and that this server can reach the internet.")
        return "pass", f"test message delivered to {host}", ""

    async def _check_miner(self, ctx, chk):
        found = await self._wait(chk, self._miners, MINER_WAIT,
                                 "waiting for a miner to connect")
        if not found:
            return ("fail", f"no miner authorized within {MINER_WAIT:.0f}s",
                    "No miner has connected. On the miner set: Stratum URL "
                    f"stratum+tcp://<your Umbrel's LAN IP>:{self.stratum_port}, username "
                    "<your BCH address>.<worker name>, any password. The miner must be on "
                    "the same network as Umbrel. If the miner itself says 'connected' "
                    "but nothing shows here, it is pointed at a different host or port; "
                    "if it says the address was rejected, the username is not a valid "
                    "BCH CashAddr.")
        names = ", ".join(f"{c.worker} ({c.model or 'unknown model'}, {c.peer[0]})"
                          for c in found[:5])
        return "pass", f"{len(found)} miner(s): {names}", ""

    async def _check_shares(self, ctx, chk):
        st = self.jobs.stats
        a0, r0 = st.lifetime_accepted, st.lifetime_rejected
        ctx["a0"], ctx["r0"] = a0, r0
        t0 = time.monotonic()
        await self._wait(chk, lambda: st.lifetime_accepted - a0, SHARE_WAIT,
                         "waiting for an accepted share")
        acc = st.lifetime_accepted - a0
        rej = st.lifetime_rejected - r0
        elapsed = time.monotonic() - t0
        miners = self._miners()
        if not acc:
            reasons = sorted({c.last_reject for c in miners if c.last_reject})
            if rej:
                why = ", ".join(reasons) or "unknown reason"
                low = why.lower()
                if "difficulty" in low:
                    fix = ("Every share is below the difficulty assigned to the miner. "
                           "Usually the miner ignores mining.set_difficulty, or its "
                           "firmware is misconfigured. Turn vardiff on (or lower the fixed "
                           "share difficulty) in the Difficulty section and restart the "
                           "miner.")
                elif "stale" in low or "job" in low:
                    fix = ("The miner submits work for jobs the server has already "
                           "replaced: it is slow to pick up new jobs. Check the network "
                           "path between miner and Umbrel (Wi-Fi quality, latency) and "
                           "restart the miner.")
                elif "ntime" in low:
                    fix = ("The miner's timestamps are outside the allowed window. Check "
                           "the miner's clock/NTP settings and the Umbrel's time.")
                elif "duplicate" in low:
                    fix = ("The miner resubmits identical work, which points to a "
                           "firmware issue. Update the miner's firmware.")
                else:
                    fix = "Check the app log for the REJECT lines to see the exact reason."
                return ("fail", f"{rej} share(s) rejected, none accepted in "
                        f"{elapsed:.0f}s ({why})", fix)
            diff = max((c.difficulty or 0) for c in miners) if miners else 0
            return ("fail", f"no shares in {elapsed:.0f}s",
                    "The miner is connected but has not submitted any work. Check the "
                    "miner's own screen shows it hashing on this pool. If its hashrate is "
                    "small, the assigned difficulty may simply be too high: with "
                    f"difficulty {diff} the expected time per share is "
                    f"{diff} x 2^32 / <hashrate> seconds. Turn vardiff on or lower the "
                    "share difficulty in the Difficulty section.")
        from status import human_hashrate
        hr = sum(c.window_hashrate(300) for c in miners)
        return ("pass", f"{acc} accepted, {rej} rejected in {elapsed:.0f}s "
                f"({human_hashrate(hr)} over 5 min)", "")

    async def _check_rejects(self, ctx, chk):
        st = self.jobs.stats
        a0, r0 = ctx.get("a0", st.lifetime_accepted), ctx.get("r0", st.lifetime_rejected)

        def enough():
            return (st.lifetime_accepted - a0) + (st.lifetime_rejected - r0) >= REJECT_SAMPLE
        await self._wait(chk, enough, REJECT_WINDOW, "sampling shares")
        acc = st.lifetime_accepted - a0
        rej = st.lifetime_rejected - r0
        total = acc + rej
        if total == 0:
            return "skip", "no shares to sample", ""
        pct = rej / total * 100
        reasons = sorted({c.last_reject for c in self._miners() if c.last_reject})
        why = f" ({', '.join(reasons)})" if reasons and rej else ""
        if pct > REJECT_WARN_PCT:
            return ("warn", f"{rej} of {total} shares rejected ({pct:.0f}%){why}",
                    "A few rejects right after a new block are normal (stale work). A "
                    "steady rate above 5% usually means high latency between miner and "
                    "server, a miner slow to switch jobs, or a difficulty the miner does "
                    "not honor. Check the miner's network connection and firmware; the "
                    "REJECT lines in the app log show the exact reasons.")
        return "pass", f"{rej} of {total} shares rejected ({pct:.1f}%)", ""
