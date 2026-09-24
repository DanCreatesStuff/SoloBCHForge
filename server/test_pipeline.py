#!/usr/bin/env python3
"""
SoloBCH Forge - end-to-end share / block pipeline test.

Drives the REAL production path  ClientConn.on_submit -> _submit_block ->
rpc.submitblock  against an in-memory fake node, building headers the way an
independent Stratum miner (ESP-Miner / NerdQaxe++) does -- from the
mining.notify fields only -- so it proves the server reconstructs exactly the
work it sent, and that a network-target solution is never lost.

Offline, standard library only:   cd server && python3 test_pipeline.py
"""
import asyncio
import hashlib
import json
import logging
import os
import sys
import tempfile
import time

DATA_DIR = tempfile.mkdtemp(prefix="solobch-test-")
os.environ.pop("SOLOBCH_STATS", None)
os.environ["APP_DATA_DIR"] = DATA_DIR         # stats.json + blocks/ go here
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import stratum                                # noqa: E402  (production module)
from bch_rpc import RPCError                  # noqa: E402
from cashaddr import to_script                # noqa: E402
from job_manager import JobManager            # noqa: E402
from merkle import merkle_root_full           # noqa: E402
from stats import Stats                       # noqa: E402

logging.getLogger("solobch").setLevel(logging.CRITICAL)   # quiet: results only
stratum.SUBMIT_RETRY_DELAYS = (0, 0, 0)

ADDR_A = "bitcoincash:qr95sy3j9xwd2ap32xkykttr4cvcu7as4y0qverfuy"
ADDR_B = "bitcoincash:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a"
EASY_BITS = 0x1f010000                        # target == 2**240 (~65k hashes)
EASY_TARGET = 0x010000 << (8 * (0x1f - 3))
assert EASY_TARGET == 2 ** 240

ok = fail = 0


def check(label, cond):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS  {label}")
    else:
        fail += 1
        print(f"FAIL  {label}")


def sha256d(b):
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def word_swap(b):
    return b"".join(b[i:i + 4][::-1] for i in range(0, len(b), 4))


def make_gbt(height, prev_display, value, ntxs=3):
    txs = []
    for i in range(ntxs):
        data = bytes([i + 1]) * (70 + i)      # opaque bytes; txid derived from them
        txs.append({"data": data.hex(), "txid": sha256d(data)[::-1].hex()})
    now = int(time.time())
    return {"height": height, "version": 0x20000000, "curtime": now,
            "mintime": now - 3600, "bits": f"{EASY_BITS:08x}",
            "coinbasevalue": value, "target": f"{EASY_TARGET:064x}",
            "previousblockhash": prev_display, "transactions": txs}


class FakeRPC:
    """A node that records submitted blocks. `fail_next` makes the next N
    submits raise a transport error; with `land_on_fail` the failing submit
    still reaches the node first (a timeout after acceptance)."""

    def __init__(self):
        self.blocks = []
        self.known = {}                       # display hash -> confirmations
        self.fail_next = 0
        self.land_on_fail = False
        self.reply = None

    @staticmethod
    def _hash(block_hex):
        return sha256d(bytes.fromhex(block_hex[:160]))[::-1].hex()

    def submitblock(self, block_hex):
        self.blocks.append(block_hex)
        if self.fail_next:
            self.fail_next -= 1
            if self.land_on_fail:
                self.known[self._hash(block_hex)] = 1
            raise RPCError({"code": -1, "message": "transport error: TimeoutError"})
        if self.reply is None:
            self.known[self._hash(block_hex)] = 1
        return self.reply

    def getblockheader(self, block_hash):
        if block_hash not in self.known:
            raise RPCError({"code": -5, "message": "Block not found"})
        return {"hash": block_hash, "confirmations": self.known[block_hash]}


class FakeWriter:
    def __init__(self, ip):
        self.buf = b""
        self.ip = ip

    def write(self, b):
        self.buf += b

    async def drain(self):
        pass

    def close(self):
        pass

    def get_extra_info(self, k):
        return (self.ip, 40000) if k == "peername" else None

    def messages(self):
        out = [json.loads(l) for l in self.buf.decode().splitlines() if l.strip()]
        self.buf = b""
        return out


class Miner:
    """Builds headers purely from mining.notify, like real firmware."""

    def __init__(self, conn, writer):
        self.conn, self.w = conn, writer
        self.notify = None
        self.en2_ctr = 0

    def absorb(self):
        reply = None
        for m in self.w.messages():
            if m.get("method") == "mining.notify":
                self.notify = m["params"]
            elif "result" in m or "error" in m:
                reply = m
        return reply

    def header(self, en2, ntime, nonce, notify=None, version=None):
        jid, prev, c1, c2, branch, ver, nbits, _nt, _clean = notify or self.notify
        cb = bytes.fromhex(c1 + self.conn.extranonce1 + en2 + c2)
        root = sha256d(cb)
        for s in branch:
            root = sha256d(root + bytes.fromhex(s))
        ver = version or ver
        return (bytes.fromhex(ver)[::-1] + word_swap(bytes.fromhex(prev)) + root
                + bytes.fromhex(ntime)[::-1] + bytes.fromhex(nbits)[::-1]
                + bytes.fromhex(nonce)[::-1]), cb

    def find(self, pred, notify=None, version=None):
        """Search nonces until pred(hash_int) holds; returns submit params.
        `version` (hex) hashes with a rolled version instead of the job's."""
        self.en2_ctr += 1
        en2 = f"{self.en2_ctr:08x}"
        n = notify or self.notify
        ntime = n[7]
        for nonce in range(0, 1 << 24):
            nh = f"{nonce:08x}"
            hdr, cb = self.header(en2, ntime, nh, n, version)
            h = int.from_bytes(sha256d(hdr), "little")
            if pred(h):
                return [self.conn.worker, n[0], en2, ntime, nh], hdr, cb, h
        raise RuntimeError("no nonce found")

    async def submit(self, params, mid=99):
        await self.conn.on_submit(mid, params)
        return self.absorb()


def parse_block(block_hex):
    b = bytes.fromhex(block_hex)
    hdr, i = b[:80], 80
    n = b[i]; i += 1                           # small varint in these tests
    start = i                                  # coinbase layout from coinbase.py
    i += 4 + 1 + 32 + 4
    sslen = b[i]; i += 1 + sslen + 4
    nout = b[i]; i += 1
    value = int.from_bytes(b[i:i + 8], "little"); i += 8
    spklen = b[i]; i += 1
    spk = b[i:i + spklen]; i += spklen + 4
    return hdr, n, b[start:i], nout, value, spk, b[i:]


def saved_candidate(block_hash):
    folder = os.path.join(DATA_DIR, stratum.BLOCK_DIR)
    for name in os.listdir(folder) if os.path.isdir(folder) else []:
        if name.endswith(f"-{block_hash}.json"):
            with open(os.path.join(folder, name)) as f:
                return json.load(f)
    return None


async def main():
    rpc = FakeRPC()
    jm = JobManager(rpc, 1, stats=Stats())
    jm.vardiff = {"enabled": False, "target_spm": 20, "min": 1, "max": 4000000}
    prev1 = "00000000000000000123456789abcdef00112233445566778899aabbccddeeff"
    gbt1 = make_gbt(950_000, prev1, 312_512_345)
    jm._new_real(gbt1, clean=True)
    jm.mode = "real"
    server = stratum.StratumServer(jm)

    async def settle():
        if server.submit_tasks:
            await asyncio.gather(*list(server.submit_tasks))

    def last_status():
        return jm.stats.blocks[-1]["status"]

    wa, wb = FakeWriter("10.0.0.5"), FakeWriter("10.0.0.6")
    ca, cb_ = stratum.ClientConn(server, None, wa), stratum.ClientConn(server, None, wb)
    server.clients |= {ca, cb_}
    for c, addr in ((ca, ADDR_A), (cb_, ADDR_B)):
        await c.on_subscribe(1, ["NerdQaxe++/test"])
        await c.on_authorize(2, [f"{addr}.rig", "x"])
    ma, mb = Miner(ca, wa), Miner(cb_, wb)
    ma.absorb(); mb.absorb()
    check("miner A received a real job",
          ma.notify is not None and ma.notify[0] == jm.current_job_id)
    check("notify nbits/version are template values",
          ma.notify[6] == f"{EASY_BITS:08x}" and ma.notify[5] == "20000000")

    # ---- share validation -------------------------------------------------
    ca.target = 2 ** 250                       # force an easy, testable share target
    params, *_ = ma.find(lambda h: h > 2 ** 250)
    r = await ma.submit(params)
    check("low-difficulty share rejected with code 23",
          r["result"] is None and r["error"][0] == 23)
    check("low-diff did not increment accepted", ca.accepted == 0 and ca.rejected == 1)

    params, *_ = ma.find(lambda h: EASY_TARGET < h <= 2 ** 250)
    r = await ma.submit(params)
    check("valid share accepted", r["result"] is True and ca.accepted == 1)
    check("non-block share NOT submitted to node", len(rpc.blocks) == 0)

    r = await ma.submit(params)
    check("duplicate rejected with code 22", r["error"] and r["error"][0] == 22)
    r = await ma.submit(["w", "deadbeef", "00000000", params[3], "00000000"])
    check("unknown job rejected with code 21", r["error"] and r["error"][0] == 21)
    r = await ma.submit(["w", params[1], "00", params[3], "00000000"])
    check("bad extranonce2 size rejected", r["error"] and r["error"][0] == 20)

    # difficulty raised while a share found at the old one is in flight
    # (targets set directly: real difficulties need ~2**32 hashes per share)
    ca._prev_difficulty, ca._prev_target = 7, 2 ** 250
    ca.target, ca._diff_changed = 2 ** 248, time.monotonic()
    params, *_ = ma.find(lambda h: 2 ** 248 < h <= 2 ** 250)   # valid at old diff
    r = await ma.submit(params)
    check("in-flight share at the old difficulty accepted within the grace",
          r["result"] is True)
    check("...and credited at the old difficulty", ca._shares[-1][1] == 7)
    ca._diff_changed = time.monotonic() - stratum.DIFF_CHANGE_GRACE - 1
    params, *_ = ma.find(lambda h: 2 ** 248 < h <= 2 ** 250)
    r = await ma.submit(params)
    check("old difficulty no longer honoured after the grace",
          r["error"] and r["error"][0] == 23)
    ca._prev_target = ca._prev_difficulty = None

    # ---- block found, even with the share target unreachable ---------------
    ca.target = 0                              # nothing can meet the share target
    params, hdr, cbraw, hval = ma.find(lambda h: h <= EASY_TARGET)
    job_a_notify = list(ma.notify)
    r = await ma.submit(params)
    check("block candidate accepted even though share target unmet", r["result"] is True)
    check("submitblock called exactly once", len(rpc.blocks) == 1)
    bhdr, ntx, coinbase, nout, value, spk, rest = parse_block(rpc.blocks[-1])
    check("submitted header == miner's header (byte-exact)", bhdr == hdr)
    check("submitted coinbase == miner's coinbase (byte-exact)", coinbase == cbraw)
    check("submitted header hash meets network target",
          int.from_bytes(sha256d(bhdr), "little") <= EASY_TARGET)
    check("tx count = coinbase + template txs", ntx == 1 + len(gbt1["transactions"]))
    check("template txs appended in order",
          rest == b"".join(bytes.fromhex(t["data"]) for t in gbt1["transactions"]))
    leaves = [sha256d(coinbase)] + [sha256d(bytes.fromhex(t["data"]))
                                    for t in gbt1["transactions"]]
    check("header merkle root == full merkle over block txs",
          bhdr[36:68] == merkle_root_full(leaves))
    check("header prevhash == template prev (internal order)",
          bhdr[4:36] == bytes.fromhex(prev1)[::-1])
    check("single coinbase output", nout == 1)
    check("coinbase pays miner A's script", spk == to_script(ADDR_A))
    check("coinbase value == GBT coinbasevalue", value == gbt1["coinbasevalue"])
    check("block recorded as accepted",
          last_status() == "accepted" and jm.stats.blocks[-1]["payout"] == ADDR_A)
    hash_be = sha256d(bhdr)[::-1].hex()
    cand = saved_candidate(hash_be)
    check("candidate saved to disk with the exact submitted block hex",
          cand is not None and cand["block_hex"] == rpc.blocks[-1])
    check("candidate records the forensic fields",
          cand is not None and cand["extranonce1"] == ca.extranonce1
          and cand["nonce"] == params[4] and cand["ntime"] == params[3]
          and cand["job_id"] == params[1] and cand["prevhash"] == prev1
          and cand["payout"] == ADDR_A and cand["target"] == f"{EASY_TARGET:064x}")

    # ---- miner B's block pays B, not A ------------------------------------
    cb_.target = 0
    params_b, *_ = mb.find(lambda h: h <= EASY_TARGET)
    await mb.submit(params_b)
    *_, spk_b, _rest = parse_block(rpc.blocks[-1])
    check("miner B's block pays miner B's script", spk_b == to_script(ADDR_B))

    # ---- same-tip refresh: the previous job stays valid --------------------
    jm._new_real(make_gbt(950_000, prev1, 312_520_000, ntxs=4), clean=False)
    await jm._broadcast()
    ma.absorb()
    ca.target = 2 ** 250
    params, *_ = ma.find(lambda h: EASY_TARGET < h <= 2 ** 250, notify=job_a_notify)
    r = await ma.submit(params)
    check("share on the pre-refresh job (same tip) still accepted", r["result"] is True)
    ca.target = 0

    # ---- template change: solution for the OLD job after a new tip --------
    prev2 ="00000000000000000fedcba98765432100112233445566778899aabbccddeeff"
    gbt2 = make_gbt(950_001, prev2, 312_600_000, ntxs=5)
    jm._new_real(gbt2, clean=True)
    await jm._broadcast()
    ma.absorb()
    check("new tip produced a new job id", ma.notify[0] != job_a_notify[0])
    params, hdr_old, *_ = ma.find(lambda h: h <= EASY_TARGET, notify=job_a_notify)
    n_before = len(rpc.blocks)
    await ma.submit(params)
    bhdr, *_ = parse_block(rpc.blocks[-1])
    check("old-job solution still submitted", len(rpc.blocks) == n_before + 1)
    check("old-job solution rebuilt from ITS template (old prevhash)",
          bhdr == hdr_old and bhdr[4:36] == bytes.fromhex(prev1)[::-1])
    ca.target = 2 ** 250
    params, *_ = ma.find(lambda h: EASY_TARGET < h <= 2 ** 250, notify=job_a_notify)
    accepted_before = ca.accepted
    r = await ma.submit(params)
    check("ordinary share on a replaced tip rejected as stale (21)",
          r["error"] and r["error"][0] == 21 and ca.accepted == accepted_before)
    ca.target = 0

    # ---- submitblock failure handling (audit C1) --------------------------
    # transport error once, then the node accepts on retry
    rpc.fail_next, rpc.land_on_fail = 1, False
    params, *_ = ma.find(lambda h: h <= EASY_TARGET)
    n_before = len(rpc.blocks)
    r = await ma.submit(params)
    check("share acked without waiting for the retries", r["result"] is True)
    await settle()
    check("failed submit retried: submitted twice", len(rpc.blocks) == n_before + 2)
    check("retried submit ends accepted", last_status() == "accepted")

    # timeout AFTER the node accepted: the retry must notice, not resubmit
    rpc.fail_next, rpc.land_on_fail = 1, True
    params, *_ = ma.find(lambda h: h <= EASY_TARGET)
    n_before = len(rpc.blocks)
    await ma.submit(params)
    await settle()
    check("block that landed despite a timeout reported accepted",
          last_status() == "accepted")
    check("...found via getblockheader, not resubmitted", len(rpc.blocks) == n_before + 1)

    # node unreachable for the whole retry window
    rpc.fail_next, rpc.land_on_fail = 99, False
    params, _, _, _ = ma.find(lambda h: h <= EASY_TARGET)
    n_before = len(rpc.blocks)
    await ma.submit(params)
    check("while retrying, status is 'submitting' (not rejected)",
          last_status() == "submitting")
    await settle()
    lost_hash = jm.stats.blocks[-1]["hash"]
    check("every retry attempted",
          len(rpc.blocks) == n_before + 1 + len(stratum.SUBMIT_RETRY_DELAYS))
    check("exhausted retries -> 'submit failed', never 'rejected'",
          last_status().startswith("submit failed") and "rejected" not in last_status())
    cand = saved_candidate(lost_hash)
    check("unsubmitted block is saved on disk for manual resubmit",
          cand is not None and cand["block_hex"] == rpc.blocks[-1])
    with open(os.path.join(DATA_DIR, "stats.json")) as f:
        persisted = json.load(f)["blocks"][-1]["status"]
    check("final status persisted to stats.json", persisted == last_status())
    rpc.fail_next = 0

    # a definitive node rejection is final: no retry
    rpc.reply = "high-hash"
    params, *_ = ma.find(lambda h: h <= EASY_TARGET)
    n_before = len(rpc.blocks)
    await ma.submit(params)
    await settle()
    check("node rejection not retried", len(rpc.blocks) == n_before + 1)
    check("node rejection recorded as 'rejected: high-hash'",
          last_status() == "rejected: high-hash")

    # 'duplicate' for a block the node has in its active chain
    rpc.reply = "duplicate"
    params, hdr_dup, *_ = ma.find(lambda h: h <= EASY_TARGET)
    rpc.known[sha256d(hdr_dup)[::-1].hex()] = 1
    await ma.submit(params)
    check("'duplicate' of a block in the active chain -> accepted",
          last_status() == "accepted")

    rpc.reply = "inconclusive"
    params, *_ = ma.find(lambda h: h <= EASY_TARGET)
    await ma.submit(params)
    check("'inconclusive' (valid, not on the active chain) is not labelled rejected",
          last_status().startswith("inconclusive") and "rejected" not in last_status())
    rpc.reply = None

    # ---- no persistent data dir: saving reports failure -------------------
    os.environ.pop("APP_DATA_DIR")
    check("no data dir -> save_block_candidate returns None (hex goes to log)",
          stratum.save_block_candidate({"height": 1, "hash": "00"}) is None)
    os.environ["APP_DATA_DIR"] = DATA_DIR

    # ---- re-authorize with a different address mid-job --------------------
    notify_before = list(ma.notify)
    await ca.on_authorize(5, [f"{ADDR_B}.rig", "x"])
    ma.absorb()
    params, hdr_pre, *_ = ma.find(lambda h: h <= EASY_TARGET, notify=notify_before)
    n_before = len(rpc.blocks)
    await ma.submit(params)
    submitted = len(rpc.blocks) > n_before
    check("solution for a job sent before re-authorize reproduces exactly",
          submitted and parse_block(rpc.blocks[-1])[0] == hdr_pre)
    check("...and pays the address that job was sent with (A)",
          submitted and parse_block(rpc.blocks[-1])[5] == to_script(ADDR_A)
          and jm.stats.blocks[-1]["payout"] == ADDR_A)
    params, *_ = ma.find(lambda h: h <= EASY_TARGET)             # job re-sent after
    await ma.submit(params)
    check("a job sent after re-authorize pays the new address (B)",
          parse_block(rpc.blocks[-1])[5] == to_script(ADDR_B))

    # ---- found-block tracking: confirmations, orphaning, maturity ---------
    loop = asyncio.get_running_loop()
    blocks = jm.stats.blocks
    acc = [b for b in blocks if b["status"] == "accepted"]
    b_conf, b_mature, b_orphan = acc[0], acc[1], acc[2]
    failed = next(b for b in blocks if b["status"].startswith("submit failed"))
    rej = next(b for b in blocks if b["status"].startswith("rejected"))
    rpc.known[b_conf["hash"]] = 5
    rpc.known[b_mature["hash"]] = 100
    rpc.known[b_orphan["hash"]] = -1
    rpc.known[failed["hash"]] = 3             # the node got it after all
    rpc.known.pop(rej["hash"], None)
    await jm._track_blocks(loop)
    check("tracked confirmations recorded",
          b_conf["confirmations"] == 5 and not b_conf.get("mature"))
    check("100 confirmations -> mature", b_mature.get("mature") is True)
    check("accepted block that left the active chain -> orphaned",
          b_orphan["status"] == "orphaned")
    check("'submit failed' block the node has on-chain -> accepted",
          failed["status"] == "accepted" and failed["confirmations"] == 3)
    check("rejected block left alone", rej["status"].startswith("rejected"))
    rpc.known[b_orphan["hash"]] = 2           # reorg back
    await jm._track_blocks(loop)
    check("orphaned block back on the active chain -> accepted",
          b_orphan["status"] == "accepted")
    with open(os.path.join(DATA_DIR, "stats.json")) as f:
        disk = {b["hash"]: b for b in json.load(f)["blocks"]}
    check("tracking results persisted", disk[b_mature["hash"]].get("mature") is True)
    calls = []
    orig_gbh = rpc.getblockheader
    rpc.getblockheader = lambda h: calls.append(h) or orig_gbh(h)
    await jm._track_blocks(loop)
    rpc.getblockheader = orig_gbh
    check("mature block no longer queried", b_mature["hash"] not in calls)

    # ---- extranonce1 is unique among live connections ---------------------
    real_token_hex = stratum.secrets.token_hex
    seq = iter([ca.extranonce1, "cafebabe"])
    stratum.secrets.token_hex = lambda n: next(seq)
    try:
        cz = stratum.ClientConn(server, None, FakeWriter("10.0.0.9"))
    finally:
        stratum.secrets.token_hex = real_token_hex
    check("colliding extranonce1 re-drawn", cz.extranonce1 == "cafebabe")

    # ---- version rolling when the base version has bits inside the mask ---
    prev3 = "00000000000000000aaaaaaaaaaaaaaa00112233445566778899aabbccddeeff"
    gbt3 = make_gbt(950_002, prev3, 312_700_000)
    gbt3["version"] = 0x20002000               # bit 13 lies inside 0x1fffe000
    jm._new_real(gbt3, clean=True)
    wc = FakeWriter("10.0.0.7")
    cc = stratum.ClientConn(server, None, wc)
    server.clients.add(cc)
    await cc.on_configure(1, [["version-rolling"], {"version-rolling.mask": "1fffe000"}])
    await cc.on_subscribe(2, ["NerdQaxe++/test"])
    await cc.on_authorize(4, [f"{ADDR_A}.rolly", "x"])
    mc = Miner(cc, wc)
    mc.absorb()
    cc.target = 0
    delta = 0x00004000
    rolled = f"{0x20002000 ^ delta:08x}"       # what the ASIC actually hashed
    params, hdr_r, *_ = mc.find(lambda h: h <= EASY_TARGET, version=rolled)
    n_before = len(rpc.blocks)
    await mc.submit(params + [f"{delta:08x}"]) # ESP-Miner submits the XOR delta
    check("XOR-style rolled version reconstructed; block submitted byte-exact",
          len(rpc.blocks) == n_before + 1 and parse_block(rpc.blocks[-1])[0] == hdr_r)
    r = await mc.submit(["w", job_a_notify[0], "00000009", params[3], "00000000"])
    check("job that was never sent to this connection rejected (21)",
          r["error"] and r["error"][0] == 21)

    # ---- block assembly failure is still recorded -------------------------
    def boom(*_a):
        raise ValueError("simulated assembly failure")
    jm.current_source.full_block = boom
    params, *_ = mc.find(lambda h: h <= EASY_TARGET)
    n_before = len(rpc.blocks)
    r = await mc.submit(params)
    check("assembly failure recorded, not silently dropped",
          last_status().startswith("block assembly failed") and len(rpc.blocks) == n_before)

    await settle()
    print(f"\n{ok} passed, {fail} failed")
    return fail == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(main()) else 1)
