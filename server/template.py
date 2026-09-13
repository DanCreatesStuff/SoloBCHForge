#!/usr/bin/env python3
"""
SoloBCH Forge - getblocktemplate -> per-miner Stratum job (Phase 2 integration).

A BlockTemplate wraps one getblocktemplate result and produces, per miner, the
coinb1/coinb2 halves (coinbase pays THAT miner's payout script) plus the shared
merkle branch, header, and full block for submitblock. Ties together cashaddr +
coinbase + merkle + block.

BYTE ORDER (confirm live via GBT proposal mode):
  * previousblockhash from GBT is display (big-endian) -> reversed to internal.
  * GBT transaction ids: reversed to internal here (GBT_TXID_IS_DISPLAY). If the
    live node's txids are already internal, flip this one flag. The merkle math is
    order-agnostic; only consistency with the node's own ordering matters, which
    proposal mode will confirm.

Standard library only.  Run directly for the self-test:  python3 template.py
"""

from coinbase import build_coinbase_parts, assemble
from merkle import (merkle_branch, merkle_root, merkle_root_full, sha256d)
from block import serialize_header, block_hash, meets_target, bits_to_target, serialize_block

GBT_TXID_IS_DISPLAY = True   # reverse GBT txid hex to internal LE (verify with proposal)


def _rev(hexstr):
    return bytes.fromhex(hexstr)[::-1]


def word_swap(b):
    return b"".join(b[i:i + 4][::-1] for i in range(0, len(b), 4))


class BlockTemplate:
    def __init__(self, gbt, template_id):
        self.template_id = template_id
        self.job_id = template_id        # JobManager/get_source key on .job_id
        self.height = gbt["height"]
        self.version = gbt["version"]
        self.curtime = gbt["curtime"]
        self.mintime = gbt.get("mintime", gbt["curtime"])
        self.nbits_hex = gbt["bits"]
        self.bits = int(gbt["bits"], 16)
        self.coinbasevalue = gbt["coinbasevalue"]
        self.target = int(gbt["target"], 16)
        self.prevhash_internal = _rev(gbt["previousblockhash"])
        self.prevhash_notify = word_swap(self.prevhash_internal).hex()

        txns = gbt.get("transactions", [])
        self.tx_data = [t["data"] for t in txns]
        self.txids = []
        for t in txns:
            raw = bytes.fromhex(t.get("txid") or t["hash"])
            self.txids.append(raw[::-1] if GBT_TXID_IS_DISPLAY else raw)
        self.merkle_branch_bytes = merkle_branch(self.txids)
        self.merkle_branch = [h.hex() for h in self.merkle_branch_bytes]

    # --- per-miner coinbase + notify ------------------------------------- #
    def coinb1_coinb2(self, payout_script, en1_size, en2_size, tag=b""):
        return build_coinbase_parts(self.height, self.coinbasevalue, payout_script,
                                    en1_size, en2_size, tag=tag)

    def notify_params(self, job_id, coinb1, coinb2, clean=True):
        return [job_id, self.prevhash_notify, coinb1, coinb2, self.merkle_branch,
                f"{self.version:08x}", self.nbits_hex, f"{self.curtime:08x}", clean]

    # --- reconstruction on submit ---------------------------------------- #
    def coinbase(self, coinb1, en1, en2, coinb2):
        raw = assemble(coinb1, en1, en2, coinb2)
        return raw, sha256d(raw)

    def header(self, coinbase_hash, version, ntime, nonce):
        root = merkle_root(coinbase_hash, self.merkle_branch_bytes)
        return serialize_header(version, self.prevhash_internal, root, ntime,
                                self.bits, nonce)

    def is_block(self, header_bytes):
        return meets_target(block_hash(header_bytes), self.target)

    def full_block(self, coinbase_raw, header_bytes):
        return serialize_block(header_bytes, coinbase_raw, self.tx_data)


# --------------------------------------------------------------------------- #
# Self-test against a sample getblocktemplate
# --------------------------------------------------------------------------- #
_SAMPLE_GBT = {
    "height": 800123,
    "version": 0x20000000,
    "curtime": 1699999999,
    "mintime": 1699999900,
    "bits": "1a44b9f2",
    "coinbasevalue": 312500000,
    "target": f"{bits_to_target(0x1a44b9f2):064x}",
    "previousblockhash":
        "0000000000000000000a1b2c3d4e5f60718293a4b5c6d7e8f9012345678abcde",
    "transactions": [
        {"data": "aa" * 100, "txid": "11" * 32},
        {"data": "bb" * 80,  "txid": "22" * 32},
        {"data": "cc" * 60,  "txid": "33" * 32},
    ],
}

# A valid BCH payout script (from cashaddr vector 2)
_PAYOUT = bytes.fromhex("76a914cb481232299cd5743151ac4b2d63ae198e7bb0a988ac")


def _selftest():
    ok = fail = 0

    def check(label, cond):
        nonlocal ok, fail
        if cond:
            ok += 1
        else:
            fail += 1
            print(f"FAIL  {label}")

    t = BlockTemplate(_SAMPLE_GBT, "aaaa0001")

    check("height parsed", t.height == 800123)
    check("coinbasevalue parsed", t.coinbasevalue == 312500000)
    check("target == bits_to_target", t.target == bits_to_target(0x1a44b9f2))
    check("prevhash notify is word-swap of internal",
          word_swap(bytes.fromhex(t.prevhash_notify)) == t.prevhash_internal)
    check("3 merkle steps for 3 txs", len(t.merkle_branch) == 3)

    en1, en2 = "deadbeef", "01020304"
    c1, c2 = t.coinb1_coinb2(_PAYOUT, 4, 4, tag=b"/SoloBCH Forge/")
    coinbase_raw, cb_hash = t.coinbase(c1, en1, en2, c2)

    # Merkle consistency: folding coinbase through the branch must equal the full
    # tree over [coinbase] + template txids.
    via_branch = merkle_root(cb_hash, t.merkle_branch_bytes)
    via_full = merkle_root_full([cb_hash] + t.txids)
    check("branch root == full-tree root (coinbase + txs)", via_branch == via_full)

    # Header + full block round-trip
    header = t.header(cb_hash, t.version, t.curtime, 0x12345678)
    check("header is 80 bytes", len(header) == 80)
    check("header prevhash region matches internal",
          header[4:36] == t.prevhash_internal)
    check("header merkle region matches computed root", header[36:68] == via_branch)

    blk = bytes.fromhex(t.full_block(coinbase_raw, header))
    check("block starts with header", blk[:80] == header)
    check("tx count = coinbase + 3", blk[80] == 4)
    check("coinbase follows count", blk[81:81 + len(coinbase_raw)] == coinbase_raw)
    check("template txs appended in order",
          blk[81 + len(coinbase_raw):] ==
          b"".join(bytes.fromhex(d) for d in t.tx_data))

    # notify params shape
    np = t.notify_params("aaaa0001", c1, c2, True)
    check("notify has 9 fields", len(np) == 9)
    check("notify version is 8 hex", np[5] == "20000000")
    check("notify nbits passthrough", np[6] == "1a44b9f2")

    # is_block: current header won't beat the tiny target
    check("random header is not a block", not t.is_block(header))

    print(f"\nsample coinbasevalue: {t.coinbasevalue} sat  height {t.height}")
    print(f"merkle branch: {t.merkle_branch}")
    print(f"{ok} passed, {fail} failed")
    return fail == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
