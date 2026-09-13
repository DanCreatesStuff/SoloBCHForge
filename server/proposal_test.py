#!/usr/bin/env python3
"""
SoloBCH Forge - block-assembly validation via getblocktemplate PROPOSAL mode.

Run this once the node is SYNCED (not IBD). It pulls a real template, assembles a
full candidate block with our code, and asks the node to validate it -- WITHOUT
needing to actually find a block. This proves coinbase/merkle/serialization and,
critically, the getblocktemplate txid byte order (GBT_TXID_IS_DISPLAY) are
consensus-correct against the live chain.

    export BCHN_RPC_PASSWORD="$BCHPASS"
    python3 proposal_test.py                 # uses a placeholder payout
    BCH_TEST_PAYOUT=bitcoincash:qxxx python3 proposal_test.py

Any valid BCH payout works for a proposal (nothing is broadcast).
"""

import os
import sys

from bch_rpc import from_env, RPCError
from cashaddr import to_script
from template import BlockTemplate, GBT_TXID_IS_DISPLAY

# A valid BCH address (cashaddr vector 2). Override with BCH_TEST_PAYOUT.
PAYOUT = os.environ.get("BCH_TEST_PAYOUT",
                        "bitcoincash:qr95sy3j9xwd2ap32xkykttr4cvcu7as4y0qverfuy")


def main():
    rpc = from_env()
    info = rpc.getblockchaininfo()
    if info.get("initialblockdownload"):
        print(f"Node still syncing ({info.get('verificationprogress', 0) * 100:.2f}%). "
              "Proposal test needs a fully-synced node — try again later.")
        return 2

    gbt = rpc.getblocktemplate()
    t = BlockTemplate(gbt, "propos01")
    script = to_script(PAYOUT)

    en1, en2 = "00000000", "00000000"
    c1, c2 = t.coinb1_coinb2(script, 4, 4, tag=b"/SoloBCH Forge/")
    coinbase_raw, cb_hash = t.coinbase(c1, en1, en2, c2)
    header = t.header(cb_hash, t.version, t.curtime, 0)   # nonce 0 (PoW not checked)
    blockhex = t.full_block(coinbase_raw, header)

    print(f"height={t.height}  txs={len(t.tx_data)}  coinbasevalue={t.coinbasevalue} sat")
    print(f"GBT_TXID_IS_DISPLAY={GBT_TXID_IS_DISPLAY}  block={len(blockhex) // 2} bytes")
    print("submitting as proposal (nothing is broadcast)...\n")

    try:
        res = rpc.call("getblocktemplate", [{"mode": "proposal", "data": blockhex}])
    except RPCError as e:
        print(f"proposal RPC error: {e}")
        return 1

    if res in (None, "", "high-hash", "inconclusive"):
        print(f"✅ PROPOSAL VALID (node returned {res!r})")
        print("   Block assembly + merkle + txid byte order are consensus-correct.")
        return 0

    print(f"❌ PROPOSAL REJECTED: {res!r}")
    if res == "bad-txnmrklroot":
        print("   -> merkle/txid order mismatch: flip GBT_TXID_IS_DISPLAY in template.py, retry.")
    elif "cb" in str(res) or "height" in str(res):
        print("   -> coinbase issue (BIP34 height / value / script).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
