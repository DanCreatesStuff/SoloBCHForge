#!/usr/bin/env python3
"""
SoloBCH Forge - merkle root + coinbase merkle branch (Phase 2 primitive).

Two things the Stratum job needs:
  * merkle_branch(txids): the sibling hashes along the path from the coinbase
    (leaf index 0) to the root. Sent hex-encoded in mining.notify; the miner and
    our on_submit reconstruct the root as  root = sha256d(root + step)  for each
    step -- exactly what job_manager.build_header_hash already does.
  * merkle_root(coinbase_hash, branch): fold the coinbase leaf through the branch.

Byte order: all inputs/outputs are INTERNAL (little-endian) 32-byte hashes -- the
same order as sha256d(raw_coinbase). getblocktemplate transaction ids must be put
in this same internal order before use (reverse the RPC hex if it is display/
big-endian); we confirm that against a real template when the node is synced.

Standard library only.  Run directly for the self-test:  python3 merkle.py
"""

import hashlib


def sha256d(b: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def merkle_root_full(leaves):
    """Full Bitcoin-style merkle root over all leaves (bytes), incl. coinbase."""
    if not leaves:
        return None
    layer = list(leaves)
    while len(layer) > 1:
        if len(layer) % 2:
            layer.append(layer[-1])          # duplicate last if odd
        layer = [sha256d(layer[i] + layer[i + 1]) for i in range(0, len(layer), 2)]
    return layer[0]


def merkle_branch(txids):
    """Coinbase merkle branch given the NON-coinbase txids (bytes), in block order.

    Returns a list of 32-byte sibling hashes (the coinbase sits at index 0)."""
    branch = []
    f = [None] + list(txids)                 # index 0 = coinbase placeholder
    while len(f) > 1:
        if len(f) % 2:
            f.append(f[-1])                  # duplicate last if odd
        branch.append(f[1])                  # sibling of the coinbase subtree
        nxt = [None]                         # ancestor of coinbase (still unknown)
        for i in range(2, len(f), 2):
            nxt.append(sha256d(f[i] + f[i + 1]))
        f = nxt
    return branch


def merkle_branch_hex(txids):
    """merkle_branch as hex strings, ready for mining.notify."""
    return [h.hex() for h in merkle_branch(txids)]


def merkle_root(coinbase_hash, branch):
    """Fold the coinbase leaf through the branch (each step is bytes)."""
    root = coinbase_hash
    for step in branch:
        root = sha256d(root + step)
    return root


# --------------------------------------------------------------------------- #
# Self-test
# --------------------------------------------------------------------------- #
def _leaf(i):
    return sha256d(f"solobch-leaf-{i}".encode())


def _selftest():
    ok = fail = 0

    def check(label, cond):
        nonlocal ok, fail
        if cond:
            ok += 1
        else:
            fail += 1
            print(f"FAIL  {label}")

    # Hand-checkable cases
    a, b, c = _leaf(0), _leaf(1), _leaf(2)
    check("N=1: empty branch, root == coinbase",
          merkle_branch([]) == [] and merkle_root(a, merkle_branch([])) == a)
    check("N=2: branch=[b], root=H(a+b)",
          merkle_branch([b]) == [b] and merkle_root(a, merkle_branch([b])) == sha256d(a + b))
    check("N=3: root=H(H(a+b)+H(c+c))",
          merkle_root(a, merkle_branch([b, c])) == sha256d(sha256d(a + b) + sha256d(c + c)))

    # Property: branch-based root must equal the full-tree root, for many sizes.
    prop_ok = True
    for n in range(1, 40):
        leaves = [_leaf(i) for i in range(n)]
        coinbase = leaves[0]
        others = leaves[1:]
        via_branch = merkle_root(coinbase, merkle_branch(others))
        via_full = merkle_root_full(leaves)
        if via_branch != via_full:
            prop_ok = False
            print(f"FAIL  n={n}: {via_branch.hex()} != {via_full.hex()}")
    check("branch root == full-tree root for n=1..39", prop_ok)

    # Show a sample branch (as sent in mining.notify)
    sample = merkle_branch_hex([_leaf(i) for i in range(1, 5)])
    print(f"\nsample merkle_branch ({len(sample)} steps): {sample}")

    print(f"\n{ok} passed, {fail} failed")
    return fail == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
