#!/usr/bin/env python3
"""
SoloBCH Forge - block header / target / full-block serialization (Phase 2).

BCH block headers are identical in form to Bitcoin's (80 bytes, SHA256d), so the
header + hashing logic is shared; the BCH-specific parts live in coinbase.py /
merkle.py / the consensus rules the node enforces on submitblock.

Provides:
  * bits_to_target(nbits)      compact nBits -> integer target
  * serialize_header(...)      80-byte header (internal byte order in, bytes out)
  * block_hash(header)         sha256d, returned little-endian (internal) bytes
  * meets_target(hash, tgt)    True if the hash satisfies the target
  * serialize_block(...)       header + coinbase + txs -> hex for submitblock

Standard library only.  Run directly for the self-test:  python3 block.py
"""

import hashlib
import struct

from coinbase import encode_varint

DIFF1_TARGET = 0x00000000FFFF0000000000000000000000000000000000000000000000000000


def sha256d(b: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def bits_to_target(bits) -> int:
    """Decode compact nBits (int or hex str) into the full integer target."""
    if isinstance(bits, str):
        bits = int(bits, 16)
    exponent = bits >> 24
    mantissa = bits & 0x007fffff
    if exponent <= 3:
        return mantissa >> (8 * (3 - exponent))
    return mantissa << (8 * (exponent - 3))


def serialize_header(version, prev_hash, merkle_root, ntime, bits, nonce) -> bytes:
    """80-byte header. prev_hash and merkle_root are 32-byte INTERNAL (LE) bytes;
    version/ntime/bits/nonce are integers."""
    assert len(prev_hash) == 32 and len(merkle_root) == 32
    return (struct.pack("<I", version) + prev_hash + merkle_root
            + struct.pack("<I", ntime) + struct.pack("<I", bits)
            + struct.pack("<I", nonce))


def block_hash(header: bytes) -> bytes:
    """sha256d of the header, in internal (little-endian) byte order."""
    return sha256d(header)


def meets_target(hash_le: bytes, target: int) -> bool:
    return int.from_bytes(hash_le, "little") <= target


def serialize_block(header: bytes, coinbase_raw: bytes, tx_hexes) -> str:
    """Full block hex for submitblock: header + varint(tx_count) + coinbase + txs."""
    tx_count = 1 + len(tx_hexes)
    body = header + encode_varint(tx_count) + coinbase_raw
    for t in tx_hexes:
        body += bytes.fromhex(t)
    return body.hex()


# --------------------------------------------------------------------------- #
# Self-test
# --------------------------------------------------------------------------- #
def _rev(h):  # display (big-endian) hex -> internal (little-endian) bytes
    return bytes.fromhex(h)[::-1]


def _selftest():
    ok = fail = 0

    def check(label, cond):
        nonlocal ok, fail
        if cond:
            ok += 1
        else:
            fail += 1
            print(f"FAIL  {label}")

    # nBits decoding
    check("bits 1d00ffff == diff-1 target", bits_to_target("1d00ffff") == DIFF1_TARGET)
    check("bits 03123456 (small exp)", bits_to_target("03123456") == 0x123456)
    check("bits 1b0404cb", bits_to_target("1b0404cb") == 0x0404cb << (8 * (0x1b - 3)))

    # Known-answer: Bitcoin block #125552 (same header format as BCH)
    version = 1
    prev = _rev("00000000000008a3a41b85b8b29ad444def299fee21793cd8b9e567eab02cd81")
    merkle = _rev("2b12fcf1b09288fcaff797d71e950e71ae42b91e8bdb2304758dfcffc2b620e3")
    ntime = 1305998791
    bits = 0x1a44b9f2
    nonce = 2504433986
    header = serialize_header(version, prev, merkle, ntime, bits, nonce)
    h = block_hash(header)
    got = h[::-1].hex()
    expected = "00000000000000001e8d6829a8a21adc5d38d0a473b144b6765798e61f98bd1d"
    check("header length 80", len(header) == 80)
    check("block #125552 hash matches", got == expected)
    check("valid block meets its target", meets_target(h, bits_to_target(bits)))

    # A hash just above target must NOT meet it
    tgt = bits_to_target(bits)
    just_above = (tgt + 1).to_bytes(32, "little")
    check("hash above target rejected", not meets_target(just_above, tgt))

    # Full-block serialization parse-back
    coinbase_raw = bytes.fromhex("01000000010000000000000000000000000000000000000000"
                                 "000000000000000000000000ffffffff0401020304ffffffff01"
                                 "00f2052a010000001976a914cb481232299cd5743151ac4b2d63"
                                 "ae198e7bb0a988ac00000000")
    tx1 = "aa" * 60
    tx2 = "bb" * 55
    blk = bytes.fromhex(serialize_block(header, coinbase_raw, [tx1, tx2]))
    check("block starts with header", blk[:80] == header)
    count = blk[80]                      # small varint (< 0xfd)
    check("tx count = 1 coinbase + 2 txs", count == 3)
    check("coinbase follows count", blk[81:81 + len(coinbase_raw)] == coinbase_raw)
    tail = blk[81 + len(coinbase_raw):]
    check("txs appended in order", tail == bytes.fromhex(tx1) + bytes.fromhex(tx2))

    print(f"\nblock #125552 hash: {got}")
    print(f"{ok} passed, {fail} failed")
    return fail == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
