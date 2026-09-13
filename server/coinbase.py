#!/usr/bin/env python3
"""
SoloBCH Forge - BCH coinbase transaction builder (Phase 2 primitive).

Builds a Bitcoin Cash coinbase tx split into Stratum coinb1/coinb2 halves, with
the extranonce inserted between them:

    full coinbase = coinb1 + extranonce1 + extranonce2 + coinb2

scriptSig layout = <BIP34 height push> + extranonce1 + extranonce2 + <tag>

BCH specifics: NO witness data / NO witness commitment output (no SegWit). A solo
coinbase has a single output paying the miner's payout script the full
`coinbasevalue` (block subsidy + fees). Standard library only.

Run directly for the self-test (offline, no node):  python3 coinbase.py
"""


def encode_varint(n: int) -> bytes:
    if n < 0xfd:
        return bytes([n])
    if n <= 0xffff:
        return b"\xfd" + n.to_bytes(2, "little")
    if n <= 0xffffffff:
        return b"\xfe" + n.to_bytes(4, "little")
    return b"\xff" + n.to_bytes(8, "little")


def script_num(n: int) -> bytes:
    """Minimal CScript number encoding (non-negative n), used for BIP34 height."""
    if n == 0:
        return b""
    out = bytearray()
    while n:
        out.append(n & 0xff)
        n >>= 8
    if out[-1] & 0x80:      # keep it unambiguously positive
        out.append(0x00)
    return bytes(out)


def bip34_height_push(height: int) -> bytes:
    """Push of the block height as required at the start of the coinbase scriptSig."""
    enc = script_num(height)
    return bytes([len(enc)]) + enc


def build_coinbase_parts(height, coinbase_value, payout_script,
                         extranonce1_size, extranonce2_size,
                         tag=b"", tx_version=2):
    """Return (coinb1_hex, coinb2_hex) for Stratum mining.notify.

    The caller inserts extranonce1 (server) + extranonce2 (miner) between them.
    """
    if isinstance(payout_script, str):
        payout_script = bytes.fromhex(payout_script)
    if isinstance(tag, str):
        tag = tag.encode()

    height_push = bip34_height_push(height)
    script_sig_len = len(height_push) + extranonce1_size + extranonce2_size + len(tag)

    # coinb1: up to (and including) the BIP34 height push; extranonce follows.
    coinb1 = (
        tx_version.to_bytes(4, "little")
        + encode_varint(1)                       # input count
        + b"\x00" * 32                           # prevout hash (null)
        + b"\xff\xff\xff\xff"                    # prevout index
        + encode_varint(script_sig_len)          # scriptSig length
        + height_push
    )

    # coinb2: tag (rest of scriptSig) + sequence + outputs + locktime.
    output = (
        int(coinbase_value).to_bytes(8, "little")
        + encode_varint(len(payout_script))
        + payout_script
    )
    coinb2 = (
        tag
        + b"\xff\xff\xff\xff"                    # sequence
        + encode_varint(1)                       # output count (solo: single payout)
        + output
        + b"\x00\x00\x00\x00"                    # locktime
    )
    return coinb1.hex(), coinb2.hex()


def assemble(coinb1_hex, extranonce1_hex, extranonce2_hex, coinb2_hex) -> bytes:
    return bytes.fromhex(coinb1_hex + extranonce1_hex + extranonce2_hex + coinb2_hex)


# --------------------------------------------------------------------------- #
# Self-test: build, then parse back and verify every field
# --------------------------------------------------------------------------- #
def _read_varint(b, i):
    n = b[i]; i += 1
    if n < 0xfd:
        return n, i
    if n == 0xfd:
        return int.from_bytes(b[i:i+2], "little"), i+2
    if n == 0xfe:
        return int.from_bytes(b[i:i+4], "little"), i+4
    return int.from_bytes(b[i:i+8], "little"), i+8


def _selftest():
    import hashlib
    ok = fail = 0

    def check(label, cond):
        nonlocal ok, fail
        if cond:
            ok += 1
        else:
            fail += 1
            print(f"FAIL  {label}")

    # BIP34 height encodings
    check("height 1 -> 0101", bip34_height_push(1).hex() == "0101")
    check("height 800000 -> 0300350c", bip34_height_push(800000).hex() == "0300350c")
    check("height 129 -> 028100 (positive sign pad)", bip34_height_push(129).hex() == "028100")

    # Build a realistic coinbase
    height = 800123
    value = 3_12500000            # 3.125 BCH subsidy example (satoshis)
    payout = bytes.fromhex("76a914cb481232299cd5743151ac4b2d63ae198e7bb0a988ac")
    en1 = "deadbeef"             # 4 bytes
    en2 = "01020304"            # 4 bytes
    tag = b"/SoloBCH Forge/"
    c1, c2 = build_coinbase_parts(height, value, payout, 4, 4, tag=tag)
    raw = assemble(c1, en1, en2, c2)

    # Parse it back
    i = 0
    ver = int.from_bytes(raw[i:i+4], "little"); i += 4
    vin, i = _read_varint(raw, i)
    prevout = raw[i:i+32]; i += 32
    index = raw[i:i+4]; i += 4
    ss_len, i = _read_varint(raw, i)
    script_sig = raw[i:i+ss_len]; i += ss_len
    seq = raw[i:i+4]; i += 4
    vout, i = _read_varint(raw, i)
    out_value = int.from_bytes(raw[i:i+8], "little"); i += 8
    spk_len, i = _read_varint(raw, i)
    spk = raw[i:i+spk_len]; i += spk_len
    locktime = raw[i:i+4]; i += 4

    check("tx version == 2", ver == 2)
    check("single input", vin == 1)
    check("null prevout", prevout == b"\x00" * 32)
    check("prevout index ffffffff", index == b"\xff\xff\xff\xff")
    check("scriptSig = height+en1+en2+tag",
          script_sig == bip34_height_push(height) + bytes.fromhex(en1)
          + bytes.fromhex(en2) + tag)
    check("scriptSig starts with BIP34 height",
          script_sig[:len(bip34_height_push(height))] == bip34_height_push(height))
    check("sequence ffffffff", seq == b"\xff\xff\xff\xff")
    check("single output", vout == 1)
    check("output value == coinbasevalue", out_value == value)
    check("output script == payout", spk == payout)
    check("locktime 0", locktime == b"\x00\x00\x00\x00")
    check("no trailing bytes (fully consumed)", i == len(raw))
    check("extranonce region is between the halves",
          raw[len(bytes.fromhex(c1)):len(bytes.fromhex(c1))+8].hex() == en1 + en2)

    # A txid can be computed (double-SHA256, shown big-endian)
    txid = hashlib.sha256(hashlib.sha256(raw).digest()).digest()[::-1].hex()
    print(f"\nsample coinbase txid: {txid}")
    print(f"coinb1 ({len(bytes.fromhex(c1))} B): {c1}")
    print(f"coinb2 ({len(bytes.fromhex(c2))} B): {c2}")

    print(f"\n{ok} passed, {fail} failed")
    return fail == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
