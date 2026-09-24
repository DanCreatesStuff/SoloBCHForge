#!/usr/bin/env python3
"""
SoloBCH Forge - CashAddr (Bitcoin Cash address) decoder/encoder.

Only CashAddr (bitcoincash:q.../p...) payout addresses are supported; legacy
Base58 addresses are NOT decoded here (convert them to CashAddr first). There is
NO SegWit/bech32 on BCH, so Bitcoin `bc1...` addresses are intentionally rejected.
We need this to turn a configured payout address into the coinbase output
script, and to validate whether a Stratum username is a usable BCH address.

Reference: https://github.com/bitcoincashorg/bitcoincash.org/blob/master/spec/cashaddr.md
Standard library only. Run directly to execute the test vectors:  python3 cashaddr.py
"""

CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_CHARMAP = {c: i for i, c in enumerate(CHARSET)}

# version-byte low 3 bits -> hash length in bytes
_SIZE = {0: 20, 1: 24, 2: 28, 3: 32, 4: 40, 5: 48, 6: 56, 7: 64}
_SIZE_INV = {v: k for k, v in _SIZE.items()}
# Types 2/3 are the CashTokens "token-aware" forms (bitcoincash:z.../r...): the
# same P2PKH/P2SH locking script, only flagged as able to receive tokens.
_KIND = {0: "p2pkh", 1: "p2sh", 2: "p2pkh-tokens", 3: "p2sh-tokens"}
_KIND_INV = {v: k for k, v in _KIND.items()}


class CashAddrError(ValueError):
    pass


def _polymod(values):
    c = 1
    for d in values:
        c0 = c >> 35
        c = ((c & 0x07ffffffff) << 5) ^ d
        if c0 & 0x01:
            c ^= 0x98f2bc8e61
        if c0 & 0x02:
            c ^= 0x79b76d99e2
        if c0 & 0x04:
            c ^= 0xf33e5fb3c4
        if c0 & 0x08:
            c ^= 0xae2eabe2a8
        if c0 & 0x10:
            c ^= 0x1e4f43e470
    return c ^ 1


def _prefix_expand(prefix):
    return [ord(x) & 0x1f for x in prefix] + [0]


def _convertbits(data, frombits, tobits, pad=True):
    acc = 0
    bits = 0
    ret = []
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for value in data:
        if value < 0 or (value >> frombits):
            return None
        acc = ((acc << frombits) | value) & max_acc
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits:
            ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        return None
    return ret


def decode(address, default_prefix="bitcoincash"):
    """Decode a CashAddr string -> {'prefix','type','hash'(bytes)}.

    Raises CashAddrError for anything that isn't a valid BCH CashAddr (including
    Bitcoin bech32 `bc1...` addresses)."""
    if not isinstance(address, str):
        raise CashAddrError("address must be a string")
    address = address.strip()
    if address != address.lower() and address != address.upper():
        raise CashAddrError("mixed-case address")
    if ":" in address:
        prefix, payload = address.split(":", 1)
    else:
        prefix, payload = default_prefix, address
    prefix = prefix.lower()
    payload = payload.lower()
    if not payload:
        raise CashAddrError("empty payload")

    try:
        data = [_CHARMAP[c] for c in payload]
    except KeyError as e:
        raise CashAddrError(f"invalid base32 character: {e}")

    if _polymod(_prefix_expand(prefix) + data) != 0:
        raise CashAddrError("bad checksum (wrong prefix or corrupted address)")

    decoded = _convertbits(data[:-8], 5, 8, pad=False)
    if not decoded:
        raise CashAddrError("bad payload padding")

    version = decoded[0]
    hash_ = bytes(decoded[1:])
    if version & 0x80:
        raise CashAddrError("reserved version bit set")
    kind = (version >> 3) & 0x0f
    size = _SIZE[version & 0x07]
    if len(hash_) != size:
        raise CashAddrError(f"hash length {len(hash_)} != declared {size}")
    if kind not in _KIND:
        raise CashAddrError(f"unsupported address type {kind}")
    return {"prefix": prefix, "type": _KIND[kind], "hash": hash_}


def encode(prefix, addr_type, hashbytes):
    """Encode {prefix,type,hash} -> CashAddr string (used for round-trip tests)."""
    kind = _KIND_INV[addr_type]
    version = (kind << 3) | _SIZE_INV[len(hashbytes)]
    payload = _convertbits([version] + list(hashbytes), 8, 5, pad=True)
    chk = _polymod(_prefix_expand(prefix) + payload + [0] * 8)
    csyms = [(chk >> 5 * (7 - i)) & 0x1f for i in range(8)]
    return prefix + ":" + "".join(CHARSET[d] for d in payload + csyms)


def to_script(address):
    """Return the coinbase output scriptPubKey (bytes) for a BCH address."""
    d = decode(address)
    h = d["hash"]
    if d["type"].startswith("p2pkh"):
        if len(h) != 20:
            raise CashAddrError("P2PKH address must carry a 160-bit hash")
        return b"\x76\xa9\x14" + h + b"\x88\xac"          # OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG
    if len(h) == 20:
        return b"\xa9\x14" + h + b"\x87"                   # OP_HASH160 <20> OP_EQUAL
    if len(h) == 32:
        return b"\xaa\x20" + h + b"\x87"                   # P2SH32: OP_HASH256 <32> OP_EQUAL
    raise CashAddrError("P2SH address must carry a 160- or 256-bit hash")


def is_valid_bch_address(address):
    try:
        decode(address)
        return True
    except CashAddrError:
        return False


# --------------------------------------------------------------------------- #
# Self-test with spec vectors
# --------------------------------------------------------------------------- #
_VECTORS = [
    # (cashaddr, type, hash160 hex, expected scriptPubKey hex)
    ("bitcoincash:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a", "p2pkh",
     "76a04053bda0a88bda5177b86a15c3b29f559873",
     "76a91476a04053bda0a88bda5177b86a15c3b29f55987388ac"),
    ("bitcoincash:qr95sy3j9xwd2ap32xkykttr4cvcu7as4y0qverfuy", "p2pkh",
     "cb481232299cd5743151ac4b2d63ae198e7bb0a9",
     "76a914cb481232299cd5743151ac4b2d63ae198e7bb0a988ac"),
    ("bitcoincash:qqq3728yw0y47sqn6l2na30mcw6zm78dzqre909m2r", "p2pkh",
     "011f28e473c95f4013d7d53ec5fbc3b42df8ed10",
     "76a914011f28e473c95f4013d7d53ec5fbc3b42df8ed1088ac"),
    ("bitcoincash:ppm2qsznhks23z7629mms6s4cwef74vcwvn0h829pq", "p2sh",
     "76a04053bda0a88bda5177b86a15c3b29f559873",
     "a91476a04053bda0a88bda5177b86a15c3b29f55987387"),
]


def _selftest():
    ok = 0
    fail = 0

    for addr, typ, h160, spk in _VECTORS:
        try:
            d = decode(addr)
            assert d["type"] == typ, f"type {d['type']} != {typ}"
            assert d["hash"].hex() == h160, f"hash {d['hash'].hex()} != {h160}"
            assert to_script(addr).hex() == spk, "script mismatch"
            # round-trip: re-encode must reproduce the address
            assert encode(d["prefix"], d["type"], d["hash"]) == addr, "encode mismatch"
            # prefix-less form decodes identically
            assert decode(addr.split(":", 1)[1])["hash"].hex() == h160, "prefixless"
            print(f"PASS  {addr}  -> {typ} {h160}")
            ok += 1
        except Exception as e:
            print(f"FAIL  {addr}  -> {e}")
            fail += 1

    # CashTokens spec pair: the token-aware (z) form pays the same script as q.
    plain = "bitcoincash:qr6m7j9njldwwzlg9v7v53unlr4jkmx6eylep8ekg2"
    token = "bitcoincash:zr6m7j9njldwwzlg9v7v53unlr4jkmx6eycnjehshe"
    try:
        same = (decode(token)["type"] == "p2pkh-tokens"
                and to_script(token) == to_script(plain))
    except CashAddrError as e:
        same = False
        print(f"      {e}")
    print(f"{'PASS' if same else 'FAIL'}  token-aware z-address -> same P2PKH script")
    ok, fail = ok + same, fail + (not same)

    # P2SH32 round-trip: 32-byte P2SH -> OP_HASH256 <32> OP_EQUAL
    h32 = bytes(range(32))
    a32 = encode("bitcoincash", "p2sh", h32)
    good = to_script(a32) == b"\xaa\x20" + h32 + b"\x87"
    print(f"{'PASS' if good else 'FAIL'}  P2SH32 {a32[:24]}... -> aa20..87")
    ok, fail = ok + good, fail + (not good)

    # Upper-case is legal; mixed case is not (spec).
    upper_ok = is_valid_bch_address(_VECTORS[0][0].upper())
    print(f"{'PASS' if upper_ok else 'FAIL'}  all-upper-case address accepted")
    ok, fail = ok + upper_ok, fail + (not upper_ok)

    # Negative cases
    negatives = [
        ("mixed-case address must be rejected",
         "bitcoincash:Qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a"),
        ("Bitcoin bech32 (bc1) must be rejected",
         "bc1q5z24l8kuz3ht4zytk9la2mxj7tlhgavw6j9k00"),
        ("corrupted checksum must be rejected",
         "bitcoincash:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6b"),
        ("wrong prefix must be rejected",
         "bchtest:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a"),
    ]
    for label, bad in negatives:
        if is_valid_bch_address(bad):
            print(f"FAIL  {label}: {bad} wrongly accepted")
            fail += 1
        else:
            print(f"PASS  {label}")
            ok += 1

    print(f"\n{ok} passed, {fail} failed")
    return fail == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
