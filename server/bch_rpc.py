#!/usr/bin/env python3
"""
SoloBCH Forge - Bitcoin Cash Node JSON-RPC client (Phase 1B).

Standard library only (urllib, json, base64, os). Synchronous by design: the
asyncio Stratum server will call it via loop.run_in_executor so a slow RPC never
blocks the event loop.

Credentials are read from the environment -- never hardcoded, never logged:
    BCHN_RPC_HOST      (default bitcoind)      # Umbrel in-network hostname
    BCHN_RPC_PORT      (default 8332)
    BCHN_RPC_USER      (default umbrel)
    BCHN_RPC_PASSWORD  (required)

Run directly to self-test against the node:
    export BCHN_RPC_PASSWORD="$BCHPASS"
    python3 bch_rpc.py
"""

import base64
import http.client
import json
import os
import urllib.error
import urllib.request


class RPCError(Exception):
    """A JSON-RPC level error returned by bitcoind (has .code and .message)."""

    def __init__(self, err: dict):
        self.code = err.get("code")
        self.message = err.get("message", "")
        super().__init__(f"RPC error {self.code}: {self.message}")


class BitcoinCashRPC:
    def __init__(self, host: str, port: int, user: str, password: str,
                 timeout: float = 30.0):
        self.host = host
        self.port = int(port)
        self.user = user
        self._timeout = timeout
        self._url = f"http://{host}:{self.port}/"
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self._auth_header = f"Basic {token}"
        self._id = 0

    def __repr__(self):  # never leak the password
        return f"<BitcoinCashRPC {self.user}@{self.host}:{self.port}>"

    def call(self, method: str, params=None):
        self._id += 1
        payload = json.dumps({
            "jsonrpc": "1.0", "id": f"solobch-{self._id}",
            "method": method, "params": params or [],
        }).encode()
        req = urllib.request.Request(
            self._url, data=payload,
            headers={"Content-Type": "text/plain",
                     "Authorization": self._auth_header},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = json.load(resp)
        except urllib.error.HTTPError as e:
            # bitcoind returns HTTP 500 for RPC errors, with a JSON error body.
            try:
                raw = e.read()
                body = json.loads(raw)
            except Exception:
                raise RPCError({"code": e.code,
                                "message": f"HTTP {e.code}: {e.reason}"})
        except urllib.error.URLError as e:
            raise RPCError({"code": -1,
                            "message": f"connection failed: {e.reason}"})
        except (OSError, http.client.HTTPException, ValueError) as e:
            # urlopen only wraps errors raised while *sending* the request in
            # URLError. A timeout or dropped connection while waiting for or
            # reading the response (TimeoutError, RemoteDisconnected,
            # IncompleteRead) propagates raw, and a truncated body fails JSON
            # decoding. Normalize all of them so callers can rely on RPCError
            # being the one failure type they need to handle.
            raise RPCError({"code": -1,
                            "message": f"transport error: "
                                       f"{e.__class__.__name__}: {e}"})
        if not isinstance(body, dict):
            raise RPCError({"code": -1, "message": "malformed RPC response"})
        if body.get("error"):
            raise RPCError(body["error"])
        return body.get("result")

    # --- convenience wrappers -------------------------------------------- #
    def getblockchaininfo(self):
        return self.call("getblockchaininfo")

    def getbestblockhash(self):
        return self.call("getbestblockhash")

    def getnetworkinfo(self):
        return self.call("getnetworkinfo")

    def getblocktemplate(self, template_request=None):
        # BCH has no SegWit -> do NOT pass {"rules":["segwit"]}. An empty
        # request object is the most compatible form for BCHN.
        return self.call("getblocktemplate", [template_request or {}])

    def submitblock(self, block_hex: str):
        return self.call("submitblock", [block_hex])


def from_env() -> "BitcoinCashRPC":
    host = os.environ.get("BCHN_RPC_HOST", "bitcoind")
    port = os.environ.get("BCHN_RPC_PORT", "8332")
    user = os.environ.get("BCHN_RPC_USER", "umbrel")
    password = os.environ.get("BCHN_RPC_PASSWORD")
    if not password:
        raise SystemExit("ERROR: set BCHN_RPC_PASSWORD (e.g. export "
                         "BCHN_RPC_PASSWORD=\"$BCHPASS\")")
    return BitcoinCashRPC(host, port, user, password)


def _selftest():
    rpc = from_env()
    print(f"Connecting: {rpc}\n")

    info = rpc.getblockchaininfo()
    print("getblockchaininfo:")
    print(f"  chain                 {info['chain']}")
    print(f"  blocks / headers      {info['blocks']} / {info['headers']}")
    print(f"  verificationprogress  {info['verificationprogress']*100:.2f}%")
    print(f"  initialblockdownload  {info['initialblockdownload']}")
    print(f"  bestblockhash         {info['bestblockhash']}")

    net = rpc.getnetworkinfo()
    print("\ngetnetworkinfo:")
    print(f"  subversion            {net.get('subversion')}")
    print(f"  connections           {net.get('connections')}")

    print(f"\ngetbestblockhash:       {rpc.getbestblockhash()}")

    print("\ngetblocktemplate:")
    try:
        tmpl = rpc.getblocktemplate()
        print(f"  OK -- height {tmpl.get('height')}, "
              f"{len(tmpl.get('transactions', []))} txs, bits {tmpl.get('bits')}")
    except RPCError as e:
        print(f"  (expected while syncing) {e}")


if __name__ == "__main__":
    _selftest()
