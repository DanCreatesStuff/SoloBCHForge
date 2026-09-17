#!/usr/bin/env python3
"""
SoloBCH Forge - minimal ZMQ SUB client for BCHN `hashblock` notifications.

Bitcoin Cash Node can publish a ZMQ message on every new block. Subscribing to
it lets the job manager rebuild templates the instant the network finds a block,
instead of waiting for the next poll - shrinking the window where miners hash on
a stale (already-solved) tip.

To stay standard-library only (no pyzmq / libzmq dependency) we speak just enough
of ZMTP 3.x - the ZeroMQ wire protocol - over a plain asyncio TCP socket to act
as a SUB peer using the NULL security mechanism. If the endpoint is unreachable
or misconfigured we keep retrying with backoff and stay quiet; the job manager's
periodic poll remains the source of truth and the fallback, so this is a pure
latency optimisation that fails safe.
"""

import asyncio
import logging
import struct

log = logging.getLogger("solobch.zmq")

# ZMTP 3.1 greeting (64 bytes): signature | version | mechanism | as-server | filler
_SIGNATURE = b"\xff" + b"\x00" * 8 + b"\x7f"
_GREETING = (_SIGNATURE
             + bytes([3, 1])                    # protocol version 3.1
             + b"NULL".ljust(20, b"\x00")       # security mechanism
             + b"\x00"                           # as-server = 0 (we connect)
             + b"\x00" * 31)                     # filler -> 64 bytes total

# frame flag bits
_MORE = 0x01
_LONG = 0x02
_COMMAND = 0x04

_RECONNECT_MIN = 2.0
_RECONNECT_MAX = 30.0
_DEFAULT_PORT = 28332


def _short_string(b: bytes) -> bytes:
    return bytes([len(b)]) + b


def _ready_command(socket_type: bytes = b"SUB") -> bytes:
    """READY command advertising our socket type (short command, body < 256)."""
    prop = (_short_string(b"Socket-Type")
            + struct.pack(">I", len(socket_type)) + socket_type)
    body = _short_string(b"READY") + prop
    return bytes([_COMMAND, len(body)]) + body


def _subscribe_frame(topic: bytes) -> bytes:
    """SUB subscription is a normal message: 0x01 followed by the topic prefix."""
    body = b"\x01" + topic
    return bytes([0x00, len(body)]) + body           # short, final message


def _pong_command(context: bytes) -> bytes:
    body = _short_string(b"PONG") + context
    return bytes([_COMMAND, len(body)]) + body


def _parse_endpoint(endpoint: str):
    ep = endpoint.strip()
    if "://" in ep:
        scheme, ep = ep.split("://", 1)
        if scheme.lower() not in ("tcp", ""):
            raise ValueError(f"unsupported ZMQ scheme {scheme!r} (only tcp)")
    host, sep, port = ep.rpartition(":")
    if not sep:                                       # no port given
        host, port = ep, str(_DEFAULT_PORT)
    if not host:
        raise ValueError(f"ZMQ endpoint needs host:port, got {endpoint!r}")
    return host, int(port)


async def _read_frame(reader):
    """Read one ZMTP frame -> (flags, body)."""
    flags = (await reader.readexactly(1))[0]
    if flags & _LONG:
        size = struct.unpack(">Q", await reader.readexactly(8))[0]
    else:
        size = (await reader.readexactly(1))[0]
    body = await reader.readexactly(size) if size else b""
    return flags, body


async def _run_once(host, port, topic, on_message):
    """One connection lifetime: handshake, subscribe, then receive until close."""
    reader, writer = await asyncio.open_connection(host, port)
    try:
        # --- ZMTP handshake (NULL mechanism) ---
        writer.write(_GREETING)
        await writer.drain()
        await reader.readexactly(64)                  # peer greeting
        writer.write(_ready_command())
        await writer.drain()
        await _read_frame(reader)                     # consume peer READY
        writer.write(_subscribe_frame(topic))
        await writer.drain()
        log.info("ZMQ connected %s:%d, subscribed to %r",
                 host, port, topic.decode("ascii", "replace"))

        # --- receive loop: reassemble multipart, dispatch matching topic ---
        parts = []
        while True:
            flags, body = await _read_frame(reader)
            if flags & _COMMAND:
                # answer ZMTP 3.1 heartbeat PINGs so the peer keeps us alive.
                # command body = short-string name | 2-byte TTL | context
                if len(body) >= 5 and body[:5] == b"\x04PING":
                    writer.write(_pong_command(body[7:]))
                    await writer.drain()
                continue
            parts.append(body)
            if not (flags & _MORE):
                if parts and parts[0] == topic:
                    on_message(parts)
                parts = []
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def watch_hashblock(endpoint, on_block, topic=b"hashblock"):
    """Connect to a BCHN ZMQ endpoint and call on_block() for each new block.

    Runs forever (until cancelled), reconnecting with exponential backoff. All
    failures are non-fatal: the job manager keeps polling regardless.
    """
    try:
        host, port = _parse_endpoint(endpoint)
    except Exception as e:
        log.warning("ZMQ disabled - bad endpoint %r: %s", endpoint, e)
        return

    backoff = _RECONNECT_MIN
    warned = False

    def _on_message(parts):
        nonlocal backoff, warned
        backoff = _RECONNECT_MIN                       # healthy connection
        warned = False
        try:
            on_block()
        except Exception as e:
            log.debug("ZMQ on_block callback error: %s", e)

    while True:
        try:
            await _run_once(host, port, topic, _on_message)
            backoff = _RECONNECT_MIN
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if not warned:
                log.warning("ZMQ %s:%d unavailable (%s) - retrying quietly; "
                            "poll fallback active", host, port, e)
                warned = True
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _RECONNECT_MAX)
