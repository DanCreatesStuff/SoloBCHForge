# SoloBCH Forge — Development Notes

> **Internal engineering notes** (architecture, phase history, BCH-specific risks).
> For the user-facing overview see the top-level [README](../README.md).
>
> Personal/LAN specifics have been generalized: the Umbrel host appears as
> `<umbrel-ip>` and the node's routable public IP as `<your-node-public-ip>`.
> Umbrel's internal app-network range (`10.21.0.0/16`) is a platform constant,
> not personal, and is kept for reference — reach the node by its hostname
> (`bitcoind`), never a hard-coded IP.

A lightweight, self-hosted **Bitcoin Cash (BCH) solo-mining Stratum server** for Umbrel + NerdQaxe++/AxeOS ASIC miners.

```
NerdQaxe++ (AxeOS)  →  SoloBCH Forge (Stratum)  →  Bitcoin Cash Node (BCHN, JSON-RPC)  →  BCH network
```

Solo mining only. No pool payout system. Block reward → one configured BCH payout address in the coinbase.

## Target environment
- Raspberry Pi 5, 16 GB RAM, UmbrelOS
- Umbrel LAN IP: `<umbrel-ip>` (your Pi's address on your LAN)
- Existing services (Bitcoin Knots, Public Pool, etc.) must NOT be disturbed.

## Port map (isolation is critical — see risks)
| Purpose            | Default (conflicts) | SoloBCH Forge uses | Notes |
|--------------------|---------------------|--------------------|-------|
| Stratum (miners)   | 3333 (Public Pool)  | **3334**           | LAN-only |
| BCHN JSON-RPC      | 8332 (Bitcoin!)     | **9332**           | localhost/Docker-net only, never public |
| BCHN P2P           | 8333 (Bitcoin!)     | **9333**           | verify free before use |

> BCHN's mainnet defaults are **identical** to Bitcoin Core/Knots (8332/8333). Running it with defaults alongside Knots will collide. We remap BCHN.
>
> **Discovered 2026-09-11:** ports **9332/9333 belong to the Bitcoin Knots app** (`bitcoin-knots_app_1`, RetroPex image). Keep clear of them (and Knots' internal 8332/8333).

## BCH node: use the existing Umbrel app
User already runs the **Bitcoin Cash Node Umbrel app** — BCHN **29.0.0 (EB32.0)**, mainnet, syncing (15.9% as of 2026-09-11), 8 clearnet peers. **We do NOT self-host a node**; SoloBCH Forge connects to this app's JSON-RPC. `getblocktemplate` will error until sync completes (node-in-IBD) — build the RPC client against `getblockchaininfo`/`getbestblockhash`/`getnetworkinfo` meanwhile.

### Node connection (from the app's own "Node RPC — for apps" panel)
- **App docker network:** `10.21.0.0/16` (Umbrel platform constant); bitcoind container `bitcoin-cash-node_bitcoind_1`, reachable by its in-network hostname **`bitcoind`** (Umbrel assigns its IP within that range — use the hostname, not a hard IP).
- **RPC:** `bitcoind:8332` (in-network). User **`umbrel`**, password from the app UI (**never commit — load via env/secret file**). RPC is NOT published to the LAN (good).
- **ZMQ:** `hashblock` on `:28332` — use for instant tip-change → new job (beats polling).
- **P2P public:** `<your-node-public-ip>:8335`.

### Reference / prior art
A separate **"LoneStrike Cash"** BCH solo Stratum app for ASICs (+ Fulcrum) already exists in this ecosystem, which confirms the architecture; SoloBCH Forge is an independent from-scratch build.

## Phase plan
- **1A — Stratum PoC (mock job): ✅ DONE (2026-09-11).** Verified against a real NerdQaxe++ (BM1370, AxeOS v1.1.0). Full handshake works; miner hashes and submits; shares validate `meets_share=True`. This proved coinbase concat, merkle, byte-order (`word_swap` prevhash + LE), version-rolling reconstruction, header serialization, and target math all match the ASIC. Notes: AxeOS sends `subscribe` BEFORE `configure`; version-rolling submit sends only the ROLLED bits (6th param), combined as `(base & ~mask) | (rolled & mask)`.
- **1B — BCHN RPC integration: ✅ DONE (2026-09-11).** `bch_rpc.py` (RPC client, env-based creds) + `job_manager.py` (live tip detection, node-readiness tracking, job lifecycle + recent-job history, notify broadcast) validated end-to-end against the live syncing node with the real NerdQaxe++: real tip changes mint clean jobs, the miner switches job_id and shares validate `meets_share=True`. Only real-template block-building remains, gated on node sync (IBD). Server run: `export BCHN_RPC_PASSWORD=…; python3 server/stratum.py`.
- **1B+ — Stats + status UI groundwork: ✅ DONE (2026-09-11).** `status.py` (stdlib async HTTP) serves `/status` JSON + a live dashboard at `http://<umbrel-ip>:3335/`. Per-miner stats (worker, IP, model, accepted/rejected, last-share, rolling hashrate est.) + node/job/server status; no secrets. Verified live with NerdQaxe++ (~7 TH/s, 0 rejects). Hashrate est. noisy early — smooth later (EWMA) or with vardiff.
- **2 — Real BCH block construction:** primitives DONE + unit-tested offline (2026-09-11): `cashaddr.py` (payout→script, 7/7), `coinbase.py` (BIP34 height + extranonce + value, no witness; parse-back verified), `merkle.py` (branch/root; cross-checked vs full-tree n=1..39). `block.py` (bits→target, header + full-block serialization) DONE + tested (real block #125552 known-answer). `template.py` (`BlockTemplate`: GBT→per-miner job) DONE + tested offline. **LIVE — real BCH solo mining active (2026-09-12).** Node fully synced; `job_manager` serves real per-miner `getblocktemplate` jobs; `on_submit` reconstructs via template, detects network-target blocks, calls `submitblock`. `proposal_test.py` returned **VALID** (node accepted a fully-assembled block — height 968279, 65 tx, coinbase ~3.1256 BCH to the user's address; `GBT_TXID_IS_DISPLAY=True` correct on first try). Bugfix: `BlockTemplate` needed `self.job_id = template_id` (JobManager keys sources on `.job_id`). **Ops note: apply compose changes with `docker compose up -d --force-recreate`** — a plain `restart` reloads bind-mounted code but does NOT apply volume/env changes (this silently broke `config.json` persistence until caught).
- **Docker (dev/standalone container): ✅ DONE (2026-09-11).** `Dockerfile` (python:3.13-slim, non-root, healthcheck) + `docker-compose.yml` (external `umbrel_main_network`, reaches `bitcoind:8332`, publishes 3334/3335, `restart: unless-stopped`) + secret in `~/bch-stratum/.env`. Verified `Up (healthy)`, container→node connection live, miner hashing. Ops: `cd ~/bch-stratum && sudo docker compose up -d --build | logs -f | restart | down`. Code changes: `scp` server files, `sudo docker compose restart` (bind-mounted `./server`, no rebuild).
- **Umbrel app packaging (pending):** `umbrel-app.yml`, app-proxy UI (status/miners/blocks/config), payout-address + settings config, optional Telegram. Do after Phase 2.

Ports in use by SoloBCH Forge: **3334** Stratum (miners), **3335** status dashboard + settings API. In the Umbrel app 3335 sits behind app_proxy auth; in the dev compose it is published to the LAN **unauthenticated**, so bind it to localhost there if the LAN is not trusted.

## Stack decision
- **Prototype: Python 3 + asyncio, standard library only** (asyncio, json, hashlib, struct). No pip deps in Phase 1A → nothing installed on the Umbrel host, maximum isolation, instant iteration.
- Small pure-Python helpers added later for CashAddr decode + block serialization (no heavy deps).
- **Production:** Python asyncio is fine for solo scale (a handful of miners on a Pi 5). A Go rewrite would only buy a single static binary + lower RAM for public distribution — a nice-to-have, not required. We revisit only if it matters.

## BCH-specific risks / notes (why this isn't "BTC with a renamed coin")
1. **No SegWit on BCH.** No witness commitment in the coinbase, no witness reserved value, no witness data. GBT must NOT request `segwit` rules. (This makes BCH coinbase construction *simpler* than modern BTC.)
2. **CashAddr payout address.** `bitcoincash:q...` must be decoded to hash160 → P2PKH scriptPubKey for the coinbase output. Only CashAddr is implemented; legacy Base58 usernames are rejected at authorize (users convert them first). Needs a CashAddr decoder (Phase 2).
3. **ASERT difficulty (aserti3-2d).** nBits comes straight from BCHN's GBT; the server just uses it. No custom DAA math needed.
4. **AxeOS/NerdQaxe expects `mining.configure` + version-rolling (ASICBoost/BIP310)** before subscribe. Phase 1A must answer `mining.configure` and honor the version mask, or behavior is flaky.
5. **Mock job must be structurally valid hex** (correct field lengths) or AxeOS may reject/disconnect even in Phase 1A.
6. **Same hashing as BTC:** SHA256d, 80-byte header — header/merkle code is shared with BTC; the *consensus/GBT/coinbase* details are where BCH differs.
7. **Verify BCHN's current `getblocktemplate` capabilities** before Phase 2 block building (do not assume BTC-Core GBT semantics).
8. **Mock-job `nBits` must be HARD, or miners/monitors report false blocks.** A miner (AxeOS) decides a share is a "block" by comparing it to the network target it reads from the job's `nBits`. Early mock jobs used `1d00ffff` (difficulty 1 — trivially easy), so *every* share looked like a block: the NerdQaxe's AxeOS and monitoring apps (e.g. Hashwatcher) flagged false "block found" events. **No block was ever built or submitted** (mock mode forces `is_block=False`; `submitblock` only runs in real/synced mode) — purely a cosmetic monitor false-positive. Fixed 2026-09-11 by setting mock `nBits = "1803ffff"` (~BCH-mainnet difficulty). In real mode `nBits` comes from the live template, so a block flag then is genuine.

## Phase 2 — BCH block construction reference (from BCHN getblocktemplate docs)
GBT result fields we use: `version`, `previousblockhash`, `bits`, `target`, `height`,
`coinbasevalue` (reward+fees, satoshis), `coinbaseaux`, `curtime`, `mintime`, `sizelimit`,
`sigoplimit` (**SigChecks** limit, BCH-specific), `transactions[]` (`data`,`txid`,`hash`,`fee`,`sigops`,`depends`,`required`).
- **NO SegWit:** no `default_witness_commitment`, no witness commitment output, no `rules:["segwit"]`. Request GBT with `{}`.
- **Coinbase:** version(4) + 1 input (null prevout, scriptSig = BIP34 height [CScript: 0x03 + 3-byte LE height] + extranonce1‖extranonce2 + miner tag) + output(s) [value=`coinbasevalue`, scriptPubKey=payout] + locktime(4). scriptSig 2–100 bytes.
- **Payout:** decode CashAddr (`bitcoincash:q...`) → hash160 → P2PKH `76a914{h160}88ac` (or P2SH `a914{h160}87`). Implemented + tested in `cashaddr.py` (7/7 vectors).
- **Payout model = ADDRESS-AS-USERNAME** (solo.ckpool style; user's choice 2026-09-11). Stratum username is `<BCH address>[.<worker>]`; the address becomes that miner's coinbase output. No server-default address. Authorize is REJECTED if the address isn't a valid BCH CashAddr — so a Bitcoin `bc1…` username is refused (would be unspendable on BCH). Prefix-less `q…` accepted (assumes `bitcoincash:`).
- **Block for submitblock:** header(80) + varint(tx_count) + coinbase_raw + each template tx `data`, concatenated hex.
- **Target/validation:** network target from `target`/`bits`; share target from configured share difficulty. Block if hash ≤ network target.
- Watch item: **May 2026 BCH upgrade** pending on this node (`upgrade_status.mempool_activated:false`) — revisit consensus deltas before mainnet block submission.

## Security posture
- BCHN RPC bound to localhost/Docker network only — never exposed.
- Stratum input validation, per-connection limits, malformed-message handling.
- RPC creds via config/env, never logged, never shown in UI.
- Minimal exposed ports (Stratum only, LAN).
