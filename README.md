<p align="center">
  <img src="solobch-forge/icon.svg" width="128" height="128" alt="SoloBCH Forge">
</p>

<h1 align="center">SoloBCH Forge</h1>

<p align="center">
  <strong>Solo mine Bitcoin Cash to your own node — no pool, no fees, no middleman.</strong>
</p>

---

SoloBCH Forge is a lightweight, self-hosted **Bitcoin Cash (BCH) solo-mining Stratum
server** for [Umbrel](https://umbrel.com). Point your ASIC miner at it and mine BCH
directly to your own **Bitcoin Cash Node** — if your hardware finds a block, the full
reward goes straight to your address.

```
ASIC miner (Stratum V1)  →  SoloBCH Forge  →  Bitcoin Cash Node (JSON-RPC)  →  BCH network
```

## Features

- **True solo mining** — no pool, no fees, no shared rewards. You against the network.
- **Address-as-username payouts** — set your miner's Stratum username to your BCH
  address; that address is paid in the coinbase. No server-side wallet, no custody.
- **Works with common ASICs** — NerdQaxe++ / AxeOS, Bitaxe, and other Stratum V1 miners.
- **Live dashboard** — node sync status, connected miners, per-worker hashrate and
  shares, and any blocks found, behind Umbrel's built-in authentication.
- **Your node, your rules** — connects to your existing Bitcoin Cash Node app; RPC
  credentials never leave your Umbrel and are never shown in the UI.
- **Lightweight** — pure-Python, standard-library Stratum server; small multi-arch
  image (amd64 + arm64), friendly to a Raspberry Pi.

## Requirements

- An **Umbrel** home server (Raspberry Pi or Umbrel Home / x86).
- The **Bitcoin Cash Node** app installed and **fully synced** (required to build real
  blocks; while the node is still syncing the server runs in a safe mock mode).
- A **Stratum V1 ASIC miner** on the same LAN as your Umbrel.

## Install

This app is distributed through a **community app store**. In Umbrel:

1. Open the **App Store**, scroll to the bottom, and choose **Community App Stores**.
2. Add this repository URL:
   ```
   https://github.com/DanCreatesStuff/SoloBCHForge
   ```
3. Open the **SoloBCH App Store** that appears and install **SoloBCH Forge**.

> Requires the **Bitcoin Cash Node** app — Umbrel will list it as a dependency.

## Configure

1. Open **SoloBCH Forge** from your Umbrel dashboard — it lands on the status page.
2. Go to **⚙ Settings** and paste your node's RPC password. Find it in the
   **Bitcoin Cash Node** app under its **"Node RPC — for apps"** panel. (Host, port,
   and user default to the Bitcoin Cash Node app and rarely need changing.)
3. Point your miner at SoloBCH Forge:
   - **URL / host:** `stratum+tcp://<your-umbrel-ip>:3334`
   - **Username / worker:** your BCH address, optionally with a worker name —
     `bitcoincash:qxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.rig1`
   - **Password:** anything (ignored).

Only a valid BCH CashAddr is accepted as the username — a Bitcoin `bc1…` address is
rejected on purpose, since it would be unspendable on BCH.

## Ports

| Port   | Purpose                        | Exposure                         |
|--------|--------------------------------|----------------------------------|
| `3334` | Stratum (miners connect here)  | Your LAN                         |
| `3335` | Status + settings dashboard    | Umbrel app UI (authenticated)    |

The Bitcoin Cash Node RPC is reached over Umbrel's internal Docker network and is
never exposed to the LAN.

## How solo mining works (read this first)

Solo mining is a **lottery**. With modest hardware you may go a very long time — often
far longer than the hardware's lifetime — without finding a block. When a block *is*
found, you keep the **entire** reward instead of a pool's fractional payout. Run this
for sovereignty, learning, and the lottery ticket — not as an income plan.

No block is ever submitted while the node is syncing or in mock mode; real
`submitblock` calls only happen once your node is fully synced.

## Security

- RPC credentials are entered in the app, stored in the app's private data volume, and
  **never logged or displayed**.
- The Stratum port is LAN-only; the RPC connection stays on Umbrel's internal network.
- The container runs as a non-root user with a minimal image.

## Development

Architecture, phase history, and BCH-specific implementation notes live in
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md). The multi-arch image is built and pushed to
GHCR by [.github/workflows/docker-publish.yml](.github/workflows/docker-publish.yml) on
each `v*.*.*` tag.

## Disclaimer

Provided **as is**, without warranty of any kind. You are responsible for your node,
your miner, and your funds. This project is not affiliated with Umbrel, Bitcoin Cash
Node, or any miner manufacturer.

## License

[MIT](LICENSE) © 2026 DanCreatesStuff
