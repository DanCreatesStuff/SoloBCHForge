# Contributing to SoloBCH Forge

Thanks for your interest in improving SoloBCH Forge! This is a small,
self-hosted Bitcoin Cash solo-mining Stratum server for Umbrel, and
contributions of all sizes are welcome — bug reports, docs, and code.

## Ways to help

- **Report a bug** — open a [bug report](https://github.com/DanCreatesStuff/SoloBCHForge/issues/new/choose).
- **Suggest a feature** — open a [feature request](https://github.com/DanCreatesStuff/SoloBCHForge/issues/new/choose).
- **Improve the docs** — even fixing a typo in the README helps.
- **Send a pull request** — see the workflow below.
- **Report a security issue** — please **don't** open a public issue; see
  [SECURITY.md](SECURITY.md).

## Project layout

```
server/                 # the Stratum server (stdlib-only Python 3)
  stratum.py            # entrypoint: Stratum V1 protocol + connection handling
  job_manager.py        # node polling, job/template management, webhooks
  status.py             # status HTTP service + dashboard + settings pages
  config.py             # settings (defaults < config.json < env vars)
  stats.py              # persistent lifetime stats (best diff, blocks, totals)
  bch_rpc.py, block.py, coinbase.py, merkle.py, cashaddr.py, template.py
Dockerfile              # multi-arch image (python:3.13-slim)
docker-compose.yml      # dev/standalone compose
solobch-forge/          # the Umbrel community-app manifest (umbrel-app.yml + compose)
.github/workflows/      # CI: build & push the multi-arch image on a version tag
```

## Design principles

- **Standard library only.** The server has no third-party Python dependencies,
  which keeps the image tiny and arm64-friendly (Raspberry Pi 5). Please don't
  add pip dependencies without discussing it first in an issue.
- **Single asyncio thread.** All live state (connected miners, jobs) is read and
  written on one event loop, so no locking is needed. RPC calls run in a thread
  executor so they never block the loop. Keep new work off the loop the same way.
- **Non-custodial.** Miners are paid directly in the coinbase via the
  address-as-username model. There is no server-side wallet — please keep it
  that way.
- **Persistent vs. session state.** Lifetime achievements (best difficulty,
  blocks found, lifetime shares) live in `stats.json`; live numbers (hashrate,
  connected workers) are per-session. Put new "never lose this" values in the
  stats store, everything else stays in RAM.

## Development setup

You can run the server locally with Docker Compose against a reachable Bitcoin
Cash Node:

```bash
# from the repo root
echo "BCHN_RPC_PASSWORD=your-node-rpc-password" > .env
docker compose up --build
```

- Stratum listens on `:3334`, the dashboard on `http://localhost:3335/`.
- The dev compose bind-mounts `./server`, so editing a file and running
  `docker compose restart` picks it up without a rebuild.
- Point a miner (or test harness) at `stratum+tcp://<host>:3334` with username
  `<your-bch-address>.worker`.

No node handy? The server serves a structurally-valid **mock** job until the
node is synced, so the connection/submit path can be exercised without real
templates.

## Pull request workflow

1. **Open an issue first** for anything beyond a small fix, so we can agree on
   the approach before you spend time on it.
2. Fork the repo and create a branch off `main`.
3. Make your change. Match the surrounding style: clear names, comments that
   explain *why* (not *what*), and stdlib only.
4. Test against a real or mock node where you can, and note what you verified in
   the PR description.
5. Open a PR against `main` and fill in the template. Keep PRs focused — one
   logical change per PR is much easier to review.

Please do **not** bump the app `version:` or the image tag in your PR — releases
are cut by the maintainer (see below).

## How releases work (maintainer)

The repo is both the source and the Umbrel community-app store:

1. Bump `version:` and `releaseNotes` in `solobch-forge/umbrel-app.yml`, and the
   image tag in `solobch-forge/docker-compose.yml`.
2. Commit, then push a `vX.Y.Z` tag — CI builds and pushes the multi-arch image
   to GHCR (`ghcr.io/dancreatesstuff/solobch-forge`).
3. Once the image is published, Umbrel shows an **Update** for the app.

Manifest-only changes (compose env, docs) don't need a new image; code changes
under `server/` do, and therefore need a version tag.

## Code of Conduct

By participating, you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE) that covers this project.
