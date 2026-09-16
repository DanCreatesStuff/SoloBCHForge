# Security Policy

SoloBCH Forge handles a Bitcoin Cash node's RPC credentials and miners' payout
addresses, so security reports are taken seriously. Thank you for helping keep
the project and its users safe.

## Supported versions

Only the latest released version receives security fixes. Please make sure you
are on the newest version (update via the Umbrel community app store) before
reporting.

| Version | Supported |
| ------- | --------- |
| Latest release | ✅ |
| Older releases | ❌ |

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

Instead, use GitHub's private vulnerability reporting:

1. Go to the repository's **Security** tab →
   **[Report a vulnerability](https://github.com/DanCreatesStuff/SoloBCHForge/security/advisories/new)**.
2. Describe the issue, the impact, and steps to reproduce.

This opens a private channel visible only to the maintainer. Please include:

- A clear description of the vulnerability and its potential impact
- Steps to reproduce, or a proof of concept
- The version you tested and your environment (Umbrel app vs. standalone)
- Any suggested remediation, if you have one

## What to expect

- **Acknowledgement** as soon as the report is seen.
- An assessment of severity and a plan for a fix.
- A released patch and, with your permission, credit for the disclosure.

Please give a reasonable amount of time for a fix to ship before any public
disclosure.

## Scope notes

- SoloBCH Forge is **non-custodial** — it holds no wallet and no private keys.
  Miners are paid directly in the block's coinbase to the BCH address they
  authenticate with.
- The dashboard and settings pages are intended to run **behind Umbrel's
  app-proxy authentication** and on a trusted LAN. Exposing the status port
  (default `3335`) directly to the public internet is not a supported
  configuration.
- The node RPC password is stored in `config.json` on the app's persistent
  volume and is never returned by the status API or logged.
