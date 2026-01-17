# Git Hooks

This repo uses local Git hooks to reduce the risk of committing secrets or private data.

## Setup

Hooks are enabled by setting `core.hooksPath` to `.githooks`. Run:

```bash
git config core.hooksPath .githooks
```

## What it does

- Blocks common secret file types like `.env`, `*.pem`, `id_rsa`
- Scans staged diffs for common token patterns (AWS, GitHub, Slack, Stripe)

If you need to override a false positive, edit the hook or unstage the offending change.
