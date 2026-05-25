<div align="center">

# 🎙 hermes-listener

### Passive voice listener for AI agent frameworks. Mic → text in your vault.

[![Go 1.23](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat-square&logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)
[![Status: Early WIP](https://img.shields.io/badge/Status-Early%20WIP-orange?style=flat-square)](#current-status)

</div>

---

A single-binary passive voice listener. Opens a microphone, runs wake-word + VAD + speaker filtering + speech-to-text, and writes time-stamped markdown to a folder.

That folder is usually an Obsidian vault — but it's just files on disk, so any agent that can read files becomes your voice-aware second brain. No API contract, no shared database, no IPC. Just markdown.

```
┌─────────────────────────────┐                ┌─────────────────────────────┐
│ hermes-listener             │                │  Your agent of choice       │
│                             │   filesystem   │                             │
│  mic → VAD → wake-word →    │  ───────────►  │  - Hermes Agent             │
│  whisper STT → speaker      │   markdown     │  - Claude Code              │
│  filter → markdown file     │     files      │  - ChatGPT desktop          │
│                             │                │  - your own scripts         │
└─────────────────────────────┘                └─────────────────────────────┘
```

## Why this exists

Most "AI second brain" projects bundle capture, storage, intelligence, and UI into one monolith. Then they get stuck on whichever layer is hardest to maintain (usually the intelligence layer, which frontier models keep obsoleting). This project does *only* capture and writes markdown. Your agent does the thinking — replace it whenever the next better one ships.

## Current status

**Early WIP — scaffold only as of {{date}}.**

This repo was extracted from [²nd-whisper-brain](https://github.com/niski84/2nd-whisper-brain) — an 18-month research project that grew into a sprawling personal-AI system. The capture pipeline there is excellent and battle-tested. This repo is the focused product version: capture only, no intelligence layer, no bespoke web UI, no special-purpose classifiers.

**Porting in progress:**
- [ ] Audio channel (mic → VAD → clip staging)
- [ ] Wake-word detection (openWakeWord sidecar)
- [ ] Speaker filter (ECAPA voiceprint matching, drops non-owner voices)
- [ ] Whisper.cpp STT integration with bounded worker pool
- [ ] TV chatter rejection (Plex caption diff)
- [ ] Daily transcript writer (`vault/listener/YYYY-MM-DD.md`)
- [ ] Web settings UI (`:9120` — mic device, wake words, vault path)
- [ ] Hermes plugin manifest (`plugin.yaml`)
- [ ] `/listener` slash command for Hermes
- [ ] install.sh one-liner
- [ ] GoReleaser CI for prebuilt binaries

## Install

### One-line (Linux + macOS)

```bash
curl -fsSL https://raw.githubusercontent.com/niski84/hermes-listener/main/scripts/install.sh | bash
```

Downloads the latest prebuilt binary from the [GitHub releases](https://github.com/niski84/hermes-listener/releases), drops it in `~/.local/bin/hermes-listener`, installs a systemd user service (Linux), and starts it on `:9120`. Opt-out env vars: `HERMES_LISTENER_NO_SERVICE=1`, `HERMES_LISTENER_NO_START=1`, `HERMES_LISTENER_VERSION=vX.Y.Z`.

### From a release archive

Grab the tarball for your platform from [releases](https://github.com/niski84/hermes-listener/releases) and extract it anywhere — the binary is a single static file.

```bash
# example
curl -L https://github.com/niski84/hermes-listener/releases/download/v0.1.0/hermes-listener_0.1.0_linux_amd64.tar.gz | tar xz
./hermes-listener
```

### From source

```bash
git clone https://github.com/niski84/hermes-listener.git
cd hermes-listener && go build -o hermes-listener ./cmd/hermes-listener
./hermes-listener
```

After install, settings live at **http://localhost:9120/** — pick your mic, set the vault path, configure optional sidecars. Listener writes to `$VAULT_PATH/listener/YYYY-MM-DD-transcript.md` by default. Any agent pointed at that vault sees the transcripts automatically.

## Architecture

```
cmd/hermes-listener/main.go       entry point
internal/api/                     HTTP server, settings, /api/health
internal/audio/                   mic capture, VAD, clip staging
internal/whisper/                 whisper.cpp client + worker pool
internal/speakerfilter/           ECAPA voiceprint match
internal/wakeword/                openWakeWord sidecar client
internal/output/                  markdown writer (vault/listener/*.md)
internal/config/                  env vars, settings persistence
web/                              settings page (templ + htmx + alpine)
scripts/reload.sh                 dev kill → build → start → health check
```

## Dependencies (planned)

- **Go 1.23+**
- **whisper.cpp server** (`whisper-server`) — local STT, OpenAI Whisper API, or Groq Whisper
- **openWakeWord sidecar** (optional, for wake-word triggering)
- **ECAPA speaker sidecar** (optional, for voiceprint filtering)
- **ffmpeg** (audio conversion)

All optional dependencies have sensible no-op fallbacks — bare minimum is just `whisper.cpp` + a microphone.

## License

MIT. See `LICENSE`.

## Lineage

- 🧪 **Research project:** [²nd-whisper-brain](https://github.com/niski84/2nd-whisper-brain) — the 18-month exploration that proved out the capture pipeline.
- 🤖 **Primary integration target:** [Hermes Agent](https://github.com/NousResearch/hermes-agent) — Nous Research's open-source AI agent framework with persistent memory and an Obsidian skill that reads your vault.
