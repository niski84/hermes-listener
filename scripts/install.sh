#!/usr/bin/env bash
# hermes-listener installer.
#
# Downloads the latest prebuilt binary from GitHub releases (or builds from
# source if Go is available and no release matches). Optionally installs a
# systemd user service so the daemon auto-starts at login.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/niski84/hermes-listener/main/scripts/install.sh | bash
#
# Env overrides:
#   HERMES_LISTENER_VERSION=v0.1.0      pin to a specific release
#   HERMES_LISTENER_PREFIX=$HOME/.local install dir (default: $HOME/.local)
#   HERMES_LISTENER_NO_SERVICE=1        skip systemd unit install
#   HERMES_LISTENER_NO_START=1          install service but don't start

set -euo pipefail

REPO="niski84/hermes-listener"
PREFIX="${HERMES_LISTENER_PREFIX:-$HOME/.local}"
BIN_DIR="$PREFIX/bin"
SHARE_DIR="$PREFIX/share/hermes-listener"
BIN="$BIN_DIR/hermes-listener"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}⚠${NC} $*"; }
fail() { echo -e "${RED}✗${NC} $*" >&2; exit 1; }

# ────────────────────────────────────────────────────────────────────────
# 1. Detect platform
# ────────────────────────────────────────────────────────────────────────
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
    linux|darwin) ;;
    *) fail "unsupported OS: $OS (linux/darwin only)" ;;
esac

ARCH_RAW="$(uname -m)"
case "$ARCH_RAW" in
    x86_64|amd64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) fail "unsupported arch: $ARCH_RAW (amd64/arm64 only)" ;;
esac
ok "platform: ${OS}_${ARCH}"

# ────────────────────────────────────────────────────────────────────────
# 2. Resolve version
# ────────────────────────────────────────────────────────────────────────
if [ -n "${HERMES_LISTENER_VERSION:-}" ]; then
    VERSION="$HERMES_LISTENER_VERSION"
else
    VERSION="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
        | grep -oE '"tag_name":\s*"v[^"]+' \
        | sed 's/.*"v/v/' || true)"
fi

if [ -z "$VERSION" ]; then
    warn "could not detect latest release — falling back to build-from-source"
    BUILD_FROM_SOURCE=1
else
    ok "version: $VERSION"
    BUILD_FROM_SOURCE=0
fi

# ────────────────────────────────────────────────────────────────────────
# 3. Install binary
# ────────────────────────────────────────────────────────────────────────
mkdir -p "$BIN_DIR" "$SHARE_DIR"

if [ "$BUILD_FROM_SOURCE" = "1" ]; then
    command -v go >/dev/null 2>&1 \
        || fail "Go toolchain not found and no GitHub release available. Install Go 1.23+ first."
    SRC="$SHARE_DIR/src"
    if [ ! -d "$SRC/.git" ]; then
        git clone --depth 1 "https://github.com/$REPO.git" "$SRC"
    else
        (cd "$SRC" && git pull --ff-only --quiet)
    fi
    (cd "$SRC" && go build -o "$BIN" ./cmd/hermes-listener)
    ok "built from source → $BIN"
else
    # Strip leading 'v' for the archive name.
    V_NUM="${VERSION#v}"
    ARCHIVE="hermes-listener_${V_NUM}_${OS}_${ARCH}.tar.gz"
    URL="https://github.com/$REPO/releases/download/$VERSION/$ARCHIVE"
    TMP="$(mktemp -d)"
    trap 'rm -rf "$TMP"' EXIT

    echo "  downloading $URL"
    if ! curl -fsSL "$URL" -o "$TMP/archive.tar.gz"; then
        fail "download failed. Check that $VERSION has built assets at https://github.com/$REPO/releases"
    fi
    tar -xzf "$TMP/archive.tar.gz" -C "$TMP"
    install -m 0755 "$TMP/hermes-listener" "$BIN"
    # Stage docs + example env in the share dir for reference.
    install -m 0644 "$TMP/README.md" "$SHARE_DIR/README.md" 2>/dev/null || true
    install -m 0644 "$TMP/LICENSE" "$SHARE_DIR/LICENSE" 2>/dev/null || true
    install -m 0644 "$TMP/.env.example" "$SHARE_DIR/.env.example" 2>/dev/null || true
    ok "installed binary → $BIN"
fi

# Warn if $BIN_DIR isn't on PATH
case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *) warn "$BIN_DIR is not on your PATH. Add to your shell profile:"
       echo "      export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac

# ────────────────────────────────────────────────────────────────────────
# 4. systemd user service (Linux only, opt-out via env)
# ────────────────────────────────────────────────────────────────────────
if [ "$OS" = "linux" ] && [ "${HERMES_LISTENER_NO_SERVICE:-}" != "1" ] && command -v systemctl >/dev/null 2>&1; then
    UNIT="$HOME/.config/systemd/user/hermes-listener.service"
    mkdir -p "$(dirname "$UNIT")"
    if [ -f "$UNIT" ]; then
        ok "systemd unit already exists at $UNIT (leaving as-is)"
    else
        cat > "$UNIT" <<EOF
[Unit]
Description=hermes-listener — passive voice listener (mic → vault)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$BIN
WorkingDirectory=$HOME
Environment="PATH=$BIN_DIR:/usr/local/bin:/usr/bin:/bin"
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
EOF
        systemctl --user daemon-reload
        if [ "${HERMES_LISTENER_NO_START:-}" = "1" ]; then
            ok "wrote systemd unit (skipped start — set HERMES_LISTENER_NO_START=)"
        else
            systemctl --user enable --now hermes-listener.service
            ok "systemd unit installed and started"
        fi
    fi
fi

# ────────────────────────────────────────────────────────────────────────
# 5. Health probe
# ────────────────────────────────────────────────────────────────────────
sleep 1
if curl -sf -o /dev/null -m 3 "http://localhost:9120/api/health"; then
    ok "hermes-listener is responding at http://localhost:9120/"
else
    warn "hermes-listener not yet responding on :9120. Check 'journalctl --user -u hermes-listener -f'"
fi

cat <<EOF

──────────────────────────────────────────────────────────────────
✓ hermes-listener installed.

Web settings UI:   http://localhost:9120/
Health check:      http://localhost:9120/api/health
Daily transcripts: \$VAULT_PATH/listener/YYYY-MM-DD-transcript.md
                   (defaults to ~/Documents/vault/listener/)

Optional sidecars (improve quality, see README):
  - whisper.cpp server   :9000  (required for transcription)
  - openWakeWord         :9201  (transcribe only after wake word)
  - ECAPA speaker filter :9200  (drop non-owner voices)
  - smart-turn           :9202  (better end-of-utterance detection)

Logs:
  journalctl --user -u hermes-listener -f

Restart:
  systemctl --user restart hermes-listener
──────────────────────────────────────────────────────────────────
EOF
