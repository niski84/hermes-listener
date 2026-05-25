"""hermes-listener plugin — slash command bridge to the listener service.

Wires a `/listener` slash command into Hermes that talks HTTP to the
local hermes-listener daemon (default http://localhost:9120).

The listener itself is a separate Go binary — this plugin doesn't run
the capture pipeline; it just exposes Hermes-side commands so the user
can check status, browse settings, or open the web UI without leaving
the chat.

Usage inside Hermes (any platform):

    /listener                  → show status, channel info, recent activity
    /listener ui               → print the URL to the web settings UI
    /listener probe            → check reachability of all sidecars
    /listener mic              → show current mic + list alternates
    /listener help             → show this list

Requires the hermes-listener daemon to be running. If it isn't, every
command returns a clear hint with the install / start instructions.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional
from urllib import error as urlerror
from urllib import request as urlrequest

logger = logging.getLogger(__name__)

# Override with HERMES_LISTENER_URL — useful when running multiple
# listener instances or behind a reverse proxy.
DEFAULT_BASE_URL = os.getenv("HERMES_LISTENER_URL", "http://localhost:9120")

# ─── HTTP helpers ──────────────────────────────────────────────────────


def _get(path: str, timeout: float = 3.0) -> Optional[Any]:
    """GET a JSON endpoint on the listener. Returns parsed JSON or None
    on any failure (which the caller is expected to handle as "daemon
    unreachable")."""
    url = DEFAULT_BASE_URL.rstrip("/") + path
    try:
        with urlrequest.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urlerror.URLError, ConnectionError, TimeoutError, ValueError) as e:
        logger.debug("hermes-listener GET %s failed: %s", url, e)
        return None


def _daemon_down_hint() -> str:
    return (
        f"hermes-listener daemon is not responding at {DEFAULT_BASE_URL}.\n\n"
        "Start it with one of:\n"
        "  systemctl --user start hermes-listener     # if installed as a service\n"
        "  cd ~/goprojects/hermes-listener && ./hermes-listener\n\n"
        "Or set HERMES_LISTENER_URL=http://host:port if it's running elsewhere."
    )


# ─── slash subcommands ────────────────────────────────────────────────


def _cmd_status() -> str:
    """Default subcommand — `/listener` with no args."""
    health = _get("/api/health")
    if not health:
        return _daemon_down_hint()

    lines = [f"🎙 hermes-listener — {health.get('status', 'unknown')}"]
    channels = health.get("channels", [])
    if not channels:
        lines.append("  No channels configured.")
    else:
        for ch in channels:
            run = "running" if ch.get("running") else "stopped"
            lines.append(
                f"  • {ch.get('id', '?')} ({ch.get('type', '?')}) — {run}, "
                f"{ch.get('utterances_count', 0)} utterances, "
                f"{ch.get('filtered_count', 0)} filtered"
            )
    lines.append("")
    lines.append(f"Web UI: {DEFAULT_BASE_URL}")
    lines.append("Try: /listener probe   /listener ui   /listener mic   /listener help")
    return "\n".join(lines)


def _cmd_ui() -> str:
    return (
        f"Settings UI: {DEFAULT_BASE_URL}\n"
        "Sections: Microphone, Whisper STT, Vault, Wake-word, Speaker filter, Plex/TV filter, Smart turn.\n"
        "Sidecar install snippets are included for each optional component."
    )


def _cmd_probe() -> str:
    probes = _get("/api/probe")
    if probes is None:
        return _daemon_down_hint()
    lines = ["🔌 Sidecar reachability:"]
    for p in probes:
        status = "✓ reachable" if p.get("ok") else ("✗ " + (p.get("error") or "unreachable"))
        url = p.get("url") or "(not configured)"
        lines.append(f"  {p.get('name', '?'):14s}  {status}   {url}")
    return "\n".join(lines)


def _cmd_mic() -> str:
    health = _get("/api/health")
    mics = _get("/api/mics")
    if health is None or mics is None:
        return _daemon_down_hint()
    lines = ["🎤 Microphone:"]
    channels = health.get("channels", [])
    if channels:
        ch = channels[0]
        device = ch.get("config", {}).get("device", "default")
        lines.append(f"  Active: {device}")
    if mics:
        lines.append(f"  Available ({len(mics)}):")
        for m in mics[:10]:
            lines.append(f"    • {m.get('name', '?')}")
        if len(mics) > 10:
            lines.append(f"    … and {len(mics) - 10} more")
    lines.append(f"\nChange via the Microphone section at {DEFAULT_BASE_URL}")
    return "\n".join(lines)


def _cmd_help() -> str:
    return (
        "hermes-listener — passive voice listener\n\n"
        "Subcommands:\n"
        "  /listener            — status + channel info\n"
        "  /listener ui         — print the settings web UI URL\n"
        "  /listener probe      — check reachability of all sidecars\n"
        "  /listener mic        — show current microphone + alternates\n"
        "  /listener help       — this message\n"
        "\n"
        f"Web UI: {DEFAULT_BASE_URL}\n"
        "Docs:   https://github.com/niski84/hermes-listener"
    )


_SUBCOMMANDS = {
    "": _cmd_status,
    "status": _cmd_status,
    "ui": _cmd_ui,
    "web": _cmd_ui,
    "probe": _cmd_probe,
    "sidecars": _cmd_probe,
    "mic": _cmd_mic,
    "mics": _cmd_mic,
    "help": _cmd_help,
    "?": _cmd_help,
}


# ─── slash dispatcher ─────────────────────────────────────────────────


def _handle_slash(args: str = "", **_kwargs) -> str:
    sub = (args or "").strip().split(None, 1)
    name = sub[0].lower() if sub else ""
    handler = _SUBCOMMANDS.get(name)
    if handler is None:
        return (
            f"Unknown subcommand: '{name}'.\n"
            f"Try one of: {', '.join(sorted(set(_SUBCOMMANDS.keys()) - {''}))}"
        )
    try:
        return handler()
    except Exception as e:
        logger.exception("hermes-listener slash handler crashed")
        return f"hermes-listener plugin error: {e}"


# ─── plugin entry point ───────────────────────────────────────────────


def register(ctx) -> None:
    """Hermes plugin entry point. Called once at plugin discovery time."""
    ctx.register_command(
        "listener",
        handler=_handle_slash,
        description="Status, settings UI, and sidecar probes for the hermes-listener daemon.",
    )
    logger.info("hermes-listener plugin registered (daemon URL: %s)", DEFAULT_BASE_URL)
