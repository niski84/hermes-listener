#!/usr/bin/env bash
# hermes-listener — kill → compile → start → poll /api/health
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="$PROJECT_DIR/hermes-listener"
PID_FILE="$PROJECT_DIR/hermes-listener.pid"
PORT="${PORT:-9120}"
LOG_FILE="$PROJECT_DIR/hermes-listener.log"

# Ensure Go toolchain is on PATH regardless of how this script was invoked.
export PATH="/usr/local/go/bin:$HOME/go/bin:$PATH"

echo "[hermes-listener] stopping..."
if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(cat "$PID_FILE")"
  kill "$OLD_PID" 2>/dev/null && sleep 0.5 || true
fi
# basename match handles both ./binary and /absolute/path invocations
pkill -f "$(basename "$BINARY")" 2>/dev/null && sleep 0.3 || true

echo "[hermes-listener] compiling..."
cd "$PROJECT_DIR"
go build -o "$BINARY" ./cmd/hermes-listener

echo "[hermes-listener] starting on port $PORT..."
PORT="$PORT" nohup "$BINARY" > "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"

echo "[hermes-listener] waiting for /api/health..."
for i in $(seq 1 30); do
  sleep 0.5
  if ! kill -0 "$NEW_PID" 2>/dev/null; then
    echo "✗ Process $NEW_PID died (port conflict or crash)"
    tail -5 "$LOG_FILE" 2>/dev/null | sed 's/^/  /' || true
    exit 1
  fi
  if curl -sf "http://localhost:$PORT/api/health" >/dev/null 2>&1; then
    echo "✓ Server ready at http://localhost:$PORT"
    exit 0
  fi
done
echo "✗ Server did not start — check $LOG_FILE"
exit 1
