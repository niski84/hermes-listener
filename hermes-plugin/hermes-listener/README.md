# hermes-listener Hermes plugin

A thin Python bridge that exposes a `/listener` slash command in Hermes Agent. The plugin doesn't ship the capture pipeline itself — that's the `hermes-listener` Go binary. This is just a Hermes-side command surface so users can check status, browse settings, and verify sidecars from any chat.

## Install

```bash
# 1. Make sure the hermes-listener daemon is installed and running.
#    See https://github.com/niski84/hermes-listener for setup.

# 2. Symlink (or copy) this directory into your Hermes plugins folder:
ln -s ~/goprojects/hermes-listener/hermes-plugin/hermes-listener \
      ~/.hermes/plugins/hermes-listener

# 3. Restart Hermes (gateway picks it up automatically).
systemctl --user restart hermes-gateway

# 4. Verify
hermes plugins list | grep hermes-listener
```

## Use

In any Hermes session (CLI, Matrix, Telegram, dashboard):

```
/listener            # status + channel info
/listener ui         # print web UI URL
/listener probe      # check sidecar reachability
/listener mic        # show current mic + alternates
/listener help       # all subcommands
```

## Config

| Env var | Default | Purpose |
|---|---|---|
| `HERMES_LISTENER_URL` | `http://localhost:9120` | Where the listener daemon is reachable |

## License

MIT
