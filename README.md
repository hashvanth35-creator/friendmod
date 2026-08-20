# FriendMod Relay Server

Since you and your friend are on different networks, your two game clients
can't just read a file off each other's computer — something reachable by
both of you has to sit in the middle and pass messages back and forth. That's
all this is: a ~70 line script that forwards messages between whoever is
connected.

## Run it locally first (to test)

```bash
cd relay-server
npm install
node server.js
```

You should see:
```
FriendMod relay server listening on ws://0.0.0.0:8080
```

## Hosting it so your friend can reach it over the internet

Pick ONE of these:

1. **Free hosting (easiest):** Deploy this folder to [Render](https://render.com),
   [Railway](https://railway.app), or [Fly.io](https://fly.io) as a Node.js
   web service ("start command": `node server.js`). They'll give you a public
   URL — use the `wss://` version of it in the mod config.
2. **Run it on your own PC:** Run `node server.js` on your machine and port-forward
   port 8080 on your router to your PC, then give your friend `ws://YOUR_PUBLIC_IP:8080`.
   This only works while your PC is on and the script is running.
3. **Run it on a cheap VPS** (DigitalOcean, etc.) — same as #2 but the box is
   always on.

Whichever you pick, both of you put the same server address into the mod's
`relay_server` setting (see the mod's SETUP_GUIDE).

Note: option 1 with a real domain gets you `wss://` (encrypted). Options 2/3
with a bare IP will be `ws://` (unencrypted) — fine for a friend-status/chat
toy, but don't put anything sensitive through it.
