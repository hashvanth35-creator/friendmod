// Simple relay server for the FriendMod Minecraft mod.
// It does one job: when one connected client sends a JSON message,
// forward it to every OTHER connected client. That's enough for two
// (or a handful of) friends to see status + chat across the internet,
// without either of you needing to read a file off the other's PC.
//
// Run with: node server.js
// Requires: npm install ws

const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// username -> ws connection
const clients = new Map();

function broadcastExcept(senderWs, payload) {
  const data = JSON.stringify(payload);
  for (const [name, ws] of clients.entries()) {
    if (ws !== senderWs && ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  }
}

wss.on('connection', (ws) => {
  let username = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return; // ignore garbage
    }

    if (msg.type === 'hello' && typeof msg.from === 'string') {
      username = msg.from;
      clients.set(username, ws);
      console.log(`[+] ${username} connected (${clients.size} online)`);
      // Tell everyone else this user just came online
      broadcastExcept(ws, {
        type: 'presence',
        from: username,
        status: 'online',
        location: msg.location || { type: 'menu', name: '' },
        timestamp: Date.now(),
      });
      return;
    }

    if (!username) return; // must say hello first

    if (msg.type === 'presence') {
      broadcastExcept(ws, {
        type: 'presence',
        from: username,
        status: 'online',
        location: msg.location || { type: 'menu', name: '' },
        timestamp: Date.now(),
      });
    } else if (msg.type === 'chat') {
      broadcastExcept(ws, {
        type: 'chat',
        from: username,
        to: msg.to,
        message: msg.message,
        timestamp: Date.now(),
      });
    }
  });

  ws.on('close', () => {
    if (username) {
      clients.delete(username);
      console.log(`[-] ${username} disconnected (${clients.size} online)`);
      broadcastExcept(ws, {
        type: 'presence',
        from: username,
        status: 'offline',
        location: { type: 'menu', name: '' },
        timestamp: Date.now(),
      });
    }
  });
});

console.log(`FriendMod relay server listening on ws://0.0.0.0:${PORT}`);
