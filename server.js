// Minimal WebSocket relay for the Friend Mod.
//
// Protocol (matches mod/src/main/java/com/friendmod/RelayClient.java exactly):
//   Client -> Server:
//     { "type": "hello",    "from": "username", "location": {"type":"menu","name":""} }
//     { "type": "presence", "from": "username", "location": {"type":"singleplayer"|"multiplayer"|"menu","name":"..."} }
//     { "type": "chat",     "from": "username", "to": "friendUsername", "message": "..." }
//     { "type": "lookup",   "from": "username", "target": "someName" }
//     { "type": "friend_request",          "from": "username", "to": "otherUsername" }
//     { "type": "friend_request_response", "from": "username", "to": "otherUsername", "accepted": true|false }
//     { "type": "who_here", "from": "username", "serverKey": "mc.example.com:25565" }
//     { "type": "voice_invite"|"voice_accept"|"voice_decline"|"voice_busy"|"voice_end",
//       "from": "username", "to": "otherUsername" }
//     <binary frame>  - one 20ms mu-law audio frame, forwarded verbatim to the paired peer
//
//   hello and presence additionally carry:
//     "serverKey": "<address you are connected to, lowercased, or empty>"
//     "discoverable": true|false
//
//   Server -> Client:
//     { "type": "presence", "from": "username", "status": "online"|"offline", "location": {...} }
//     { "type": "chat",     "from": "username", "message": "...", "timestamp": 1234567890 }
//     { "type": "lookup_result", "target": "someName", "online": true|false }
//     { "type": "friend_request",          "from": "username" }
//     { "type": "friend_request_response", "from": "username", "accepted": true|false }
//     { "type": "here_list", "players": ["name", ...] }
//     { "type": "voice_invite"|"voice_accept"|"voice_decline"|"voice_busy"|"voice_end", "from": "username" }
//     <binary frame>  - one 20ms mu-law audio frame from the peer you are in a call with
//
// No auth, no friend-graph on the server side — it just broadcasts presence to
// everyone connected and routes chat/requests by username. The mod itself only acts on
// messages from users already on (or being added to) your local friends list, so this
// stays simple on purpose. Good enough for two friends; don't expose this to a large
// group without adding real auth.
//
// "lookup" is a best-effort presence check, not a real account search: since cracked
// (offline) usernames aren't tied to any central account system, the server has no way
// to confirm a name "exists" beyond noticing it's currently connected. So lookup just
// reports whether that username is online right now - the client still lets you send a
// request to someone who comes back "not online", since they may just not be playing yet.
//
// friend_request / friend_request_response route the same way chat does: delivered
// immediately if the target is connected, queued to disk otherwise so it's waiting for
// them next time they open the game. Everything here is keyed by username alone, so
// this works identically for cracked (offline) and premium accounts - there's no Mojang
// auth involved anywhere in this protocol.

const http = require('http');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

// Voice control messages. Everything here is routed straight through by username; only
// voice_accept and voice_end/voice_decline additionally set or clear the audio pairing.
const VOICE_SIGNALS = new Set([
    'voice_invite', 'voice_accept', 'voice_decline', 'voice_busy', 'voice_end',
]);

// username (lowercase) -> { ws, username, location }
const clients = new Map();

// Queue chat messages for users who are currently offline so they get them
// on reconnect. Persisted to disk so a Render restart doesn't lose them.
const QUEUE_FILE = path.join(__dirname, 'offline_queue.json');
let offlineQueue = loadQueue();

function loadQueue() {
    try {
        if (fs.existsSync(QUEUE_FILE)) {
            return JSON.parse(fs.readFileSync(QUEUE_FILE, 'utf8'));
        }
    } catch (e) {
        console.error('Failed to load offline queue:', e);
    }
    return {}; // lowercase username -> [ full server->client payload, ... ]
}

function saveQueue() {
    try {
        fs.writeFileSync(QUEUE_FILE, JSON.stringify(offlineQueue));
    } catch (e) {
        console.error('Failed to save offline queue:', e);
    }
}

// Delivers a payload to targetKey right now if they're connected, otherwise queues the
// exact payload to send verbatim next time they say hello. Used for chat, friend
// requests, and request responses - anything that shouldn't just vanish because the
// other player wasn't online at that exact moment.
function routeOrQueue(targetKey, payload) {
    const target = clients.get(targetKey);
    if (target) {
        send(target.ws, payload);
        return;
    }
    if (!offlineQueue[targetKey]) offlineQueue[targetKey] = [];
    offlineQueue[targetKey].push(payload);
    saveQueue();
}

const server = http.createServer((req, res) => {
    // Simple health check so Render (and you) can confirm it's alive.
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('friendmod relay ok\n');
});

const wss = new WebSocketServer({ server });

function send(ws, obj) {
    if (ws.readyState === ws.OPEN) {
        ws.send(JSON.stringify(obj));
    }
}

function broadcastPresence(fromUsername, status, location) {
    const msg = { type: 'presence', from: fromUsername, status, location };
    for (const [key, client] of clients) {
        if (key !== fromUsername.toLowerCase()) {
            send(client.ws, msg);
        }
    }
}

wss.on('connection', (ws) => {
    let username = null;

    ws.on('message', (raw, isBinary) => {
        // Binary frames are audio and are handled by the dedicated listener below.
        if (isBinary) return;

        let data;
        try {
            data = JSON.parse(raw.toString());
        } catch (e) {
            return; // ignore malformed messages
        }
        if (!data || !data.type || !data.from) return;

        if (data.type === 'hello') {
            username = data.from;
            const key = username.toLowerCase();
            clients.set(key, {
                ws,
                username,
                location: data.location || { type: 'menu', name: '' },
                serverKey: data.serverKey || '',
                discoverable: data.discoverable !== false,
                // Set when a voice call is accepted; while it is set, binary frames from this
                // socket are forwarded straight to that peer. See the voice section below.
                voicePeer: null,
            });

            // Tell the joining client who is ALREADY here, before announcing them to
            // everyone else.
            //
            // Without this, presence only ever flows forwards in time: you hear about people
            // who connect after you, and never about people who were already on. Whoever
            // starts the game second therefore sees every friend as offline, which also
            // greys out anything gated on a friend being online - the Call button in
            // particular. Costs one message per connected user, once, at connect.
            for (const [otherKey, other] of clients) {
                if (otherKey === key) continue;
                send(ws, {
                    type: 'presence',
                    from: other.username,
                    status: 'online',
                    location: other.location || { type: 'menu', name: '' },
                });
            }

            broadcastPresence(username, 'online', data.location || { type: 'menu', name: '' });

            // Flush anything that arrived while this user was offline (chat, friend
            // requests, request responses - whatever got queued, sent verbatim).
            const queued = offlineQueue[key];
            if (queued && queued.length) {
                for (const payload of queued) {
                    send(ws, payload);
                }
                delete offlineQueue[key];
                saveQueue();
            }
            return;
        }

        if (!username) return; // must say hello first

        if (data.type === 'presence') {
            const key = username.toLowerCase();
            const entry = clients.get(key);
            if (entry) {
                entry.location = data.location;
                // Presence doubles as the "which server am I on" announcement, so the roster
                // below always answers against where people currently are.
                if (typeof data.serverKey === 'string') entry.serverKey = data.serverKey;
                if (typeof data.discoverable === 'boolean') entry.discoverable = data.discoverable;
            }
            broadcastPresence(username, 'online', data.location);
            return;
        }

        // "Who else on my server has FriendMod?" - the thing that makes adding someone one
        // click instead of asking them to spell their username. Only discoverable clients are
        // listed, and only discoverable clients get an answer, so opting out is symmetrical.
        if (data.type === 'who_here') {
            const serverKey = (data.serverKey || '').toLowerCase();
            const me = clients.get(username.toLowerCase());
            if (!serverKey || !me || !me.discoverable) {
                send(ws, { type: 'here_list', players: [] });
                return;
            }

            const players = [];
            for (const [key, client] of clients) {
                if (key === username.toLowerCase()) continue;
                if (!client.discoverable) continue;
                if ((client.serverKey || '').toLowerCase() !== serverKey) continue;
                players.push(client.username);
            }
            send(ws, { type: 'here_list', players });
            return;
        }

        // ----- voice signalling -----
        //
        // Pure routing: the relay never decides whether a call should happen, it just carries
        // invite/accept/decline/busy/end between two usernames. The one piece of state it does
        // keep is the voicePeer link, set on accept, which is what lets the audio frames below
        // be forwarded with no per-frame addressing at all.
        if (VOICE_SIGNALS.has(data.type)) {
            const toKey = (data.to || '').toLowerCase();
            const target = clients.get(toKey);
            const me = clients.get(username.toLowerCase());

            if (data.type === 'voice_accept' && target && me) {
                me.voicePeer = toKey;
                target.voicePeer = username.toLowerCase();
            }
            if (data.type === 'voice_end' || data.type === 'voice_decline') {
                if (me) me.voicePeer = null;
                if (target) target.voicePeer = null;
            }

            // Voice is realtime and pointless queued - if they are not connected right now,
            // tell the caller immediately rather than storing a call for later.
            if (target) {
                send(target.ws, { type: data.type, from: username });
            } else if (data.type === 'voice_invite') {
                // Distinct from voice_decline on purpose: "they are not online" and "they
                // said no" are different things and the caller deserves to be told which.
                send(ws, { type: 'voice_unavailable', from: data.to || '' });
            }
            return;
        }

        if (data.type === 'lookup') {
            const targetName = data.target || '';
            const targetKey = targetName.toLowerCase();
            send(ws, { type: 'lookup_result', target: targetName, online: clients.has(targetKey) });
            return;
        }

        if (data.type === 'chat') {
            const toKey = (data.to || '').toLowerCase();
            routeOrQueue(toKey, {
                type: 'chat',
                from: username,
                message: data.message,
                timestamp: Date.now(),
            });
            return;
        }

        if (data.type === 'friend_request') {
            const toKey = (data.to || '').toLowerCase();
            routeOrQueue(toKey, { type: 'friend_request', from: username });
            return;
        }

        if (data.type === 'friend_request_response') {
            const toKey = (data.to || '').toLowerCase();
            routeOrQueue(toKey, {
                type: 'friend_request_response',
                from: username,
                accepted: !!data.accepted,
            });
            return;
        }
    });

    // Audio frames. Deliberately not parsed: this is opaque mu-law that we forward to
    // whichever socket is currently paired with this one, or drop if there is no call. The
    // relay therefore never decodes, inspects, buffers or stores any voice data.
    ws.on('message', (raw, isBinary) => {
        if (!isBinary || !username) return;

        const me = clients.get(username.toLowerCase());
        if (!me || !me.voicePeer) return;

        const peer = clients.get(me.voicePeer);
        if (peer && peer.ws.readyState === peer.ws.OPEN) {
            peer.ws.send(raw, { binary: true });
        }
    });

    ws.on('close', () => {
        if (username) {
            const key = username.toLowerCase();
            const me = clients.get(key);

            // Dropping off mid-call has to end it on the other side too, or they sit in a
            // call with someone who is gone and no audio ever arrives.
            if (me && me.voicePeer) {
                const peer = clients.get(me.voicePeer);
                if (peer) {
                    peer.voicePeer = null;
                    send(peer.ws, { type: 'voice_end', from: username });
                }
            }

            clients.delete(key);
            broadcastPresence(username, 'offline', { type: 'menu', name: '' });
        }
    });
});

server.listen(PORT, () => {
    console.log(`friendmod relay listening on ${PORT}`);
});
