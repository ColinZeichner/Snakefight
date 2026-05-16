// Snake Fight — Multiplayer Server
// Run with: node server.js

const http    = require('http');
const fs      = require('fs');
const path    = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// ── HTTP server: serves the game HTML file ──
const httpServer = http.createServer((req, res) => {
  const filePath = path.join(__dirname, 'snakefight.html');
  if (!fs.existsSync(filePath)) {
    res.writeHead(404); res.end('snakefight.html not found — place it in the same folder as server.js'); return;
  }
  res.writeHead(200, { 'Content-Type': 'text/html' });
  fs.createReadStream(filePath).pipe(res);
});

// ── WebSocket server ──
const wss = new WebSocketServer({ server: httpServer });

// Room system — each room is a game lobby
const rooms = new Map(); // roomCode => { players: Map<id, ws>, state: {} }

let nextId = 1;
function genId()   { return nextId++; }
function genRoom() { return Math.random().toString(36).slice(2,7).toUpperCase(); }

function broadcast(room, data, excludeId = null) {
  const msg = JSON.stringify(data);
  room.players.forEach((ws, id) => {
    if (id !== excludeId && ws.readyState === 1) ws.send(msg);
  });
}

function roomInfo(room) {
  const players = [];
  room.players.forEach((ws, id) => {
    players.push({ id, name: ws.playerName, color: ws.playerColor, ready: ws.ready });
  });
  return players;
}

wss.on('connection', (ws) => {
  ws.id = genId();
  ws.roomCode = null;
  ws.playerName = 'PLAYER';
  ws.playerColor = '#00f5ff';
  ws.ready = false;
  ws.alive = true;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'ping': {
        ws.send(JSON.stringify({ type: 'pong' }));
        break;
      }

      // Client wants to create a new room
      case 'create': {
        const code = genRoom();
        rooms.set(code, { players: new Map(), host: ws.id, started: false, gameState: {} });
        const room = rooms.get(code);
        ws.roomCode = code;
        ws.playerName  = msg.name  || 'PLAYER';
        ws.playerColor = msg.color || '#00f5ff';
        room.players.set(ws.id, ws);
        ws.send(JSON.stringify({ type: 'joined', code, yourId: ws.id, isHost: true }));
        ws.send(JSON.stringify({ type: 'roster', players: roomInfo(room) }));
        console.log(`Room ${code} created by ${ws.playerName}`);
        break;
      }

      // Client wants to join an existing room
      case 'join': {
        const code = (msg.code || '').toUpperCase();
        const room = rooms.get(code);
        if (!room) { ws.send(JSON.stringify({ type: 'error', msg: 'Room not found' })); return; }
        if (room.started) { ws.send(JSON.stringify({ type: 'error', msg: 'Game already started' })); return; }
        if (room.players.size >= 8) { ws.send(JSON.stringify({ type: 'error', msg: 'Room is full (max 8)' })); return; }
        ws.roomCode = code;
        ws.playerName  = msg.name  || 'PLAYER';
        ws.playerColor = msg.color || '#ff2d6e';
        room.players.set(ws.id, ws);
        ws.send(JSON.stringify({ type: 'joined', code, yourId: ws.id, isHost: room.host === ws.id }));
        broadcast(room, { type: 'roster', players: roomInfo(room) });
        console.log(`${ws.playerName} joined room ${code} (${room.players.size} players)`);
        break;
      }

      // Host starts the game
      case 'start': {
        const room = rooms.get(ws.roomCode);
        if (!room || room.host !== ws.id) return;
        room.started = true;
        const playerList = [];
        room.players.forEach((p, id) => {
          playerList.push({ id, name: p.playerName, color: p.playerColor });
        });
        broadcast(room, { type: 'start', players: playerList, settings: msg.settings || {} });
        console.log(`Room ${ws.roomCode} game started with ${room.players.size} players`);
        break;
      }

      // Player sends their snake position/state every frame
      case 'state': {
        const room = rooms.get(ws.roomCode);
        if (!room || !room.started) return;
        // Relay to everyone else in the room
        broadcast(room, {
          type: 'state',
          id: ws.id,
          head: msg.head,
          segs: msg.segs,       // send a thinned array for bandwidth
          angle: msg.angle,
          boosting: msg.boosting,
          score: msg.score,
          alive: msg.alive,
          color: msg.color,
          name: msg.name
        }, ws.id);
        break;
      }

      // Player reports a kill (they hit someone)
      case 'kill': {
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        // Tell the victim they died
        const victim = room.players.get(msg.victimId);
        if (victim && victim.readyState === 1) {
          victim.send(JSON.stringify({ type: 'killed', killerId: ws.id, killerName: ws.playerName }));
        }
        broadcast(room, { type: 'kill', killerId: ws.id, victimId: msg.victimId,
          killerName: ws.playerName, victimName: msg.victimName }, null);
        break;
      }

      // Player respawned
      case 'respawn': {
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        broadcast(room, { type: 'respawn', id: ws.id }, ws.id);
        break;
      }

      // Chat message (optional but handy)
      case 'chat': {
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        const text = (msg.text || '').slice(0, 120);
        broadcast(room, { type: 'chat', id: ws.id, name: ws.playerName, text }, ws.id);
        break;
      }

      // Player changes their name/color in lobby
      case 'update': {
        if (msg.name)  ws.playerName  = msg.name;
        if (msg.color) ws.playerColor = msg.color;
        const room = rooms.get(ws.roomCode);
        if (room) broadcast(room, { type: 'roster', players: roomInfo(room) });
        break;
      }
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    room.players.delete(ws.id);
    broadcast(room, { type: 'leave', id: ws.id, name: ws.playerName });
    broadcast(room, { type: 'roster', players: roomInfo(room) });
    // If host left, assign new host
    if (room.host === ws.id && room.players.size > 0) {
      room.host = room.players.keys().next().value;
      const newHost = room.players.get(room.host);
      if (newHost) newHost.send(JSON.stringify({ type: 'promoted' }));
    }
    // Clean up empty rooms
    if (room.players.size === 0) {
      rooms.delete(ws.roomCode);
      console.log(`Room ${ws.roomCode} closed (empty)`);
    }
    console.log(`${ws.playerName} disconnected`);
  });

  ws.on('error', () => {});
});

httpServer.listen(PORT, () => {
  console.log('');
  console.log('  Snake Fight server running');
  console.log(`  Local:   http://localhost:${PORT}`);
  console.log('  Share your Railway/Render URL with friends');
  console.log('');
});
