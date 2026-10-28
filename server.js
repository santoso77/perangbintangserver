// Server multiplayer Perang Bintang: relay posisi dan event antar pemain.
// Opsional: set ALLOWED_ORIGINS="https://domainmu.com" supaya hanya situsmu yang boleh terhubung.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ALLOW = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const MAX_PLAYERS = 16;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Server Perang Bintang aktif. Pemain online: ' + peers.size + '\n');
});
const wss = new WebSocketServer({ server, maxPayload: 4096 });
const peers = new Map();
let seq = 0;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const bcast = (from, o) => { const s = JSON.stringify(o); for (const [k, p] of peers) if (k !== from && p.ws.readyState === 1) p.ws.send(s); };

wss.on('connection', (ws, req) => {
  if (ALLOW.length && !ALLOW.includes(req.headers.origin)) return ws.close(1008, 'origin');
  if (peers.size >= MAX_PLAYERS) return ws.close(1013, 'penuh');
  const id = 'p' + (++seq).toString(36) + Math.random().toString(36).slice(2, 5);
  const me = { ws, d: {}, alive: true, lastPres: 0, evWin: 0, evN: 0 };
  const snap = {};
  for (const [k, p] of peers) snap[k] = p.d;
  peers.set(id, me);
  send(ws, { t: 'hi', id, snap });

  ws.on('pong', () => { me.alive = true; });
  ws.on('message', buf => {
    let m; try { m = JSON.parse(buf); } catch { return; }
    const now = Date.now();
    if (m.t === 'pres' && m.d && typeof m.d === 'object') {
      if (now - me.lastPres < 30) return;      // maksimal sekitar 30 pembaruan per detik
      me.lastPres = now; me.d = m.d;
      bcast(id, { t: 'pres', id, d: m.d });
    } else if (m.t === 'ev' && typeof m.topic === 'string' && /^[a-z][a-z0-9_.-]{0,31}$/.test(m.topic)) {
      if (now - me.evWin > 1000) { me.evWin = now; me.evN = 0; }
      if (++me.evN > 30) return;               // maksimal 30 event per detik
      bcast(id, { t: 'ev', id, topic: m.topic, data: m.data });
    }
  });
  ws.on('close', () => { peers.delete(id); bcast(id, { t: 'left', id }); });
  ws.on('error', () => {});
});

// Buang koneksi yang sudah mati
setInterval(() => {
  for (const [id, p] of peers) {
    if (!p.alive) { p.ws.terminate(); continue; }
    p.alive = false; try { p.ws.ping(); } catch {}
  }
}, 30000);

server.listen(PORT, () => console.log('Perang Bintang server berjalan di port ' + PORT));
