'use strict';
// Самостоятельный сервер для "Кристальные Стражи". Только Node.js >= 18.
const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const dataDir = process.env.DATA_DIR || path.join(__dirname, 'data');
fs.mkdirSync(dataDir, { recursive: true });
const dataFile = path.join(dataDir, 'leaderboard.json');
let saved = { profiles: {}, names: {} };
try { const d = JSON.parse(fs.readFileSync(dataFile, 'utf8')); if (d && d.profiles && d.names) saved = d; } catch (_) {}
function persist() { try { const tmp = dataFile + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(saved)); fs.renameSync(tmp, dataFile); } catch (e) { console.error('Не удалось записать таблицу:', e.message); } }
function isId(id) { return typeof id === 'string' && /^[a-zA-Z0-9_-]{5,70}$/.test(id); }
function validNick(n) { return typeof n === 'string' && /^[A-Za-zА-Яа-яЁё0-9 _-]{2,16}$/.test(n) && n.trim() === n; }
const nickKey = n => n.toLocaleLowerCase('ru-RU');
function setNick(id, nick) {
  if (!isId(id) || !validNick(nick)) return 'Некорректный ник или идентификатор';
  const key = nickKey(nick), taken = saved.names[key];
  if (taken && taken !== id) return 'Этот ник уже занят';
  for (const [k, owner] of Object.entries(saved.names)) if (owner === id && k !== key) delete saved.names[k];
  saved.names[key] = id;
  const entry = saved.profiles[id] || { id, mmr: 1200, g: 0, w: 0, l: 0 };
  entry.nick = nick;
  saved.profiles[id] = entry;
  persist();
  return '';
}
const bound = (x, max = 9999) => Number.isFinite(Number(x)) ? Math.max(0, Math.min(max, Math.floor(Number(x)))) : 0;
function board() { return Object.values(saved.profiles).filter(p => p.g >= 10).sort((a, b) => b.mmr - a.mmr).slice(0, 100); }
const json = (res, status, obj) => { const body = JSON.stringify(obj); res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' }); res.end(body); };
function readBody(req) { return new Promise((resolve, reject) => { let out = '', big = false; req.on('data', chunk => { out += chunk; if (out.length > 16384) { big = true; req.destroy(); reject(Error('Слишком большой запрос')); } }); req.on('end', () => { if (!big) { try { resolve(JSON.parse(out)); } catch (_) { reject(Error('Неправильный JSON')); } } }); req.on('error', reject); }); }
const html = fs.readFileSync(path.join(__dirname, 'index.html'));
const server = http.createServer(async (req, res) => {
  const pathname = (req.url || '/').split('?')[0];
  if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' }); res.end(html); return;
  }
  if (req.method === 'GET' && pathname === '/api/board') { json(res, 200, { rows: board() }); return; }
  if (req.method === 'GET' && pathname === '/health') { json(res, 200, { ok: true, players: sockets.size, rooms: matches.size }); return; }
  if (req.method === 'POST' && (pathname === '/api/nick' || pathname === '/api/score')) {
    try {
      const d = await readBody(req);
      if (!isId(d.id) || !validNick(d.nick)) { json(res, 400, { error: 'Некорректный ник или идентификатор' }); return; }
      if (pathname === '/api/nick') { const err = setNick(d.id, d.nick); json(res, err ? 409 : 200, err ? { error: err } : { ok: true }); return; }
      const owner = saved.names[nickKey(d.nick)];
      if (owner && owner !== d.id) { json(res, 409, { error: 'Ник уже принадлежит другому игроку' }); return; }
      if (!owner) { const err = setNick(d.id, d.nick); if (err) { json(res, 409, { error: err }); return; } }
      const prev = saved.profiles[d.id] || { id: d.id };
      const g = bound(d.g, 1000000), w = Math.min(g, bound(d.w, 1000000)), l = Math.min(g - w, bound(d.l, 1000000));
      saved.profiles[d.id] = { id: d.id, nick: d.nick, mmr: bound(d.mmr), g, w, l, t: Date.now() };
      persist(); json(res, 200, { ok: true });
    } catch (e) { if (!res.writableEnded) json(res, 400, { error: e.message }); }
    return;
  }
  json(res, 404, { error: 'Не найдено' });
});

// WebSocket framing RFC 6455, text/close/ping/pong, no npm-пакетов.
const sockets = new Set(), queue = [], invites = new Map(), matches = new Map();
const MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function sendFrame(s, opcode, payload) {
  if (s.closed || s.socket.destroyed) return;
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload || '');
  const n = body.length;
  const hdr = Buffer.alloc(n < 126 ? 2 : n <= 65535 ? 4 : 10);
  hdr[0] = 0x80 | opcode;
  if (n < 126) hdr[1] = n;
  else if (n <= 65535) { hdr[1] = 126; hdr.writeUInt16BE(n, 2); }
  else { hdr[1] = 127; hdr.writeUInt32BE(0, 2); hdr.writeUInt32BE(n, 6); }
  s.socket.write(Buffer.concat([hdr, body]));
}
function send(s, data) { if (s) sendFrame(s, 1, JSON.stringify(data)); }
function removeQueue(s) { for (let i = queue.length - 1; i >= 0; i--) if (queue[i] === s) queue.splice(i, 1); }
function clearWaiting(s) { removeQueue(s); if (s.invite && invites.get(s.invite) === s) invites.delete(s.invite); s.invite = null; }
function leave(s, tell = true) {
  clearWaiting(s);
  const m = s.match;
  if (!m) return;
  matches.delete(m.id);
  const other = m.host === s ? m.guest : m.host;
  s.match = null; s.role = null;
  if (other && other.match === m) {
    other.match = null; other.role = null;
    if (tell) send(other, { type: 'peer-left' });
  }
}
function codeGen() { const abc = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let code; do { code = Array.from(crypto.randomBytes(6), v => abc[v % abc.length]).join(''); } while (invites.has(code)); return code; }
function pair(a, b) {
  clearWaiting(a); clearWaiting(b);
  const id = crypto.randomBytes(12).toString('hex');
  const match = { hn: a.nick, gn: b.nick, hh: a.hero, gh: b.hero, hm: a.mmr, gm: b.mmr };
  const m = { id, host: a, guest: b, match };
  matches.set(id, m);
  a.match = m; a.role = 'h'; a.ready = false;
  b.match = m; b.role = 'g'; b.ready = false;
  send(a, { type: 'found', mid: id, role: 'h', match });
  send(b, { type: 'found', mid: id, role: 'g', match });
}
function handle(s, d) {
  if (!d || typeof d.type !== 'string') return;
  if (d.type === 'hello') {
    s.id = isId(d.id) ? d.id : null;
    s.nick = validNick(d.nick) ? d.nick : '';
    s.mmr = bound(d.mmr);
    send(s, { type: 'hello' }); return;
  }
  if (d.type === 'ping') { send(s, { type: 'pong' }); return; }
  if (d.type === 'cancel' || d.type === 'leave') { leave(s); return; }
  if (d.type === 'queue' || d.type === 'create' || d.type === 'join') {
    if (!s.id || !s.nick) { send(s, { type: 'error', message: 'Сначала выберите никнейм' }); return; }
    if (s.match) { send(s, { type: 'error', message: 'Вы уже в матче' }); return; }
    clearWaiting(s);
    s.hero = bound(d.h, 31); s.mmr = bound(d.mmr); s.nick = validNick(d.nick) ? d.nick : s.nick;
    if (d.type === 'create') {
      s.invite = codeGen(); invites.set(s.invite, s);
      send(s, { type: 'waiting', code: s.invite }); return;
    }
    if (d.type === 'join') {
      const code = typeof d.code === 'string' ? d.code.toUpperCase() : '';
      const host = invites.get(code);
      if (!host || host === s || host.closed || host.match) { send(s, { type: 'error', message: 'Комната не найдена или уже занята' }); return; }
      if (host.id === s.id) { send(s, { type: 'error', message: 'Используйте другое устройство или браузерный профиль' }); return; }
      pair(host, s); return;
    }
    let opp;
    while (queue.length && !opp) { const next = queue.shift(); if (next !== s && !next.closed && !next.match && next.id !== s.id) opp = next; }
    if (opp) pair(opp, s);
    else { queue.push(s); send(s, { type: 'queued' }); }
    return;
  }
  const m = s.match;
  if (!m) return;
  const other = m.host === s ? m.guest : m.host;
  if (!other || other.closed) return;
  if (d.type === 'presence') {
    s.ready = true;
    if (other.ready) { send(s, { type: 'presence', peer: other.role }); send(other, { type: 'presence', peer: s.role }); }
    return;
  }
  if (d.type === 'relay' && s.ready && other.ready) {
    const allowed = s.role === 'h' ? ['ctl', 'snap', 'crea', 'ext'] : ['ctl', 'cmd'];
    if (!allowed.includes(d.event)) return;
    send(other, { type: 'relay', event: d.event, data: d.data });
  }
}
server.on('upgrade', (req, socket) => {
  if ((req.url || '').split('?')[0] !== '/ws' || req.headers.upgrade?.toLowerCase() !== 'websocket' || !/^13$/.test(req.headers['sec-websocket-version'] || '') || !/^[a-zA-Z0-9+/]{22}==$/.test(req.headers['sec-websocket-key'] || '')) { socket.destroy(); return; }
  // Browser requests always come from the same app origin.
  const origin = req.headers.origin;
  if (origin) { try { const url = new URL(origin); if (url.host !== req.headers.host || !['http:', 'https:'].includes(url.protocol)) { socket.destroy(); return; } } catch (_) { socket.destroy(); return; } }
  const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + MAGIC).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const s = { socket, buf: Buffer.alloc(0), closed: false, id: null, nick: '', mmr: 1200, hero: 0, match: null, invite: null, ready: false, fragments: [], fragType: 0, messages: 0, stamp: Date.now() };
  sockets.add(s);
  socket.setNoDelay(true);
  socket.on('data', chunk => {
    if (s.closed) return;
    s.buf = Buffer.concat([s.buf, chunk]);
    if (s.buf.length > 2 * 1024 * 1024) { socket.destroy(); return; }
    while (s.buf.length >= 2) {
      const b = s.buf, op = b[0] & 15, fin = !!(b[0] & 128), masked = !!(b[1] & 128), type = b[1] & 127;
      if (!masked || (b[0] & 0x70)) { socket.destroy(); return; }
      let start = 2, len = type;
      if (type === 126) { if (b.length < 4) break; len = b.readUInt16BE(2); start = 4; }
      else if (type === 127) { if (b.length < 10) break; if (b.readUInt32BE(2)) { socket.destroy(); return; } len = b.readUInt32BE(6); start = 10; }
      if (len > 1024 * 1024) { socket.destroy(); return; }
      if (b.length < start + 4 + len) break;
      const mask = b.subarray(start, start + 4), bytes = Buffer.from(b.subarray(start + 4, start + 4 + len));
      for (let i = 0; i < bytes.length; i++) bytes[i] ^= mask[i % 4];
      s.buf = b.subarray(start + 4 + len);
      if (op === 8) { socket.end(); return; }
      if (op === 9) { sendFrame(s, 10, bytes); continue; }
      if (op === 10) continue;
      if (op === 1 || op === 0) {
        if (op === 1) { s.fragments = []; s.fragType = 1; }
        if (s.fragType !== 1) { socket.destroy(); return; }
        s.fragments.push(bytes);
        if (fin) {
          const now = Date.now(); if (now - s.stamp > 1000) { s.messages = 0; s.stamp = now; }
          if (++s.messages > 160) { socket.destroy(); return; }
          try { handle(s, JSON.parse(Buffer.concat(s.fragments).toString('utf8'))); } catch (_) {}
          s.fragments = []; s.fragType = 0;
        }
        continue;
      }
      socket.destroy(); return;
    }
  });
  socket.on('error', () => {});
  socket.on('close', () => { if (s.closed) return; s.closed = true; sockets.delete(s); leave(s); });
});
server.listen(PORT, HOST, () => console.log(`Кристальные Стражи запущены: http://localhost:${PORT}`));
process.on('SIGTERM', () => { server.close(); for (const s of sockets) s.socket.end(); });
