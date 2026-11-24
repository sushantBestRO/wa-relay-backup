/**
 * 95 · BRL WhatsApp relay                                                        v1.1 · 19-Nov-2026 (Director)
 * ------------------------------------------------------------------------------------------------------------
 * v1.0 (18 Nov): robots post a template message here; this service calls DoubleTick from our server, because
 *                DoubleTick blocks calls from Google Apps Script.
 * v1.1 (19 Nov): REPLIES COME BACK. DoubleTick posts every incoming WhatsApp (webhook "Message Received") to
 *                /inbound/<inbound_token>. The relay keeps them for 7 days in inbox.jsonl. The RM Desk robots read
 *                them with GET /inbox and write a branch's / broker's rate on the enquiry row.
 *
 * - The DoubleTick API key lives ONLY here (config.js).
 * - /send forwards one thing only: an approved TEMPLATE message. Callers send the secret in X-BRL-Relay-Key.
 * - /inbox needs the same X-BRL-Relay-Key. /inbound is protected by the long token in its URL (DoubleTick sends
 *   no signature), so keep that URL private.
 * - No dependencies. Node 18+. PM2 behind nginx.
 *
 * POST /send              JSON { "to": "919812345678", "templateName": "pod_followup_v2", "language": "en",
 *                                "placeholders": ["...", "..."] }
 *                    ->   { "ok": true, "status": 201, "body": "..." }  or  { "ok": false, "status": 4xx, "body": "..." }
 * POST /inbound/<token>   DoubleTick webhook body (Message Received)                  -> { "ok": true }
 * GET  /inbox?after=<seq>&limit=200   (header X-BRL-Relay-Key)
 *                    ->   { "ok": true, "last": <seq>, "messages": [ { seq, at, from, name, type, text, payload,
 *                           buttonId, replyTo, template, messageId } ] }
 * GET  /health       ->   { "ok": 1, "sentThisHour": n, "inbox": n }
 *
 * config.js (new keys, both optional):
 *   inbound_token: 'PASTE_A_LONG_RANDOM_STRING',   // openssl rand -hex 24 · without it /inbound is switched off
 *   inbox_keep_days: 7
 */
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const pathMod = require('path');
const cfg = require('./config.js');

const PORT = Number(cfg.port || 3091);
const DT_URL = 'https://public.doubletick.io/whatsapp/message/template';
const MAX_PER_HOUR = Number(cfg.max_per_hour || 1500);
const TEMPLATES = Array.isArray(cfg.allowed_templates) ? cfg.allowed_templates : null;   // null = any template
const INBOUND_TOKEN = cfg.inbound_token && !/^PASTE_/.test(String(cfg.inbound_token)) ? String(cfg.inbound_token) : '';
const INBOX_FILE = pathMod.join(__dirname, 'inbox.jsonl');
const KEEP_MS = Number(cfg.inbox_keep_days || 7) * 86400000;
const INBOX_MAX = 5000;

['dt_api_key', 'dt_from', 'relay_secret'].forEach(function (k) {
  if (!cfg[k] || /^PASTE_/.test(String(cfg[k]))) { console.error('config.js: ' + k + ' is missing'); process.exit(1); }
});
if (String(cfg.relay_secret).length < 24) { console.error('config.js: relay_secret must be at least 24 characters'); process.exit(1); }
if (INBOUND_TOKEN && INBOUND_TOKEN.length < 24) { console.error('config.js: inbound_token must be at least 24 characters'); process.exit(1); }

let hour = 0, sent = 0;
function quotaOk() {
  const h = Math.floor(Date.now() / 3600000);
  if (h !== hour) { hour = h; sent = 0; }
  return sent < MAX_PER_HOUR;
}
function same(a, b) {
  a = Buffer.from(String(a || '')); b = Buffer.from(String(b || ''));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
function secretOk(got) { return same(got, cfg.relay_secret); }
function reply(res, code, obj) {
  const s = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(s), 'Cache-Control': 'no-store' });
  res.end(s);
}
function readBody(req, max) {
  return new Promise(function (resolve, reject) {
    let n = 0; const parts = [];
    req.on('data', function (c) { n += c.length; if (n > max) { reject(new Error('too large')); req.destroy(); } else parts.push(c); });
    req.on('end', function () { resolve(Buffer.concat(parts).toString('utf8')); });
    req.on('error', reject);
  });
}

/* ------------------------------------------------------------------ inbox (replies) */
let inbox = [], lastSeq = 0; const seenIds = new Set();
function inboxLoad() {
  try {
    const cut = Date.now() - KEEP_MS;
    fs.readFileSync(INBOX_FILE, 'utf8').split('\n').forEach(function (l) {
      if (!l) return;
      try { const m = JSON.parse(l); if (m && m.seq && Date.parse(m.at) >= cut) { inbox.push(m); if (m.messageId) seenIds.add(m.messageId); if (m.seq > lastSeq) lastSeq = m.seq; } } catch (e) {}
    });
    inbox = inbox.slice(-INBOX_MAX);
    inboxRewrite();
  } catch (e) { /* no file yet */ }
}
function inboxRewrite() {
  try { fs.writeFileSync(INBOX_FILE, inbox.map(function (m) { return JSON.stringify(m); }).join('\n') + (inbox.length ? '\n' : ''), { mode: 0o600 }); } catch (e) { console.error('inbox write: ' + e.message); }
}
function inboxPrune() {
  const cut = Date.now() - KEEP_MS, n = inbox.length;
  inbox = inbox.filter(function (m) { return Date.parse(m.at) >= cut; }).slice(-INBOX_MAX);
  if (inbox.length !== n) { seenIds.clear(); inbox.forEach(function (m) { if (m.messageId) seenIds.add(m.messageId); }); inboxRewrite(); }
}
function inboxAdd(b) {
  const msg = b && b.message; if (!msg || !b.from) return 'ignored';
  const id = String(b.messageId || b.dtMessageId || ''); if (id && seenIds.has(id)) return 'duplicate';
  lastSeq = Math.max(lastSeq + 1, Date.now());
  const m = {
    seq: lastSeq, at: new Date().toISOString(),
    from: String(b.from).replace(/\D/g, ''), name: String((b.contact && b.contact.name) || '').slice(0, 80),
    type: String(msg.type || '').toUpperCase().slice(0, 20), text: String(msg.text || msg.caption || '').slice(0, 1500),
    payload: String(msg.payload || '').slice(0, 200), buttonId: String(msg.id || '').slice(0, 100),
    replyTo: String((msg.context && msg.context.id) || '').slice(0, 200), template: String(b.dtPairedTemplateName || '').slice(0, 64),
    messageId: id.slice(0, 200)
  };
  inbox.push(m); if (id) seenIds.add(id);
  try { fs.appendFileSync(INBOX_FILE, JSON.stringify(m) + '\n', { mode: 0o600 }); } catch (e) { console.error('inbox append: ' + e.message); }
  if (inbox.length > INBOX_MAX + 500) inboxPrune();
  console.log(m.at + ' IN   ' + m.type + ' from ..' + m.from.slice(-4));
  return 'stored';
}
inboxLoad();
setInterval(inboxPrune, 3600000).unref();

/* ------------------------------------------------------------------ server */
const server = http.createServer(async function (req, res) {
  try {
    const url = String(req.url || ''), path = url.split('?')[0].replace(/\/+$/, '') || '/';
    if (req.method === 'GET' && path === '/health') return reply(res, 200, { ok: 1, sentThisHour: sent, inbox: inbox.length });

    /* DoubleTick -> us */
    if (req.method === 'POST' && path.indexOf('/inbound/') === 0) {
      if (!INBOUND_TOKEN || !same(path.slice(9), INBOUND_TOKEN)) return reply(res, 404, { ok: false, body: 'not found' });
      let b; try { b = JSON.parse(await readBody(req, 300000)); } catch (e) { return reply(res, 200, { ok: true, note: 'bad json ignored' }); }
      let note = 'ignored';
      try { (Array.isArray(b) ? b : [b]).forEach(function (x) { note = inboxAdd(x && x.payload && x.payload.message ? x.payload : x); }); } catch (e) { console.error('inbound: ' + e.message); }
      return reply(res, 200, { ok: true, note: note });
    }

    /* robots read the replies */
    if (req.method === 'GET' && path === '/inbox') {
      if (!secretOk(req.headers['x-brl-relay-key'])) return reply(res, 401, { ok: false, body: 'bad relay key' });
      const q = new URLSearchParams(url.split('?')[1] || ''), after = Number(q.get('after') || 0), limit = Math.min(500, Math.max(1, Number(q.get('limit') || 200)));
      const list = inbox.filter(function (m) { return m.seq > after; }).slice(0, limit);
      return reply(res, 200, { ok: true, last: list.length ? list[list.length - 1].seq : after, now: Date.now(), messages: list });
    }

    if (req.method !== 'POST' || path !== '/send') return reply(res, 404, { ok: false, body: 'not found' });
    if (!secretOk(req.headers['x-brl-relay-key'])) return reply(res, 401, { ok: false, body: 'bad relay key' });

    let b;
    try { b = JSON.parse(await readBody(req, 20000)); } catch (e) { return reply(res, 400, { ok: false, body: 'bad json' }); }
    const to = String(b.to || '').replace(/\D/g, '');
    const tpl = String(b.templateName || '').trim();
    const lang = String(b.language || 'en').trim();
    const ph = Array.isArray(b.placeholders) ? b.placeholders : [];
    if (!/^\d{10,15}$/.test(to)) return reply(res, 400, { ok: false, body: 'bad "to" number' });
    if (!/^[a-z0-9_]{1,64}$/.test(tpl)) return reply(res, 400, { ok: false, body: 'bad templateName' });
    if (TEMPLATES && TEMPLATES.indexOf(tpl) < 0) return reply(res, 403, { ok: false, body: 'template not on the allowed list' });
    if (!/^[a-zA-Z_]{2,8}$/.test(lang)) return reply(res, 400, { ok: false, body: 'bad language' });
    if (ph.length > 12) return reply(res, 400, { ok: false, body: 'too many placeholders' });
    if (!quotaOk()) return reply(res, 429, { ok: false, body: 'hourly cap reached (' + MAX_PER_HOUR + ')' });

    const payload = { messages: [{ to: '+' + to, from: String(cfg.dt_from).replace(/\D/g, ''), content: {
      templateName: tpl, language: lang,
      templateData: { body: { placeholders: ph.map(function (x) { return String(x === null || x === undefined || x === '' ? '-' : x).replace(/\s+/g, ' ').slice(0, 1000); }) } } } }] };

    const ctl = new AbortController(); const timer = setTimeout(function () { ctl.abort(); }, 20000);
    let r, text;
    try {
      r = await fetch(DT_URL, { method: 'POST', headers: { Authorization: cfg.dt_api_key, 'Content-Type': 'application/json' }, body: JSON.stringify(payload), signal: ctl.signal });
      text = await r.text();
    } finally { clearTimeout(timer); }
    const ok = r.status < 300 && !/"success"\s*:\s*false/.test(text);
    if (ok) sent++;
    console.log(new Date().toISOString() + ' ' + (ok ? 'SENT ' : 'FAIL ') + r.status + ' ' + tpl + ' to ..' + to.slice(-4) + (ok ? '' : ' ' + text.slice(0, 160)));
    return reply(res, ok ? 200 : 502, { ok: ok, status: r.status, body: text.slice(0, 300) });
  } catch (e) {
    console.error(new Date().toISOString() + ' ERROR ' + (e && e.message));
    return reply(res, 500, { ok: false, body: 'relay error: ' + String(e && e.message).slice(0, 120) });
  }
});
server.listen(PORT, '127.0.0.1', function () { console.log('BRL WhatsApp relay v1.1 on 127.0.0.1:' + PORT + (INBOUND_TOKEN ? ' · inbound ON' : ' · inbound OFF (no inbound_token)')); });