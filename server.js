'use strict';

const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const path = require('path');
const https = require('https');
const { Pool } = require('pg');
const TelegramBot = require('node-telegram-bot-api');

// ============================================================
// CONFIG
// ============================================================
const BOT_TOKEN   = process.env.TELEGRAM_TOKEN || '9027713238:AAEic04yZItSl5UhnNnXGDz_0kAnlreElVA';
const ADMIN_ID    = process.env.ADMIN_ID ? Number(process.env.ADMIN_ID) : 7757046138;
const ADMIN_USER  = (process.env.ADMIN_USER || 'tranhoang2286').toLowerCase();
const NOTIFY_CHANNEL_ID = process.env.NOTIFY_CHANNEL_ID || '';

const BANK_CODE   = 'BIDV';
const BANK_ACC    = '96247V4IWI';
const BANK_HOLDER = 'TRAN NHAT HOANG';

// ✅ API BANK MỚI
const API_BANK = 'https://api-nganhang.onrender.com/lich-su';

// ✅ HOST API GAME MỚI
const GAME_API_HOST = 'https://fictional-happiness-betf.onrender.com';

const PORT     = process.env.PORT || 8000;
const DATABASE_URL = process.env.DATABASE_URL;
const RESET_DATABASE = process.env.RESET_DATABASE === 'true';

const SECRET_KEY      = process.env.SECRET_KEY || crypto.randomBytes(32).toString('hex');
const SECRET_KEY_PREV = process.env.SECRET_KEY_PREV || '';

const TOKEN_ROTATE_MS       = 3_600_000;
const RATE_LIMIT_MAX        = 60;
const RATE_LIMIT_WINDOW_MS  = 60_000;
const API_TIMEOUT_MS        = 8_000;
const API_RETRY             = 1;
const CIRCUIT_FAIL_THRESH   = 5;
const CIRCUIT_OPEN_MS       = 30_000;

const ACTIVATE_FAIL_MAX     = 20;
const ACTIVATE_LOCK_MS      = 15 * 60_000;
const ACTIVATE_FAILS        = new Map();

const PREDICT_CACHE_MS      = 3_000;
const PREDICT_CACHE         = new Map();

// ============================================================
// GAME API MAP — ĐỦ 4 GAME × 2 TYPE
// ============================================================
const API_GAME_RAW = {
  lc79_md5:    `${GAME_API_HOST}/json-predict/lc79/md5`,
  lc79_hu:     `${GAME_API_HOST}/json-predict/lc79/hu`,
  betvip_md5:  `${GAME_API_HOST}/json-predict/betvip/md5`,
  betvip_hu:   `${GAME_API_HOST}/json-predict/betvip/hu`,
  hitclub_md5: `${GAME_API_HOST}/json-predict/hitclub/md5`,
  hitclub_hu:  `${GAME_API_HOST}/json-predict/hitclub/hu`,
  b52_md5:     `${GAME_API_HOST}/json-predict/b52/md5`,
  b52_hu:      `${GAME_API_HOST}/json-predict/b52/hu`,
  luckywin:    `${GAME_API_HOST}/json-predict/sicbo/luckywin`,
  sumclub:     `${GAME_API_HOST}/json-predict/sicbo/sumclub`,
};

const GAME_TYPE = {
  lc79_md5: 'md5hu', lc79_hu: 'md5hu',
  betvip_md5: 'md5hu', betvip_hu: 'md5hu',
  hitclub_md5: 'md5hu', hitclub_hu: 'md5hu',
  b52_md5: 'md5hu', b52_hu: 'md5hu',
  luckywin: 'sicbo', sumclub: 'sicbo',
};

// ============================================================
// GAME LIST cho /api/games
// ============================================================
const GAME_LIST = [
  { token: 'lc79_md5',    name: 'LC79 MD5',    tagline: 'Phân tích MD5 LC79',    icon: 'fa-dice',          badge: 'HOT' },
  { token: 'lc79_hu',     name: 'LC79 HŨ',     tagline: 'Phân tích HŨ LC79',     icon: 'fa-coins',         badge: 'PRO' },
  { token: 'betvip_md5',  name: 'BETVIP MD5',  tagline: 'Phân tích MD5 BetVIP',  icon: 'fa-crown',         badge: 'NEW' },
  { token: 'betvip_hu',   name: 'BETVIP HŨ',   tagline: 'Phân tích HŨ BetVIP',   icon: 'fa-trophy',        badge: 'HOT' },
  { token: 'hitclub_md5', name: 'HITCLUB MD5', tagline: 'Phân tích MD5 HitClub', icon: 'fa-fire',          badge: 'NEW' },
  { token: 'hitclub_hu',  name: 'HITCLUB HŨ',  tagline: 'Phân tích HŨ HitClub',  icon: 'fa-rocket',        badge: '' },
  { token: 'b52_md5',     name: 'B52 MD5',     tagline: 'Phân tích MD5 B52',     icon: 'fa-gamepad',       badge: 'PRO' },
  { token: 'b52_hu',      name: 'B52 HŨ',      tagline: 'Phân tích HŨ B52',      icon: 'fa-bolt',          badge: '' },
];

const PRICES = {
  '1d':   { price: 20_000,  days: 1,    label: '1 Ngày' },
  '4d':   { price: 30_000,  days: 4,    label: '4 Ngày' },
  '1w':   { price: 60_000,  days: 7,    label: '1 Tuần' },
  '1m':   { price: 120_000, days: 30,   label: '1 Tháng' },
  '3m':   { price: 150_000, days: 90,   label: '3 Tháng' },
  'life': { price: 220_000, days: null, label: 'Vĩnh Viễn' },
};

// ============================================================
// DATABASE
// ============================================================
const pool = DATABASE_URL
  ? new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 10 })
  : new (require('pg-mem').newDb().adapters.createPg().Pool)();

async function initDB() {
  try {
    if (RESET_DATABASE) {
      console.log('⚠️ RESET_DATABASE=true — Xóa toàn bộ dữ liệu cũ...');
      await pool.query('DROP TABLE IF EXISTS keys');
      await pool.query('DROP TABLE IF EXISTS orders');
    }

    await pool.query(`
      CREATE TABLE IF NOT EXISTS keys (
        key TEXT PRIMARY KEY,
        plan TEXT NOT NULL,
        duration_days INTEGER,
        created_at DOUBLE PRECISION NOT NULL,
        activated_at DOUBLE PRECISION,
        expires_at DOUBLE PRECISION,
        is_active INTEGER DEFAULT 0,
        device_id TEXT
      );
      CREATE TABLE IF NOT EXISTS orders (
        id TEXT PRIMARY KEY,
        user_id BIGINT, username TEXT, full_name TEXT, plan TEXT,
        amount INTEGER, content TEXT, status TEXT DEFAULT 'pending',
        created_at DOUBLE PRECISION, paid_at DOUBLE PRECISION, key_issued TEXT
      );
    `);
    await pool.query(`ALTER TABLE keys ADD COLUMN IF NOT EXISTS device_id TEXT`).catch(() => {});
    console.log('✅ Database Ready!');
  } catch (err) {
    console.error('❌ DB Error:', err.message);
    throw err;
  }
}

// ============================================================
// UTILS
// ============================================================
function now() { return Date.now(); }

function genKey() {
  const raw = crypto.randomBytes(9).toString('base64')
    .replace(/[^A-Z0-9]/gi, '').toUpperCase().slice(0, 12);
  return 'HT' + raw;
}

function signKey(key) {
  return crypto.createHmac('sha256', SECRET_KEY).update(key).digest('hex').slice(0, 16);
}

function verifyKeySig(key, sig) {
  const expected = signKey(key);
  try {
    return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(String(sig || '')));
  } catch { return false; }
}

function normalizePred(p) {
  if (!p) return '';
  const s = String(p).trim().toUpperCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (s === 'TAI' || s === 'BIG' || s === 'T') return 'TÀI';
  if (s === 'XIU' || s === 'SMALL' || s === 'X') return 'XỈU';
  return '';
}

// ============================================================
// RATE LIMIT
// ============================================================
const RL = new Map();
function rateLimit(ip) {
  const n = now();
  const rec = RL.get(ip) || { count: 0, reset: n + RATE_LIMIT_WINDOW_MS };
  if (n > rec.reset) { rec.count = 0; rec.reset = n + RATE_LIMIT_WINDOW_MS; }
  rec.count++;
  RL.set(ip, rec);
  return rec.count <= RATE_LIMIT_MAX;
}

// ============================================================
// CIRCUIT BREAKER
// ============================================================
const CIRCUIT = new Map();
function circuitCheck(id) {
  const c = CIRCUIT.get(id);
  if (!c) return true;
  if (c.fails >= CIRCUIT_FAIL_THRESH && now() - c.last < CIRCUIT_OPEN_MS) return false;
  return true;
}
function circuitFail(id) {
  const c = CIRCUIT.get(id) || { fails: 0, last: 0 };
  c.fails++; c.last = now(); CIRCUIT.set(id, c);
}
function circuitOk(id) { CIRCUIT.delete(id); }

// ============================================================
// FETCH API GAME
// ============================================================
async function fetchGameApi(gameId) {
  const url = API_GAME_RAW[gameId];
  if (!url) throw new Error('GAME_NOT_FOUND');
  if (!circuitCheck(gameId)) throw new Error('CIRCUIT_OPEN');

  let lastErr;
  for (let i = 0; i <= API_RETRY; i++) {
    try {
      const r = await axios.get(url, { timeout: API_TIMEOUT_MS });
      circuitOk(gameId);
      return r.data;
    } catch (e) {
      lastErr = e;
      if (e.response && e.response.status === 404) break;
    }
  }
  circuitFail(gameId);
  throw lastErr || new Error('API_FAIL');
}

// ============================================================
// TRANSFORM PREDICTION
// ============================================================
function transformPrediction(raw, gameId) {
  const type = GAME_TYPE[gameId] || 'md5hu';
  let phien = raw.phien || raw.session || raw.ky || raw.id || '';
  let du_doan = normalizePred(raw.du_doan || raw.prediction || raw.pred || raw.ket_qua_du_doan || '');
  let tin_cay = Number(raw.tin_cay || raw.confidence || raw.rate || raw.do_tin_cay || 0);
  let history = [];

  if (Array.isArray(raw.history)) {
    history = raw.history.map(h => ({
      phien: h.phien || h.session || h.id || '',
      du_doan: normalizePred(h.du_doan || h.prediction || h.pred || ''),
      ket_qua: normalizePred(h.ket_qua || h.result || h.kq || ''),
      danh_gia: h.danh_gia || h.eval || h.status || '',
    }));
  } else if (Array.isArray(raw.data)) {
    history = raw.data.map(h => ({
      phien: h.phien || h.session || h.id || '',
      du_doan: normalizePred(h.du_doan || h.prediction || h.pred || ''),
      ket_qua: normalizePred(h.ket_qua || h.result || h.kq || ''),
      danh_gia: h.danh_gia || h.eval || h.status || '',
    }));
  }

  if (!phien && history.length > 0) phien = history[0].phien;
  if (!du_doan) {
    const last = history.find(h => !h.ket_qua);
    if (last) du_doan = last.du_doan;
  }
  if (!tin_cay) {
    let win = 0, total = 0;
    for (const h of history) {
      if (!h.du_doan || !h.ket_qua) continue;
      total++;
      if (h.du_doan === h.ket_qua) win++;
    }
    tin_cay = total > 0 ? Math.round(win / total * 100) : 55;
  }

  return {
    ok: true,
    game: gameId,
    type,
    phien: String(phien || ''),
    phien_du_doan: String(phien || ''),
    du_doan,
    tin_cay: Math.max(0, Math.min(100, Math.round(tin_cay))),
    history,
    ts: now(),
  };
}

// ============================================================
// BOT TELEGRAM
// ============================================================
let bot = null;
if (BOT_TOKEN && BOT_TOKEN.includes(':')) {
  try {
    bot = new TelegramBot(BOT_TOKEN, { polling: true });
    bot.on('polling_error', e => console.error('Polling error:', e.message));
    console.log('🤖 Bot started');
  } catch (e) {
    console.error('Bot init failed:', e.message);
  }
}

async function notifyAdmin(text) {
  if (!bot || !ADMIN_ID) return;
  try { await bot.sendMessage(ADMIN_ID, text, { parse_mode: 'HTML' }); } catch {}
}

// ============================================================
// EXPRESS
// ============================================================
const app = express();
app.use(express.json({ limit: '256kb' }));
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('Referrer-Policy', 'no-referrer');
  next();
});

// Static
app.use(express.static(path.join(__dirname), { extensions: ['html'] }));

// ── API: GAMES ──────────────────────────────────────────────
app.get('/api/games', (req, res) => {
  res.json({ ok: true, games: GAME_LIST });
});

// ── API: PREDICT ────────────────────────────────────────────
app.get('/api/predict/:gameId', async (req, res) => {
  const { gameId } = req.params;
  const ip = req.ip || req.connection.remoteAddress || 'x';

  if (!rateLimit(ip)) return res.status(429).json({ ok: false, error: 'RATE_LIMITED' });
  if (!API_GAME_RAW[gameId]) return res.status(404).json({ ok: false, error: 'GAME_NOT_FOUND' });

  const key = String(req.headers['x-kairo-key'] || '');
  const deviceId = String(req.headers['x-device-id'] || '');

  if (!key) return res.status(401).json({ ok: false, error: 'NO_KEY' });
  if (!key.startsWith('HT')) return res.status(401).json({ ok: false, error: 'BAD_KEY_FORMAT' });

  // Verify key in DB
  try {
    const r = await pool.query('SELECT * FROM keys WHERE key = $1', [key]);
    const row = r.rows[0];
    if (!row) return res.status(401).json({ ok: false, error: 'KEY_NOT_FOUND' });
    if (!row.is_active) return res.status(403).json({ ok: false, error: 'KEY_NOT_ACTIVATED' });
    if (row.expires_at && row.expires_at < now()) return res.status(403).json({ ok: false, error: 'KEY_EXPIRED' });
    if (row.device_id && row.device_id !== deviceId) return res.status(403).json({ ok: false, error: 'DEVICE_MISMATCH' });
  } catch (e) {
    return res.status(500).json({ ok: false, error: 'DB_ERROR' });
  }

  // Cache
  const cached = PREDICT_CACHE.get(gameId);
  if (cached && now() - cached.at < PREDICT_CACHE_MS) {
    return res.json(cached.data);
  }

  try {
    const raw = await fetchGameApi(gameId);
    const data = transformPrediction(raw, gameId);
    PREDICT_CACHE.set(gameId, { data, at: now() });
    res.json(data);
  } catch (e) {
    res.status(502).json({ ok: false, error: 'API_FAIL', message: e.message });
  }
});

// ── API: KEY VERIFY ─────────────────────────────────────────
app.get('/api/key/verify', async (req, res) => {
  const key = String(req.headers['x-kairo-key'] || '');
  const deviceId = String(req.headers['x-device-id'] || '');
  if (!key) return res.status(401).json({ ok: false, error: 'NO_KEY' });

  try {
    const r = await pool.query('SELECT * FROM keys WHERE key = $1', [key]);
    const row = r.rows[0];
    if (!row) return res.status(401).json({ ok: false, error: 'KEY_NOT_FOUND' });
    if (!row.is_active) return res.status(403).json({ ok: false, error: 'KEY_NOT_ACTIVATED' });
    if (row.expires_at && row.expires_at < now()) return res.status(403).json({ ok: false, error: 'KEY_EXPIRED' });
    if (row.device_id && row.device_id !== deviceId) return res.status(403).json({ ok: false, error: 'DEVICE_MISMATCH' });

    res.json({
      ok: true,
      key,
      plan: row.plan,
      expires_at: row.expires_at,
      days_left: row.expires_at ? Math.max(0, Math.ceil((row.expires_at - now()) / 86400000)) : null,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'DB_ERROR' });
  }
});

// ── API: KEY ACTIVATE ───────────────────────────────────────
app.post('/api/key/activate', async (req, res) => {
  const ip = req.ip || 'x';
  const failRec = ACTIVATE_FAILS.get(ip);
  if (failRec && failRec.lockUntil > now()) {
    return res.status(429).json({ ok: false, error: 'LOCKED_TOO_MANY_FAILS' });
  }

  const key = String(req.body?.key || '').trim();
  const deviceId = String(req.headers['x-device-id'] || req.body?.device_id || '').trim();

  if (!key) return res.status(400).json({ ok: false, error: 'NO_KEY' });
  if (!key.startsWith('HT') || key.length < 8) {
    return res.status(400).json({ ok: false, error: 'BAD_KEY_FORMAT' });
  }
  if (!deviceId) return res.status(400).json({ ok: false, error: 'NO_DEVICE_ID' });

  try {
    const r = await pool.query('SELECT * FROM keys WHERE key = $1', [key]);
    const row = r.rows[0];

    if (!row) {
      const fr = ACTIVATE_FAILS.get(ip) || { count: 0, lockUntil: 0 };
      fr.count++;
      if (fr.count >= ACTIVATE_FAIL_MAX) { fr.lockUntil = now() + ACTIVATE_LOCK_MS; fr.count = 0; }
      ACTIVATE_FAILS.set(ip, fr);
      return res.status(404).json({ ok: false, error: 'KEY_NOT_FOUND' });
    }

    if (row.is_active) {
      // Already activated
      if (row.device_id && row.device_id !== deviceId) {
        return res.status(403).json({ ok: false, error: 'KEY_USED_OTHER_DEVICE' });
      }
      if (row.expires_at && row.expires_at < now()) {
        return res.status(403).json({ ok: false, error: 'KEY_EXPIRED' });
      }
      return res.json({ ok: true, plan: row.plan, expires_at: row.expires_at, already: true });
    }

    // Activate
    const activatedAt = now();
    const expiresAt = row.duration_days
      ? activatedAt + row.duration_days * 86400000
      : null;

    await pool.query(
      'UPDATE keys SET is_active = 1, activated_at = $1, expires_at = $2, device_id = $3 WHERE key = $4',
      [activatedAt, expiresAt, deviceId, key]
    );

    notifyAdmin(`🔑 Key activated\n<code>${key}</code>\nPlan: <b>${row.plan}</b>\nDevice: <code>${deviceId}</code>`);

    res.json({ ok: true, plan: row.plan, expires_at: expiresAt });
  } catch (e) {
    console.error('Activate error:', e.message);
    res.status(500).json({ ok: false, error: 'DB_ERROR' });
  }
});

// ── API: BANK HISTORY (proxy) ───────────────────────────────
app.get('/api/bank/history', async (req, res) => {
  try {
    const r = await axios.get(API_BANK, { timeout: API_TIMEOUT_MS });
    res.json({ ok: true, data: r.data });
  } catch (e) {
    res.status(502).json({ ok: false, error: 'BANK_API_FAIL' });
  }
});

// ── Root → index ────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/kho', (req, res) => res.sendFile(path.join(__dirname, 'kho.html')));
app.get('/kho.html', (req, res) => res.sendFile(path.join(__dirname, 'kho.html')));

// ============================================================
// START
// ============================================================
(async () => {
  try {
    await initDB();
    app.listen(PORT, () => console.log(`🚀 Server listening on port ${PORT}`));
  } catch (e) {
    console.error('Fatal startup error:', e.message);
    process.exit(1);
  }
})();
