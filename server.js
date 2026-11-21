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
const BOT_TOKEN   = process.env.TELEGRAM_TOKEN || '9027713238:AAHUWF-FR5w0lfKpOOCMMJ14_5yRM3lWv2c';
const ADMIN_ID    = process.env.ADMIN_ID ? Number(process.env.ADMIN_ID) : 7757046138;
const ADMIN_USER  = (process.env.ADMIN_USER || 'tranhoang2286').toLowerCase();

const BANK_CODE   = 'BIDV';
const BANK_ACC    = '96247V4IWI';
const BANK_HOLDER = 'TRAN NHAT HOANG';
const API_BANK = 'https://api-nganhang.onrender.com/lich-su';

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

process.on('unhandledRejection', (reason) => console.error('⚠️ Unhandled Rejection:', reason));
process.on('uncaughtException', (err) => console.error('⚠️ Uncaught Exception:', err.message));

// ============================================================
// GAME API MAP
// ============================================================
const API_GAME_RAW = {
  lc79_md5:    `${GAME_API_HOST}/api/predict/lc79/md5`,
  lc79_hu:     `${GAME_API_HOST}/api/predict/lc79/hu`,
  betvip_md5:  `${GAME_API_HOST}/api/predict/betvip/md5`,
  betvip_hu:   `${GAME_API_HOST}/api/predict/betvip/hu`,
  hitclub_md5: `${GAME_API_HOST}/api/predict/hitclub/md5`,
  hitclub_hu:  `${GAME_API_HOST}/api/predict/hitclub/hu`,
  b52_md5:     `${GAME_API_HOST}/api/predict/b52/md5`,
  b52_hu:      `${GAME_API_HOST}/api/predict/b52/hu`,
  luckywin:    `${GAME_API_HOST}/api/predict/sicbo/luckywin`,
  sumclub:     `${GAME_API_HOST}/api/predict/sicbo/sumclub`,
};

const GAME_TYPE = {
  lc79_md5: 'md5hu', lc79_hu: 'md5hu',
  betvip_md5: 'md5hu', betvip_hu: 'md5hu',
  hitclub_md5: 'md5hu', hitclub_hu: 'md5hu',
  b52_md5: 'md5hu', b52_hu: 'md5hu',
  luckywin: 'sicbo', sumclub: 'sicbo',
};

const PRICES = {
  '1d':   { price: 20_000,  days: 1,    label: '1 Ngày' },
  '4d':   { price: 30_000,  days: 4,    label: '4 Ngày' },
  '1w':   { price: 60_000,  days: 7,    label: '1 Tuần' },
  '1m':   { price: 120_000, days: 30,   label: '1 Tháng' },
  '3m':   { price: 150_000, days: 90,   label: '3 Tháng' },
  'life': { price: 220_000, days: null, label: 'Vĩnh Viễn' },
};

const GAME_META = {
  lc79_md5:    { name: 'LC79 MD5',    icon: 'fa-fingerprint' },
  lc79_hu:     { name: 'LC79 Hũ',     icon: 'fa-coins' },
  betvip_md5:  { name: 'BetVIP MD5',  icon: 'fa-fingerprint' },
  betvip_hu:   { name: 'BetVIP Hũ',   icon: 'fa-coins' },
  hitclub_md5: { name: 'HitClub MD5', icon: 'fa-fingerprint' },
  hitclub_hu:  { name: 'HitClub Hũ',  icon: 'fa-coins' },
  b52_md5:     { name: 'B52 MD5',     icon: 'fa-fingerprint' },
  b52_hu:      { name: 'B52 Hũ',      icon: 'fa-coins' },
  luckywin:    { name: 'Sicbo LuckyWin', icon: 'fa-dice' },
  sumclub:     { name: 'Sicbo SumClub',  icon: 'fa-dice-d6' },
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
// UTILS & TOKEN
// ============================================================
function nowMs() { return Date.now(); }

function genKey(plan) {
  const pref = { '1d': 'HT1D', '4d': 'HT4D', '1w': 'HT1W', '1m': 'HT1M', '3m': 'HT3M', 'life': 'HTLIFE' }[plan] || 'HTX';
  return `${pref}-${crypto.randomBytes(8).toString('hex').toUpperCase()}`;
}

const RATE_BUCKETS = new Map();
function clientIp(req) { return req.ip || req.socket.remoteAddress || 'unknown'; }
function rateLimit(max = RATE_LIMIT_MAX, windowMs = RATE_LIMIT_WINDOW_MS) {
  return (req, res, next) => {
    const ip = clientIp(req); const n = nowMs();
    let rec = RATE_BUCKETS.get(ip);
    if (!rec || n > rec.reset) { rec = { count: 0, reset: n + windowMs }; RATE_BUCKETS.set(ip, rec); }
    rec.count++;
    if (rec.count > max) return res.status(429).json({ ok: false, error: 'Quá nhiều request' });
    next();
  };
}

const CIRCUIT = new Map();
function circuitCheck(id) {
  const c = CIRCUIT.get(id);
  if (!c) return true;
  if (c.fails >= CIRCUIT_FAIL_THRESH && nowMs() - c.last < CIRCUIT_OPEN_MS) return false;
  return true;
}
function circuitFail(id) {
  const c = CIRCUIT.get(id) || { fails: 0, last: 0 };
  c.fails++; c.last = nowMs(); CIRCUIT.set(id, c);
}
function circuitOk(id) { CIRCUIT.delete(id); }

let TOKEN_MAP = {}, TOKEN_MAP_PREV = {}, CURRENT_BUCKET = -1;
const hourBucket = () => Math.floor(nowMs() / TOKEN_ROTATE_MS);
function signWith(game, bucket, secret) { return crypto.createHmac('sha256', secret).update(`${game}:${bucket}`).digest('base64url').slice(0, 16); }

function rebuildTokenMap() {
  const bucket = hourBucket();
  if (bucket === CURRENT_BUCKET) return;
  CURRENT_BUCKET = bucket;
  TOKEN_MAP = {}; TOKEN_MAP_PREV = {};
  for (const game of Object.keys(API_GAME_RAW)) {
    TOKEN_MAP[signWith(game, bucket, SECRET_KEY)] = game;
    if (SECRET_KEY_PREV) TOKEN_MAP[signWith(game, bucket, SECRET_KEY_PREV)] = game;
    TOKEN_MAP_PREV[signWith(game, bucket - 1, SECRET_KEY)] = game;
    if (SECRET_KEY_PREV) TOKEN_MAP_PREV[signWith(game, bucket - 1, SECRET_KEY_PREV)] = game;
  }
}
function tokenToGame(token) { return TOKEN_MAP[token] || TOKEN_MAP_PREV[token] || null; }
function gameToToken(game) { for (const [t, g] of Object.entries(TOKEN_MAP)) { if (g === game) return t; } return null; }
rebuildTokenMap();

function predictSicbo(history, gameNum = '') {
  if (!Array.isArray(history) || history.length < 3) return { du_doan: 'TÀI', tin_cay: 55 };
  const recent = history.slice(0, 15).map(h => Number(h.resultType)).filter(v => v === 0 || v === 1).reverse();
  if (recent.length < 3) return { du_doan: 'TÀI', tin_cay: 55 };
  const taiCount = recent.filter(v => v === 1).length;
  const xiuCount = recent.length - taiCount;
  let streak = 1;
  for (let i = recent.length - 1; i > 0; i--) { if (recent[i] === recent[i - 1]) streak++; else break; }
  const last = recent[recent.length - 1];
  let predict = ''; let confidence = 0;
  if (streak >= 5) { predict = last === 1 ? 'TÀI' : 'XỈU'; confidence = 65 + Math.min(streak * 2, 15); }
  else if (streak === 3 || streak === 4) { predict = last === 1 ? 'XỈU' : 'TÀI'; confidence = 60 + streak * 2; }
  else if (recent.length >= 4) {
    const a = recent[recent.length - 1], b = recent[recent.length - 2], c = recent[recent.length - 3], d = recent[recent.length - 4];
    if (a !== b && b !== c && c !== d) { predict = a === 1 ? 'XỈU' : 'TÀI'; confidence = 65; }
    else if (c === d && a === b && a !== c) { predict = a === 1 ? 'XỈU' : 'TÀI'; confidence = 60; }
  }
  if (!predict) {
    if (taiCount === xiuCount) {
      const h = crypto.createHash('sha256').update(String(gameNum || recent.join(''))).digest();
      predict = (h[0] & 1) === 1 ? 'TÀI' : 'XỈU';
      confidence = 50;
    }
    else { predict = taiCount > xiuCount ? 'TÀI' : 'XỈU'; confidence = 50 + Math.abs(taiCount - xiuCount) * 2; }
  }
  return { du_doan: predict, tin_cay: Math.min(85, Math.max(50, Math.round(confidence))) };
}

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
// EXPRESS APP
// ============================================================
const app = express();
app.disable('x-powered-by');
app.set('trust proxy', 1);
app.use(express.json({ limit: '32kb' }));

app.use((req, res, next) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, private');
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Headers', 'Content-Type, x-kairo-key, x-device-id, Authorization');
  next();
});

// ✅ FIX LỖI "NOT FOUND": Cho phép truy cập mọi đường dẫn trang chủ đều ra file HTML
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/index.html', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/kho.html', (req, res) => res.sendFile(path.join(__dirname, 'cac2.html')));
app.get('/cac2.html', (req, res) => res.sendFile(path.join(__dirname, 'cac2.html')));
app.get('/tool', (req, res) => res.sendFile(path.join(__dirname, 'cac2.html')));
app.get('/healthz', (_, res) => res.status(200).send('ok'));

// ============================================================
// MIDDLEWARES
// ============================================================
async function requireValidKey(req, res, next) {
  try {
    let rawKey = req.headers['x-kairo-key'];
    if (!rawKey && req.headers.authorization?.startsWith('Bearer ')) rawKey = req.headers.authorization.slice(7);
    const key = String(rawKey || '').trim().toUpperCase();
    if (!key) return res.status(401).json({ ok: false, error: 'Thiếu key xác thực' });

    const deviceId = String(req.headers['x-device-id'] || '').trim();
    if (!deviceId) return res.status(400).json({ ok: false, error: 'Lỗi: Không tìm thấy Device ID' });

    const result = await pool.query('SELECT * FROM keys WHERE key = $1', [key]);
    const row = result.rows[0];
    if (!row || row.is_active !== 1) return res.status(401).json({ ok: false, error: 'Key không hợp lệ hoặc chưa kích hoạt' });
    if (row.expires_at != null && nowMs() > row.expires_at) return res.status(401).json({ ok: false, error: 'Key đã hết hạn' });

    if (row.device_id == null) {
      await pool.query(`UPDATE keys SET device_id = $1 WHERE key = $2 AND device_id IS NULL`, [deviceId, key]);
      row.device_id = deviceId;
    } else if (row.device_id !== deviceId) {
      return res.status(403).json({ ok: false, error: 'Key đã được sử dụng trên thiết bị khác' });
    }

    req.keyRow = row;
    next();
  } catch (e) {
    return res.status(500).json({ ok: false, error: 'Lỗi server' });
  }
}

// ============================================================
// API ROUTES
// ============================================================
app.get('/api/key/verify', rateLimit(), requireValidKey, (req, res) => {
  res.json({ ok: true, plan: req.keyRow.plan, expires_at: req.keyRow.expires_at });
});

app.get('/api/games', rateLimit(), requireValidKey, (req, res) => {
  rebuildTokenMap();
  const games = [];
  for (const [k, m] of Object.entries(GAME_META)) {
    const t = gameToToken(k);
    if (t) games.push({ id: k, token: t, ...m });
  }
  const rotateAt = (hourBucket() + 1) * TOKEN_ROTATE_MS;
  res.json({ games, rotate_at: rotateAt });
});

// ✅ FIX LỖI API: Đọc đúng 100% cấu trúc JSON mới (phienDoan, duDoan, ketQua, tiLe)
app.get('/api/predict/:token', rateLimit(), requireValidKey, async (req, res) => {
  rebuildTokenMap();
  const gameId = tokenToGame(req.params.token);
  if (!gameId) return res.status(403).json({ error: 'Token không hợp lệ' });

  const cached = PREDICT_CACHE.get(gameId);
  if (cached && nowMs() - cached.at < PREDICT_CACHE_MS) return res.json(cached.data);

  try {
    const raw = await fetchGameApi(gameId);
    const type = GAME_TYPE[gameId] || 'sicbo';
    let data = {};

    if (type === 'md5hu') {
      // Lấy chính xác từng trường
      data = {
        phien: raw.phien || null,
        ketQua: raw.ketQua || null,
        tong: raw.tong || null,
        phienDoan: raw.phienDoan || null,
        duDoan: String(raw.duDoan || '').toUpperCase(),
        tiLe: raw.tiLe || '0%'
      };
    } else {
      const history = Array.isArray(raw) ? raw : (raw.data || raw.history || []);
      if (!history.length) throw new Error('Rỗng');
      const phien = String(history[0].gameNum || history[0].phien || '');
      const pred = predictSicbo(history, phien);
      data = { phien, phienDoan: Number(phien) + 1, duDoan: pred.du_doan, tiLe: pred.tin_cay + '%', ketQua: null, tong: null };
    }
    
    PREDICT_CACHE.set(gameId, { data, at: nowMs() });
    res.json(data);
  } catch (e) {
    console.error(`Lỗi API Game (${gameId}):`, e.message);
    res.json({ phien: null, ketQua: null, tong: null, phienDoan: null, duDoan: 'BẢO TRÌ', tiLe: '0%' });
  }
});

app.post('/api/key/activate', rateLimit(10, 60_000), async (req, res) => {
  try {
    const ip = clientIp(req);
    const fail = ACTIVATE_FAILS.get(ip);
    if (fail && fail.lockUntil && nowMs() < fail.lockUntil) {
      return res.status(429).json({ ok: false, error: 'Quá nhiều lần thử sai. Vui lòng đợi 15 phút.' });
    }

    const key = String(req.body?.key || '').trim().toUpperCase();
    const deviceId = String(req.body?.device_id || req.headers['x-device-id'] || '').trim();
    
    if (!key) return res.status(400).json({ ok: false, error: 'Thiếu key' });
    if (!deviceId) return res.status(400).json({ ok: false, error: 'Thiếu Device ID' });

    const result = await pool.query('SELECT * FROM keys WHERE key = $1', [key]);
    const row = result.rows[0];
    if (!row) {
      const rec = ACTIVATE_FAILS.get(ip) || { count: 0, lockUntil: 0 };
      rec.count++;
      if (rec.count >= ACTIVATE_FAIL_MAX) rec.lockUntil = nowMs() + ACTIVATE_LOCK_MS;
      ACTIVATE_FAILS.set(ip, rec);
      return res.json({ ok: false, error: 'Key không tồn tại' });
    }

    if (row.activated_at == null) {
      const finalExp = row.duration_days == null ? null : nowMs() + row.duration_days * 86400000;
      const upd = await pool.query(
        `UPDATE keys SET activated_at = $1, expires_at = $2, is_active = 1, device_id = $3 WHERE key = $4`,
        [nowMs(), finalExp, deviceId, key]
      );
      if (upd.rowCount === 0) return res.json({ ok: false, error: 'Key đang bị thao tác' });
      ACTIVATE_FAILS.delete(ip);
      return res.json({ ok: true, first_time: true, message: 'Kích hoạt thành công' });
    }

    if (row.device_id && row.device_id !== deviceId) {
      return res.status(403).json({ ok: false, error: 'Key này đã được kích hoạt trên thiết bị khác!' });
    }

    if (row.duration_days != null && row.expires_at && nowMs() > row.expires_at) {
      return res.json({ ok: false, error: 'Key đã hết hạn' });
    }
    ACTIVATE_FAILS.delete(ip);
    return res.json({ ok: true, first_time: false, message: 'Kích hoạt thành công' });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'Lỗi server' });
  }
});

app.get('/api/bank/history', async (req, res) => {
  try {
    const r = await axios.get(API_BANK, { timeout: API_TIMEOUT_MS });
    res.json({ ok: true, data: r.data });
  } catch (e) {
    res.status(502).json({ ok: false, error: 'BANK_API_FAIL' });
  }
});

// ============================================================
// TELEGRAM BOT (Đã lược bớt phần menu dài dòng cho gọn file)
// ============================================================
let bot = null;
if (BOT_TOKEN) {
  const httpsAgent = new https.Agent({ rejectUnauthorized: false, family: 4 });
  bot = new TelegramBot(BOT_TOKEN, {
    polling: { autoStart: false, interval: 1000, params: { timeout: 10 }, conflictRetryDelayMs: 3000 },
    request: { agent: httpsAgent },
  });

  bot.deleteWebHook().then(() => {
    bot.startPolling();
    console.log('🤖 Bot Telegram đã khởi động');
  }).catch(e => console.error('Bot error:', e.message));
}

// ============================================================
// STARTUP
// ============================================================
initDB()
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => { 
      console.log(`🌐 Web Server running on port ${PORT}`); 
      console.log(`👉 Truy cập: https://en-omg-tun-net.onrender.com/kho.html`);
    });
  })
  .catch(err => {
    console.error('❌ Không thể khởi động server vì lỗi DB:', err.message);
    process.exit(1);
  });
