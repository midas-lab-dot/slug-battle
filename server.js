// ============================================================
// CRYPTO PORTFOLIO BATTLE — Backend Server
// Node.js + Express + better-sqlite3
// ============================================================

const express = require('express');
const Database = require('better-sqlite3');
const cors = require('cors');
const crypto = require('crypto');

const app = express();
app.use(express.json());
app.use(cors());

const CONFIG = {
  BOT_TOKEN: process.env.BOT_TOKEN || 'PASTE_YOUR_NEW_TOKEN_HERE',
  SLUG_CONTRACT: 'axm1nzvd5njkc0gqdezgzzqw00tu056rgxgwaqgq25g9ku0y5au3nwmqxqtr0l',
  PROJECT_WALLET: 'axm1fnhlykzpra0j6xzy0eenpfwn7anp5upm2jxm9k',
  AXIOME_REST: 'https://api-chain.axiomechain.org',
  NETWORK: 'axiome',
  FEE_PERCENT: 2,
  PORT: process.env.PORT || 3000,
};

// ============================================================
// DATABASE
// ============================================================
const db = new Database('battles.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS battles (
    id          TEXT PRIMARY KEY,
    creator     TEXT NOT NULL,
    stake       INTEGER NOT NULL,
    max_players INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    status      TEXT DEFAULT 'open',
    created_at  INTEGER DEFAULT (unixepoch()),
    started_at  INTEGER,
    ends_at     INTEGER,
    winner      TEXT,
    pool_total  INTEGER DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS participants (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    battle_id       TEXT NOT NULL,
    wallet          TEXT NOT NULL,
    tg_user_id      TEXT,
    tg_username     TEXT,
    joined_at       INTEGER DEFAULT (unixepoch()),
    portfolio_start TEXT,
    portfolio_end   TEXT,
    growth_pct      REAL,
    paid            INTEGER DEFAULT 0,
    UNIQUE(battle_id, wallet)
  );
  CREATE TABLE IF NOT EXISTS pending_payments (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    battle_id  TEXT NOT NULL,
    wallet     TEXT NOT NULL,
    amount     INTEGER NOT NULL,
    memo       TEXT NOT NULL,
    created_at INTEGER DEFAULT (unixepoch()),
    verified   INTEGER DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS seen_txs (
    tx_hash TEXT PRIMARY KEY,
    seen_at INTEGER DEFAULT (unixepoch())
  );
`);

// ============================================================
// AXIOME API
// ============================================================
const KNOWN_TOKENS = [
  { contract: 'axm1nzvd5njkc0gqdezgzzqw00tu056rgxgwaqgq25g9ku0y5au3nwmqxqtr0l', symbol: 'SLUG', decimals: 6 },
];

async function getCW20Balance(wallet, contract) {
  try {
    const q = Buffer.from(JSON.stringify({ balance: { address: wallet } })).toString('base64');
    const res = await fetch(`${CONFIG.AXIOME_REST}/cosmwasm/wasm/v1/contract/${contract}/smart/${q}`, { signal: AbortSignal.timeout(6000) });
    const data = await res.json();
    return parseInt(data?.data?.balance || '0');
  } catch { return 0; }
}

async function getNativeBalance(wallet) {
  try {
    const res = await fetch(`${CONFIG.AXIOME_REST}/cosmos/bank/v1beta1/balances/${wallet}`, { signal: AbortSignal.timeout(6000) });
    const data = await res.json();
    const uaxm = data?.balances?.find(b => b.denom === 'uaxm');
    return parseInt(uaxm?.amount || '0');
  } catch { return 0; }
}

async function getFullPortfolio(wallet) {
  const portfolio = {};
  const axmRaw = await getNativeBalance(wallet);
  portfolio['AXM'] = { symbol: 'AXM', amount: axmRaw / 1e6, contract: 'native' };
  for (const t of KNOWN_TOKENS) {
    const raw = await getCW20Balance(wallet, t.contract);
    portfolio[t.symbol] = { symbol: t.symbol, amount: raw / Math.pow(10, t.decimals), contract: t.contract };
  }
  return portfolio;
}

async function getTokenPrices() {
  const prices = { SLUG: 0.008 };
  try {
    const res = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=axiome&vs_currencies=usd', { signal: AbortSignal.timeout(4000) });
    const data = await res.json();
    prices['AXM'] = data?.axiome?.usd || 0.004;
  } catch { prices['AXM'] = 0.004; }
  return prices;
}

async function calcPortfolioUsd(portfolio, prices) {
  let total = 0;
  for (const [sym, d] of Object.entries(portfolio)) total += d.amount * (prices[sym] || 0);
  return total;
}


// ============================================================
// PAYMENT VERIFICATION — проверяет входящие транзакции каждые 5 сек
// ============================================================

async function getRecentIncomingTxs() {
  try {
    const url = `${CONFIG.AXIOME_REST}/cosmos/tx/v1beta1/txs?events=wasm._contract_address%3D%27${CONFIG.SLUG_CONTRACT}%27&events=wasm.recipient%3D%27${CONFIG.PROJECT_WALLET}%27&order_by=ORDER_DESC&limit=20`;
    const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
    const data = await res.json();
    return data?.txs || [];
  } catch (e) {
    console.error('getRecentIncomingTxs error:', e.message);
    return [];
  }
}

function parseMemo(memo) {
  if (!memo?.startsWith('BATTLE:')) return null;
  const parts = memo.split(':');
  if (parts.length < 3) return null;
  return { battleId: parts[1], wallet: parts[2] };
}

async function confirmPayment(battleId, wallet, txHash) {
  try {
    db.prepare('INSERT INTO seen_txs (tx_hash) VALUES (?)').run(txHash);
  } catch { return; }

  db.prepare('UPDATE participants SET paid=1 WHERE battle_id=? AND wallet=?').run(battleId, wallet);
  db.prepare('UPDATE pending_payments SET verified=1 WHERE battle_id=? AND wallet=?').run(battleId, wallet);
  console.log(`Payment confirmed: battle=${battleId} wallet=${wallet} tx=${txHash}`);

  const p = db.prepare('SELECT tg_user_id FROM participants WHERE battle_id=? AND wallet=?').get(battleId, wallet);
  const b = db.prepare('SELECT * FROM battles WHERE id=?').get(battleId);
  if (!b) return;

  const paidCount = db.prepare('SELECT COUNT(*) as c FROM participants WHERE battle_id=? AND paid=1').get(battleId).c;

  if (p?.tg_user_id) {
    await sendTg(p.tg_user_id,
      `Payment confirmed for battle #${battleId}! ` +
      `Players paid: ${paidCount}/${b.max_players}`
    );
  }

  if (paidCount >= b.max_players && b.status === 'open') {
    const endsAt = Math.floor(Date.now() / 1000) + Math.floor(b.duration_ms / 1000);
    db.prepare("UPDATE battles SET status='live', started_at=unixepoch(), ends_at=? WHERE id=?").run(endsAt, battleId);
    console.log(`Battle ${battleId} started! Ends at ${new Date(endsAt * 1000).toISOString()}`);

    const allParts = db.prepare('SELECT tg_user_id FROM participants WHERE battle_id=? AND tg_user_id IS NOT NULL').all(battleId);
    for (const part of allParts) {
      await sendTg(part.tg_user_id,
        `Battle #${battleId} started! Bank: ${(b.stake * b.max_players).toLocaleString()} SLUG. Good luck!`
      );
    }
  }
}

async function checkPayments() {
  const pending = db.prepare('SELECT * FROM pending_payments WHERE verified=0').all();
  if (!pending.length) return;

  const txs = await getRecentIncomingTxs();
  if (!txs.length) return;

  for (const tx of txs) {
    const txHash = tx.txhash;
    if (!txHash) continue;
    const seen = db.prepare('SELECT 1 FROM seen_txs WHERE tx_hash=?').get(txHash);
    if (seen) continue;
    const memo = tx.body?.memo || '';
    const parsed = parseMemo(memo);
    if (!parsed) continue;
    const pend = pending.find(p => p.battle_id === parsed.battleId && p.wallet === parsed.wallet);
    if (!pend) continue;
    if (tx.code !== undefined && tx.code !== 0) continue;
    console.log(`Found payment tx: ${txHash} for battle ${parsed.battleId}`);
    await confirmPayment(parsed.battleId, parsed.wallet, txHash);
  }
}

setInterval(checkPayments, 5000);
console.log('Payment watcher started (5s interval)');

// ============================================================
// TELEGRAM BOT
// ============================================================
async function sendTg(chatId, text, extra = {}) {
  try {
    await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML', ...extra }),
    });
  } catch (e) { console.error('TG send error:', e.message); }
}

async function notifyWinner(battle, winner, prize) {
  const p = db.prepare('SELECT tg_user_id FROM participants WHERE battle_id=? AND wallet=?').get(battle.id, winner);
  if (!p?.tg_user_id) return;

  const payloadObj = {
    type: 'cosmwasm_execute',
    network: CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT,
    funds: [],
    msg: { transfer: { recipient: winner, amount: String(prize * 1e6) } },
    memo: `WIN:${battle.id}`,
  };
  const qr = `axiomesign://${Buffer.from(JSON.stringify(payloadObj)).toString('base64')}`;

  await sendTg(p.tg_user_id,
    `🏆 <b>Ты победил в битве #${battle.id}!</b>\n\n` +
    `💰 Приз: <b>${prize.toLocaleString()} SLUG</b>\n\n` +
    `Для получения выплаты:\n` +
    `1. Открой Axiome Wallet\n` +
    `2. Axiome Connect → Сканировать QR\n\n` +
    `QR-строка (скопируй в Connect):\n<code>${qr.substring(0, 100)}...</code>\n\n` +
    `Полная строка отправлена отдельным сообщением.`
  );
  await sendTg(p.tg_user_id, `<code>${qr}</code>`);
}

// Telegram webhook handler
app.post(`/webhook/${CONFIG.BOT_TOKEN}`, async (req, res) => {
  res.sendStatus(200);
  const msg = req.body?.message;
  if (!msg) return;

  const chatId = msg.chat.id;
  const text = msg.text || '';
  const userId = msg.from.id;

  if (text === '/start') {
    await sendTg(chatId,
      `🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\n` +
      `Собери портфель мем-токенов в Axiome Wallet и сражайся с другими игроками!\n\n` +
      `Чей портфель вырастет больше — забирает банк 🏆\n\n` +
      `Нажми кнопку ниже чтобы открыть игру 👇`,
      {
        reply_markup: {
          inline_keyboard: [[
            { text: '⚔️ Открыть игру', web_app: { url: process.env.FRONTEND_URL || 'https://slug-token.netlify.app/battle' } }
          ]]
        }
      }
    );
  }

  if (text === '/battles') {
    const battles = db.prepare("SELECT * FROM battles WHERE status IN ('open','live') ORDER BY created_at DESC LIMIT 5").all();
    if (!battles.length) {
      await sendTg(chatId, '😴 Активных битв нет. Создай первую!');
      return;
    }
    let txt = '⚔️ <b>Активные битвы:</b>\n\n';
    for (const b of battles) {
      const cnt = db.prepare('SELECT COUNT(*) as c FROM participants WHERE battle_id=?').get(b.id);
      txt += `🔸 <b>#${b.id}</b> | Ставка: ${b.stake} SLUG | ${cnt.c}/${b.max_players} игроков | ${b.status.toUpperCase()}\n`;
    }
    await sendTg(chatId, txt);
  }
});

// Установить webhook
async function setWebhook(baseUrl) {
  const url = `${baseUrl}/webhook/${CONFIG.BOT_TOKEN}`;
  const res = await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/setWebhook?url=${encodeURIComponent(url)}`);
  const data = await res.json();
  console.log('Webhook set:', data);
}

// ============================================================
// REST API ROUTES
// ============================================================
app.get('/health', (_, res) => res.json({ ok: true }));

app.get('/battles', (_, res) => {
  res.json(db.prepare('SELECT * FROM battles ORDER BY created_at DESC LIMIT 50').all());
});

app.get('/battles/:id', (req, res) => {
  const b = db.prepare('SELECT * FROM battles WHERE id=?').get(req.params.id);
  if (!b) return res.status(404).json({ error: 'Not found' });
  const parts = db.prepare('SELECT wallet, tg_username, growth_pct, paid FROM participants WHERE battle_id=? ORDER BY growth_pct DESC').all(req.params.id);
  res.json({ ...b, participants: parts });
});

app.post('/battles', async (req, res) => {
  const { wallet, stake, max_players, duration_key, tg_user_id, tg_username } = req.body;
  if (!wallet?.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });
  if (!stake || stake < 100) return res.status(400).json({ error: 'Min stake 100 SLUG' });

  const durMap = { '1h': 3600000, '24h': 86400000, '3d': 259200000, '7d': 604800000 };
  const dur = durMap[duration_key];
  if (!dur) return res.status(400).json({ error: 'Invalid duration' });
  if (!max_players || max_players < 2 || max_players > 50) return res.status(400).json({ error: 'Players 2-50' });

  const id = crypto.randomBytes(4).toString('hex').toUpperCase();
  db.prepare('INSERT INTO battles (id,creator,stake,max_players,duration_ms) VALUES (?,?,?,?,?)').run(id, wallet, stake, max_players, dur);

  // Сразу записываем создателя как первого участника
  const portfolio = await getFullPortfolio(wallet);
  const prices = await getTokenPrices();
  const usdVal = await calcPortfolioUsd(portfolio, prices);
  const snap = JSON.stringify({ tokens: portfolio, prices, total_usd: usdVal, ts: Date.now() });

  db.prepare('INSERT INTO participants (battle_id,wallet,tg_user_id,tg_username,portfolio_start) VALUES (?,?,?,?,?)').run(id, wallet, tg_user_id || null, tg_username || null, snap);
  db.prepare('UPDATE battles SET pool_total=? WHERE id=?').run(stake, id);

  // QR для оплаты ставки создателем
  const qrPayload = {
    type: 'cosmwasm_execute', network: CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT, funds: [],
    msg: { transfer: { recipient: CONFIG.PROJECT_WALLET, amount: String(stake * 1e6) } },
    memo: `BATTLE:${id}:${wallet}`,
  };
  const qr = `axiomesign://${Buffer.from(JSON.stringify(qrPayload)).toString('base64')}`;

  // Добавляем ожидающий платёж для верификации
  db.prepare('INSERT INTO pending_payments (battle_id, wallet, amount, memo) VALUES (?,?,?,?)').run(id, wallet, stake * 1e6, `BATTLE:${id}:${wallet}`);

  res.json({ id, qr_string: qr, message: 'Battle created' });
});

app.post('/battles/:id/join', async (req, res) => {
  const { wallet, tg_user_id, tg_username } = req.body;
  if (!wallet?.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });

  const b = db.prepare('SELECT * FROM battles WHERE id=?').get(req.params.id);
  if (!b) return res.status(404).json({ error: 'Not found' });
  if (b.status !== 'open') return res.status(400).json({ error: 'Battle not open' });

  const cnt = db.prepare('SELECT COUNT(*) as c FROM participants WHERE battle_id=?').get(req.params.id);
  if (cnt.c >= b.max_players) return res.status(400).json({ error: 'Battle full' });

  const slugBal = await getCW20Balance(wallet, CONFIG.SLUG_CONTRACT);
  if (slugBal < b.stake * 1e6) return res.status(400).json({ error: `Need ${b.stake} SLUG` });

  const portfolio = await getFullPortfolio(wallet);
  const prices = await getTokenPrices();
  const usdVal = await calcPortfolioUsd(portfolio, prices);
  const snap = JSON.stringify({ tokens: portfolio, prices, total_usd: usdVal, ts: Date.now() });

  try {
    db.prepare('INSERT INTO participants (battle_id,wallet,tg_user_id,tg_username,portfolio_start) VALUES (?,?,?,?,?)').run(req.params.id, wallet, tg_user_id || null, tg_username || null, snap);
  } catch (e) {
    if (e.message.includes('UNIQUE')) return res.status(400).json({ error: 'Already joined' });
    throw e;
  }

  const newPool = (b.pool_total || 0) + b.stake;
  db.prepare('UPDATE battles SET pool_total=? WHERE id=?').run(newPool, req.params.id);

  const newCnt = cnt.c + 1;
  if (newCnt >= b.max_players) {
    const endsAt = Math.floor(Date.now() / 1000) + Math.floor(b.duration_ms / 1000);
    db.prepare("UPDATE battles SET status='live',started_at=unixepoch(),ends_at=? WHERE id=?").run(endsAt, req.params.id);
  }

  const qrPayload = {
    type: 'cosmwasm_execute', network: CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT, funds: [],
    msg: { transfer: { recipient: CONFIG.PROJECT_WALLET, amount: String(b.stake * 1e6) } },
    memo: `BATTLE:${req.params.id}:${wallet}`,
  };
  const qr = `axiomesign://${Buffer.from(JSON.stringify(qrPayload)).toString('base64')}`;

  // Добавляем ожидающий платёж для верификации
  db.prepare('INSERT OR REPLACE INTO pending_payments (battle_id, wallet, amount, memo) VALUES (?,?,?,?)').run(req.params.id, wallet, b.stake * 1e6, `BATTLE:${req.params.id}:${wallet}`);

  res.json({ message: 'Joined!', qr_string: qr, portfolio_usd: usdVal });
});

app.post('/battles/:id/finalize', async (req, res) => {
  const b = db.prepare('SELECT * FROM battles WHERE id=?').get(req.params.id);
  if (!b) return res.status(404).json({ error: 'Not found' });
  if (b.status !== 'live') return res.status(400).json({ error: 'Not live' });

  const now = Math.floor(Date.now() / 1000);
  if (b.ends_at && now < b.ends_at) return res.status(400).json({ error: `Ends in ${b.ends_at - now}s` });

  const parts = db.prepare('SELECT * FROM participants WHERE battle_id=?').all(req.params.id);
  const prices = await getTokenPrices();

  let winner = null, best = -Infinity;
  for (const p of parts) {
    const start = JSON.parse(p.portfolio_start || '{}');
    const cur = await getFullPortfolio(p.wallet);
    const curUsd = await calcPortfolioUsd(cur, prices);
    const growth = start.total_usd > 0 ? ((curUsd - start.total_usd) / start.total_usd) * 100 : 0;
    db.prepare('UPDATE participants SET portfolio_end=?,growth_pct=? WHERE battle_id=? AND wallet=?')
      .run(JSON.stringify({ tokens: cur, prices, total_usd: curUsd, ts: Date.now() }), growth, b.id, p.wallet);
    if (growth > best) { best = growth; winner = p.wallet; }
  }

  const pool = b.pool_total || b.stake * parts.length;
  const fee = Math.round(pool * CONFIG.FEE_PERCENT / 100);
  const prize = pool - fee;

  db.prepare("UPDATE battles SET status='ended',winner=? WHERE id=?").run(winner, b.id);
  await notifyWinner(b, winner, prize);

  res.json({ winner, growth_pct: best.toFixed(2), prize_slug: prize, fee_slug: fee });
});

// Проверить статус оплаты
app.get('/battles/:id/payment-status/:wallet', (req, res) => {
  const p = db.prepare('SELECT paid FROM participants WHERE battle_id=? AND wallet=?').get(req.params.id, req.params.wallet);
  if (!p) return res.status(404).json({ error: 'Not found' });
  res.json({ paid: p.paid === 1, battle_id: req.params.id, wallet: req.params.wallet });
});

app.get('/portfolio/:wallet', async (req, res) => {
  if (!req.params.wallet.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });
  const [portfolio, prices] = await Promise.all([getFullPortfolio(req.params.wallet), getTokenPrices()]);
  const total_usd = await calcPortfolioUsd(portfolio, prices);
  res.json({ wallet: req.params.wallet, portfolio, prices, total_usd });
});

// ============================================================
// AUTO FINALIZE every 5 min
// ============================================================
setInterval(async () => {
  const expired = db.prepare("SELECT id FROM battles WHERE status='live' AND ends_at<=?").all(Math.floor(Date.now()/1000));
  for (const b of expired) {
    try { await fetch(`http://localhost:${CONFIG.PORT}/battles/${b.id}/finalize`, { method: 'POST' }); }
    catch (e) { console.error(e.message); }
  }
}, 5 * 60 * 1000);

// ============================================================
// START
// ============================================================
app.listen(CONFIG.PORT, async () => {
  console.log(`🐌 SLUG Battle Server on port ${CONFIG.PORT}`);
  if (process.env.RAILWAY_STATIC_URL) {
    await setWebhook(`https://${process.env.RAILWAY_STATIC_URL}`);
  }
});
