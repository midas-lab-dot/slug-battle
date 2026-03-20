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
  POOL_WALLET: 'axm1ufejnwmpdg5ukzhqh3r2nc4hsy3wuukp0n97c0',
  AXIOME_REST: 'https://api-chain.axiomechain.org',
  NETWORK: 'axiome',
  FEE_PERCENT: 2,
  PORT: process.env.PORT || 3000,
  // NFT prices in SLUG (no decimals)
  NFT_PRICES: { common: 7375, rare: 12375, epic: 24875 },
  // NFT max supply
  NFT_MAX: { common: 388, rare: 233, epic: 156 },
  // Weekly payouts per NFT
  NFT_WEEKLY: { common: 26, rare: 75, epic: 144 },
  // Total earn per NFT over 78 weeks
  NFT_TOTAL: { common: 2005, rare: 5843, epic: 11218 },
  // Total periods
  NFT_PERIODS: 78,
  // SLUG decimals
  SLUG_DECIMALS: 6,
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

  -- NFT MODULE
  CREATE TABLE IF NOT EXISTS nft_holders (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet      TEXT NOT NULL UNIQUE,
    rank        TEXT NOT NULL,
    nft_id      INTEGER NOT NULL,
    purchased_at INTEGER DEFAULT (unixepoch()),
    total_received INTEGER DEFAULT 0,
    tx_memo     TEXT
  );
  CREATE TABLE IF NOT EXISTS nft_counters (
    rank        TEXT PRIMARY KEY,
    next_id     INTEGER DEFAULT 1,
    sold        INTEGER DEFAULT 0,
    max_supply  INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS nft_distributions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    period      INTEGER NOT NULL,
    distributed_at INTEGER DEFAULT (unixepoch()),
    total_slug  INTEGER NOT NULL,
    recipients  INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS nft_pending_payments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet      TEXT NOT NULL,
    rank        TEXT NOT NULL,
    amount      INTEGER NOT NULL,
    memo        TEXT NOT NULL,
    created_at  INTEGER DEFAULT (unixepoch()),
    verified    INTEGER DEFAULT 0
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
// NFT MODULE
// ============================================================

// Инициализация счётчиков при старте
function initNftCounters() {
  for (const [rank, max] of Object.entries(CONFIG.NFT_MAX)) {
    const exists = db.prepare('SELECT 1 FROM nft_counters WHERE rank=?').get(rank);
    if (!exists) {
      db.prepare('INSERT INTO nft_counters (rank, next_id, sold, max_supply) VALUES (?,1,0,?)')
        .run(rank, max);
    }
  }
}
initNftCounters();

// Статистика продаж NFT
app.get('/nft/stats', (req, res) => {
  const counters = db.prepare('SELECT * FROM nft_counters').all();
  const stats = {};
  for (const c of counters) {
    stats[c.rank] = {
      sold: c.sold,
      available: c.max_supply - c.sold,
      max: c.max_supply,
    };
  }
  const totalSold = counters.reduce((s, c) => s + c.sold, 0);
  res.json({ stats, total_sold: totalSold, total_max: 777 });
});

// Получить данные холдера по кошельку
app.get('/nft/holder/:wallet', (req, res) => {
  const { wallet } = req.params;
  if (!wallet.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });

  const holder = db.prepare('SELECT * FROM nft_holders WHERE wallet=?').get(wallet);
  if (!holder) return res.status(404).json({ error: 'NFT not found for this wallet' });

  const weekly = CONFIG.NFT_WEEKLY[holder.rank] || 0;
  const totalEarn = CONFIG.NFT_TOTAL[holder.rank] || 0;
  const periodsLeft = CONFIG.NFT_PERIODS - (db.prepare('SELECT COUNT(*) as c FROM nft_distributions').get().c);

  res.json({
    wallet: holder.wallet,
    rank: holder.rank,
    nft_id: holder.nft_id,
    purchased_at: new Date(holder.purchased_at * 1000).toLocaleDateString('ru-RU'),
    total_received: holder.total_received,
    weekly_slug: weekly,
    total_earn: totalEarn,
    periods_left: periodsLeft,
  });
});

// Все холдеры (для выплат)
app.get('/nft/holders', (req, res) => {
  const holders = db.prepare('SELECT * FROM nft_holders ORDER BY purchased_at DESC').all();
  res.json({ holders, total: holders.length });
});

// Инициировать покупку NFT — возвращает QR строку и резервирует ID
app.post('/nft/buy', (req, res) => {
  const { wallet, rank } = req.body;

  if (!wallet?.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });
  if (!['common','rare','epic'].includes(rank)) return res.status(400).json({ error: 'Invalid rank' });

  // Проверяем что кошелёк ещё не купил NFT
  const existing = db.prepare('SELECT 1 FROM nft_holders WHERE wallet=?').get(wallet);
  if (existing) return res.status(400).json({ error: 'This wallet already owns an NFT' });

  // Получаем счётчик
  const counter = db.prepare('SELECT * FROM nft_counters WHERE rank=?').get(rank);
  if (!counter) return res.status(400).json({ error: 'Invalid rank' });
  if (counter.sold >= counter.max_supply) return res.status(400).json({ error: `${rank} sold out` });

  const nftId = counter.next_id;
  const price = CONFIG.NFT_PRICES[rank];
  const memo  = `NFT:${rank.toUpperCase()}:${nftId}:${wallet}`;

  // Генерируем QR строку для оплаты
  const qrPayload = {
    type: 'cosmwasm_execute',
    network: CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT,
    funds: [],
    msg: {
      transfer: {
        recipient: CONFIG.PROJECT_WALLET,
        amount: String(price * Math.pow(10, CONFIG.SLUG_DECIMALS)),
      }
    },
    memo,
  };
  const qrString = `axiomesign://${Buffer.from(JSON.stringify(qrPayload)).toString('base64')}`;

  // Записываем ожидающий платёж
  db.prepare('INSERT OR REPLACE INTO nft_pending_payments (wallet, rank, amount, memo) VALUES (?,?,?,?)')
    .run(wallet, rank, price * Math.pow(10, CONFIG.SLUG_DECIMALS), memo);

  res.json({
    wallet,
    rank,
    nft_id: nftId,
    price_slug: price,
    memo,
    qr_string: qrString,
    message: `Оплати ${price.toLocaleString()} SLUG чтобы получить ${rank.toUpperCase()} #${nftId}`,
  });
});

// Подтвердить покупку вручную (admin) — после проверки транзакции
app.post('/nft/confirm', (req, res) => {
  const { wallet, rank, nft_id, admin_key } = req.body;

  if (admin_key !== process.env.ADMIN_KEY && admin_key !== 'slug-admin-2024') {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  confirmNftPurchase(wallet, rank, nft_id);
  res.json({ success: true, wallet, rank, nft_id });
});

function confirmNftPurchase(wallet, rank, nftId) {
  // Записываем холдера
  try {
    db.prepare(`
      INSERT INTO nft_holders (wallet, rank, nft_id, tx_memo)
      VALUES (?, ?, ?, ?)
    `).run(wallet, rank, nftId, `NFT:${rank.toUpperCase()}:${nftId}:${wallet}`);

    // Обновляем счётчик
    db.prepare('UPDATE nft_counters SET sold=sold+1, next_id=next_id+1 WHERE rank=?').run(rank);

    // Убираем из ожидающих
    db.prepare('UPDATE nft_pending_payments SET verified=1 WHERE wallet=? AND rank=?').run(wallet, rank);

    console.log(`✅ NFT confirmed: ${rank} #${nftId} → ${wallet}`);
  } catch(e) {
    console.error('confirmNftPurchase error:', e.message);
  }
}

// Авто-верификация платежей за NFT (каждые 5 секунд)
async function checkNftPayments() {
  const pending = db.prepare('SELECT * FROM nft_pending_payments WHERE verified=0').all();
  if (!pending.length) return;

  try {
    const url = `${CONFIG.AXIOME_REST}/cosmos/tx/v1beta1/txs?events=wasm._contract_address%3D%27${CONFIG.SLUG_CONTRACT}%27&events=wasm.recipient%3D%27${CONFIG.PROJECT_WALLET}%27&order_by=ORDER_DESC&limit=20`;
    const res  = await fetch(url, { signal: AbortSignal.timeout(5000) });
    const data = await res.json();
    const txs  = data?.txs || [];

    for (const tx of txs) {
      const txHash = tx.txhash;
      if (!txHash) continue;

      const seen = db.prepare('SELECT 1 FROM seen_txs WHERE tx_hash=?').get(txHash);
      if (seen) continue;

      const memo = tx.body?.memo || '';
      if (!memo.startsWith('NFT:')) continue;

      // Парсим memo: NFT:RARE:42:axm1...
      const parts = memo.split(':');
      if (parts.length < 4) continue;

      const rank   = parts[1].toLowerCase();
      const nftId  = parseInt(parts[2]);
      const wallet = parts.slice(3).join(':');

      const pend = pending.find(p => p.wallet === wallet && p.rank === rank);
      if (!pend) continue;
      if (tx.code !== undefined && tx.code !== 0) continue;

      // Помечаем транзакцию
      db.prepare('INSERT INTO seen_txs (tx_hash) VALUES (?)').run(txHash);

      // Подтверждаем покупку
      confirmNftPurchase(wallet, rank, nftId);

      // Уведомляем покупателя в Telegram
      const tgUser = db.prepare('SELECT tg_user_id FROM participants WHERE wallet=? LIMIT 1').get(wallet);
      if (tgUser?.tg_user_id) {
        await sendTg(tgUser.tg_user_id,
          `🎉 <b>NFT куплена!</b>\n\n` +
          `🐌 Slug ${rank.charAt(0).toUpperCase() + rank.slice(1)} <b>#${nftId}</b>\n` +
          `💰 Еженедельная выплата: <b>${CONFIG.NFT_WEEKLY[rank]} SLUG</b>\n\n` +
          `Смотри свою улитку: https://slug-token.netlify.app/nft-cabinet?reveal=${rank}:${nftId}`
        );
      }

      console.log(`🐌 NFT payment confirmed: ${rank} #${nftId} → ${wallet} (tx: ${txHash})`);
    }
  } catch(e) {
    console.error('checkNftPayments error:', e.message);
  }
}

setInterval(checkNftPayments, 5000);

// Еженедельная выплата всем холдерам (admin)
app.post('/nft/distribute', async (req, res) => {
  const { admin_key } = req.body;
  if (admin_key !== process.env.ADMIN_KEY && admin_key !== 'slug-admin-2024') {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const holders = db.prepare('SELECT * FROM nft_holders').all();
  if (!holders.length) return res.status(400).json({ error: 'No holders' });

  const period = db.prepare('SELECT COUNT(*) as c FROM nft_distributions').get().c + 1;
  if (period > CONFIG.NFT_PERIODS) return res.status(400).json({ error: 'All periods completed' });

  // Генерируем список транзакций для выплат
  const transfers = holders.map(h => ({
    wallet: h.wallet,
    rank: h.rank,
    nft_id: h.nft_id,
    amount: CONFIG.NFT_WEEKLY[h.rank] || 0,
  }));

  const totalSlug = transfers.reduce((s, t) => s + t.amount, 0);

  // Генерируем axiomesign QR для пакетной выплаты
  // (На практике нужно отправлять по одному — Axiome не поддерживает batch)
  const qrPayloads = transfers.map(t => {
    const payload = {
      type: 'cosmwasm_execute',
      network: CONFIG.NETWORK,
      contract_addr: CONFIG.SLUG_CONTRACT,
      funds: [],
      msg: {
        transfer_from: {
          owner: CONFIG.POOL_WALLET,
          recipient: t.wallet,
          amount: String(t.amount * Math.pow(10, CONFIG.SLUG_DECIMALS)),
        }
      },
      memo: `DIST:${period}:${t.rank}:${t.nft_id}`,
    };
    return {
      wallet: t.wallet,
      amount: t.amount,
      qr: `axiomesign://${Buffer.from(JSON.stringify(payload)).toString('base64')}`,
    };
  });

  // Записываем выплату в историю
  db.prepare('INSERT INTO nft_distributions (period, total_slug, recipients) VALUES (?,?,?)')
    .run(period, totalSlug, holders.length);

  // Обновляем total_received у каждого холдера
  for (const t of transfers) {
    db.prepare('UPDATE nft_holders SET total_received=total_received+? WHERE wallet=?')
      .run(t.amount, t.wallet);
  }

  res.json({
    period,
    total_slug: totalSlug,
    recipients: holders.length,
    transfers: qrPayloads,
    message: `Период ${period} из ${CONFIG.NFT_PERIODS}. Всего: ${totalSlug.toLocaleString()} SLUG → ${holders.length} холдеров`,
  });
});

// История выплат
app.get('/nft/distributions', (req, res) => {
  const history = db.prepare('SELECT * FROM nft_distributions ORDER BY distributed_at DESC LIMIT 20').all();
  res.json({ history, total: history.length });
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
