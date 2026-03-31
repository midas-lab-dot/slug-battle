// ============================================================
// SLUG — Backend Server v5
// Node.js + Express + PostgreSQL
// ============================================================

const express  = require('express');
const { Pool } = require('pg');
const cors     = require('cors');
const crypto   = require('crypto');

const app = express();
app.use(express.json());
app.use(cors());

// ============================================================
// КОНФИГ
// ============================================================
const CONFIG = {
  BOT_TOKEN:     process.env.BOT_TOKEN     || '',
  SLUG_CONTRACT: 'axm1nzvd5njkc0gqdezgzzqw00tu056rgxgwaqgq25g9ku0y5au3nwmqxqtr0l',
  PROJECT_WALLET:'axm1fnhlykzpra0j6xzy0eenpfwn7anp5upm2jxm9k',
  POOL_WALLET:   'axm1ufejnwmpdg5ukzhqh3r2nc4hsy3wuukp0n97c0',
  NFT_CONTRACT:  process.env.NFT_CONTRACT  || '',   // заполнить после деплоя контракта
  AXIOME_REST:   'https://api-chain.axiomechain.org',
  NETWORK:       'axiome',
  ADMIN_KEY:     process.env.ADMIN_KEY     || 'slug-admin-2024',
  FEE_PERCENT:   2,
  PORT:          process.env.PORT          || 3000,
  SLUG_DECIMALS: 6,

  // NFT параметры
  NFT_PRICES:  { common: 7375,  rare: 12375, epic: 24875  },  // в SLUG
  NFT_MAX:     { common: 388,   rare: 233,   epic: 156    },
  NFT_WEEKLY:  { common: 26,    rare: 75,    epic: 144    },  // SLUG/нед на 1 NFT
  NFT_TOTAL:   { common: 2005,  rare: 5843,  epic: 11218  },  // за 78 недель
  NFT_PERIODS: 78,
};

// ============================================================
// POSTGRESQL
// ============================================================
const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('railway') ? { rejectUnauthorized: false } : false,
});

async function initDB() {
  await db.query(`
    -- Portfolio Battle
    CREATE TABLE IF NOT EXISTS battles (
      id           TEXT PRIMARY KEY,
      creator      TEXT NOT NULL,
      stake        INTEGER NOT NULL,
      max_players  INTEGER NOT NULL,
      duration_ms  BIGINT NOT NULL,
      status       TEXT DEFAULT 'open',
      created_at   BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
      started_at   BIGINT,
      ends_at      BIGINT,
      winner       TEXT,
      pool_total   INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS participants (
      id              SERIAL PRIMARY KEY,
      battle_id       TEXT NOT NULL,
      wallet          TEXT NOT NULL,
      tg_user_id      TEXT,
      tg_username     TEXT,
      joined_at       BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
      portfolio_start TEXT,
      portfolio_end   TEXT,
      growth_pct      REAL,
      paid            INTEGER DEFAULT 0,
      UNIQUE(battle_id, wallet)
    );
    CREATE TABLE IF NOT EXISTS pending_payments (
      id         SERIAL PRIMARY KEY,
      battle_id  TEXT NOT NULL,
      wallet     TEXT NOT NULL,
      amount     BIGINT NOT NULL,
      memo       TEXT NOT NULL,
      created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
      verified   INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS seen_txs (
      tx_hash TEXT PRIMARY KEY,
      seen_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
    );

    -- NFT MODULE
    CREATE TABLE IF NOT EXISTS nft_holders (
      id           SERIAL PRIMARY KEY,
      wallet       TEXT NOT NULL,
      rank         TEXT NOT NULL,
      nft_id       INTEGER NOT NULL,
      nft_key      TEXT NOT NULL,           -- "common:42"
      purchased_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
      total_received BIGINT DEFAULT 0,
      tx_hash      TEXT,
      UNIQUE(wallet, nft_key)
    );
    CREATE INDEX IF NOT EXISTS idx_nft_holders_wallet ON nft_holders(wallet);
    CREATE INDEX IF NOT EXISTS idx_nft_holders_rank   ON nft_holders(rank);

    CREATE TABLE IF NOT EXISTS nft_counters (
      rank       TEXT PRIMARY KEY,
      next_id    INTEGER DEFAULT 1,
      sold       INTEGER DEFAULT 0,
      max_supply INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS nft_distributions (
      id             SERIAL PRIMARY KEY,
      period         INTEGER NOT NULL,
      distributed_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
      total_slug     BIGINT NOT NULL,
      recipients     INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS nft_pending_buy (
      id         SERIAL PRIMARY KEY,
      wallet     TEXT NOT NULL,
      rank       TEXT NOT NULL,
      nft_id     INTEGER NOT NULL,
      nft_key    TEXT NOT NULL,
      amount     BIGINT NOT NULL,
      created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
      verified   INTEGER DEFAULT 0,
      UNIQUE(wallet, nft_key)
    );
  `);

  // Инициализация счётчиков NFT
  for (const [rank, max] of Object.entries(CONFIG.NFT_MAX)) {
    await db.query(`
      INSERT INTO nft_counters (rank, next_id, sold, max_supply)
      VALUES ($1, 1, 0, $2)
      ON CONFLICT (rank) DO NOTHING
    `, [rank, max]);
  }

  console.log('✅ PostgreSQL DB initialized');
}

// ============================================================
// AXIOME CHAIN API
// ============================================================
async function getCW20Balance(wallet, contract) {
  try {
    const q = Buffer.from(JSON.stringify({ balance: { address: wallet } })).toString('base64');
    const res = await fetch(
      `${CONFIG.AXIOME_REST}/cosmwasm/wasm/v1/contract/${contract}/smart/${q}`,
      { signal: AbortSignal.timeout(6000) }
    );
    const data = await res.json();
    return parseInt(data?.data?.balance || '0');
  } catch { return 0; }
}

async function getNativeBalance(wallet) {
  try {
    const res = await fetch(
      `${CONFIG.AXIOME_REST}/cosmos/bank/v1beta1/balances/${wallet}`,
      { signal: AbortSignal.timeout(6000) }
    );
    const data = await res.json();
    const uaxm = data?.balances?.find(b => b.denom === 'uaxm');
    return parseInt(uaxm?.amount || '0');
  } catch { return 0; }
}

async function getFullPortfolio(wallet) {
  const portfolio = {};
  const axmRaw = await getNativeBalance(wallet);
  portfolio['AXM'] = { symbol: 'AXM', amount: axmRaw / 1e6, contract: 'native' };
  const KNOWN_TOKENS = [
    { contract: CONFIG.SLUG_CONTRACT, symbol: 'SLUG', decimals: 6 },
  ];
  for (const t of KNOWN_TOKENS) {
    const raw = await getCW20Balance(wallet, t.contract);
    portfolio[t.symbol] = { symbol: t.symbol, amount: raw / Math.pow(10, t.decimals), contract: t.contract };
  }
  return portfolio;
}

async function getTokenPrices() {
  const prices = { SLUG: 0.008 };
  try {
    const res = await fetch(
      'https://api.coingecko.com/api/v3/simple/price?ids=axiome&vs_currencies=usd',
      { signal: AbortSignal.timeout(4000) }
    );
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
// QR ГЕНЕРАТОРЫ
// ============================================================

// Правильный QR для покупки NFT через cw20.send() — атомарно
function generateBuyNftQR(wallet, rank, nftId, price) {
  const hookMsg = Buffer.from(JSON.stringify({ buy_nft: { rarity: rank } })).toString('base64');
  const payload = {
    type: 'cosmwasm_execute',
    network: CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT,
    funds: [],
    msg: {
      send: {
        contract: CONFIG.NFT_CONTRACT || CONFIG.PROJECT_WALLET,
        amount: String(price * Math.pow(10, CONFIG.SLUG_DECIMALS)),
        msg: hookMsg,
      }
    },
    memo: `NFT:${rank.toUpperCase()}:${nftId}:${wallet}`,
  };
  return `axiomesign://${Buffer.from(JSON.stringify(payload)).toString('base64')}`;
}

// QR для выплаты одному холдеру (ты подписываешь через Axiome Wallet)
function generateDistributeQR(recipient, amountSlug, period) {
  const payload = {
    type: 'cosmwasm_execute',
    network: CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT,
    funds: [],
    msg: {
      transfer_from: {
        owner:     CONFIG.POOL_WALLET,
        recipient: recipient,
        amount:    String(amountSlug * Math.pow(10, CONFIG.SLUG_DECIMALS)),
      }
    },
    memo: `DIST:${period}`,
  };
  return `axiomesign://${Buffer.from(JSON.stringify(payload)).toString('base64')}`;
}

// QR для Portfolio Battle
function generateBattleQR(battleId, wallet, stake) {
  const payload = {
    type: 'cosmwasm_execute',
    network: CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT,
    funds: [],
    msg: {
      transfer: {
        recipient: CONFIG.PROJECT_WALLET,
        amount: String(stake * Math.pow(10, CONFIG.SLUG_DECIMALS)),
      }
    },
    memo: `BATTLE:${battleId}:${wallet}`,
  };
  return `axiomesign://${Buffer.from(JSON.stringify(payload)).toString('base64')}`;
}

// ============================================================
// TELEGRAM BOT
// ============================================================
async function sendTg(chatId, text, extra = {}) {
  if (!CONFIG.BOT_TOKEN) return;
  try {
    await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML', ...extra }),
    });
  } catch(e) { console.error('TG error:', e.message); }
}

// Webhook handler
app.post(`/webhook/${CONFIG.BOT_TOKEN}`, async (req, res) => {
  res.sendStatus(200);
  const msg = req.body?.message;
  if (!msg) return;
  const chatId = msg.chat.id;
  const text   = msg.text || '';

  if (text === '/start') {
    await sendTg(chatId,
      `🐌 <b>SLUG — Крипто Экосистема</b>\n\n` +
      `🏆 Portfolio Battle — соревнуйся портфелем\n` +
      `🎨 NFT Улитки — пассивный доход в SLUG\n` +
      `🎮 SLUG Runner — игра с наградами\n\n` +
      `Выбери что хочешь открыть:`,
      {
        reply_markup: {
          inline_keyboard: [
            [{ text: '⚔️ Portfolio Battle', web_app: { url: `${process.env.FRONTEND_URL || 'https://slug-token.netlify.app'}/battle` } }],
            [{ text: '🐌 NFT Коллекция',    web_app: { url: `${process.env.FRONTEND_URL || 'https://slug-token.netlify.app'}/nft-shop` } }],
            [{ text: '💼 Мой NFT кабинет',  web_app: { url: `${process.env.FRONTEND_URL || 'https://slug-token.netlify.app'}/nft-cabinet` } }],
          ]
        }
      }
    );
  }

  if (text === '/battles') {
    const { rows } = await db.query(
      "SELECT * FROM battles WHERE status IN ('open','live') ORDER BY created_at DESC LIMIT 5"
    );
    if (!rows.length) { await sendTg(chatId, '😴 Активных битв нет. Создай первую!'); return; }
    let txt = '⚔️ <b>Активные битвы:</b>\n\n';
    for (const b of rows) {
      const { rows: cnt } = await db.query('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1', [b.id]);
      txt += `🔸 <b>#${b.id}</b> | ${b.stake} SLUG | ${cnt[0].c}/${b.max_players} | ${b.status.toUpperCase()}\n`;
    }
    await sendTg(chatId, txt);
  }

  if (text === '/mynft') {
    const userId = msg.from.id.toString();
    const { rows } = await db.query(
      'SELECT n.* FROM nft_holders n JOIN participants p ON n.wallet = p.wallet WHERE p.tg_user_id=$1 LIMIT 10',
      [userId]
    );
    if (!rows.length) {
      await sendTg(chatId, '🐌 У тебя пока нет NFT улиток.\n\n<a href="https://slug-token.netlify.app/nft-shop">Купить NFT →</a>');
      return;
    }
    let txt = '🐌 <b>Твои NFT улитки:</b>\n\n';
    for (const n of rows) {
      const weekly = CONFIG.NFT_WEEKLY[n.rank] || 0;
      txt += `• ${n.rank.toUpperCase()} #${n.nft_id} — <b>${weekly} SLUG/нед</b>\n`;
    }
    await sendTg(chatId, txt);
  }
});

async function setWebhook(baseUrl) {
  if (!CONFIG.BOT_TOKEN) return;
  const url  = `${baseUrl}/webhook/${CONFIG.BOT_TOKEN}`;
  const res  = await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/setWebhook?url=${encodeURIComponent(url)}`);
  const data = await res.json();
  console.log('Webhook:', data.ok ? '✅ set' : '❌ ' + data.description);
}

// ============================================================
// REST API — HEALTH
// ============================================================
app.get('/health', (_, res) => res.json({ ok: true, version: 5 }));

// ============================================================
// REST API — NFT
// ============================================================

// Статистика продаж
app.get('/nft/stats', async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM nft_counters');
    const stats = {};
    for (const c of rows) {
      stats[c.rank] = { sold: c.sold, available: c.max_supply - c.sold, max: c.max_supply };
    }
    const totalSold = rows.reduce((s, c) => s + c.sold, 0);
    const { rows: dist } = await db.query('SELECT COUNT(*) as c FROM nft_distributions');
    res.json({ stats, total_sold: totalSold, total_max: 777, distributions_done: parseInt(dist[0].c) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Инициировать покупку — резервируем ID и генерируем QR
app.post('/nft/buy', async (req, res) => {
  const { wallet, rank, tg_user_id } = req.body;
  if (!wallet?.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });
  if (!['common','rare','epic'].includes(rank)) return res.status(400).json({ error: 'Invalid rank' });

  try {
    // Получаем счётчик с блокировкой
    const { rows } = await db.query(
      'SELECT * FROM nft_counters WHERE rank=$1 FOR UPDATE', [rank]
    );
    const counter = rows[0];
    if (!counter) return res.status(400).json({ error: 'Invalid rank' });
    if (counter.sold >= counter.max_supply) return res.status(400).json({ error: `${rank} sold out` });

    const nftId  = counter.next_id;
    const nftKey = `${rank}:${nftId}`;
    const price  = CONFIG.NFT_PRICES[rank];

    // Проверяем не куплен ли уже этот конкретный NFT
    const { rows: existing } = await db.query(
      'SELECT 1 FROM nft_holders WHERE nft_key=$1', [nftKey]
    );
    if (existing.length) return res.status(400).json({ error: 'NFT already sold' });

    // Обновляем счётчик
    await db.query(
      'UPDATE nft_counters SET next_id=next_id+1, sold=sold+1 WHERE rank=$1', [rank]
    );

    // Записываем ожидающий платёж
    await db.query(`
      INSERT INTO nft_pending_buy (wallet, rank, nft_id, nft_key, amount)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (wallet, nft_key) DO UPDATE SET created_at=EXTRACT(EPOCH FROM NOW()), verified=0
    `, [wallet, rank, nftId, nftKey, price * Math.pow(10, CONFIG.SLUG_DECIMALS)]);

    // Генерируем QR через правильный cw20.send()
    const qrString = generateBuyNftQR(wallet, rank, nftId, price);

    res.json({
      wallet, rank, nft_id: nftId, nft_key: nftKey,
      price_slug: price,
      memo: `NFT:${rank.toUpperCase()}:${nftId}:${wallet}`,
      qr_string: qrString,
      message: `Зарезервирован ${rank.toUpperCase()} #${nftId}. Оплати ${price.toLocaleString()} SLUG`,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Данные холдера по кошельку (все его NFT)
app.get('/nft/holder/:wallet', async (req, res) => {
  const { wallet } = req.params;
  if (!wallet.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });

  try {
    const { rows } = await db.query(
      'SELECT * FROM nft_holders WHERE wallet=$1 ORDER BY purchased_at ASC', [wallet]
    );
    if (!rows.length) return res.status(404).json({ error: 'No NFTs found' });

    const { rows: dist } = await db.query('SELECT COUNT(*) as c FROM nft_distributions');
    const distDone  = parseInt(dist[0].c);
    const periodsLeft = CONFIG.NFT_PERIODS - distDone;

    const nfts = rows.map(n => ({
      rank:           n.rank,
      nft_id:         n.nft_id,
      nft_key:        n.nft_key,
      purchased_at:   new Date(n.purchased_at * 1000).toLocaleDateString('ru-RU'),
      total_received: n.total_received,
      weekly_slug:    CONFIG.NFT_WEEKLY[n.rank] || 0,
      total_earn:     CONFIG.NFT_TOTAL[n.rank]  || 0,
    }));

    const totalWeekly = nfts.reduce((s, n) => s + n.weekly_slug, 0);

    res.json({
      wallet,
      nfts,
      total_nfts:   rows.length,
      total_weekly: totalWeekly,
      periods_left: periodsLeft,
      distributions_done: distDone,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Статус оплаты конкретного NFT
app.get('/nft/payment-status/:wallet/:nft_key', async (req, res) => {
  const { wallet, nft_key } = req.params;
  const { rows } = await db.query(
    'SELECT 1 FROM nft_holders WHERE wallet=$1 AND nft_key=$2', [wallet, nft_key]
  );
  res.json({ paid: rows.length > 0, wallet, nft_key });
});

// Все холдеры (для выплат)
app.get('/nft/holders', async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM nft_holders ORDER BY rank, nft_id');
    res.json({ holders: rows, total: rows.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Ручное подтверждение покупки (admin) — после проверки транзакции в эксплорере
app.post('/nft/confirm', async (req, res) => {
  const { wallet, rank, nft_id, tx_hash, admin_key } = req.body;
  if (admin_key !== CONFIG.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  try {
    await confirmNftPurchase(wallet, rank, nft_id, tx_hash);
    res.json({ success: true, wallet, rank, nft_id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

async function confirmNftPurchase(wallet, rank, nftId, txHash) {
  const nftKey = `${rank}:${nftId}`;

  await db.query(`
    INSERT INTO nft_holders (wallet, rank, nft_id, nft_key, tx_hash)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (wallet, nft_key) DO NOTHING
  `, [wallet, rank, nftId, nftKey, txHash || null]);

  await db.query(
    'UPDATE nft_pending_buy SET verified=1 WHERE wallet=$1 AND nft_key=$2',
    [wallet, nftKey]
  );

  console.log(`✅ NFT confirmed: ${nftKey} → ${wallet}`);

  // Уведомляем в Telegram
  const { rows } = await db.query(
    'SELECT DISTINCT tg_user_id FROM participants WHERE wallet=$1 AND tg_user_id IS NOT NULL LIMIT 1',
    [wallet]
  );
  if (rows[0]?.tg_user_id) {
    const weekly = CONFIG.NFT_WEEKLY[rank] || 0;
    await sendTg(rows[0].tg_user_id,
      `🎉 <b>NFT куплена!</b>\n\n` +
      `🐌 Slug ${rank.charAt(0).toUpperCase()+rank.slice(1)} <b>#${nftId}</b>\n` +
      `💰 Еженедельно: <b>${weekly} SLUG</b>\n\n` +
      `<a href="https://slug-token.netlify.app/nft-cabinet?reveal=${rank}:${nftId}">Открыть свою улитку →</a>`
    );
  }
}

// Авто-верификация платежей за NFT (каждые 5 сек)
async function checkNftPayments() {
  const { rows: pending } = await db.query(
    'SELECT * FROM nft_pending_buy WHERE verified=0 AND created_at > EXTRACT(EPOCH FROM NOW()) - 3600'
  );
  if (!pending.length) return;

  try {
    const url = `${CONFIG.AXIOME_REST}/cosmos/tx/v1beta1/txs` +
      `?events=wasm._contract_address%3D%27${CONFIG.SLUG_CONTRACT}%27` +
      `&events=wasm.recipient%3D%27${CONFIG.PROJECT_WALLET}%27` +
      `&order_by=ORDER_DESC&limit=20`;
    const res  = await fetch(url, { signal: AbortSignal.timeout(5000) });
    const data = await res.json();
    const txs  = data?.txs || [];

    for (const tx of txs) {
      const txHash = tx.txhash;
      if (!txHash) continue;

      const { rows: seen } = await db.query('SELECT 1 FROM seen_txs WHERE tx_hash=$1', [txHash]);
      if (seen.length) continue;

      const memo = tx.body?.memo || '';
      if (!memo.startsWith('NFT:')) continue;

      // Парсим memo: NFT:RARE:42:axm1...
      const parts  = memo.split(':');
      if (parts.length < 4) continue;
      const rank   = parts[1].toLowerCase();
      const nftId  = parseInt(parts[2]);
      const wallet = parts.slice(3).join(':');
      const nftKey = `${rank}:${nftId}`;

      const pend = pending.find(p => p.wallet === wallet && p.nft_key === nftKey);
      if (!pend) continue;
      if (tx.code !== undefined && tx.code !== 0) continue;

      await db.query('INSERT INTO seen_txs (tx_hash) VALUES ($1) ON CONFLICT DO NOTHING', [txHash]);
      await confirmNftPurchase(wallet, rank, nftId, txHash);
    }
  } catch(e) { console.error('checkNftPayments:', e.message); }
}

setInterval(checkNftPayments, 5000);

// ============================================================
// ВЫПЛАТЫ — генерируем список QR для подписи через Axiome Wallet
// ============================================================
app.post('/nft/distribute', async (req, res) => {
  const { admin_key } = req.body;
  if (admin_key !== CONFIG.ADMIN_KEY) return res.status(401).json({ error: 'Unauthorized' });

  try {
    const { rows: holders } = await db.query('SELECT * FROM nft_holders');
    if (!holders.length) return res.status(400).json({ error: 'No holders' });

    const { rows: dist } = await db.query('SELECT COUNT(*) as c FROM nft_distributions');
    const period = parseInt(dist[0].c) + 1;
    if (period > CONFIG.NFT_PERIODS) return res.status(400).json({ error: 'All periods completed' });

    // Агрегируем выплаты по кошелькам (несколько NFT = одна транзакция)
    const walletAmounts = {};
    for (const h of holders) {
      const weekly = CONFIG.NFT_WEEKLY[h.rank] || 0;
      walletAmounts[h.wallet] = (walletAmounts[h.wallet] || 0) + weekly;
    }

    const totalSlug   = Object.values(walletAmounts).reduce((s, a) => s + a, 0);
    const uniqueWallets = Object.keys(walletAmounts).length;

    // Генерируем QR для каждого кошелька
    const qrList = Object.entries(walletAmounts).map(([wallet, amount]) => ({
      wallet,
      amount_slug: amount,
      qr_string: generateDistributeQR(wallet, amount, period),
    }));

    // Записываем выплату
    await db.query(
      'INSERT INTO nft_distributions (period, total_slug, recipients) VALUES ($1, $2, $3)',
      [period, totalSlug, holders.length]
    );

    // Обновляем total_received
    for (const [wallet, amount] of Object.entries(walletAmounts)) {
      await db.query(
        'UPDATE nft_holders SET total_received=total_received+$1 WHERE wallet=$2',
        [amount, wallet]
      );
    }

    res.json({
      period,
      total_slug:      totalSlug,
      unique_wallets:  uniqueWallets,
      total_nfts:      holders.length,
      periods_left:    CONFIG.NFT_PERIODS - period,
      qr_list:         qrList,
      message: `Период ${period}/${CONFIG.NFT_PERIODS}. ${uniqueWallets} кошельков, ${totalSlug} SLUG`,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// История выплат
app.get('/nft/distributions', async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM nft_distributions ORDER BY distributed_at DESC LIMIT 20');
    res.json({ history: rows, total: rows.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ============================================================
// REST API — PORTFOLIO BATTLE
// ============================================================
app.get('/battles', async (req, res) => {
  const { rows } = await db.query('SELECT * FROM battles ORDER BY created_at DESC LIMIT 50');
  res.json(rows);
});

app.get('/battles/:id', async (req, res) => {
  const { rows: b } = await db.query('SELECT * FROM battles WHERE id=$1', [req.params.id]);
  if (!b.length) return res.status(404).json({ error: 'Not found' });
  const { rows: parts } = await db.query(
    'SELECT wallet, tg_username, growth_pct, paid FROM participants WHERE battle_id=$1 ORDER BY growth_pct DESC',
    [req.params.id]
  );
  res.json({ ...b[0], participants: parts });
});

app.post('/battles', async (req, res) => {
  const { wallet, stake, max_players, duration_key, tg_user_id, tg_username } = req.body;
  if (!wallet?.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });
  if (!stake || stake < 100) return res.status(400).json({ error: 'Min stake 100 SLUG' });

  const durMap = { '1h':3600000,'24h':86400000,'3d':259200000,'7d':604800000 };
  const dur = durMap[duration_key];
  if (!dur) return res.status(400).json({ error: 'Invalid duration' });
  if (!max_players || max_players < 2 || max_players > 10) return res.status(400).json({ error: 'Players 2-10' });

  const id = crypto.randomBytes(4).toString('hex').toUpperCase();
  await db.query(
    'INSERT INTO battles (id,creator,stake,max_players,duration_ms) VALUES ($1,$2,$3,$4,$5)',
    [id, wallet, stake, max_players, dur]
  );

  const portfolio = await getFullPortfolio(wallet);
  const prices    = await getTokenPrices();
  const usdVal    = await calcPortfolioUsd(portfolio, prices);
  const snap      = JSON.stringify({ tokens: portfolio, prices, total_usd: usdVal, ts: Date.now() });

  await db.query(
    'INSERT INTO participants (battle_id,wallet,tg_user_id,tg_username,portfolio_start) VALUES ($1,$2,$3,$4,$5)',
    [id, wallet, tg_user_id || null, tg_username || null, snap]
  );
  await db.query('UPDATE battles SET pool_total=$1 WHERE id=$2', [stake, id]);

  const qr = generateBattleQR(id, wallet, stake);
  await db.query(
    'INSERT INTO pending_payments (battle_id,wallet,amount,memo) VALUES ($1,$2,$3,$4)',
    [id, wallet, stake * Math.pow(10, CONFIG.SLUG_DECIMALS), `BATTLE:${id}:${wallet}`]
  );

  res.json({ id, qr_string: qr, message: 'Battle created' });
});

app.post('/battles/:id/join', async (req, res) => {
  const { wallet, tg_user_id, tg_username } = req.body;
  if (!wallet?.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });

  const { rows: b } = await db.query('SELECT * FROM battles WHERE id=$1', [req.params.id]);
  if (!b.length) return res.status(404).json({ error: 'Not found' });
  if (b[0].status !== 'open') return res.status(400).json({ error: 'Battle not open' });

  const { rows: cnt } = await db.query(
    'SELECT COUNT(*) as c FROM participants WHERE battle_id=$1', [req.params.id]
  );
  if (parseInt(cnt[0].c) >= b[0].max_players) return res.status(400).json({ error: 'Battle full' });

  const slugBal = await getCW20Balance(wallet, CONFIG.SLUG_CONTRACT);
  if (slugBal < b[0].stake * Math.pow(10, CONFIG.SLUG_DECIMALS))
    return res.status(400).json({ error: `Need ${b[0].stake} SLUG` });

  const portfolio = await getFullPortfolio(wallet);
  const prices    = await getTokenPrices();
  const usdVal    = await calcPortfolioUsd(portfolio, prices);
  const snap      = JSON.stringify({ tokens: portfolio, prices, total_usd: usdVal, ts: Date.now() });

  try {
    await db.query(
      'INSERT INTO participants (battle_id,wallet,tg_user_id,tg_username,portfolio_start) VALUES ($1,$2,$3,$4,$5)',
      [req.params.id, wallet, tg_user_id || null, tg_username || null, snap]
    );
  } catch(e) {
    if (e.message.includes('unique')) return res.status(400).json({ error: 'Already joined' });
    throw e;
  }

  const newPool = (b[0].pool_total || 0) + b[0].stake;
  await db.query('UPDATE battles SET pool_total=$1 WHERE id=$2', [newPool, req.params.id]);

  const newCnt = parseInt(cnt[0].c) + 1;
  if (newCnt >= b[0].max_players) {
    const endsAt = Math.floor(Date.now() / 1000) + Math.floor(b[0].duration_ms / 1000);
    await db.query(
      "UPDATE battles SET status='live',started_at=EXTRACT(EPOCH FROM NOW()),ends_at=$1 WHERE id=$2",
      [endsAt, req.params.id]
    );
  }

  const qr = generateBattleQR(req.params.id, wallet, b[0].stake);
  await db.query(
    'INSERT INTO pending_payments (battle_id,wallet,amount,memo) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING',
    [req.params.id, wallet, b[0].stake * Math.pow(10, CONFIG.SLUG_DECIMALS), `BATTLE:${req.params.id}:${wallet}`]
  );

  res.json({ message: 'Joined!', qr_string: qr, portfolio_usd: usdVal });
});

app.get('/battles/:id/payment-status/:wallet', async (req, res) => {
  const { rows } = await db.query(
    'SELECT paid FROM participants WHERE battle_id=$1 AND wallet=$2',
    [req.params.id, req.params.wallet]
  );
  if (!rows.length) return res.status(404).json({ error: 'Not found' });
  res.json({ paid: rows[0].paid === 1, battle_id: req.params.id, wallet: req.params.wallet });
});

app.post('/battles/:id/finalize', async (req, res) => {
  const { rows: b } = await db.query('SELECT * FROM battles WHERE id=$1', [req.params.id]);
  if (!b.length) return res.status(404).json({ error: 'Not found' });
  if (b[0].status !== 'live') return res.status(400).json({ error: 'Not live' });

  const now = Math.floor(Date.now() / 1000);
  if (b[0].ends_at && now < b[0].ends_at)
    return res.status(400).json({ error: `Ends in ${b[0].ends_at - now}s` });

  const { rows: parts } = await db.query('SELECT * FROM participants WHERE battle_id=$1', [req.params.id]);
  const prices = await getTokenPrices();

  let winner = null, best = -Infinity;
  for (const p of parts) {
    const start  = JSON.parse(p.portfolio_start || '{}');
    const cur    = await getFullPortfolio(p.wallet);
    const curUsd = await calcPortfolioUsd(cur, prices);
    const growth = start.total_usd > 0 ? ((curUsd - start.total_usd) / start.total_usd) * 100 : 0;
    await db.query(
      'UPDATE participants SET portfolio_end=$1,growth_pct=$2 WHERE battle_id=$3 AND wallet=$4',
      [JSON.stringify({ tokens: cur, prices, total_usd: curUsd, ts: Date.now() }), growth, b[0].id, p.wallet]
    );
    if (growth > best) { best = growth; winner = p.wallet; }
  }

  const pool  = b[0].pool_total || b[0].stake * parts.length;
  const fee   = Math.round(pool * CONFIG.FEE_PERCENT / 100);
  const prize = pool - fee;

  await db.query("UPDATE battles SET status='ended',winner=$1 WHERE id=$2", [winner, b[0].id]);

  // QR для выплаты победителю
  const winQR = generateBattleQR(b[0].id + '-WIN', winner, prize);
  const { rows: wp } = await db.query(
    'SELECT tg_user_id FROM participants WHERE battle_id=$1 AND wallet=$2', [b[0].id, winner]
  );
  if (wp[0]?.tg_user_id) {
    await sendTg(wp[0].tg_user_id,
      `🏆 <b>Победа в битве #${b[0].id}!</b>\n\n💰 Приз: <b>${prize.toLocaleString()} SLUG</b>\n\n` +
      `QR для получения приза отправлен следующим сообщением.`
    );
    await sendTg(wp[0].tg_user_id, `<code>${winQR}</code>`);
  }

  res.json({ winner, growth_pct: best.toFixed(2), prize_slug: prize, fee_slug: fee });
});

app.get('/portfolio/:wallet', async (req, res) => {
  if (!req.params.wallet.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });
  const [portfolio, prices] = await Promise.all([getFullPortfolio(req.params.wallet), getTokenPrices()]);
  const total_usd = await calcPortfolioUsd(portfolio, prices);
  res.json({ wallet: req.params.wallet, portfolio, prices, total_usd });
});

// ============================================================
// АВТО-ВЕРИФИКАЦИЯ BATTLE PAYMENTS
// ============================================================
async function checkBattlePayments() {
  const { rows: pending } = await db.query('SELECT * FROM pending_payments WHERE verified=0');
  if (!pending.length) return;

  try {
    const url = `${CONFIG.AXIOME_REST}/cosmos/tx/v1beta1/txs` +
      `?events=wasm._contract_address%3D%27${CONFIG.SLUG_CONTRACT}%27` +
      `&events=wasm.recipient%3D%27${CONFIG.PROJECT_WALLET}%27` +
      `&order_by=ORDER_DESC&limit=20`;
    const res  = await fetch(url, { signal: AbortSignal.timeout(5000) });
    const data = await res.json();
    const txs  = data?.txs || [];

    for (const tx of txs) {
      const txHash = tx.txhash;
      if (!txHash) continue;
      const { rows: seen } = await db.query('SELECT 1 FROM seen_txs WHERE tx_hash=$1', [txHash]);
      if (seen.length) continue;
      const memo = tx.body?.memo || '';
      if (!memo.startsWith('BATTLE:')) continue;

      const parts    = memo.split(':');
      const battleId = parts[1];
      const wallet   = parts[2];

      const pend = pending.find(p => p.battle_id === battleId && p.wallet === wallet);
      if (!pend) continue;
      if (tx.code !== undefined && tx.code !== 0) continue;

      await db.query('INSERT INTO seen_txs (tx_hash) VALUES ($1) ON CONFLICT DO NOTHING', [txHash]);
      await db.query('UPDATE participants SET paid=1 WHERE battle_id=$1 AND wallet=$2', [battleId, wallet]);
      await db.query('UPDATE pending_payments SET verified=1 WHERE battle_id=$1 AND wallet=$2', [battleId, wallet]);

      const { rows: paid } = await db.query(
        'SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=1', [battleId]
      );
      const { rows: batt } = await db.query('SELECT * FROM battles WHERE id=$1', [battleId]);
      if (!batt.length) continue;

      const { rows: p } = await db.query(
        'SELECT tg_user_id FROM participants WHERE battle_id=$1 AND wallet=$2', [battleId, wallet]
      );
      if (p[0]?.tg_user_id) {
        await sendTg(p[0].tg_user_id,
          `✅ <b>Оплата подтверждена!</b>\n⚔️ Битва <b>#${battleId}</b>\n` +
          `👥 Оплатили: ${paid[0].c}/${batt[0].max_players}`
        );
      }

      if (parseInt(paid[0].c) >= batt[0].max_players && batt[0].status === 'open') {
        const endsAt = Math.floor(Date.now() / 1000) + Math.floor(batt[0].duration_ms / 1000);
        await db.query(
          "UPDATE battles SET status='live',started_at=EXTRACT(EPOCH FROM NOW()),ends_at=$1 WHERE id=$2",
          [endsAt, battleId]
        );
        const { rows: allParts } = await db.query(
          'SELECT tg_user_id FROM participants WHERE battle_id=$1 AND tg_user_id IS NOT NULL', [battleId]
        );
        for (const part of allParts) {
          await sendTg(part.tg_user_id, `🚀 <b>Битва #${battleId} началась!</b>`);
        }
      }
    }
  } catch(e) { console.error('checkBattlePayments:', e.message); }
}

setInterval(checkBattlePayments, 5000);

// ============================================================
// АВТО-ФИНАЛИЗАЦИЯ БИТВ
// ============================================================
setInterval(async () => {
  const { rows } = await db.query(
    "SELECT id FROM battles WHERE status='live' AND ends_at<=$1",
    [Math.floor(Date.now() / 1000)]
  );
  for (const b of rows) {
    try {
      await fetch(`http://localhost:${CONFIG.PORT}/battles/${b.id}/finalize`, { method: 'POST' });
    } catch(e) { console.error('finalize error:', e.message); }
  }
}, 5 * 60 * 1000);

// ============================================================
// START
// ============================================================
async function start() {
  await initDB();
  app.listen(CONFIG.PORT, async () => {
    console.log(`🐌 SLUG Server v5 on port ${CONFIG.PORT}`);
    if (process.env.RAILWAY_PUBLIC_DOMAIN) {
      await setWebhook(`https://${process.env.RAILWAY_PUBLIC_DOMAIN}`);
    }
  });
}

start().catch(console.error);
