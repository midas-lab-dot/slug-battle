// ============================================================
// CRYPTO PORTFOLIO BATTLE — Backend Server v3 (PostgreSQL)
// ============================================================

const express    = require('express');

// ── Глобальные обработчики — процесс не падает ───────────────
process.on('uncaughtException', (err) => {
  console.error('[FATAL] uncaughtException:', err.message, err.stack);
  // Не убиваем процесс — логируем и продолжаем
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[FATAL] unhandledRejection:', reason?.message || reason);
  // Не убиваем процесс
});
const { Pool }   = require('pg');
const cors       = require('cors');
const crypto     = require('crypto');
const { DirectSecp256k1HdWallet, DirectSecp256k1Wallet } = require('@cosmjs/proto-signing');
const { SigningCosmWasmClient } = require('@cosmjs/cosmwasm-stargate');
const { GasPrice } = require('@cosmjs/stargate');
const { HttpBatchClient, Tendermint37Client } = require('@cosmjs/tendermint-rpc');

const app = express();

// ── Rate Limiter (40 req/min per IP) ─────────────────────────
const _rateLimitMap = new Map();
const RATE_LIMIT    = 40;   // запросов
const RATE_WINDOW   = 60000; // за 1 минуту (мс)

function rateLimiter(req, res, next) {
  // Пропускаем healthcheck без ограничений
  if (req.path === '/health') return next();

  const ip  = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress || 'unknown';
  const now = Date.now();
  const rec = _rateLimitMap.get(ip);

  if (!rec || now - rec.start > RATE_WINDOW) {
    // Новое окно
    _rateLimitMap.set(ip, { count: 1, start: now });
    return next();
  }

  if (rec.count >= RATE_LIMIT) {
    const retryAfter = Math.ceil((RATE_WINDOW - (now - rec.start)) / 1000);
    res.setHeader('Retry-After', retryAfter);
    return res.status(429).json({
      error: `Too many requests. Try again in ${retryAfter}s`,
      retry_after: retryAfter,
    });
  }

  rec.count++;
  return next();
}

// Чистим старые записи каждые 5 минут чтобы не было утечки памяти
setInterval(() => {
  const now = Date.now();
  for (const [ip, rec] of _rateLimitMap.entries()) {
    if (now - rec.start > RATE_WINDOW) _rateLimitMap.delete(ip);
  }
}, 60 * 1000); // чистим каждую минуту

app.use(express.json({ limit: '1mb' }));
app.use(cors());
app.use(rateLimiter); // 40 req/min per IP
// Таймаут запроса 30 секунд
app.use((req, res, next) => {
  res.setTimeout(30000, () => {
    res.status(503).json({ error: 'Request timeout' });
  });
  next();
});

const CONFIG = {
  BOT_TOKEN:       process.env.BOT_TOKEN       || 'YOUR_BOT_TOKEN',
  ADMIN_KEY:       process.env.ADMIN_KEY        || 'slug-admin-2024',
  FRONTEND_URL:    process.env.FRONTEND_URL     || 'https://slug-token.netlify.app/battle',
  SLUG_CONTRACT:   'axm1nzvd5njkc0gqdezgzzqw00tu056rgxgwaqgq25g9ku0y5au3nwmqxqtr0l',
  PROJECT_WALLET:  'axm1du6r0ueg8v8zv87m9jfla385p7jqljfqevm70v',
  AXIOME_REST:     'http://api-docs.axiomeinfo.org:1317',
  AXIOME_RPC:      'http://api-docs.axiomeinfo.org:26657',
  NETWORK:         'axiome',
  FEE_PERCENT:     10, // 10% total: 5% platform + 3% ref1 + 1.5% ref2 + 0.5% ref3

  // Реферальные проценты от ставки (не от банка)
  REF_PERCENT: [3, 1.5, 0.5], // 1й, 2й, 3й уровень
  PORT:            process.env.PORT || 3000,
  ESCROW_CONTRACT: process.env.ESCROW_CONTRACT || '',
  MIN_STAKE:       20,
  PAYMENT_TIMEOUT_SEC: 3600, // 60 минут
  KNOWN_TOKENS: [
    { contract: 'axm1nzvd5njkc0gqdezgzzqw00tu056rgxgwaqgq25g9ku0y5au3nwmqxqtr0l', symbol: 'SLUG',   decimals: 6 },
    { contract: 'axm1kzhwsnndy3fp2la8vplj2wnjztw0cg5yhu3ucx2x2km790s2j95styrldq', symbol: 'SLUG2',  decimals: 6 },
  ],

  // Адреса пулов Axiome Swap (пары токен/AXM)
  // Из резервов пула вычисляем цену токена в AXM
  SWAP_POOLS: [
    { symbol: 'AXP',    pool: 'axm1qsufk856fmakeycjq5swmtd53l4drqtu4xefdfuu9h4m5ce89rhschkv0c', decimals: 6 },
    { symbol: 'KEPY',   pool: 'axm1n8zk28ycq7s5epdknwytvvpt8848954yfuz0vcf6l3yz73g4sd2s36s3wm', decimals: 6 },
    { symbol: 'TRVS',   pool: 'axm1x7kcezlz3dex8x6lgum7wpuaqcw4k2m2ajggrqlnavrqgsg0rzuqpq2786', decimals: 6 },
    { symbol: 'RBP',    pool: 'axm1ywpctek72gsccmr8gjgaamzs8vhh7ju3sjtvwk82daafavw5u0eskv9ltk', decimals: 6 },
    { symbol: 'SIMBA',  pool: 'axm1tzv867e4qz06rue60k4v2pk5xevj9gn3r7274qv0nprl68kvykgs6w4emy', decimals: 6 },
    { symbol: 'CHAPA',  pool: 'axm1zjd5lwhch4ndnmayqxurja4x5y5mavy9ktrk6fzsyzan4wcgawnqeh4llv', decimals: 6 },
    { symbol: 'AXMB',   pool: 'axm1g0tvy7kahkn4fcs7qdh7sqlu89jv3kya59n3208430egzhcrrzwqz6e75h', decimals: 6 },
    { symbol: 'RIP',    pool: 'axm1qetvausf453rrtt8lg0lrwc3txkju9tpma5kxagua58u26wkrvfquhrmxg', decimals: 6 },
    { symbol: 'SLUG',   pool: 'axm1kzhwsnndy3fp2la8vplj2wnjztw0cg5yhu3ucx2x2km790s2j95styrldq', decimals: 6 },
    { symbol: 'LSTB',   pool: 'axm1l6j9z82fvpn0mzkztmjz0zu78kj9nuh68vdd6czs8tq00ngltnxqenlqfg', decimals: 6 },
    { symbol: 'SHRIM',  pool: 'axm1lq9fjtgtd50kzf30uqgae3mh0tmp54ek4gmak8d97930dhgy4rgqaxzuzz', decimals: 6 },
    { symbol: 'RBS',    pool: 'axm1z6wep7u638fmyehatcyc7j7tjxl8lw4dk3jlzkq90yfxfq66vsnsmxcml6', decimals: 6 },
    { symbol: 'BRITVA', pool: 'axm1hhhyzy8u7d0ujkfr8sq3yrjlam0pr9zsgcf2t7ugnnjzfsv9s55q643ee3', decimals: 6 },
    { symbol: 'ZACHEM', pool: 'axm1sm722dtmzxhf4kaz538tthk8y07y2skakmkexmr0wasn94n23c0st4kexc', decimals: 6 },
    { symbol: 'ARTR',   pool: 'axm1yflkpv5s456h4un6a6j3zzday43z8kdk0xn0g05g6dzyxespcdhqwzvtc9', decimals: 6 },
    { symbol: 'CLX',    pool: 'axm1gkkr2446fsm2ecxztdkyyyp6tj8cu7g2a59ln03rnmdf06velt7q65y55a', decimals: 6 },
    { symbol: 'SUB',    pool: 'axm10crkytgrggv9kmvh4rr2e6lvzjf2k3se9k7esxgg8z0a0hpjmecsps88mg', decimals: 6 },
    { symbol: 'PRED',   pool: 'axm1srg729l8nc63230zg4x36yjw3y8q6e4qraykwst07vwqvmar7tgqwsmhlm', decimals: 6 },
    { symbol: 'WHO',    pool: 'axm1yygzkgghw83a9skn2ufeyfhq4lkuvd79g9u4rgfjwthz8gykhevs7vw9g4', decimals: 6 },
    { symbol: 'GOAT',   pool: 'axm17krydtdfc9xxhx0rn6yq573zxua6uct9mjqs576cqucuesrcwzmqu52kjp', decimals: 6 },
    { symbol: 'PUMP',   pool: 'axm1cm60qwjptgep3s6nke033cpe73d345zqls8talh8zcn09tdfv65q306w67', decimals: 6 },
    { symbol: 'WOLF',   pool: 'axm1hm3euhj0qkyz25yrha9mraqq8sx2dsc7n8gdhd0k3ykrmdqyh8equ0ymkv', decimals: 6 },
    { symbol: 'MUCH',   pool: 'axm1e3j47h79pkqv34jdujzu2062k5cczxm9lrg9auyp9zn4axgme67s8cwvdk', decimals: 6 },
  ],
};

// ── PostgreSQL ────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 20,                // максимум 20 соединений
  min: 2,                 // минимум 2 всегда живых
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});
pool.on('error', (err) => console.error('[pg] Pool error:', err.message));

async function q(sql, params = []) {
  const client = await pool.connect();
  try {
    await client.query('SET statement_timeout = 10000'); // 10 сек максимум
    return await client.query(sql, params);
  } finally { client.release(); }
}

async function initDB() {
  await q(`CREATE TABLE IF NOT EXISTS battles (
    id TEXT PRIMARY KEY, creator TEXT NOT NULL, stake BIGINT NOT NULL,
    max_players INTEGER NOT NULL, duration_ms BIGINT NOT NULL,
    status TEXT DEFAULT 'open',
    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
    started_at BIGINT, ends_at BIGINT, winner TEXT, pool_total BIGINT DEFAULT 0
  )`);
  await q(`CREATE TABLE IF NOT EXISTS participants (
    id SERIAL PRIMARY KEY, battle_id TEXT NOT NULL, wallet TEXT NOT NULL,
    tg_user_id TEXT, tg_username TEXT,
    joined_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
    portfolio_start TEXT, portfolio_end TEXT, growth_pct REAL,
    paid INTEGER DEFAULT 0, UNIQUE(battle_id, wallet)
  )`);
  await q(`CREATE TABLE IF NOT EXISTS pending_payments (
    id SERIAL PRIMARY KEY, battle_id TEXT NOT NULL, wallet TEXT NOT NULL,
    amount BIGINT NOT NULL, memo TEXT NOT NULL UNIQUE,
    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()), verified INTEGER DEFAULT 0
  )`);
  await q(`CREATE TABLE IF NOT EXISTS seen_txs (
    tx_hash TEXT PRIMARY KEY, seen_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
  )`);
  await q(`CREATE INDEX IF NOT EXISTS idx_pending_verified ON pending_payments(verified)`);
  await q(`CREATE INDEX IF NOT EXISTS idx_participants_paid ON participants(battle_id, paid)`);
  await q(`CREATE INDEX IF NOT EXISTS idx_battles_status ON battles(status)`);
  // Таблица турнира
  await q(`CREATE TABLE IF NOT EXISTS tournament (
    id        SERIAL PRIMARY KEY,
    starts_at BIGINT NOT NULL,
    ends_at   BIGINT NOT NULL,
    status    TEXT DEFAULT 'active',
    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
  )`);
  // Лидерборд турнира
  await q(`CREATE TABLE IF NOT EXISTS tournament_scores (
    id            SERIAL PRIMARY KEY,
    tournament_id INTEGER NOT NULL,
    wallet        TEXT NOT NULL,
    tg_username   TEXT,
    wins          INTEGER DEFAULT 0,
    total_battles INTEGER DEFAULT 0,
    total_earned  BIGINT DEFAULT 0,
    best_growth   REAL DEFAULT 0,
    updated_at    BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
    UNIQUE(tournament_id, wallet)
  )`);
  await q(`CREATE TABLE IF NOT EXISTS user_settings (
    tg_user_id TEXT PRIMARY KEY,
    lang       TEXT DEFAULT 'ru',
    notify_new_battle INTEGER DEFAULT 0,
    wallet     TEXT,
    referred_by TEXT,
    updated_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
  )`);

  await q(`CREATE TABLE IF NOT EXISTS referral_payouts (
    id          SERIAL  PRIMARY KEY,
    battle_id   TEXT    NOT NULL,
    to_wallet   TEXT    NOT NULL,
    from_wallet TEXT    NOT NULL,
    level       INTEGER NOT NULL,
    amount_slug BIGINT  NOT NULL,
    tx_hash     TEXT,
    created_at  BIGINT  DEFAULT EXTRACT(EPOCH FROM NOW())
  )`);

  await q(`CREATE INDEX IF NOT EXISTS idx_ref_wallet ON referral_payouts(to_wallet)`);

  // Миграции — добавляем колонки если не существуют
  await q(`ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS referred_by TEXT`);
  await q(`ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS wallet TEXT`);
  await q(`ALTER TABLE battles ADD COLUMN IF NOT EXISTS notified_5min INTEGER DEFAULT NULL`);

  console.log('[db] PostgreSQL ready');
}

// ── Axiome API ────────────────────────────────────────────────
async function getCW20Balance(wallet, contract) {
  try {
    const qb  = Buffer.from(JSON.stringify({ balance: { address: wallet } })).toString('base64');
    const res = await fetch(`${CONFIG.AXIOME_REST}/cosmwasm/wasm/v1/contract/${contract}/smart/${qb}`, { signal: AbortSignal.timeout(5000) });
    return parseInt((await res.json())?.data?.balance || '0');
  } catch { return 0; }
}

async function getNativeBalance(wallet) {
  try {
    const res  = await fetch(`${CONFIG.AXIOME_REST}/cosmos/bank/v1beta1/balances/${wallet}`, { signal: AbortSignal.timeout(5000) });
    return parseInt((await res.json())?.balances?.find(b => b.denom === 'uaxm')?.amount || '0');
  } catch { return 0; }
}

// Получаем контракт токена из пула (token1 или token2 — не AXM)
async function getTokenContractFromPool(poolAddr) {
  try {
    const qb  = Buffer.from(JSON.stringify({ info: {} })).toString('base64');
    const res = await fetch(
      `${CONFIG.AXIOME_REST}/cosmwasm/wasm/v1/contract/${poolAddr}/smart/${qb}`,
      { signal: AbortSignal.timeout(5000) }
    );
    const data = await res.json();
    const d = data?.data;
    if (!d) return null;
    // Возвращаем CW20 адрес (не нативный AXM)
    if (d.token1_denom?.cw20) return d.token1_denom.cw20;
    if (d.token2_denom?.cw20) return d.token2_denom.cw20;
    return null;
  } catch { return null; }
}

// Кэш контрактов токенов (заполняется один раз при старте)
const TOKEN_CONTRACTS = {};

async function loadTokenContracts() {
  console.log('[portfolio] Loading token contracts from pools...');
  const results = await Promise.allSettled(
    CONFIG.SWAP_POOLS.map(async pool => {
      const contract = await getTokenContractFromPool(pool.pool);
      return { symbol: pool.symbol, contract, decimals: pool.decimals };
    })
  );
  for (const r of results) {
    if (r.status === 'fulfilled' && r.value.contract) {
      TOKEN_CONTRACTS[r.value.symbol] = { contract: r.value.contract, decimals: r.value.decimals };
    }
  }
  console.log('[portfolio] Loaded contracts:', Object.keys(TOKEN_CONTRACTS).join(', '));
}

// ── Кэш портфелей (5 минут) ─────────────────────────────────
const _portfolioCache = new Map();
const PORTFOLIO_CACHE_TTL = 5 * 60 * 1000; // 5 минут

async function getFullPortfolio(wallet) {
  // Проверяем кэш
  const cached = _portfolioCache.get(wallet);
  if (cached && Date.now() - cached.time < PORTFOLIO_CACHE_TTL) {
    return cached.data;
  }
  const portfolio = {};

  // AXM нативный
  const axmRaw = await getNativeBalance(wallet);
  if (axmRaw > 0) {
    portfolio['AXM'] = { symbol: 'AXM', amount: axmRaw / 1e6, contract: 'native' };
  }

  // Все токены из пулов — параллельно
  const checks = await Promise.allSettled(
    Object.entries(TOKEN_CONTRACTS).map(async ([symbol, { contract, decimals }]) => {
      const raw = await getCW20Balance(wallet, contract);
      return { symbol, amount: raw / Math.pow(10, decimals), contract };
    })
  );

  for (const r of checks) {
    if (r.status === 'fulfilled' && r.value.amount > 0) {
      const { symbol, amount, contract } = r.value;
      portfolio[symbol] = { symbol, amount, contract };
    }
  }

  // Если портфель пустой — всё равно добавляем AXM с нулём
  if (!portfolio['AXM']) {
    portfolio['AXM'] = { symbol: 'AXM', amount: 0, contract: 'native' };
  }

  return portfolio;
}

// Получаем цену токена из пула в AXM
// Цена = axm_reserve / token_reserve
async function getTokenPriceFromPool(poolAddr) {
  try {
    const qb  = Buffer.from(JSON.stringify({ info: {} })).toString('base64');
    const res = await fetch(
      `${CONFIG.AXIOME_REST}/cosmwasm/wasm/v1/contract/${poolAddr}/smart/${qb}`,
      { signal: AbortSignal.timeout(5000) }
    );
    const d = (await res.json())?.data;
    if (!d) return null;
    const isToken1Native = d.token1_denom?.native === 'uaxm';
    const axmReserve   = parseFloat(isToken1Native ? d.token1_reserve : d.token2_reserve);
    const tokenReserve = parseFloat(isToken1Native ? d.token2_reserve : d.token1_reserve);
    if (!tokenReserve || tokenReserve === 0) return null;
    return axmReserve / tokenReserve; // цена в AXM
  } catch { return null; }
}

// Получаем цены всех токенов в AXM (без USD конвертации)
// AXM = 1 (базовая единица)
// ── Кэш цен токенов (30 сек) ─────────────────────────────────
let _priceCache     = null;
let _priceCacheTime = 0;
let _priceFetchPromise = null; // защита от параллельных запросов

async function getTokenPrices() {
  const now = Date.now();

  // Возвращаем кэш если свежее 30 сек
  if (_priceCache && (now - _priceCacheTime) < 30000) {
    return _priceCache;
  }

  // Если уже идёт запрос — ждём его вместо нового
  if (_priceFetchPromise) {
    return _priceFetchPromise;
  }

  _priceFetchPromise = (async () => {
    try {
      const prices = { AXM: 1 };
      const results = await Promise.allSettled(
        CONFIG.SWAP_POOLS.map(async pool => {
          const priceInAxm = await getTokenPriceFromPool(pool.pool);
          return { symbol: pool.symbol, priceInAxm };
        })
      );
      for (const r of results) {
        if (r.status === 'fulfilled' && r.value.priceInAxm !== null) {
          prices[r.value.symbol] = r.value.priceInAxm;
        }
      }
      _priceCache     = prices;
      _priceCacheTime = Date.now();
      console.log(`[prices] Updated cache: ${Object.keys(prices).length} tokens`);
      return prices;
    } catch (e) {
      console.error('[prices] Failed to fetch:', e.message);
      // При ошибке возвращаем старый кэш если есть
      return _priceCache || { AXM: 1 };
    } finally {
      _priceFetchPromise = null;
    }
  })();

  return _priceFetchPromise;
}

// Считаем стоимость портфеля в AXM (не в USD)
// AXM = 1, все остальные токены = их цена в AXM из пула
function calcPortfolioAxm(portfolio, prices) {
  return Object.values(portfolio).reduce((t, d) => t + d.amount * (prices[d.symbol] || 0), 0);
}

// Оставляем алиас для совместимости
async function calcPortfolioUsd(portfolio, prices) {
  return calcPortfolioAxm(portfolio, prices);
}

// ── QR builders ───────────────────────────────────────────────
function buildQR(recipient, amountSlug, memo) {
  return `axiomesign://${Buffer.from(JSON.stringify({
    type: 'cosmwasm_execute', network: CONFIG.NETWORK, contract_addr: CONFIG.SLUG_CONTRACT, funds: [],
    msg: { transfer: { recipient, amount: String(Math.round(amountSlug * 1e6)) } }, memo,
  })).toString('base64')}`;
}

function buildEscrowJoinQR(battleId, amountSlug) {
  const hookMsg = Buffer.from(JSON.stringify({ join_battle: { battle_id: battleId } })).toString('base64');
  return `axiomesign://${Buffer.from(JSON.stringify({
    type: 'cosmwasm_execute', network: CONFIG.NETWORK, contract_addr: CONFIG.SLUG_CONTRACT, funds: [],
    msg: { send: { contract: CONFIG.ESCROW_CONTRACT, amount: String(Math.round(amountSlug * 1e6)), msg: hookMsg } },
    memo: `JOIN:${battleId}`,
  })).toString('base64')}`;
}

function buildJoinQR(battleId, amountSlug, memo) {
  return CONFIG.ESCROW_CONTRACT ? buildEscrowJoinQR(battleId, amountSlug) : buildQR(CONFIG.PROJECT_WALLET, amountSlug, memo);
}

// ── Telegram ──────────────────────────────────────────────────
async function sendTg(chatId, text, extra = {}) {
  if (!chatId || CONFIG.BOT_TOKEN === 'YOUR_BOT_TOKEN') return;
  try {
    await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/sendMessage`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML', ...extra }),
    });
  } catch {}
}


// ── Автовыплата через SigningCosmWasmClient ─────────────────
let _signingClient = null;

async function getSigningClient() {
  if (_signingClient) return _signingClient;

  const privkeyHex = process.env.PROJECT_WALLET_PRIVKEY;
  const mnemonic   = process.env.PROJECT_WALLET_MNEMONIC;

  let wallet;
  if (privkeyHex) {
    const privkeyBytes = Uint8Array.from(Buffer.from(privkeyHex, 'hex'));
    wallet = await DirectSecp256k1Wallet.fromKey(privkeyBytes, 'axm');
  } else if (mnemonic) {
    wallet = await DirectSecp256k1HdWallet.fromMnemonic(mnemonic, { prefix: 'axm' });
  } else {
    throw new Error('PROJECT_WALLET_PRIVKEY or PROJECT_WALLET_MNEMONIC required');
  }

  const accounts = await wallet.getAccounts();
  console.log(`[payout] Wallet: ${accounts[0].address}`);

  if (accounts[0].address !== CONFIG.PROJECT_WALLET) {
    throw new Error(`Address mismatch: got ${accounts[0].address}, expected ${CONFIG.PROJECT_WALLET}`);
  }

  const tmClient = await Tendermint37Client.create(
    new HttpBatchClient('http://api-docs.axiomeinfo.org:26657', { dispatchInterval: 500 })
  );

  _signingClient = await SigningCosmWasmClient.createWithSigner(tmClient, wallet, {
    gasPrice: GasPrice.fromString('4000uaxm'),
  });

  const chainId = await _signingClient.getChainId();
  console.log(`[payout] Connected chainId=${chainId}`);
  return _signingClient;
}

async function autoPayout(battleId, winner, prizeSlug) {
  return queuePayout(async () => {
    try {
      const client   = await getSigningClient();
      const prizeRaw = String(Math.round(prizeSlug * 1e6));

      const result = await client.execute(
        CONFIG.PROJECT_WALLET,
        CONFIG.SLUG_CONTRACT,
        { transfer: { recipient: winner, amount: prizeRaw } },
        { amount: [{ denom: 'uaxm', amount: '600000' }], gas: '200000' },
        `WIN:${battleId}`,
      );

      console.log(`[payout] ✅ Prize sent: battle=${battleId} winner=${winner} prize=${prizeSlug} tx=${result.transactionHash}`);
      return { ok: true, txHash: result.transactionHash };

    } catch (e) {
      console.error(`[payout] ❌ Failed: ${e.message}`);
      _signingClient = null;
      return { ok: false, error: e.message };
    }
  });
}



// ── Очередь выплат (последовательная отправка) ───────────────
const _payoutQueue = [];
let _payoutRunning = false;

async function queuePayout(fn) {
  return new Promise((resolve, reject) => {
    _payoutQueue.push({ fn, resolve, reject });
    if (!_payoutRunning) processPayoutQueue();
  });
}

async function processPayoutQueue() {
  if (_payoutRunning || !_payoutQueue.length) return;
  _payoutRunning = true;
  while (_payoutQueue.length) {
    const { fn, resolve, reject } = _payoutQueue.shift();
    try {
      const result = await fn();
      resolve(result);
    } catch (e) {
      reject(e);
    }
    // Пауза 3 сек между транзакциями чтобы sequence успел обновиться
    if (_payoutQueue.length) await new Promise(r => setTimeout(r, 3000));
  }
  _payoutRunning = false;
}


// ── Рассылка уведомлений о новой битве ──────────────────────
async function notifyNewBattle(battleId, stake, maxPlayers, creatorUsername) {
  try {
    const subscribers = (await q(
      "SELECT tg_user_id, lang FROM user_settings WHERE notify_new_battle=1 AND tg_user_id IS NOT NULL"
    )).rows;

    if (!subscribers.length) return;

    const prize = Math.round(stake * maxPlayers * 0.9);
    const creator = creatorUsername ? `@${creatorUsername}` : 'Anonymous';

    console.log(`[notify] Broadcasting new battle #${battleId} to ${subscribers.length} subscribers`);

    // Отправляем порциями по 20 с паузой 1 сек — защита от flood
    for (let i = 0; i < subscribers.length; i += 20) {
      const batch = subscribers.slice(i, i + 20);
      await Promise.allSettled(batch.map(async (sub) => {
        try {
          const lang = sub.lang || 'ru';
          const notifyTexts = {
            ru: `⚔️ <b>Новая битва!</b>\n\nСоздана битва #${battleId}\nСтавка: <b>${stake} SLUG</b>\nИгроков: <b>${maxPlayers}</b>\nПриз: <b>${prize} SLUG</b>\n\nЗаходи пока есть места!`,
            en: `⚔️ <b>New Battle!</b>\n\nBattle #${battleId} created\nStake: <b>${stake} SLUG</b>\nPlayers: <b>${maxPlayers}</b>\nPrize: <b>${prize} SLUG</b>\n\nJoin while there are spots!`,
            de: `⚔️ <b>Neuer Kampf!</b>\n\nKampf #${battleId} erstellt\nEinsatz: <b>${stake} SLUG</b>\nSpieler: <b>${maxPlayers}</b>\nPreis: <b>${prize} SLUG</b>\n\nTritt bei solange Plätze frei sind!`,
            bg: `⚔️ <b>Нова битка!</b>\n\nСъздадена битка #${battleId}\nЗалог: <b>${stake} SLUG</b>\nИграчи: <b>${maxPlayers}</b>\nНаграда: <b>${prize} SLUG</b>\n\nВлез докато има места!`,
            uk: `⚔️ <b>Нова битва!</b>\n\nСтворено битву #${battleId}\nСтавка: <b>${stake} SLUG</b>\nГравців: <b>${maxPlayers}</b>\nПриз: <b>${prize} SLUG</b>\n\nЗаходь поки є місця!`,
            kz: `⚔️ <b>Жаңа шайқас!</b>\n\nШайқас #${battleId} жасалды\nСтавка: <b>${stake} SLUG</b>\nОйыншылар: <b>${maxPlayers}</b>\nСыйлық: <b>${prize} SLUG</b>\n\nОрын бар кезде кір!`,
            fr: `⚔️ <b>Nouveau Combat!</b>\n\nCombat #${battleId} créé\nMise: <b>${stake} SLUG</b>\nJoueurs: <b>${maxPlayers}</b>\nPrix: <b>${prize} SLUG</b>\n\nRejoins pendant qu'il y a des places!`,
            bn: `⚔️ <b>নতুন যুদ্ধ!</b>\n\nযুদ্ধ #${battleId} তৈরি\nবাজি: <b>${stake} SLUG</b>\nখেলোয়াড়: <b>${maxPlayers}</b>\nপুরস্কার: <b>${prize} SLUG</b>\n\nজায়গা থাকতে যোগ দাও!`,
          };
          const text = notifyTexts[lang] || notifyTexts['en'];
          await sendTg(sub.tg_user_id, text, {
            reply_markup: { inline_keyboard: [[
              { text: lang==='ru' ? '⚔️ Открыть игру' : '⚔️ Open game',
                web_app: { url: CONFIG.FRONTEND_URL + '?lang=' + lang } }
            ]]}
          });
        } catch(e) { /* тихо — пользователь мог заблокировать бота */ }
      }));
      if (i + 20 < subscribers.length) await new Promise(r => setTimeout(r, 1000));
    }
  } catch(e) {
    console.error('[notify] broadcast error:', e.message);
  }
}


// ── Обновление турнирного лидерборда ────────────────────────
async function updateTournamentScores(battleId) {
  const t = (await q("SELECT id FROM tournament WHERE status='active' ORDER BY id DESC LIMIT 1")).rows[0];
  if (!t) return;

  const b = (await q('SELECT * FROM battles WHERE id=$1', [battleId])).rows[0];
  if (!b || !b.winner) return;

  const parts = (await q('SELECT wallet, tg_username, growth_pct FROM participants WHERE battle_id=$1 AND paid=1', [battleId])).rows;

  for (const p of parts) {
    const isWinner = p.wallet === b.winner;
    const earned   = isWinner ? Math.round(parseInt(b.stake) * parts.length * 0.9) : 0;
    const growth   = parseFloat(p.growth_pct) || 0;

    await q(`
      INSERT INTO tournament_scores (tournament_id, wallet, tg_username, wins, total_battles, total_earned, best_growth, updated_at)
      VALUES ($1, $2, $3, $4, 1, $5, $6, EXTRACT(EPOCH FROM NOW()))
      ON CONFLICT (tournament_id, wallet) DO UPDATE SET
        wins          = tournament_scores.wins + $4,
        total_battles = tournament_scores.total_battles + 1,
        total_earned  = tournament_scores.total_earned + $5,
        best_growth   = GREATEST(tournament_scores.best_growth, $6),
        tg_username   = COALESCE($3, tournament_scores.tg_username),
        updated_at    = EXTRACT(EPOCH FROM NOW())
    `, [t.id, p.wallet, p.tg_username, isWinner ? 1 : 0, earned, growth]);
  }
  console.log(`[tournament] Scores updated for battle ${battleId}`);
}

// ── Получаем цепочку рефералов для кошелька (до 3 уровней) ──
async function getReferralChain(wallet) {
  const chain = [];
  let current = wallet;

  for (let level = 1; level <= 3; level++) {
    // Ищем по wallet напрямую ИЛИ через tg_user_id из participants
    const r = await q(
      `SELECT us.referred_by FROM user_settings us
       WHERE us.wallet = $1 AND us.referred_by IS NOT NULL
       UNION
       SELECT us.referred_by FROM participants p
       JOIN user_settings us ON us.tg_user_id = p.tg_user_id
       WHERE p.wallet = $1 AND us.referred_by IS NOT NULL
       LIMIT 1`,
      [current]
    );
    const ref = r.rows[0]?.referred_by;
    if (!ref || ref === current) break;
    chain.push({ level, wallet: ref });
    current = ref;
  }

  console.log(`[ref] Chain for ${wallet}:`, chain);
  return chain;
}

// ── Выплачиваем рефералам при оплате ставки ─────────────────
async function payReferrals(battleId, playerWallet, stakeSlug) {
  const chain = await getReferralChain(playerWallet);
  if (!chain.length) return;

  for (const ref of chain) {
    const pct    = CONFIG.REF_PERCENT[ref.level - 1];
    const amount = Math.round(stakeSlug * pct / 100);
    if (amount < 1) continue;

    try {
      const result = await autoPayout(`REF:${battleId}:L${ref.level}`, ref.wallet, amount);
      await q(
        'INSERT INTO referral_payouts (battle_id,to_wallet,from_wallet,level,amount_slug,tx_hash) VALUES ($1,$2,$3,$4,$5,$6)',
        [battleId, ref.wallet, playerWallet, ref.level, amount, result.txHash || null]
      );
      console.log(`[ref] Level ${ref.level} payout: ${amount} SLUG → ${ref.wallet} tx=${result.txHash}`);
    } catch (e) {
      console.error(`[ref] Payout failed level ${ref.level}: ${e.message}`);
    }
  }
}

async function notifyWinner(battle, winner, prize) {
  const p = (await q('SELECT tg_user_id FROM participants WHERE battle_id=$1 AND wallet=$2', [battle.id, winner])).rows[0];

  // Автоматически выплачиваем приз
  const result = await autoPayout(battle.id, winner, prize);

  if (result.ok) {
    // Успешная выплата — уведомляем победителя
    if (p?.tg_user_id) {
      await sendTg(p.tg_user_id,
        (MSG.winner_ok[(await getUserLangByWallet(winner))] || MSG.winner_ok['en'])(battle.id, prize, result.txHash)
      );
    }
    // Уведомляем себя
    const adminId = process.env.ADMIN_TG_ID;
    if (adminId) {
      await sendTg(adminId,
        `✅ <b>Битва #${battle.id} завершена</b>\nПобедитель: <code>${winner}</code>\nВыплачено: ${prize.toLocaleString()} SLUG\nTx: <code>${result.txHash}</code>`
      );
    }
  } else {
    // Ошибка выплаты — отправляем QR тебе для ручной подписи
    const qr      = buildQR(winner, prize, `WIN:${battle.id}`);
    const adminId = process.env.ADMIN_TG_ID;
    if (adminId) {
      await sendTg(adminId,
        `⚠️ <b>Автовыплата не удалась!</b>\nБитва #${battle.id}\nПобедитель: <code>${winner}</code>\nПриз: ${prize.toLocaleString()} SLUG\nОшибка: ${result.error}\n\nПодпиши вручную в Axiome Connect:`
      );
      await sendTg(adminId, `<code>${qr}</code>`);
    }
    if (p?.tg_user_id) {
      await sendTg(p.tg_user_id,
        (MSG.winner_pending[(await getUserLangByWallet(winner))] || MSG.winner_pending['en'])(battle.id, prize)
      );
    }
  }
}

async function notifyWinnerAuto(battle, winner, prize) {
  const p = (await q('SELECT tg_user_id FROM participants WHERE battle_id=$1 AND wallet=$2', [battle.id, winner])).rows[0];
  if (!p?.tg_user_id) return;
  const _langW = await getUserLang(p.tg_user_id) || 'ru';
  await sendTg(p.tg_user_id, (MSG.winner_ok[_langW] || MSG.winner_ok['en'])(battle.id, prize, ''));
}

async function callEscrowFinalize(battleId, winner) {
  const qr = `axiomesign://${Buffer.from(JSON.stringify({
    type: 'cosmwasm_execute', network: CONFIG.NETWORK, contract_addr: CONFIG.ESCROW_CONTRACT, funds: [],
    msg: { finalize: { battle_id: battleId, winner } }, memo: `FINALIZE:${battleId}`,
  })).toString('base64')}`;
  const adminId = process.env.ADMIN_TG_ID;
  if (adminId) {
    await sendTg(adminId, `🔑 <b>Финализация #${battleId}</b>\nПобедитель: <code>${winner}</code>\n\nВставь в Axiome Connect:`);
    await sendTg(adminId, `<code>${qr}</code>`);
  }
}

// ── Payment watcher ───────────────────────────────────────────
// Получаем последние входящие транзакции
// Режим зависит от того задеплоен ли Escrow контракт:
// - Если да: ищем транзакции на ESCROW_CONTRACT (игроки делают SLUG.send() на него)
// - Если нет: ищем переводы на PROJECT_WALLET
async function getRecentIncomingTxs() {
  try {
    const walletEnc = encodeURIComponent(`'${CONFIG.PROJECT_WALLET}'`);
    const actionEnc = encodeURIComponent(`'transfer'`);
    const url = `${CONFIG.AXIOME_REST}/cosmos/tx/v1beta1/txs?query=wasm.action%3D${actionEnc}%20AND%20wasm.to%3D${walletEnc}&limit=200&order_by=ORDER_BY_DESC&pagination.reverse=true`;
    const data = await (await fetch(url, { signal: AbortSignal.timeout(5000) })).json();
    // API возвращает tx_responses (с events) или txs
    return data?.tx_responses || data?.txs || [];
  } catch (e) { console.error('[watcher]', e.message); return []; }
}

function parseBattleMemo(memo) {
  if (!memo?.startsWith('BATTLE:') && !memo?.startsWith('JOIN:')) return null;

  // Формат JOIN:{battleId} (escrow режим, wallet берём из события)
  if (memo.startsWith('JOIN:')) {
    const battleId = memo.slice(5);
    return battleId ? { battleId, wallet: null } : null;
  }

  // Формат BATTLE:{battleId}:{wallet}
  const i1 = memo.indexOf(':'), i2 = memo.indexOf(':', i1 + 1);
  if (i2 === -1) return null;
  const battleId = memo.slice(i1 + 1, i2), wallet = memo.slice(i2 + 1);
  return (battleId && wallet.startsWith('axm1')) ? { battleId, wallet } : null;
}

// Извлекаем сумму и отправителя из wasm events транзакции
// Поддерживает два формата events: logs[].events и tx_responses[].events
function extractPaymentInfo(tx) {
  try {
    // Сначала пробуем events напрямую (новый формат API)
    const allEvents = tx.events || tx.logs?.[0]?.events || [];

    for (const ev of allEvents) {
      if (ev.type !== 'wasm') continue;
      const attrs = Object.fromEntries(ev.attributes.map(a => [a.key, a.value]));

      // Fallback режим: CW20 transfer — ищем to = PROJECT_WALLET
      if (attrs.action === 'transfer' && attrs.to === CONFIG.PROJECT_WALLET && attrs.amount) {
        return {
          wallet: attrs.from || null,
          amount: parseInt(attrs.amount),
        };
      }
    }
    return { wallet: null, amount: null };
  } catch { return { wallet: null, amount: null }; }
}

function extractTransferAmount(tx) {
  return extractPaymentInfo(tx).amount;
}

async function confirmPayment(battleId, wallet, txHash, paidAmount) {
  // Защита от двойной оплаты: tx_hash уже видели
  try { await q('INSERT INTO seen_txs (tx_hash) VALUES ($1)', [txHash]); } catch { return; }

  // Защита от двойной оплаты: участник уже оплатил
  const alreadyPaid = (await q('SELECT paid FROM participants WHERE battle_id=$1 AND wallet=$2', [battleId, wallet])).rows[0];
  if (!alreadyPaid) { console.log(`[watcher] Skip: no participant record ${battleId}/${wallet}`); return; }
  if (alreadyPaid.paid === 1) { console.log(`[watcher] Skip: already paid ${battleId}/${wallet}`); return; }

  // Проверяем лимит игроков — битва не должна быть переполнена
  const battleCheck = (await q('SELECT max_players, status FROM battles WHERE id=$1', [battleId])).rows[0];
  if (!battleCheck) { console.log(`[watcher] Skip: battle ${battleId} not found`); return; }

  // Разрешаем подтверждение для open И live (второй игрок может платить пока первый уже стартовал)
  if (battleCheck.status === 'cancelled' || battleCheck.status === 'ended') {
    console.log(`[watcher] Skip+refund: battle ${battleId} is ${battleCheck.status}`);
    autoPayout('REFUND_' + battleId.slice(-4) + '_' + wallet.slice(-4), wallet, paidAmount / 1e6)
      .catch(e => console.error('[watcher] refund error:', e.message));
    return;
  }

  // Проверяем что мест ещё хватает
  const currentPaid = +(await q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=1', [battleId])).rows[0].c;
  if (currentPaid >= battleCheck.max_players) {
    console.log(`[watcher] Skip+refund: battle ${battleId} already full (${currentPaid}/${battleCheck.max_players})`);
    autoPayout('REFUND_FULL_' + battleId.slice(-4), wallet, paidAmount / 1e6)
      .catch(e => console.error('[watcher] refund error:', e.message));
    return;
  }

  await q('UPDATE pending_payments SET verified=1 WHERE battle_id=$1 AND wallet=$2', [battleId, wallet]);
  await q('UPDATE participants SET paid=1 WHERE battle_id=$1 AND wallet=$2', [battleId, wallet]);
  console.log(`[watcher] Payment confirmed: battle=${battleId} wallet=${wallet}`);

  // Выплачиваем рефералам сразу после подтверждения ставки
  const bRef = (await q('SELECT stake FROM battles WHERE id=$1', [battleId])).rows[0];
  if (bRef) {
    payReferrals(battleId, wallet, parseInt(bRef.stake)).catch(e =>
      console.error('[ref] payReferrals error:', e.message)
    );
  }

  const p = (await q('SELECT tg_user_id FROM participants WHERE battle_id=$1 AND wallet=$2', [battleId, wallet])).rows[0];
  const b = (await q('SELECT * FROM battles WHERE id=$1', [battleId])).rows[0];
  if (!b) return;

  const paidCount = +(await q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=1', [battleId])).rows[0].c;
  if (p?.tg_user_id) {
    const _lang1 = await getUserLang(p.tg_user_id) || 'ru';
    await sendTg(p.tg_user_id, (MSG.stake_accepted[_lang1] || MSG.stake_accepted['en'])(battleId, paidCount, b.max_players));
  }

  // Атомарный старт: используем UPDATE с условием чтобы только один поток запустил битву
  if (paidCount >= b.max_players) {
    const durationMs = parseInt(b.duration_ms) || 600000;
    const endsAt = Math.floor(Date.now() / 1000) + Math.floor(durationMs / 1000);
    const updateResult = await q(
      "UPDATE battles SET status='live', started_at=$1, ends_at=$2 WHERE id=$3 AND status='open' RETURNING id",
      [Math.floor(Date.now()/1000), endsAt, battleId]
    );
    // Только если UPDATE реально обновил строку (не был уже live)
    if (updateResult.rows.length > 0) {
      console.log(`[watcher] Battle ${battleId} started!`);
      const prize = Math.round(b.stake * b.max_players * (1 - CONFIG.FEE_PERCENT / 100));
      const parts = await q('SELECT tg_user_id FROM participants WHERE battle_id=$1 AND tg_user_id IS NOT NULL', [battleId]);
      for (const part of parts.rows) {
        const _lang2 = await getUserLang(part.tg_user_id) || 'ru';
        await sendTg(part.tg_user_id, (MSG.battle_started[_lang2] || MSG.battle_started['en'])(battleId, parseInt(b.stake) * b.max_players, prize));
      }
    }
  }
}

async function checkPayments() {
  const pending = (await q('SELECT * FROM pending_payments WHERE verified=0')).rows;
  if (!pending.length) return;

  const now = Math.floor(Date.now() / 1000);
  for (const p of pending) {
    if (now - parseInt(p.created_at) < CONFIG.PAYMENT_TIMEOUT_SEC) continue;
    await q('UPDATE pending_payments SET verified=2 WHERE id=$1', [p.id]);
    await q('DELETE FROM participants WHERE battle_id=$1 AND wallet=$2 AND paid=0', [p.battle_id, p.wallet]);
    const b = (await q('SELECT * FROM battles WHERE id=$1', [p.battle_id])).rows[0];
    if (b) await q('UPDATE battles SET pool_total=$1 WHERE id=$2', [Math.max(0, parseInt(b.pool_total) - parseInt(b.stake)), p.battle_id]);
  }

  // Только неистёкшие и неподтверждённые
  const active = pending.filter(p => p.verified === 0 && now - parseInt(p.created_at) < CONFIG.PAYMENT_TIMEOUT_SEC);
  if (!active.length) return;

  const txs = await getRecentIncomingTxs();
  if (!txs.length) return;

  for (const tx of txs) {
    const txHash = tx.txhash;
    if (!txHash || (tx.code !== undefined && tx.code !== 0)) continue;
    if ((await q('SELECT 1 FROM seen_txs WHERE tx_hash=$1', [txHash])).rows.length) continue;

    const { wallet: senderWallet, amount: paidAmount } = extractPaymentInfo(tx);
    if (!senderWallet || !paidAmount) continue;

    // Ищем pending платёж от этого отправителя
    const memo = (tx.tx?.body?.memo || tx.body?.memo || '');
    const memoData = parseBattleMemo(memo);

    // Ищем pending платёж — отправитель должен совпадать с участником
    let pend = null;
    if (memoData?.battleId && memoData?.wallet) {
      // По memo — но проверяем что отправитель = участник
      const candidate = active.find(p => p.battle_id === memoData.battleId && p.wallet === memoData.wallet);
      if (candidate && candidate.wallet === senderWallet) {
        pend = candidate;
      } else if (candidate && candidate.wallet !== senderWallet) {
        console.log(`[watcher] REJECTED: sender ${senderWallet} tried to pay for ${candidate.wallet} in battle ${memoData.battleId}`);
        // Возвращаем деньги отправителю
        autoPayout('REFUND_WRONG_SENDER_' + txHash.slice(-8), senderWallet, paidAmount / 1e6)
          .catch(e => console.error('[watcher] refund failed:', e.message));
        continue;
      }
    }
    // Fallback: ищем по кошельку отправителя ТОЛЬКО если memo не распознан
    // и только точное совпадение суммы
    if (!pend && !memoData?.battleId) {
      pend = active.find(p => p.wallet === senderWallet && paidAmount >= parseInt(p.amount));
    }
    if (!pend) continue;

    await confirmPayment(pend.battle_id, pend.wallet, txHash, paidAmount);
  }
}

setInterval(async () => {
  try { await checkPayments(); }
  catch (e) { console.error('[watcher] interval error:', e.message); }
}, 15000);

// ── Middleware ────────────────────────────────────────────────
function requireWallet(req, res, next) {
  const wallet = req.headers['x-wallet'] || req.body?.wallet;
  if (!wallet?.startsWith('axm1')) return res.status(401).json({ error: 'Axiome wallet required' });
  req.wallet = wallet;
  next();
}

function requireAdmin(req, res, next) {
  if ((req.headers['x-admin-key'] || req.body?.admin_key) !== CONFIG.ADMIN_KEY)
    return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── Routes ────────────────────────────────────────────────────
app.get('/health', (_, res) => res.json({ ok: true, time: Date.now() }));

app.post('/admin/fix-ref', requireAdmin, async (req, res) => {
  const { wallet, referred_by } = req.body;
  if (!wallet || !referred_by) return res.status(400).json({ error: 'wallet and referred_by required' });
  if (wallet === referred_by) return res.status(400).json({ error: 'Cannot refer yourself' });
  await q('UPDATE user_settings SET referred_by=$1 WHERE wallet=$2', [referred_by, wallet]);
  res.json({ ok: true, message: `${wallet} now referred by ${referred_by}` });
});

app.post('/admin/recalc-refs', requireAdmin, async (req, res) => {
  const battles = await q(
    "SELECT DISTINCT p.battle_id, p.wallet, b.stake FROM participants p JOIN battles b ON b.id=p.battle_id WHERE p.paid=1 AND b.status='ended'"
  );
  let paid = 0, skipped = 0;
  for (const row of battles.rows) {
    const existing = await q('SELECT id FROM referral_payouts WHERE battle_id=$1 AND from_wallet=$2', [row.battle_id, row.wallet]);
    if (existing.rows.length > 0) { skipped++; continue; }
    const chain = await getReferralChain(row.wallet);
    if (!chain.length) { skipped++; continue; }
    await payReferrals(row.battle_id, row.wallet, parseInt(row.stake));
    paid++;
  }
  res.json({ ok: true, processed: battles.rows.length, paid, skipped });
});

app.post('/admin/test-refs', requireAdmin, async (req, res) => {
  const { wallet, stake } = req.body;
  if (!wallet || !stake) return res.status(400).json({ error: 'wallet and stake required' });

  const chain = await getReferralChain(wallet);
  console.log(`[test-refs] Chain for ${wallet}:`, chain);

  if (!chain.length) return res.json({ ok: false, message: 'No referral chain found', chain: [] });

  const results = [];
  for (const ref of chain) {
    const pct    = CONFIG.REF_PERCENT[ref.level - 1];
    const amount = Math.round(stake * pct / 100);
    results.push({ level: ref.level, wallet: ref.wallet, pct, amount, status: 'simulated' });
  }

  res.json({ ok: true, chain, results, message: 'Simulation only — no real payout. Use test-refs-real to send real tokens.' });
});

app.post('/admin/test-refs-real', requireAdmin, async (req, res) => {
  const { wallet, stake } = req.body;
  if (!wallet || !stake) return res.status(400).json({ error: 'wallet and stake required' });

  await payReferrals('TEST', wallet, parseInt(stake));
  res.json({ ok: true, message: `Referral payouts triggered for ${wallet} with stake ${stake} SLUG` });
});

app.get('/admin/debug-refs', requireAdmin, async (req, res) => {
  const settings = await q('SELECT tg_user_id, wallet, referred_by FROM user_settings LIMIT 50');
  const payouts  = await q('SELECT * FROM referral_payouts ORDER BY created_at DESC LIMIT 20');
  res.json({ user_settings: settings.rows, payouts: payouts.rows });
});

// ── Тест автовыплаты (только для отладки) ────────────────────
app.post('/admin/test-payout', requireAdmin, async (req, res) => {
  const { winner, amount } = req.body;
  if (!winner || !amount) return res.status(400).json({ error: 'winner and amount required' });
  const result = await autoPayout('TEST', winner, parseInt(amount));
  res.json(result);
});

// ── Регистрация кошелька (вызывается когда игрок подключает кошелёк) ──
app.post('/register', async (req, res) => {
  const { wallet, tg_user_id, referred_by } = req.body;
  if (!wallet?.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });

  // Не давать рефером самого себя
  const refWallet = (referred_by && referred_by !== wallet) ? referred_by : null;

  await q(`INSERT INTO user_settings (tg_user_id, wallet, referred_by)
    VALUES ($1, $2, $3)
    ON CONFLICT (tg_user_id) DO UPDATE SET
      wallet=$2,
      referred_by=COALESCE(user_settings.referred_by, $3),
      updated_at=EXTRACT(EPOCH FROM NOW())`,
    [String(tg_user_id || ''), wallet, refWallet]
  );

  // Также привязываем по wallet если tg_user_id уже есть с referred_by
  if (refWallet) {
    await q(`UPDATE user_settings SET wallet=$1
      WHERE tg_user_id=$2 AND wallet IS NULL`,
      [wallet, String(tg_user_id || '')]
    );
  }

  res.json({ ok: true, wallet, referred_by: refWallet });
});

app.get('/battles', async (req, res) => {
  const battles = req.query.status
    ? await q('SELECT * FROM battles WHERE status=$1 ORDER BY created_at DESC LIMIT 50', [req.query.status])
    : await q('SELECT * FROM battles ORDER BY created_at DESC LIMIT 50');

  const result = await Promise.all(battles.rows.map(async b => {
    const [total, paid, parts] = await Promise.all([
      q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1', [b.id]),
      q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=1', [b.id]),
      // Для open битв загружаем участников чтобы показать кто уже вошёл
      b.status === 'open'
        ? q('SELECT wallet, tg_username, paid FROM participants WHERE battle_id=$1 ORDER BY joined_at ASC', [b.id])
        : q('SELECT wallet, tg_username, growth_pct, paid FROM participants WHERE battle_id=$1 AND paid=1 ORDER BY growth_pct DESC NULLS LAST', [b.id]),
    ]);
    return {
      ...b,
      participants_count: +total.rows[0].c,
      paid_count:         +paid.rows[0].c,
      participants:       parts.rows,
    };
  }));
  res.json(result);
});

// ── Live состояние битвы — текущий рост портфелей ───────────
app.get('/battles/:id/live', async (req, res) => {
  const bRes = await q('SELECT * FROM battles WHERE id=$1', [req.params.id]);
  if (!bRes.rows.length) return res.status(404).json({ error: 'Not found' });
  const b = bRes.rows[0];

  if (b.status !== 'live') return res.json({ status: b.status, participants: [] });

  const parts = await q(
    'SELECT wallet, tg_username, portfolio_start, paid FROM participants WHERE battle_id=$1 AND paid=1',
    [req.params.id]
  );

  const prices = await getTokenPrices();
  const now    = Math.floor(Date.now() / 1000);

  const participants = await Promise.all(parts.rows.map(async p => {
    const start       = JSON.parse(p.portfolio_start || '{}');
    const startTokens = start.tokens || {};
    const startPrices = start.prices || {};

    // Среднее арифметическое % изменения цены каждого токена (без AXM)
    const growthList = [];
    for (const [sym, data] of Object.entries(startTokens)) {
      if (sym === 'AXM') continue;
      const priceBefore = startPrices[sym] || 0;
      const priceAfter  = prices[sym]      || 0;
      if (priceBefore <= 0 || priceAfter <= 0) continue;
      growthList.push(((priceAfter - priceBefore) / priceBefore) * 100);
    }
    const growth = growthList.length > 0
      ? growthList.reduce((a, b) => a + b, 0) / growthList.length
      : 0;

    return {
      wallet:      p.wallet,
      tg_username: p.tg_username,
      growth_pct:  parseFloat(growth.toFixed(2)),
    };
  }));

  // Сортируем по росту
  participants.sort((a, b) => b.growth_pct - a.growth_pct);

  res.json({
    status:       b.status,
    ends_at:      b.ends_at,
    secs_left:    Math.max(0, parseInt(b.ends_at) - now),
    participants,
  });
});

app.get('/battles/:id', async (req, res) => {
  const bRes = await q('SELECT * FROM battles WHERE id=$1', [req.params.id]);
  if (!bRes.rows.length) return res.status(404).json({ error: 'Not found' });
  const parts = await q('SELECT wallet,tg_username,joined_at,growth_pct,paid FROM participants WHERE battle_id=$1 ORDER BY growth_pct DESC', [req.params.id]);
  res.json({ ...bRes.rows[0], participants: parts.rows });
});

app.post('/battles', requireWallet, async (req, res) => {
  const { stake, max_players, duration_key, tg_user_id, tg_username } = req.body;
  if (!stake || stake < CONFIG.MIN_STAKE) return res.status(400).json({ error: `Мин. ставка: ${CONFIG.MIN_STAKE} SLUG` });

  // Лимит: максимум 3 открытых битвы с одного кошелька
  const openByWallet = (await q(
    "SELECT COUNT(*) as c FROM battles WHERE creator=$1 AND status='open'",
    [req.wallet]
  )).rows[0].c;
  if (parseInt(openByWallet) >= 7) {
    return res.status(400).json({ error: LANG === 'en' ? 'Max 7 open battles per wallet' : 'Максимум 7 открытых битв с одного кошелька' });
  }
  if (stake > 50000) return res.status(400).json({ error: 'Макс. ставка: 50,000 SLUG' });
  if (stake > 50000) return res.status(400).json({ error: 'Макс. ставка: 50,000 SLUG' });
  if (!max_players || max_players < 2 || max_players > 20) return res.status(400).json({ error: 'Игроков: 2-20' });
  // Принимаем duration_ms напрямую или через duration_key
  const DURATIONS = { '10m': 600000, '1h': 3600000, '24h': 86400000, '3d': 259200000, '7d': 604800000 };
  let duration_ms = DURATIONS[duration_key] || req.body.duration_ms;
  // Поддержка кастомной длительности в минутах
  if (!duration_ms && req.body.duration_minutes) {
    duration_ms = parseInt(req.body.duration_minutes) * 60 * 1000;
  }
  if (!duration_ms) return res.status(400).json({ error: 'Укажи длительность' });
  // Ограничения: от 1 часа до 7 дней
  if (duration_ms < 600000)    duration_ms = 600000;   // минимум 10 минут
  if (duration_ms > 604800000) duration_ms = 604800000;

  const slugBal = await getCW20Balance(req.wallet, CONFIG.SLUG_CONTRACT);
  if (slugBal < stake * 1e6) return res.status(400).json({ error: `Недостаточно SLUG. Нужно: ${stake}` });

  const id      = crypto.randomBytes(4).toString('hex').toUpperCase();
  const memo    = `BATTLE:${id}:${req.wallet}`;
  const portfolio = await getFullPortfolio(req.wallet);
  const prices    = await getTokenPrices();
  const snap      = JSON.stringify({ tokens: portfolio, prices, total_axm: calcPortfolioAxm(portfolio, prices), total_usd: calcPortfolioAxm(portfolio, prices), ts: Date.now() });

  await q('INSERT INTO battles (id,creator,stake,max_players,duration_ms,status,pool_total) VALUES ($1,$2,$3,$4,$5,$6,0)', [id,req.wallet,stake,max_players,duration_ms,'open']);
  await q('INSERT INTO participants (battle_id,wallet,tg_user_id,tg_username,portfolio_start) VALUES ($1,$2,$3,$4,$5)', [id,req.wallet,tg_user_id||null,tg_username||null,snap]);
  await q('INSERT INTO pending_payments (battle_id,wallet,amount,memo) VALUES ($1,$2,$3,$4)', [id,req.wallet,stake*1e6,memo]);

  // Уведомляем подписчиков (асинхронно — не блокируем ответ)
  notifyNewBattle(id, stake, max_players, tg_username).catch(e => console.error('[notify]', e.message));

  res.json({ id, qr_string: buildJoinQR(id, stake, memo), memo, stake, message: `Битва #${id} создана!`, payment_timeout_sec: CONFIG.PAYMENT_TIMEOUT_SEC });
});

app.post('/battles/:id/join', requireWallet, async (req, res) => {
  const { tg_user_id, tg_username } = req.body;
  const b = (await q('SELECT * FROM battles WHERE id=$1', [req.params.id])).rows[0];
  if (!b) return res.status(404).json({ error: 'Not found' });
  if (b.status !== 'open') return res.status(400).json({ error: 'Битва не открыта' });

  // Защита от двойного входа: уже есть запись (оплаченная или ожидающая)
  const existingParticipant = (await q(
    'SELECT paid FROM participants WHERE battle_id=$1 AND wallet=$2',
    [req.params.id, req.wallet]
  )).rows[0];
  if (existingParticipant) {
    if (existingParticipant.paid === 1) return res.status(400).json({ error: LANG === 'en' ? 'You already paid for this battle' : 'Ты уже оплатил эту битву' });
    // Уже есть pending — возвращаем те же данные для оплаты
    const memo = `BATTLE:${req.params.id}:${req.wallet}`;
    return res.json({ id: req.params.id, qr_string: buildJoinQR(req.params.id, b.stake, memo), memo, stake: b.stake, message: 'Pending payment exists', payment_timeout_sec: CONFIG.PAYMENT_TIMEOUT_SEC });
  }

  // Проверяем оплативших и тех кто в процессе оплаты (не старше 15 минут)
  const paidCnt   = +(await q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=1', [req.params.id])).rows[0].c;
  const now15     = Math.floor(Date.now()/1000) - 15*60;
  const pendingCnt= +(await q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=0 AND joined_at > $2', [req.params.id, now15])).rows[0].c;
  if (paidCnt >= b.max_players) return res.status(400).json({ error: LANG === 'en' ? 'Battle is full' : 'Битва заполнена' });
  if (paidCnt + pendingCnt >= b.max_players) return res.status(400).json({ error: LANG === 'en' ? 'All slots reserved, try again in 15 min' : 'Все места заняты ожидающими оплаты, попробуй через 15 минут' });



  const exist = (await q('SELECT paid FROM participants WHERE battle_id=$1 AND wallet=$2', [req.params.id, req.wallet])).rows[0];
  if (exist?.paid) return res.status(400).json({ error: 'Уже оплатил' });

  const slugBal = await getCW20Balance(req.wallet, CONFIG.SLUG_CONTRACT);
  if (slugBal < b.stake * 1e6) return res.status(400).json({ error: `Недостаточно SLUG. Нужно: ${b.stake}` });

  const portfolio = await getFullPortfolio(req.wallet);
  const prices    = await getTokenPrices();
  const usdVal    = await calcPortfolioUsd(portfolio, prices);
  const snap      = JSON.stringify({ tokens: portfolio, prices, total_usd: usdVal, ts: Date.now() });
  const memo      = `BATTLE:${req.params.id}:${req.wallet}`;

  try {
    await q('INSERT INTO participants (battle_id,wallet,tg_user_id,tg_username,portfolio_start) VALUES ($1,$2,$3,$4,$5)', [req.params.id,req.wallet,tg_user_id||null,tg_username||null,snap]);
  } catch (e) {
    if (e.message.includes('unique') || e.message.includes('duplicate')) return res.status(400).json({ error: 'Уже участвуешь' });
    throw e;
  }
  await q('INSERT INTO pending_payments (battle_id,wallet,amount,memo) VALUES ($1,$2,$3,$4) ON CONFLICT (memo) DO NOTHING', [req.params.id,req.wallet,b.stake*1e6,memo]);

  res.json({ message: `Оплати ставку ${b.stake} SLUG`, qr_string: buildJoinQR(req.params.id, b.stake, memo), memo, stake: b.stake, portfolio_usd: usdVal.toFixed(2), payment_timeout_sec: CONFIG.PAYMENT_TIMEOUT_SEC });
});

app.get('/battles/:id/payment-status/:wallet', async (req, res) => {
  const pRes = await q('SELECT paid FROM participants WHERE battle_id=$1 AND wallet=$2', [req.params.id, req.params.wallet]);
  if (!pRes.rows.length) return res.status(404).json({ error: 'Not found' });
  const pend = (await q('SELECT verified,created_at FROM pending_payments WHERE battle_id=$1 AND wallet=$2', [req.params.id, req.params.wallet])).rows[0];
  const now  = Math.floor(Date.now() / 1000);
  res.json({ paid: pRes.rows[0].paid === 1, expired: pend?.verified === 2, timeout_sec: Math.max(0, pend ? CONFIG.PAYMENT_TIMEOUT_SEC - (now - parseInt(pend.created_at)) : 0) });
});

app.post('/battles/:id/finalize', async (req, res) => {
  const b = (await q('SELECT * FROM battles WHERE id=$1', [req.params.id])).rows[0];
  if (!b) return res.status(404).json({ error: 'Not found' });
  if (b.status !== 'live') return res.status(400).json({ error: 'Не активна' });

  const now = Math.floor(Date.now() / 1000);
  if (b.ends_at && now < parseInt(b.ends_at)) return res.status(400).json({ error: `Завершится через ${parseInt(b.ends_at)-now}с` });

  const parts = (await q('SELECT * FROM participants WHERE battle_id=$1 AND paid=1', [req.params.id])).rows;
  if (!parts.length) {
    await q("UPDATE battles SET status='cancelled' WHERE id=$1", [req.params.id]);
    return res.json({ message: 'Отменена — нет оплативших' });
  }

  const prices = await getTokenPrices();
  let winner = null, bestGrowth = -Infinity;

  for (const p of parts) {
    const start = JSON.parse(p.portfolio_start || '{}');
    const startTokens  = start.tokens || {};
    const startPrices  = start.prices || {};

    // НОВАЯ ЛОГИКА: среднее арифметическое % изменения цены каждого токена
    // Исключаем AXM (всегда 0%) и токены с нулевой стартовой ценой
    const growthList = [];
    for (const [symbol, data] of Object.entries(startTokens)) {
      if (symbol === 'AXM') continue;                    // AXM не считаем
      const priceBefore = startPrices[symbol] || 0;
      const priceAfter  = prices[symbol]      || 0;
      if (priceBefore <= 0 || priceAfter <= 0) continue; // нулевые цены не считаем
      const pct = ((priceAfter - priceBefore) / priceBefore) * 100;
      growthList.push(pct);
    }

    // Среднее арифметическое
    const growth = growthList.length > 0
      ? growthList.reduce((a, b) => a + b, 0) / growthList.length
      : 0;

    console.log(`[finalize] ${p.wallet} growth=${growth.toFixed(4)}% (${growthList.length} tokens: ${growthList.map(g=>g.toFixed(2)+'%').join(', ')})`);

    await q('UPDATE participants SET portfolio_end=$1,growth_pct=$2 WHERE battle_id=$3 AND wallet=$4',
      [JSON.stringify({ tokens: startTokens, startPrices, prices, endPrices: prices, growth_list: growthList, ts: Date.now() }), growth, b.id, p.wallet]);
    if (growth > bestGrowth) { bestGrowth = growth; winner = p.wallet; }
  }

  const prize = Math.round(parseInt(b.stake) * parts.length * (1 - CONFIG.FEE_PERCENT / 100));
  const fee   = Math.round(parseInt(b.stake) * parts.length * CONFIG.FEE_PERCENT / 100);

  await q("UPDATE battles SET status='ended',winner=$1 WHERE id=$2", [winner, b.id]);
  console.log(`[finalize] Battle ${b.id} ended. Winner: ${winner} prize=${prize}`);

  // Обновляем турнирный лидерборд сразу при финализации
  updateTournamentScores(b.id).catch(e => console.error('[tournament]', e.message));

  if (CONFIG.ESCROW_CONTRACT) {
    try { await callEscrowFinalize(b.id, winner); await notifyWinnerAuto(b, winner, prize); }
    catch (e) { await notifyWinner(b, winner, prize); }
  } else {
    await notifyWinner(b, winner, prize);
  }

  for (const loser of parts.filter(p => p.wallet !== winner)) {
    if (!loser.tg_user_id) continue;
    const _langL = await getUserLang(loser.tg_user_id) || 'ru';
    const _growth = parts.find(p=>p.wallet===loser.wallet)?.growth_pct || 0;
    await sendTg(loser.tg_user_id, (MSG.loser[_langL] || MSG.loser['en'])(b.id, winner, _growth));
  }

  res.json({ winner, growth_pct: bestGrowth.toFixed(2), prize_slug: prize, fee_slug: fee, paid_players: parts.length });
});

// ── Результаты битвы ─────────────────────────────────────────
app.get('/battles/:id/results', async (req, res) => {
  const bRes = await q('SELECT * FROM battles WHERE id=$1', [req.params.id]);
  if (!bRes.rows.length) return res.status(404).json({ error: 'Not found' });
  const b = bRes.rows[0];

  const parts = await q(
    'SELECT wallet, tg_username, portfolio_start, portfolio_end, growth_pct, paid, joined_at FROM participants WHERE battle_id=$1 AND paid=1 ORDER BY growth_pct DESC',
    [req.params.id]
  );

  const result = parts.rows.map((p, i) => {
    const start = JSON.parse(p.portfolio_start || '{}');
    const end   = JSON.parse(p.portfolio_end   || '{}');

    // Детальный отчёт по каждому токену
    const tokenReport = [];
    const startTokens = start.tokens || {};
    const startPrices = start.prices || {};
    const endPrices   = end.prices || end.endPrices || {};

    for (const [symbol, data] of Object.entries(startTokens)) {
      if (!data.amount || data.amount === 0) continue;
      // AXM always = 1, never 0
      const priceBefore = (symbol === 'AXM' ? 1 : 0) || startPrices[symbol] || 0;
      const priceAfter  = (symbol === 'AXM' ? 1 : 0) || endPrices[symbol]   || startPrices[symbol] || 0;
      const valBefore   = data.amount * priceBefore;
      const valAfter    = data.amount * priceAfter;
      const change      = priceBefore > 0 ? ((priceAfter - priceBefore) / priceBefore) * 100 : 0;

      // Skip tokens with no price data at all
      if (priceBefore === 0 && priceAfter === 0) continue;
      tokenReport.push({
        symbol,
        amount:       data.amount,
        price_before: priceBefore,
        price_after:  priceAfter,
        val_before:   valBefore,
        val_after:    valAfter,
        change_pct:   parseFloat(change.toFixed(2)),
        counted_in_result: symbol !== 'AXM' && priceBefore > 0 && priceAfter > 0,
      });
    }

    // Сортируем по влиянию на результат
    tokenReport.sort((a, z) => Math.abs(z.change_pct) - Math.abs(a.change_pct));

    return {
      rank:         i + 1,
      wallet:       p.wallet,
      tg_username:  p.tg_username,
      growth_pct:   parseFloat((p.growth_pct || 0).toFixed(2)),
      portfolio_start_axm: parseFloat((start.total_axm || start.total_usd || 0).toFixed(4)),
      portfolio_end_axm:   parseFloat((end.total_usd   || 0).toFixed(4)),
      is_winner:    p.wallet === b.winner,
      tokens:       tokenReport,
    };
  });

  res.json({
    battle: {
      id:         b.id,
      status:     b.status,
      stake:      parseInt(b.stake),
      started_at: b.started_at,
      ends_at:    b.ends_at,
      winner:     b.winner,
    },
    participants: result,
  });
});

// ── Реферальная ссылка ───────────────────────────────────────
app.get('/referral/:wallet', async (req, res) => {
  const wallet = req.params.wallet;
  if (!wallet.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });

  // Статистика рефералов
  const refs = await q(
    'SELECT COUNT(*) as count, SUM(amount_slug) as total FROM referral_payouts WHERE to_wallet=$1',
    [wallet]
  );
  // Считаем количество уникальных рефералов (кто указал этот кошелёк как реферера)
  const refCount = await q(
    'SELECT COUNT(*) as count FROM user_settings WHERE referred_by=$1',
    [wallet]
  );

  const refLink = `https://t.me/${process.env.BOT_USERNAME || 'slug_battle_bot'}?start=ref_${wallet}`;

  res.json({
    wallet,
    ref_link:      refLink,
    total_refs:    parseInt(refCount.rows[0].count) || 0,
    total_earned:  parseInt(refs.rows[0].total) || 0,
    ref_percent:   CONFIG.REF_PERCENT,
  });
});

// ── История реферальных выплат ────────────────────────────────
app.get('/referral/:wallet/history', async (req, res) => {
  const payouts = await q(
    'SELECT * FROM referral_payouts WHERE to_wallet=$1 ORDER BY created_at DESC LIMIT 50',
    [req.params.wallet]
  );
  res.json(payouts.rows);
});

// ── Таблица лидеров ──────────────────────────────────────────
// ── История битв игрока ──────────────────────────────────────
app.get('/history/:wallet', async (req, res) => {
  const wallet = req.params.wallet;
  const limit  = Math.min(parseInt(req.query.limit) || 20, 50);

  const rows = await q(`
    SELECT
      b.id, b.stake, b.max_players, b.status,
      b.started_at, b.ends_at, b.winner,
      p.growth_pct, p.paid, p.joined_at,
      (SELECT COUNT(*) FROM participants WHERE battle_id=b.id AND paid=1) as player_count
    FROM participants p
    JOIN battles b ON b.id = p.battle_id
    WHERE p.wallet = $1 AND p.paid = 1 AND b.status IN ('ended','live')
    ORDER BY b.started_at DESC
    LIMIT $2
  `, [wallet, limit]);

  const history = rows.rows.map(r => {
    const isWinner  = r.winner === wallet;
    const prize     = Math.round(r.stake * r.player_count * 0.90);
    const earned    = isWinner ? prize : -parseInt(r.stake);
    return {
      battle_id:   r.id,
      status:      r.status,
      stake:       parseInt(r.stake),
      max_players: r.max_players,
      player_count: parseInt(r.player_count),
      growth_pct:  parseFloat(r.growth_pct || 0),
      is_winner:   isWinner,
      earned:      earned,
      prize:       prize,
      started_at:  r.started_at,
      ends_at:     r.ends_at,
    };
  });

  const stats = {
    total:    history.length,
    wins:     history.filter(h => h.is_winner).length,
    losses:   history.filter(h => !h.is_winner && h.status === 'ended').length,
    total_earned: history.filter(h => h.is_winner).reduce((s, h) => s + h.prize, 0),
    total_lost:   history.filter(h => !h.is_winner && h.status === 'ended').reduce((s, h) => s + h.stake, 0),
    best_growth:  history.length ? Math.max(...history.map(h => h.growth_pct)) : 0,
  };

  res.json({ wallet, stats, history });
});

app.get('/leaderboard', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 20, 50);

  // Топ по количеству побед
  const topWins = await q(`
    SELECT
      p.wallet,
      p.tg_username,
      COUNT(*) FILTER (WHERE b.winner = p.wallet) as wins,
      COUNT(*) as total_battles,
      SUM(b.stake) FILTER (WHERE b.winner = p.wallet) as total_earned,
      MAX(p.growth_pct) as best_growth,
      AVG(p.growth_pct) FILTER (WHERE p.growth_pct IS NOT NULL) as avg_growth
    FROM participants p
    JOIN battles b ON b.id = p.battle_id
    WHERE p.paid = 1 AND b.status = 'ended'
    GROUP BY p.wallet, p.tg_username
    ORDER BY wins DESC, total_earned DESC
    LIMIT $1
  `, [limit]);

  // Топ по лучшему росту портфеля за одну битву
  const topGrowth = await q(`
    SELECT
      p.wallet,
      p.tg_username,
      p.growth_pct,
      b.id as battle_id,
      b.stake
    FROM participants p
    JOIN battles b ON b.id = p.battle_id
    WHERE p.paid = 1 AND p.growth_pct IS NOT NULL AND b.status = 'ended'
    ORDER BY p.growth_pct DESC
    LIMIT $1
  `, [limit]);

  res.json({
    top_winners: topWins.rows,
    top_growth:  topGrowth.rows,
  });
});

app.get('/portfolio/:wallet', async (req, res) => {
  if (!req.params.wallet.startsWith('axm1')) return res.status(400).json({ error: 'Invalid wallet' });
  const [portfolio, prices] = await Promise.all([getFullPortfolio(req.params.wallet), getTokenPrices()]);
  res.json({ wallet: req.params.wallet, portfolio, prices, total_usd: await calcPortfolioUsd(portfolio, prices) });
});

// ── Admin: сброс участника (если платёж истёк) ──────────────
app.post('/admin/confirm-payment', requireAdmin, async (req, res) => {
  const { battle_id, wallet, tx_hash, tg_username } = req.body;
  if (!battle_id || !wallet || !tx_hash) return res.status(400).json({ error: 'battle_id, wallet, tx_hash required' });

  const b = (await q('SELECT * FROM battles WHERE id=$1', [battle_id])).rows[0];
  if (!b) return res.status(404).json({ error: 'Battle not found' });

  const p = (await q('SELECT * FROM participants WHERE battle_id=$1 AND wallet=$2', [battle_id, wallet])).rows[0];
  if (!p) {
    await q('INSERT INTO participants (wallet, battle_id, tg_username, joined_at, paid) VALUES ($1,$2,$3,$4,0)',
      [wallet, battle_id, tg_username || wallet.slice(0,8), Math.floor(Date.now()/1000)]);
    await q('INSERT INTO pending_payments (battle_id, wallet, amount, created_at) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING',
      [battle_id, wallet, b.stake, Math.floor(Date.now()/1000)]);
  } else if (p.paid) {
    return res.status(400).json({ error: 'Already paid' });
  }

  // Удаляем из seen_txs чтобы можно было переиспользовать хеш
  await q('DELETE FROM seen_txs WHERE tx_hash=$1', [tx_hash]);
  await confirmPayment(battle_id, wallet, tx_hash, parseInt(b.stake) * 1e6);
  res.json({ ok: true, message: `Payment confirmed for ${wallet} in battle ${battle_id}` });
});

app.post('/admin/reset-participant', requireAdmin, async (req, res) => {
  const { battle_id, wallet } = req.body;
  if (!battle_id || !wallet) return res.status(400).json({ error: 'battle_id and wallet required' });
  await q('DELETE FROM participants WHERE battle_id=$1 AND wallet=$2 AND paid=0', [battle_id, wallet]);
  await q('DELETE FROM pending_payments WHERE battle_id=$1 AND wallet=$2', [battle_id, wallet]);
  res.json({ ok: true, message: `Participant ${wallet} reset from battle ${battle_id}` });
});

// ── Admin: отменить битву и удалить всё ──────────────────────
app.post('/admin/cancel-battle', requireAdmin, async (req, res) => {
  const { battle_id } = req.body;
  if (!battle_id) return res.status(400).json({ error: 'battle_id required' });
  await q("UPDATE battles SET status='cancelled' WHERE id=$1", [battle_id]);
  await q('DELETE FROM participants WHERE battle_id=$1 AND paid=0', [battle_id]);
  await q('DELETE FROM pending_payments WHERE battle_id=$1', [battle_id]);
  res.json({ ok: true, message: `Battle ${battle_id} cancelled` });
});


// ── Найти битвы без выплат ────────────────────────────────
app.get('/admin/ended-no-payout', requireAdmin, async (req, res) => {
  try {
    // Битвы со статусом ended у которых есть победитель но нет записи в referral_payouts
    // или битвы которые должны были завершиться (ends_at прошёл) но статус ещё live
    const now = Math.floor(Date.now() / 1000);

    // 1. Live битвы у которых время вышло
    const stuckLive = (await q(
      "SELECT * FROM battles WHERE status='live' AND ends_at < $1",
      [now]
    )).rows;

    // 2. Ended битвы где победитель есть но нет tx_hash выплаты (нет в referral_payouts как выплата победителю)
    // Проверяем через отсутствие записи о выплате победителю
    const endedNoPayout = (await q(`
      SELECT b.id, b.winner, b.stake, b.max_players, b.ends_at,
             COUNT(p.wallet) as paid_count
      FROM battles b
      LEFT JOIN participants p ON p.battle_id = b.id AND p.paid = 1
      WHERE b.status = 'ended' AND b.winner IS NOT NULL
      GROUP BY b.id, b.winner, b.stake, b.max_players, b.ends_at
      ORDER BY b.ends_at DESC
      LIMIT 100
    `)).rows;

    res.json({
      stuck_live: stuckLive.map(b => ({
        id: b.id, ends_at: b.ends_at,
        overdue_min: Math.floor((now - parseInt(b.ends_at)) / 60),
        stake: b.stake, max_players: b.max_players
      })),
      ended_count: endedNoPayout.length,
      note: 'Use /admin/finalize-stuck to process stuck live battles'
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Финализировать все зависшие live битвы ────────────────
app.post('/admin/finalize-stuck', requireAdmin, async (req, res) => {
  try {
    const now = Math.floor(Date.now() / 1000);
    const stuck = (await q(
      "SELECT * FROM battles WHERE status='live' AND ends_at < $1",
      [now]
    )).rows;

    if (!stuck.length) return res.json({ ok: true, message: 'No stuck battles', count: 0 });

    const results = [];
    for (const b of stuck) {
      try {
        console.log(`[admin] Force finalizing stuck battle ${b.id}`);
        const resp = await fetch(`http://localhost:${process.env.PORT || 3000}/battles/${b.id}/finalize`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-wallet': 'admin' }
        });
        const data = await resp.json();
        results.push({ id: b.id, ok: !data.error, result: data });
      } catch (e) {
        results.push({ id: b.id, ok: false, error: e.message });
      }
    }

    res.json({ ok: true, count: stuck.length, results });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});


// ── Ручной рефанд (автоматически через мнемонику) ───────────
app.post('/admin/manual-refund', requireAdmin, async (req, res) => {
  const { wallet, amount_slug, memo } = req.body;
  if (!wallet || !amount_slug) return res.status(400).json({ error: 'wallet and amount_slug required' });
  const memoStr = memo || `REFUND:${wallet.slice(-8)}`;

  try {
    const client   = await getSigningClient();
    const amountRaw = String(Math.round(parseFloat(amount_slug) * 1e6));
    const result = await queuePayout(async () => {
      const c = await getSigningClient();
      return c.execute(
        CONFIG.PROJECT_WALLET,
        CONFIG.SLUG_CONTRACT,
        { transfer: { recipient: wallet, amount: amountRaw } },
        { amount: [{ denom: 'uaxm', amount: '600000' }], gas: '200000' },
        memoStr,
      );
    });
    console.log(`[refund] ✅ Sent ${amount_slug} SLUG → ${wallet} tx=${result.transactionHash}`);
    res.json({ ok: true, wallet, amount_slug, tx_hash: result.transactionHash });
  } catch (e) {
    console.error(`[refund] ❌ Failed: ${e.message}`);
    _signingClient = null;
    // Fallback — QR для ручной подписи
    const qr = buildQR(wallet, amount_slug, memoStr);
    res.json({ ok: false, error: e.message, qr_string: qr, memo: memoStr });
  }
});


function formatTimeLeft(seconds) {
  if (seconds <= 0) return '00:00:00';
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (d > 0) return `${d}д ${h}ч ${m}м`;
  if (h > 0) return `${h}ч ${m}м ${s}с`;
  return `${m}м ${s}с`;
}


// ── Турнир: старт ────────────────────────────────────────────
app.post('/admin/tournament/start', requireAdmin, async (req, res) => {
  try {
    const durationDays = req.body.days || 7;
    const now     = Math.floor(Date.now() / 1000);
    const endsAt  = now + durationDays * 24 * 60 * 60;

    // Деактивируем предыдущий турнир если есть
    await q("UPDATE tournament SET status='ended' WHERE status='active'");

    const result = await q(
      "INSERT INTO tournament (starts_at, ends_at, status) VALUES ($1, $2, 'active') RETURNING id",
      [now, endsAt]
    );
    const tournamentId = result.rows[0].id;

    console.log(`[tournament] Started #${tournamentId} ends ${new Date(endsAt*1000).toISOString()}`);
    res.json({
      ok: true,
      tournament_id: tournamentId,
      starts_at: now,
      ends_at: endsAt,
      ends_at_human: new Date(endsAt * 1000).toISOString(),
      duration_days: durationDays,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Турнир: текущий статус ───────────────────────────────────

// ── Турнир: пересчёт всех битв за период ────────────────────
app.post('/admin/tournament/recalc', requireAdmin, async (req, res) => {
  try {
    const t = (await q("SELECT * FROM tournament WHERE status='active' ORDER BY id DESC LIMIT 1")).rows[0];
    if (!t) return res.status(400).json({ error: 'No active tournament' });

    // Берём все завершённые битвы за период турнира
    const battles = (await q(
      "SELECT id FROM battles WHERE status='ended' AND created_at >= $1",
      [t.starts_at]
    )).rows;

    console.log(`[tournament] Recalculating ${battles.length} battles for tournament #${t.id}`);
    let updated = 0;
    for (const b of battles) {
      await updateTournamentScores(b.id);
      updated++;
    }

    res.json({ ok: true, tournament_id: t.id, battles_processed: updated });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});


app.get('/admin/tournament/status', requireAdmin, async (req, res) => {
  try {
    const t = (await q("SELECT * FROM tournament WHERE status='active' ORDER BY id DESC LIMIT 1")).rows[0];
    if (!t) return res.json({ active: false });
    const now = Math.floor(Date.now() / 1000);
    const scores = (await q(
      `SELECT ts.wallet, ts.tg_username, ts.wins, ts.total_battles, ts.total_earned, ts.best_growth
       FROM tournament_scores ts WHERE ts.tournament_id=$1
       ORDER BY ts.wins DESC, ts.best_growth DESC LIMIT 10`,
      [t.id]
    )).rows;
    res.json({
      active: true,
      id: t.id,
      starts_at: t.starts_at,
      ends_at: t.ends_at,
      time_left_sec: Math.max(0, t.ends_at - now),
      time_left_human: formatTimeLeft(t.ends_at - now),
      top10: scores,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Турнир: топ (публичный) ──────────────────────────────────
app.get('/tournament/leaderboard', async (req, res) => {
  try {
    const t = (await q("SELECT * FROM tournament WHERE status='active' ORDER BY id DESC LIMIT 1")).rows[0];
    if (!t) return res.json({ active: false, leaderboard: [] });
    const now = Math.floor(Date.now() / 1000);
    const scores = (await q(
      `SELECT ts.wallet, ts.tg_username, ts.wins, ts.total_battles, ts.total_earned, ts.best_growth
       FROM tournament_scores ts WHERE ts.tournament_id=$1
       ORDER BY ts.wins DESC, ts.best_growth DESC LIMIT 50`,
      [t.id]
    )).rows;
    res.json({
      active: true,
      id: t.id,
      ends_at: t.ends_at,
      time_left_sec: Math.max(0, t.ends_at - now),
      time_left_human: formatTimeLeft(t.ends_at - now),
      leaderboard: scores,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});



// ── Принудительный старт зависшей битвы ─────────────────────
app.post('/admin/force-start', requireAdmin, async (req, res) => {
  const { battle_id } = req.body;
  if (!battle_id) return res.status(400).json({ error: 'battle_id required' });

  const b = (await q('SELECT * FROM battles WHERE id=$1', [battle_id])).rows[0];
  if (!b) return res.status(404).json({ error: 'Battle not found' });
  if (b.status !== 'open') return res.status(400).json({ error: `Battle status is ${b.status}` });

  const paidCount = +(await q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=1', [battle_id])).rows[0].c;
  if (paidCount < 2) return res.status(400).json({ error: `Not enough paid players: ${paidCount}` });

  const durationMs = parseInt(b.duration_ms) || 600000;
  const now    = Math.floor(Date.now() / 1000);
  const endsAt = now + Math.floor(durationMs / 1000);

  await q("UPDATE battles SET status='live', started_at=$1, ends_at=$2 WHERE id=$3", [now, endsAt, battle_id]);

  const prize = Math.round(parseInt(b.stake) * paidCount * (1 - CONFIG.FEE_PERCENT / 100));
  const parts = await q('SELECT tg_user_id FROM participants WHERE battle_id=$1 AND tg_user_id IS NOT NULL AND paid=1', [battle_id]);
  for (const p of parts.rows) {
    const lang = await getUserLang(p.tg_user_id) || 'ru';
    await sendTg(p.tg_user_id, MSG.battle_started[lang] || MSG.battle_started['en'](battle_id, parseInt(b.stake) * paidCount, prize));
  }

  console.log(`[admin] Force started battle ${battle_id} ends_at=${endsAt}`);
  res.json({ ok: true, battle_id, started_at: now, ends_at: endsAt, ends_in_min: Math.floor(durationMs/60000), paid_players: paidCount });
});


app.get('/admin/stats', requireAdmin, async (req, res) => {
  const [o,l,e,c,pend,conf,exp,tot,paid] = await Promise.all([
    q("SELECT COUNT(*) as c FROM battles WHERE status='open'"),
    q("SELECT COUNT(*) as c FROM battles WHERE status='live'"),
    q("SELECT COUNT(*) as c FROM battles WHERE status='ended'"),
    q("SELECT COUNT(*) as c FROM battles WHERE status='cancelled'"),
    q('SELECT COUNT(*) as c FROM pending_payments WHERE verified=0'),
    q('SELECT COUNT(*) as c FROM pending_payments WHERE verified=1'),
    q('SELECT COUNT(*) as c FROM pending_payments WHERE verified=2'),
    q('SELECT COUNT(*) as c FROM participants'),
    q('SELECT COUNT(*) as c FROM participants WHERE paid=1'),
  ]);
  res.json({
    battles:      { open: +o.rows[0].c, live: +l.rows[0].c, ended: +e.rows[0].c, cancelled: +c.rows[0].c },
    payments:     { pending: +pend.rows[0].c, confirmed: +conf.rows[0].c, expired: +exp.rows[0].c },
    participants: { total: +tot.rows[0].c, paid: +paid.rows[0].c },
  });
});

// ── Переводы для бота ────────────────────────────────────────
const I18N = {
  ru: {
    welcome:      '🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\nСобери портфель токенов Axiome и сражайся с другими трейдерами!\n\nЧей портфель вырастет больше — забирает весь банк 🏆\n\nСтавки в $SLUG • Автовыплаты • Реальные цены',
    open_game:    '⚔️ Открыть игру',
    how_to_play:  '📖 Как играть',
    no_battles:   'Активных битв нет. Создай первую!',
    battles_title:'⚔️ <b>Активные битвы:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
    guide: `🐌 <b>Как играть в SLUG Battle</b>

SLUG Battle — это PvP игра на реальных портфелях. Ты соревнуешься с другими игроками чей портфель токенов вырастет больше за время битвы.

━━━━━━━━━━━━━━━━━━
🔑 <b>Шаг 1 — Подключи кошелёк</b>

Нажми <b>"Подключить"</b> в правом верхнем углу и введи свой Axiome адрес (<code>axm1...</code>).
Ты даёшь доступ <b>только для чтения</b> кошелька.

━━━━━━━━━━━━━━━━━━
⚔️ <b>Шаг 2 — Создай или войди в битву</b>

<b>Создать битву:</b>
• Выбери ставку (от 20 SLUG)
• Выбери длительность (10 минут — 7 дней)
• Выбери количество игроков (2–20)
• Нажми <b>"Создать битву"</b> и оплати ставку

<b>Войти в чужую битву:</b>
• Найди открытую битву в списке
• Нажми <b>"Войти в битву"</b> и оплати ставку

━━━━━━━━━━━━━━━━━━
💳 <b>Шаг 3 — Как оплатить ставку</b>

1. Открой <b>Axiome Wallet</b> → раздел <b>Connect</b> (значок розетки сверху)
2. Скопируй строку из игры (кнопка 📋)
3. Вставь в поле и нажми Connect, подтверди отправку
4. Готово — игра автоматически подтвердит платёж

⏱ Битва отменяется если игроки не набрались за 60 минут. Ставка вернётся на кошелёк.

━━━━━━━━━━━━━━━━━━
🏁 <b>Шаг 4 — Битва идёт</b>

Как только все игроки оплатили — битва стартует. Игра фиксирует цены всех токенов в твоём кошельке на момент старта.

Нажми <b>"Смотреть гонку"</b> и следи кто лидирует в реальном времени.

━━━━━━━━━━━━━━━━━━
🧮 <b>Шаг 5 — Как определяется победитель</b>

Игра считает <b>средний % изменения цены</b> каждого токена в портфеле (кроме AXM).

Количество токенов не важно — важно движение цены. Это уравнивает всех игроков независимо от размера портфеля.

━━━━━━━━━━━━━━━━━━
💰 <b>Шаг 6 — Выплата</b>

Победитель получает <b>90% призового банка</b> автоматически на кошелёк.
5% — комиссия платформы, 5% — выплата рефералам.

━━━━━━━━━━━━━━━━━━
👥 <b>Реферальная программа</b>

Приглашай друзей и получай автоматически с каждой их ставки:
• Lvl 1 — 3% (прямой реферал)
• Lvl 2 — 1.5%
• Lvl 3 — 0.5%

Скопируй реф. ссылку во вкладке 👥 Рефы.

━━━━━━━━━━━━━━━━━━
💡 <b>Советы</b>

• Чем больше разных токенов в портфеле — тем стабильнее результат
• Ты можешь влиять на рост своего портфеля через покупки токенов на бирже!`,
  },
  en: {
    welcome:      '🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\nBuild an Axiome token portfolio and battle other traders!\n\nWhose portfolio grows more — takes the entire prize pool 🏆\n\nStakes in $SLUG • Auto payouts • Real prices',
    open_game:    '⚔️ Open game',
    how_to_play:  '📖 How to play',
    no_battles:   'No active battles. Create the first one!',
    battles_title:'⚔️ <b>Active battles:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
    guide: `🐌 <b>How to play SLUG Battle</b>

SLUG Battle is a PvP game on real portfolios. You compete with other players to see whose token portfolio grows the most during the battle.

━━━━━━━━━━━━━━━━━━
🔑 <b>Step 1 — Connect your wallet</b>

Tap <b>"Connect"</b> in the top right corner and enter your Axiome address (<code>axm1...</code>).
You give <b>read-only access</b> to your wallet.

━━━━━━━━━━━━━━━━━━
⚔️ <b>Step 2 — Create or join a battle</b>

<b>Create a battle:</b>
• Choose a stake (from 20 SLUG)
• Choose duration (10 minutes — 7 days)
• Choose number of players (2–20)
• Tap <b>"Create Battle"</b> and pay the stake

<b>Join someone else's battle:</b>
• Find an open battle in the list
• Tap <b>"Join Battle"</b> and pay the stake

━━━━━━━━━━━━━━━━━━
💳 <b>Step 3 — How to pay the stake</b>

1. Open <b>Axiome Wallet</b> → <b>Connect</b> section (socket icon at the top)
2. Copy the string from the game (📋 button)
3. Paste into the field and tap Connect, confirm the transfer
4. Done — the game will automatically confirm your payment

⏱ Battle is cancelled if players don't fill up within 60 minutes. Your stake is returned.

━━━━━━━━━━━━━━━━━━
🏁 <b>Step 4 — Battle is on</b>

Once all players have paid — the battle starts. The game locks the prices of all tokens in your wallet at the moment of start.

Tap <b>"Watch Race"</b> to follow who is leading in real time.

━━━━━━━━━━━━━━━━━━
🧮 <b>Step 5 — How the winner is determined</b>

The game calculates the <b>average % price change</b> of each token in your portfolio (excluding AXM).

The amount of tokens doesn't matter — only price movement counts. This levels the playing field for everyone.

━━━━━━━━━━━━━━━━━━
💰 <b>Step 6 — Payout</b>

The winner receives <b>90% of the prize pool</b> automatically to their wallet.
5% — platform fee, 5% — referral payouts.

━━━━━━━━━━━━━━━━━━
👥 <b>Referral program</b>

Invite friends and earn automatically from every stake they make:
• Lvl 1 — 3% (direct referral)
• Lvl 2 — 1.5%
• Lvl 3 — 0.5%

Copy your referral link in the 👥 Refs tab.

━━━━━━━━━━━━━━━━━━
💡 <b>Tips</b>

• The more different tokens in your portfolio — the more stable your result
• You can influence your portfolio growth by buying tokens on the exchange!`,
  },


  de: {
    welcome:      '🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\nBaue ein Axiome-Token-Portfolio und kämpfe gegen andere Trader!\n\nWessen Portfolio mehr wächst — gewinnt den gesamten Preispool 🏆\n\nEinsätze in $SLUG • Automatische Auszahlungen • Echte Preise',
    open_game:    '⚔️ Spiel öffnen',
    how_to_play:  '📖 Wie man spielt',
    no_battles:   'Keine aktiven Kämpfe. Erstelle den ersten!',
    battles_title:'⚔️ <b>Aktive Kämpfe:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
    guide: `🐌 <b>Wie man SLUG Battle spielt</b>

SLUG Battle ist ein PvP-Spiel mit echten Portfolios. Du wetteiferst mit anderen Spielern, wessen Token-Portfolio während des Kampfes am meisten wächst.

━━━━━━━━━━━━━━━━━━
🔑 <b>Schritt 1 — Wallet verbinden</b>

Tippe auf <b>"Verbinden"</b> oben rechts und gib deine Axiome-Adresse ein (<code>axm1...</code>).
Du gibst nur <b>Lesezugriff</b> auf dein Wallet.

━━━━━━━━━━━━━━━━━━
⚔️ <b>Schritt 2 — Kampf erstellen oder beitreten</b>

<b>Kampf erstellen:</b>
• Einsatz wählen (ab 20 SLUG)
• Dauer wählen (10 Minuten — 7 Tage)
• Spieleranzahl wählen (2–20)
• Auf <b>"Kampf erstellen"</b> tippen und Einsatz bezahlen

<b>Einem Kampf beitreten:</b>
• Offenen Kampf in der Liste finden
• Auf <b>"Kampf beitreten"</b> tippen und Einsatz bezahlen

━━━━━━━━━━━━━━━━━━
💳 <b>Schritt 3 — Einsatz bezahlen</b>

1. <b>Axiome Wallet</b> öffnen → Bereich <b>Connect</b> (Steckdosen-Symbol oben)
2. Zeichenkette aus dem Spiel kopieren (📋 Schaltfläche)
3. In das Feld einfügen, Connect tippen und Überweisung bestätigen
4. Fertig — das Spiel bestätigt deine Zahlung automatisch

⏱ Kampf wird abgebrochen wenn Spieler sich nicht innerhalb von 60 Minuten füllen. Einsatz wird zurückgegeben.

━━━━━━━━━━━━━━━━━━
🏁 <b>Schritt 4 — Kampf läuft</b>

Sobald alle Spieler bezahlt haben beginnt der Kampf. Das Spiel sperrt die Preise aller Token beim Start.

Tippe auf <b>"Rennen ansehen"</b> um in Echtzeit zu verfolgen wer führt.

━━━━━━━━━━━━━━━━━━
🧮 <b>Schritt 5 — Gewinner</b>

Das Spiel berechnet die <b>durchschnittliche % Preisänderung</b> jedes Tokens im Portfolio (ohne AXM).

Die Token-Menge spielt keine Rolle — nur die Preisbewegung zählt.

━━━━━━━━━━━━━━━━━━
💰 <b>Schritt 6 — Auszahlung</b>

Der Gewinner erhält <b>90% des Preispools</b> automatisch auf sein Wallet.
5% Plattformgebühr, 5% Empfehlungsauszahlungen.

━━━━━━━━━━━━━━━━━━
👥 <b>Empfehlungsprogramm</b>

Lade Freunde ein und verdiene automatisch von jedem ihrer Einsätze:
• Lvl 1 — 3% (direkter Empfehler)
• Lvl 2 — 1.5%
• Lvl 3 — 0.5%

Kopiere deinen Empfehlungslink im Tab 👥 Refs.

━━━━━━━━━━━━━━━━━━
💡 <b>Tipps</b>

• Je mehr verschiedene Token im Portfolio — desto stabiler dein Ergebnis
• Du kannst dein Portfolio-Wachstum durch den Kauf von Token an der Börse beeinflussen!`,
  },

  bg: {
    welcome:      '🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\nСъздай портфолио от Axiome токени и се бий с други трейдъри!\n\nЧието портфолио нарасне повече — взима целия банк 🏆\n\nЗалози в $SLUG • Автоматични плащания • Реални цени',
    open_game:    '⚔️ Отвори играта',
    how_to_play:  '📖 Как се играе',
    no_battles:   'Няма активни битки. Създай първата!',
    battles_title:'⚔️ <b>Активни битки:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
    guide: `🐌 <b>Как се играе SLUG Battle</b>

SLUG Battle е PvP игра с реални портфолиа. Състезаваш се с други играчи чието токен портфолио ще нарасне повече по време на битката.

━━━━━━━━━━━━━━━━━━
🔑 <b>Стъпка 1 — Свържи портфейла</b>

Натисни <b>"Свързване"</b> горе вдясно и въведи своя Axiome адрес (<code>axm1...</code>).
Давате достъп само за <b>четене</b> на портфейла.

━━━━━━━━━━━━━━━━━━
⚔️ <b>Стъпка 2 — Създай или влез в битка</b>

<b>Създай битка:</b>
• Избери залог (от 20 SLUG)
• Избери продължителност (10 минути — 7 дни)
• Избери брой играчи (2–20)
• Натисни <b>"Създай битка"</b> и плати залога

<b>Влез в чужда битка:</b>
• Намери отворена битка в списъка
• Натисни <b>"Влез в битката"</b> и плати залога

━━━━━━━━━━━━━━━━━━
💳 <b>Стъпка 3 — Как да платиш залога</b>

1. Отвори <b>Axiome Wallet</b> → раздел <b>Connect</b> (икона контакт горе)
2. Копирай низа от играта (бутон 📋)
3. Постави в полето и натисни Connect, потвърди изпращането
4. Готово — играта автоматично ще потвърди плащането ти

⏱ Битката се отменя ако играчите не се наберат за 60 минути. Залогът се връща.

━━━━━━━━━━━━━━━━━━
🏁 <b>Стъпка 4 — Битката върви</b>

Щом всички играчи са платили — битката започва. Играта фиксира цените на всички токени в портфейла ти в момента на старта.

Натисни <b>"Гледай надпреварата"</b> за да следиш кой води в реално време.

━━━━━━━━━━━━━━━━━━
🧮 <b>Стъпка 5 — Как се определя победителят</b>

Играта изчислява <b>средния % промяна на цената</b> на всеки токен в портфолиото (без AXM).

Количеството токени няма значение — важно е движението на цената.

━━━━━━━━━━━━━━━━━━
💰 <b>Стъпка 6 — Изплащане</b>

Победителят получава <b>90% от наградния банк</b> автоматично на портфейла.
5% комисионна на платформата, 5% реферални плащания.

━━━━━━━━━━━━━━━━━━
👥 <b>Реферална програма</b>

Покани приятели и печели автоматично от всеки техен залог:
• Ниво 1 — 3% (пряк реферал)
• Ниво 2 — 1.5%
• Ниво 3 — 0.5%

Копирай рефералния си линк в таба 👥 Реферали.

━━━━━━━━━━━━━━━━━━
💡 <b>Съвети</b>

• Колкото повече различни токени в портфолиото — толкова по-стабилен резултат
• Можеш да влияеш на ръста на портфолиото си чрез покупки на токени на борсата!`,
  },

  uk: {
    welcome:      '🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\nСтвори портфель токенів Axiome та змагайся з іншими трейдерами!\n\nЧий портфель виросте більше — забирає весь банк 🏆\n\nСтавки в $SLUG • Автовиплати • Реальні ціни',
    open_game:    '⚔️ Відкрити гру',
    how_to_play:  '📖 Як грати',
    no_battles:   'Немає активних битв. Створи першу!',
    battles_title:'⚔️ <b>Активні битви:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
    guide: `🐌 <b>Як грати в SLUG Battle</b>

SLUG Battle — це PvP гра на реальних портфелях. Ти змагаєшся з іншими гравцями, чий портфель токенів виросте більше за час битви.

━━━━━━━━━━━━━━━━━━
🔑 <b>Крок 1 — Підключи гаманець</b>

Натисни <b>"Підключити"</b> у правому верхньому куті та введи свою Axiome адресу (<code>axm1...</code>).
Ти надаєш доступ лише для <b>читання</b> гаманця.

━━━━━━━━━━━━━━━━━━
⚔️ <b>Крок 2 — Створи або увійди в битву</b>

<b>Створити битву:</b>
• Обери ставку (від 20 SLUG)
• Обери тривалість (10 хвилин — 7 днів)
• Обери кількість гравців (2–20)
• Натисни <b>"Створити битву"</b> та оплати ставку

<b>Увійти в чужу битву:</b>
• Знайди відкриту битву у списку
• Натисни <b>"Увійти в битву"</b> та оплати ставку

━━━━━━━━━━━━━━━━━━
💳 <b>Крок 3 — Як оплатити ставку</b>

1. Відкрий <b>Axiome Wallet</b> → розділ <b>Connect</b> (іконка розетки вгорі)
2. Скопіюй рядок з гри (кнопка 📋)
3. Встав у поле та натисни Connect, підтверди відправку
4. Готово — гра автоматично підтвердить платіж

⏱ Битва скасовується якщо гравці не набрались за 60 хвилин. Ставка повертається.

━━━━━━━━━━━━━━━━━━
🏁 <b>Крок 4 — Битва йде</b>

Як тільки всі гравці оплатили — битва стартує. Гра фіксує ціни всіх токенів у твоєму гаманці на момент старту.

Натисни <b>"Дивитись гонку"</b> та стеж хто лідирує в реальному часі.

━━━━━━━━━━━━━━━━━━
🧮 <b>Крок 5 — Як визначається переможець</b>

Гра рахує <b>середній % зміни ціни</b> кожного токена в портфелі (без AXM).

Кількість токенів не важлива — важливий рух ціни.

━━━━━━━━━━━━━━━━━━
💰 <b>Крок 6 — Виплата</b>

Переможець отримує <b>90% призового банку</b> автоматично на гаманець.
5% — комісія платформи, 5% — реферальні виплати.

━━━━━━━━━━━━━━━━━━
👥 <b>Реферальна програма</b>

Запрошуй друзів та отримуй автоматично з кожної їх ставки:
• Рівень 1 — 3% (прямий реферал)
• Рівень 2 — 1.5%
• Рівень 3 — 0.5%

Скопіюй реферальне посилання у вкладці 👥 Рефи.

━━━━━━━━━━━━━━━━━━
💡 <b>Поради</b>

• Чим більше різних токенів у портфелі — тим стабільніший результат
• Ти можеш впливати на ріст свого портфеля через купівлю токенів на біржі!`,
  },

  kz: {
    welcome:      '🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\nAxiome токендерінен портфель құр және басқа трейдерлермен шайқас!\n\nКімнің портфелі көп өссе — бүкіл банкті алады 🏆\n\nСтавкалар $SLUG • Автоматты төлемдер • Нақты бағалар',
    open_game:    '⚔️ Ойынды ашу',
    how_to_play:  '📖 Қалай ойнау керек',
    no_battles:   'Белсенді шайқастар жоқ. Біріншісін жасаңыз!',
    battles_title:'⚔️ <b>Белсенді шайқастар:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
    guide: `🐌 <b>SLUG Battle-да қалай ойнау керек</b>

SLUG Battle — нақты портфельдердегі PvP ойын. Сен шайқас уақытында кімнің токен портфелі көп өсетінін анықтау үшін басқа ойыншылармен бәсекелесесің.

━━━━━━━━━━━━━━━━━━
🔑 <b>1-қадам — Әмиянды қосу</b>

Жоғарғы оң жақтағы <b>"Қосу"</b> түймесін басып, Axiome мекенжайыңды енгіз (<code>axm1...</code>).
Сен әмиянға тек <b>оқу</b> рұқсатын бересің.

━━━━━━━━━━━━━━━━━━
⚔️ <b>2-қадам — Шайқас жасау немесе кіру</b>

<b>Шайқас жасау:</b>
• Ставка таңда (20 SLUG-тан)
• Ұзақтықты таңда (10 минут — 7 күн)
• Ойыншылар санын таңда (2–20)
• <b>"Шайқас жасау"</b> түймесін басып, ставканы төле

<b>Басқаның шайқасына кіру:</b>
• Тізімнен ашық шайқасты тап
• <b>"Шайқасқа кіру"</b> түймесін басып, ставканы төле

━━━━━━━━━━━━━━━━━━
💳 <b>3-қадам — Ставканы қалай төлеу керек</b>

1. <b>Axiome Wallet</b> ашып → <b>Connect</b> бөліміне өт (жоғарыдағы розетка белгісі)
2. Ойыннан жолды көшір (📋 түймесі)
3. Өріске қой және Connect басып, жіберуді растай
4. Дайын — ойын төлемді автоматты түрде растайды

⏱ Ойыншылар 60 минут ішінде жиналмаса шайқас тоқтатылады. Ставка қайтарылады.

━━━━━━━━━━━━━━━━━━
🏁 <b>4-қадам — Шайқас жүріп жатыр</b>

Барлық ойыншылар төлегеннен кейін — шайқас басталады. Ойын старт кезінде әмиянындағы барлық токендердің бағасын бекітеді.

Нақты уақытта кім алда екенін бақылау үшін <b>"Жарысты қарау"</b> түймесін бас.

━━━━━━━━━━━━━━━━━━
🧮 <b>5-қадам — Жеңімпаз қалай анықталады</b>

Ойын портфельдегі әр токеннің <b>орташа % бағасының өзгеруін</b> есептейді (AXM-сыз).

Токен саны маңызды емес — бағаның қозғалысы маңызды.

━━━━━━━━━━━━━━━━━━
💰 <b>6-қадам — Төлем</b>

Жеңімпаз <b>сыйлық банкінің 90%-ын</b> автоматты түрде әмиянына алады.
5% — платформа комиссиясы, 5% — реферал төлемдері.

━━━━━━━━━━━━━━━━━━
👥 <b>Реферал бағдарламасы</b>

Достарыңды шақыр және олардың әр ставкасынан автоматты түрде тап:
• 1-деңгей — 3% (тікелей реферал)
• 2-деңгей — 1.5%
• 3-деңгей — 0.5%

Реферал сілтемеңді 👥 Рефс қойындысынан көшір.

━━━━━━━━━━━━━━━━━━
💡 <b>Кеңестер</b>

• Портфельде неғұрлым көп токен — нәтиже соғұрлым тұрақты
• Биржада токен сатып алу арқылы портфелінің өсуіне әсер ете аласың!`,
  },

  fr: {
    welcome:      '🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\nConstruis un portefeuille de tokens Axiome et affronte d\'autres traders!\n\nCelui dont le portefeuille croît le plus — remporte toute la cagnotte 🏆\n\nMises en $SLUG • Paiements automatiques • Prix réels',
    open_game:    '⚔️ Ouvrir le jeu',
    how_to_play:  '📖 Comment jouer',
    no_battles:   'Aucun combat actif. Crée le premier!',
    battles_title:'⚔️ <b>Combats actifs:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
    guide: `🐌 <b>Comment jouer à SLUG Battle</b>

SLUG Battle est un jeu PvP sur des portefeuilles réels. Tu te bats avec d'autres joueurs pour voir dont le portefeuille de tokens croît le plus pendant le combat.

━━━━━━━━━━━━━━━━━━
🔑 <b>Étape 1 — Connecter le portefeuille</b>

Appuie sur <b>"Connecter"</b> en haut à droite et entre ton adresse Axiome (<code>axm1...</code>).
Tu donnes uniquement un accès en <b>lecture seule</b> à ton portefeuille.

━━━━━━━━━━━━━━━━━━
⚔️ <b>Étape 2 — Créer ou rejoindre un combat</b>

<b>Créer un combat:</b>
• Choisir la mise (à partir de 20 SLUG)
• Choisir la durée (10 minutes — 7 jours)
• Choisir le nombre de joueurs (2–20)
• Appuyer sur <b>"Créer un combat"</b> et payer la mise

<b>Rejoindre un combat:</b>
• Trouver un combat ouvert dans la liste
• Appuyer sur <b>"Rejoindre"</b> et payer la mise

━━━━━━━━━━━━━━━━━━
💳 <b>Étape 3 — Comment payer la mise</b>

1. Ouvrir <b>Axiome Wallet</b> → section <b>Connect</b> (icône prise en haut)
2. Copier la chaîne du jeu (bouton 📋)
3. Coller dans le champ et appuyer sur Connect, confirmer l'envoi
4. Terminé — le jeu confirmera ton paiement automatiquement

⏱ Le combat est annulé si les joueurs ne se remplissent pas dans 60 minutes. La mise est remboursée.

━━━━━━━━━━━━━━━━━━
🏁 <b>Étape 4 — Le combat commence</b>

Dès que tous les joueurs ont payé — le combat démarre. Le jeu fixe les prix de tous les tokens dans ton portefeuille au moment du départ.

Appuie sur <b>"Regarder la course"</b> pour suivre qui est en tête en temps réel.

━━━━━━━━━━━━━━━━━━
🧮 <b>Étape 5 — Comment le gagnant est déterminé</b>

Le jeu calcule la <b>variation % moyenne du prix</b> de chaque token dans le portefeuille (sans AXM).

La quantité de tokens n'a pas d'importance — seul le mouvement des prix compte.

━━━━━━━━━━━━━━━━━━
💰 <b>Étape 6 — Paiement</b>

Le gagnant reçoit <b>90% de la cagnotte</b> automatiquement sur son portefeuille.
5% commission de plateforme, 5% paiements de parrainage.

━━━━━━━━━━━━━━━━━━
👥 <b>Programme de parrainage</b>

Invite des amis et gagne automatiquement sur chacune de leurs mises:
• Niveau 1 — 3% (parrainage direct)
• Niveau 2 — 1.5%
• Niveau 3 — 0.5%

Copie ton lien de parrainage dans l'onglet 👥 Refs.

━━━━━━━━━━━━━━━━━━
💡 <b>Conseils</b>

• Plus il y a de tokens différents dans le portefeuille — plus le résultat est stable
• Tu peux influencer la croissance de ton portefeuille en achetant des tokens en bourse!`,
  },

  bn: {
    welcome:      '🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\nAxiome টোকেন দিয়ে পোর্টফোলিও তৈরি করো এবং অন্য ট্রেডারদের সাথে লড়াই করো!\n\nযার পোর্টফোলিও বেশি বাড়বে — সে পুরো ব্যাংক নেবে 🏆\n\n$SLUG-এ বাজি • স্বয়ংক্রিয় পেমেন্ট • সত্যিকারের দাম',
    open_game:    '⚔️ গেম খুলুন',
    how_to_play:  '📖 কীভাবে খেলবেন',
    no_battles:   'কোনো সক্রিয় যুদ্ধ নেই। প্রথমটি তৈরি করুন!',
    battles_title:'⚔️ <b>সক্রিয় যুদ্ধ:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
    guide: `🐌 <b>SLUG Battle কীভাবে খেলবেন</b>

SLUG Battle হলো বাস্তব পোর্টফোলিওতে একটি PvP গেম। আপনি অন্য খেলোয়াড়দের সাথে প্রতিযোগিতা করেন যার টোকেন পোর্টফোলিও যুদ্ধের সময় সবচেয়ে বেশি বাড়বে।

━━━━━━━━━━━━━━━━━━
🔑 <b>ধাপ ১ — ওয়ালেট সংযুক্ত করুন</b>

উপরের ডানদিকে <b>"সংযুক্ত করুন"</b> বাটনে চাপুন এবং আপনার Axiome ঠিকানা লিখুন (<code>axm1...</code>).
আপনি শুধুমাত্র ওয়ালেটে <b>পড়ার</b> অ্যাক্সেস দিচ্ছেন।

━━━━━━━━━━━━━━━━━━
⚔️ <b>ধাপ ২ — যুদ্ধ তৈরি বা যোগ দিন</b>

<b>যুদ্ধ তৈরি:</b>
• বাজি বেছে নিন (২০ SLUG থেকে)
• সময়কাল বেছে নিন (১০ মিনিট — ৭ দিন)
• খেলোয়াড়ের সংখ্যা বেছে নিন (২–২০)
• <b>"যুদ্ধ তৈরি করুন"</b> চাপুন এবং বাজি দিন

<b>অন্যের যুদ্ধে যোগ দিন:</b>
• তালিকায় একটি খোলা যুদ্ধ খুঁজুন
• <b>"যুদ্ধে যোগ দিন"</b> চাপুন এবং বাজি দিন

━━━━━━━━━━━━━━━━━━
💳 <b>ধাপ ৩ — বাজি কীভাবে দেবেন</b>

1. <b>Axiome Wallet</b> খুলুন → <b>Connect</b> বিভাগ (উপরে সকেট আইকন)
2. গেম থেকে স্ট্রিং কপি করুন (📋 বাটন)
3. ফিল্ডে পেস্ট করুন এবং Connect চাপুন, পাঠানো নিশ্চিত করুন
4. হয়ে গেছে — গেম স্বয়ংক্রিয়ভাবে আপনার পেমেন্ট নিশ্চিত করবে

⏱ ৬০ মিনিটের মধ্যে খেলোয়াড় না হলে যুদ্ধ বাতিল হয়। বাজি ফেরত দেওয়া হয়।

━━━━━━━━━━━━━━━━━━
🏁 <b>ধাপ ৪ — যুদ্ধ চলছে</b>

সব খেলোয়াড় পেমেন্ট করলে — যুদ্ধ শুরু হয়। গেম শুরুর সময় আপনার ওয়ালেটের সব টোকেনের দাম লক করে।

<b>"রেস দেখুন"</b> চাপুন এবং রিয়েল টাইমে কে এগিয়ে আছে দেখুন।

━━━━━━━━━━━━━━━━━━
🧮 <b>ধাপ ৫ — বিজয়ী কীভাবে নির্ধারিত হয়</b>

গেম পোর্টফোলিওর প্রতিটি টোকেনের <b>গড় % মূল্য পরিবর্তন</b> হিসাব করে (AXM ছাড়া)।

টোকেনের পরিমাণ গুরুত্বপূর্ণ নয় — মূল্যের গতিবিধি গুরুত্বপূর্ণ।

━━━━━━━━━━━━━━━━━━
💰 <b>ধাপ ৬ — পেমেন্ট</b>

বিজয়ী স্বয়ংক্রিয়ভাবে ওয়ালেটে <b>পুরস্কার ব্যাংকের ৯০%</b> পায়।
৫% প্ল্যাটফর্ম কমিশন, ৫% রেফারেল পেমেন্ট।

━━━━━━━━━━━━━━━━━━
👥 <b>রেফারেল প্রোগ্রাম</b>

বন্ধুদের আমন্ত্রণ জানান এবং তাদের প্রতিটি বাজি থেকে স্বয়ংক্রিয়ভাবে উপার্জন করুন:
• স্তর ১ — ৩% (সরাসরি রেফারেল)
• স্তর ২ — ১.৫%
• স্তর ৩ — ০.৫%

👥 Refs ট্যাবে আপনার রেফারেল লিঙ্ক কপি করুন।

━━━━━━━━━━━━━━━━━━
💡 <b>টিপস</b>

• পোর্টফোলিওতে যত বেশি বিভিন্ন টোকেন — ফলাফল তত স্থিতিশীল
• এক্সচেঞ্জে টোকেন কিনে পোর্টফোলিওর বৃদ্ধিতে প্রভাব ফেলতে পারো!`,
  },
};

async function getUserLang(tgUserId) {
  const r = await q('SELECT lang FROM user_settings WHERE tg_user_id=$1', [String(tgUserId)]);
  return r.rows[0]?.lang || null;
}

async function getUserLangByWallet(wallet) {
  if (!wallet) return 'ru';
  const r = await q('SELECT lang FROM user_settings WHERE wallet=$1', [wallet]);
  return r.rows[0]?.lang || 'ru';
}

const MSG = {
  stake_accepted: {
    ru: (id, paid, max) => `✅ Ставка принята! Битва #${id}. Оплатили: ${paid}/${max}`,
    en: (id, paid, max) => `✅ Stake accepted! Battle #${id}. Paid: ${paid}/${max}`,
    de: (id, paid, max) => `✅ Einsatz angenommen! Kampf #${id}. Bezahlt: ${paid}/${max}`,
    bg: (id, paid, max) => `✅ Залогът е приет! Битка #${id}. Платили: ${paid}/${max}`,
    uk: (id, paid, max) => `✅ Ставку прийнято! Битва #${id}. Оплатили: ${paid}/${max}`,
    kz: (id, paid, max) => `✅ Ставка қабылданды! Шайқас #${id}. Төледі: ${paid}/${max}`,
    fr: (id, paid, max) => `✅ Mise acceptée! Combat #${id}. Payé: ${paid}/${max}`,
    bn: (id, paid, max) => `✅ বাজি গৃহীত! যুদ্ধ #${id}. পরিশোধ: ${paid}/${max}`,
  },
  battle_started: {
    ru: (id, bank, prize) => `🚀 Битва #${id} началась!\n\nБанк: ${bank.toLocaleString()} SLUG\nПриз: ${prize.toLocaleString()} SLUG\n\nЧей портфель вырастет больше — забирает всё!`,
    en: (id, bank, prize) => `🚀 Battle #${id} started!\n\nPool: ${bank.toLocaleString()} SLUG\nPrize: ${prize.toLocaleString()} SLUG\n\nWhose portfolio grows more — takes it all!`,
    de: (id, bank, prize) => `🚀 Kampf #${id} hat begonnen!\n\nPool: ${bank.toLocaleString()} SLUG\nPreis: ${prize.toLocaleString()} SLUG\n\nWessen Portfolio mehr wächst — gewinnt alles!`,
    bg: (id, bank, prize) => `🚀 Битка #${id} започна!\n\nБанк: ${bank.toLocaleString()} SLUG\nНаграда: ${prize.toLocaleString()} SLUG\n\nЧието портфолио нарасне повече — взима всичко!`,
    uk: (id, bank, prize) => `🚀 Битва #${id} почалась!\n\nБанк: ${bank.toLocaleString()} SLUG\nПриз: ${prize.toLocaleString()} SLUG\n\nЧий портфель виросте більше — забирає все!`,
    kz: (id, bank, prize) => `🚀 Шайқас #${id} басталды!\n\nБанк: ${bank.toLocaleString()} SLUG\nСыйлық: ${prize.toLocaleString()} SLUG\n\nКімнің портфелі көп өссе — барлығын алады!`,
    fr: (id, bank, prize) => `🚀 Combat #${id} commencé!\n\nCagnotte: ${bank.toLocaleString()} SLUG\nPrix: ${prize.toLocaleString()} SLUG\n\nCelui dont le portefeuille croît le plus — remporte tout!`,
    bn: (id, bank, prize) => `🚀 যুদ্ধ #${id} শুরু!\n\nব্যাংক: ${bank.toLocaleString()} SLUG\nপুরস্কার: ${prize.toLocaleString()} SLUG\n\nযার পোর্টফোলিও বেশি বাড়বে — সব নেবে!`,
  },
  winner_ok: {
    ru: (id, prize, tx) => `🏆 <b>Поздравляем! Ты победил!</b>\n\nБитва: <b>#${id}</b>\nПриз: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 Токены уже отправлены на твой кошелёк!\nТранзакция: <code>${tx}</code>`,
    en: (id, prize, tx) => `🏆 <b>Congratulations! You won!</b>\n\nBattle: <b>#${id}</b>\nPrize: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 Tokens sent to your wallet!\nTransaction: <code>${tx}</code>`,
    de: (id, prize, tx) => `🏆 <b>Glückwunsch! Du hast gewonnen!</b>\n\nKampf: <b>#${id}</b>\nPreis: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 Tokens wurden an dein Wallet gesendet!\nTransaktion: <code>${tx}</code>`,
    bg: (id, prize, tx) => `🏆 <b>Поздравления! Победи!</b>\n\nБитка: <b>#${id}</b>\nНаграда: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 Токените са изпратени на портфейла ти!\nТранзакция: <code>${tx}</code>`,
    uk: (id, prize, tx) => `🏆 <b>Вітаємо! Ти переміг!</b>\n\nБитва: <b>#${id}</b>\nПриз: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 Токени відправлені на твій гаманець!\nТранзакція: <code>${tx}</code>`,
    kz: (id, prize, tx) => `🏆 <b>Құттықтаймыз! Жеңдің!</b>\n\nШайқас: <b>#${id}</b>\nСыйлық: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 Токендер әмиянға жіберілді!\nТранзакция: <code>${tx}</code>`,
    fr: (id, prize, tx) => `🏆 <b>Félicitations! Tu as gagné!</b>\n\nCombat: <b>#${id}</b>\nPrix: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 Tokens envoyés à ton portefeuille!\nTransaction: <code>${tx}</code>`,
    bn: (id, prize, tx) => `🏆 <b>অভিনন্দন! তুমি জিতেছ!</b>\n\nযুদ্ধ: <b>#${id}</b>\nপুরস্কার: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 টোকেন ওয়ালেটে পাঠানো হয়েছে!\nট্রানজেকশন: <code>${tx}</code>`,
  },
  winner_pending: {
    ru: (id, prize) => `🏆 <b>Ты победил!</b>\n\nБитва: <b>#${id}</b>\nПриз: <b>${prize.toLocaleString()} SLUG</b>\n\nВыплата обрабатывается, скоро получишь токены.`,
    en: (id, prize) => `🏆 <b>You won!</b>\n\nBattle: <b>#${id}</b>\nPrize: <b>${prize.toLocaleString()} SLUG</b>\n\nPayout is being processed, you will receive tokens soon.`,
    de: (id, prize) => `🏆 <b>Du hast gewonnen!</b>\n\nKampf: <b>#${id}</b>\nPreis: <b>${prize.toLocaleString()} SLUG</b>\n\nAuszahlung wird verarbeitet, du erhältst bald Tokens.`,
    bg: (id, prize) => `🏆 <b>Победи!</b>\n\nБитка: <b>#${id}</b>\nНаграда: <b>${prize.toLocaleString()} SLUG</b>\n\nПлащането се обработва, скоро ще получиш токените.`,
    uk: (id, prize) => `🏆 <b>Ти переміг!</b>\n\nБитва: <b>#${id}</b>\nПриз: <b>${prize.toLocaleString()} SLUG</b>\n\nВиплата обробляється, незабаром отримаєш токени.`,
    kz: (id, prize) => `🏆 <b>Жеңдің!</b>\n\nШайқас: <b>#${id}</b>\nСыйлық: <b>${prize.toLocaleString()} SLUG</b>\n\nТөлем өңделуде, жақында токендерді аласың.`,
    fr: (id, prize) => `🏆 <b>Tu as gagné!</b>\n\nCombat: <b>#${id}</b>\nPrix: <b>${prize.toLocaleString()} SLUG</b>\n\nLe paiement est en cours, tu recevras les tokens bientôt.`,
    bn: (id, prize) => `🏆 <b>তুমি জিতেছ!</b>\n\nযুদ্ধ: <b>#${id}</b>\nপুরস্কার: <b>${prize.toLocaleString()} SLUG</b>\n\nপেমেন্ট প্রক্রিয়া হচ্ছে, শীঘ্রই টোকেন পাবে।`,
  },
  loser: {
    ru: (id, winner, growth) => `Битва #${id} завершена. Победитель: ${winner.slice(0,8)}... Твой результат: ${growth.toFixed(2)}%`,
    en: (id, winner, growth) => `Battle #${id} ended. Winner: ${winner.slice(0,8)}... Your result: ${growth.toFixed(2)}%`,
    de: (id, winner, growth) => `Kampf #${id} beendet. Gewinner: ${winner.slice(0,8)}... Dein Ergebnis: ${growth.toFixed(2)}%`,
    bg: (id, winner, growth) => `Битка #${id} завърши. Победител: ${winner.slice(0,8)}... Твой резултат: ${growth.toFixed(2)}%`,
    uk: (id, winner, growth) => `Битва #${id} завершена. Переможець: ${winner.slice(0,8)}... Твій результат: ${growth.toFixed(2)}%`,
    kz: (id, winner, growth) => `Шайқас #${id} аяқталды. Жеңімпаз: ${winner.slice(0,8)}... Нәтижең: ${growth.toFixed(2)}%`,
    fr: (id, winner, growth) => `Combat #${id} terminé. Gagnant: ${winner.slice(0,8)}... Ton résultat: ${growth.toFixed(2)}%`,
    bn: (id, winner, growth) => `যুদ্ধ #${id} শেষ। বিজয়ী: ${winner.slice(0,8)}... তোমার ফলাফল: ${growth.toFixed(2)}%`,
  },
  warn_5min: {
    ru: (id) => `⏰ <b>Битва #${id} заканчивается через 5 минут!</b>\n\nПроверь гонку — кто лидирует?`,
    en: (id) => `⏰ <b>Battle #${id} ends in 5 minutes!</b>\n\nCheck the race — who's leading?`,
    de: (id) => `⏰ <b>Kampf #${id} endet in 5 Minuten!</b>\n\nSchau dir das Rennen an — wer führt?`,
    bg: (id) => `⏰ <b>Битка #${id} приключва след 5 минути!</b>\n\nПровери надпреварата — кой води?`,
    uk: (id) => `⏰ <b>Битва #${id} закінчується через 5 хвилин!</b>\n\nПеревір гонку — хто лідирує?`,
    kz: (id) => `⏰ <b>Шайқас #${id} 5 минуттан кейін аяқталады!</b>\n\nЖарысты тексер — кім алда?`,
    fr: (id) => `⏰ <b>Combat #${id} se termine dans 5 minutes!</b>\n\nVérifie la course — qui est en tête?`,
    bn: (id) => `⏰ <b>যুদ্ধ #${id} ৫ মিনিটে শেষ হবে!</b>\n\nরেস চেক করো — কে এগিয়ে?`,
  },
  watch_race: { ru: '🐌 Смотреть гонку', en: '🐌 Watch race', de: '🐌 Rennen ansehen', bg: '🐌 Гледай надпреварата', uk: '🐌 Дивитись гонку', kz: '🐌 Жарысты қарау', fr: '🐌 Regarder la course', bn: '🐌 রেস দেখুন' },
  refund: {
    ru: (id, stake) => `↩️ <b>Битва #${id} отменена</b>\n\nНикто не присоединился за 60 минут.\nСтавка <b>${stake} SLUG</b> возвращена на твой кошелёк автоматически.`,
    en: (id, stake) => `↩️ <b>Battle #${id} cancelled</b>\n\nNobody joined within 60 minutes.\nStake <b>${stake} SLUG</b> returned to your wallet automatically.`,
    de: (id, stake) => `↩️ <b>Kampf #${id} abgebrochen</b>\n\nNiemand ist innerhalb von 60 Minuten beigetreten.\nEinsatz <b>${stake} SLUG</b> wurde automatisch zurückgegeben.`,
    bg: (id, stake) => `↩️ <b>Битка #${id} отменена</b>\n\nНикой не се присъедини за 60 минути.\nЗалогът <b>${stake} SLUG</b> е върнат автоматично на портфейла ти.`,
    uk: (id, stake) => `↩️ <b>Битва #${id} скасована</b>\n\nНіхто не приєднався за 60 хвилин.\nСтавка <b>${stake} SLUG</b> повернута на твій гаманець автоматично.`,
    kz: (id, stake) => `↩️ <b>Шайқас #${id} тоқтатылды</b>\n\n60 минут ішінде ешкім қосылмады.\nСтавка <b>${stake} SLUG</b> әмиянға автоматты түрде қайтарылды.`,
    fr: (id, stake) => `↩️ <b>Combat #${id} annulé</b>\n\nPersonne n'a rejoint en 60 minutes.\nMise <b>${stake} SLUG</b> remboursée automatiquement sur ton portefeuille.`,
    bn: (id, stake) => `↩️ <b>যুদ্ধ #${id} বাতিল</b>\n\n৬০ মিনিটে কেউ যোগ দেয়নি।\nবাজি <b>${stake} SLUG</b> স্বয়ংক্রিয়ভাবে ওয়ালেটে ফেরত দেওয়া হয়েছে।`,
  },
  cancel_empty: {
    ru: (id) => `❌ <b>Битва #${id} отменена</b>\n\nНикто не вошёл за 60 минут.`,
    en: (id) => `❌ <b>Battle #${id} cancelled</b>\n\nNobody joined within 60 minutes.`,
    de: (id) => `❌ <b>Kampf #${id} abgebrochen</b>\n\nNiemand ist innerhalb von 60 Minuten beigetreten.`,
    bg: (id) => `❌ <b>Битка #${id} отменена</b>\n\nНикой не влезе за 60 минути.`,
    uk: (id) => `❌ <b>Битва #${id} скасована</b>\n\nНіхто не ввійшов за 60 хвилин.`,
    kz: (id) => `❌ <b>Шайқас #${id} тоқтатылды</b>\n\n60 минут ішінде ешкім кірмеді.`,
    fr: (id) => `❌ <b>Combat #${id} annulé</b>\n\nPersonne n'a rejoint dans 60 minutes.`,
    bn: (id) => `❌ <b>যুদ্ধ #${id} বাতিল</b>\n\n৬০ মিনিটে কেউ যোগ দেয়নি।`,
  },
};


async function setUserLang(tgUserId, lang) {
  await q(`INSERT INTO user_settings (tg_user_id, lang) VALUES ($1, $2)
    ON CONFLICT (tg_user_id) DO UPDATE SET lang=$2, updated_at=EXTRACT(EPOCH FROM NOW())`,
    [String(tgUserId), lang]);
}

app.post(`/webhook/${CONFIG.BOT_TOKEN}`, async (req, res) => {
  res.sendStatus(200);
  const update   = req.body;
  const msg      = update?.message;
  const callback = update?.callback_query;

  // ── Обработка callback (выбор языка) ──────────────────────
  if (callback) {
    const chatId = callback.message.chat.id;
    const userId = callback.from.id;
    const data   = callback.data;

    if (['lang_ru','lang_en','lang_de','lang_bg','lang_uk','lang_kz','lang_fr','lang_bn'].includes(data)) {
      const lang = data.replace('lang_', '');
      await setUserLang(userId, lang);
      const t = I18N[lang];

      await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: callback.id }),
      });

      // 1. Сначала отправляем инструкцию
      await sendTg(chatId, t.guide, { parse_mode: 'HTML' });

      // 2. Затем welcome с кнопкой открыть игру
      await sendTg(chatId, t.welcome, {
        reply_markup: {
          inline_keyboard: [[
            { text: t.open_game, web_app: { url: CONFIG.FRONTEND_URL + '?lang=' + lang } },
          ]]
        }
      });
    }

    // Подписка на уведомления
    if (data === 'notify_on' || data === 'notify_off') {
      const val = data === 'notify_on' ? 1 : 0;
      const lang = await getUserLang(userId) || 'ru';
      await q(`INSERT INTO user_settings (tg_user_id, notify_new_battle) VALUES ($1, $2)
        ON CONFLICT (tg_user_id) DO UPDATE SET notify_new_battle=$2`,
        [String(userId), val]
      );
      await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: callback.id }),
      });
      const txt = lang === 'ru'
        ? (val ? '✅ Уведомления включены! Будешь получать сообщение когда создаётся новая битва.' : '❌ Уведомления выключены.')
        : (val ? '✅ Notifications enabled! You will get a message when a new battle is created.' : '❌ Notifications disabled.');
      await sendTg(chatId, txt);
      return;
    }

    // Кнопка "Как играть" в любой момент
    if (['how_to_play_ru','how_to_play_en','how_to_play_de','how_to_play_bg','how_to_play_uk','how_to_play_kz','how_to_play_fr','how_to_play_bn'].includes(data)) {
      const lang = data.includes('_ru') ? 'ru' : data.includes('_de') ? 'de' : data.includes('_bg') ? 'bg' : data.includes('_uk') ? 'uk' : data.includes('_kz') ? 'kz' : data.includes('_fr') ? 'fr' : data.includes('_bn') ? 'bn' : 'en';
      const t = I18N[lang];
      await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: callback.id }),
      });
      await sendTg(chatId, t.guide, { parse_mode: 'HTML' });
    }
    return;
  }

  if (!msg) return;
  const chatId = msg.chat.id;
  const userId = msg.from?.id;

  if (msg.text?.startsWith('/start')) {
    const lang = await getUserLang(userId);

    // Проверяем реферальный параметр: /start ref_axm1...
    const parts   = msg.text.split(' ');
    const refParam = parts[1] || '';
    if (refParam.startsWith('ref_')) {
      const refWallet = refParam.slice(4);
      // Проверяем что реферер не сам себя приглашает
      const selfCheck = await q('SELECT wallet FROM user_settings WHERE tg_user_id=$1', [String(userId)]);
      const ownWallet = selfCheck.rows[0]?.wallet;
      if (refWallet.startsWith('axm1') && refWallet !== 'unknown' && refWallet !== ownWallet) {
        // Сохраняем реферера — обновим когда пользователь подключит кошелёк
        await q(`INSERT INTO user_settings (tg_user_id, referred_by)
          VALUES ($1, $2)
          ON CONFLICT (tg_user_id) DO UPDATE SET referred_by=COALESCE(user_settings.referred_by, $2)`,
          [String(userId), refWallet]
        );
        console.log(`[ref] User ${userId} referred by ${refWallet}`);
      }
    }

    if (!lang) {
      await sendTg(chatId, I18N.ru.lang_select, {
        reply_markup: {
          inline_keyboard: [
            [
              { text: '🇷🇺 Русский', callback_data: 'lang_ru' },
              { text: '🇬🇧 English', callback_data: 'lang_en' },
            ],
            [
              { text: '🇩🇪 Deutsch', callback_data: 'lang_de' },
              { text: '🇧🇬 Български', callback_data: 'lang_bg' },
            ],
            [
              { text: '🇺🇦 Українська', callback_data: 'lang_uk' },
              { text: '🇰🇿 Қазақша', callback_data: 'lang_kz' },
            ],
            [
              { text: '🇫🇷 Français', callback_data: 'lang_fr' },
              { text: '🇧🇩 বাংলা', callback_data: 'lang_bn' },
            ],
          ]
        }
      });
    } else {
      const t = I18N[lang] || I18N.ru;
      await sendTg(chatId, t.welcome, {
        reply_markup: {
          inline_keyboard: [
            [{ text: t.open_game, web_app: { url: CONFIG.FRONTEND_URL + '?lang=' + lang } }],
            [{ text: t.how_to_play, callback_data: 'how_to_play_' + lang }],
            [{ text: lang==='ru' ? '🔔 Уведомления о битвах' : '🔔 Battle notifications', callback_data: lang==='ru' ? 'notify_on' : 'notify_on' }],
          ]
        }
      });
    }
  }

  if (msg.text === '/lang') {
    await sendTg(chatId, I18N.ru.lang_select, {
      reply_markup: {
        inline_keyboard: [
          [
            { text: '🇷🇺 Русский', callback_data: 'lang_ru' },
            { text: '🇬🇧 English', callback_data: 'lang_en' },
          ],
          [
            { text: '🇩🇪 Deutsch', callback_data: 'lang_de' },
            { text: '🇧🇬 Български', callback_data: 'lang_bg' },
          ],
          [
            { text: '🇺🇦 Українська', callback_data: 'lang_uk' },
            { text: '🇰🇿 Қазақша', callback_data: 'lang_kz' },
          ],
          [
            { text: '🇫🇷 Français', callback_data: 'lang_fr' },
            { text: '🇧🇩 বাংলা', callback_data: 'lang_bn' },
          ],
        ]
      }
    });
  }

  if (msg.text === '/notify') {
    const lang = await getUserLang(userId) || 'ru';
    const cur = (await q('SELECT notify_new_battle FROM user_settings WHERE tg_user_id=$1', [String(userId)]).catch(() => ({ rows: [] }))).rows[0];
    const isOn = cur?.notify_new_battle === 1;
    const text = lang === 'ru'
      ? `🔔 Уведомления о новых битвах: ${isOn ? '✅ включены' : '❌ выключены'}`
      : `🔔 New battle notifications: ${isOn ? '✅ enabled' : '❌ disabled'}`;
    await sendTg(chatId, text, {
      reply_markup: {
        inline_keyboard: [[
          { text: lang==='ru' ? (isOn ? '❌ Выключить' : '✅ Включить') : (isOn ? '❌ Disable' : '✅ Enable'),
            callback_data: isOn ? 'notify_off' : 'notify_on' }
        ]]
      }
    });
  }

  if (msg.text === '/battles') {
    const lang = await getUserLang(userId) || 'ru';
    const t    = I18N[lang];
    const battles = await q("SELECT * FROM battles WHERE status IN ('open','live') ORDER BY created_at DESC LIMIT 5");
    if (!battles.rows.length) { await sendTg(chatId, t.no_battles); return; }
    let txt = t.battles_title;
    for (const b of battles.rows) {
      const cnt = await q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=1', [b.id]);
      txt += `#${b.id} | ${b.stake} SLUG | ${cnt.rows[0].c}/${b.max_players} | ${b.status.toUpperCase()}\n`;
    }
    await sendTg(chatId, txt);
  }
});

setInterval(async () => {
  try {
  const now = Math.floor(Date.now() / 1000);

  // 0. Уведомление за 5 минут до конца битвы
  const WARN_BEFORE = 5 * 60; // 5 минут
  const soonBattles = await q(
    `SELECT b.*, array_agg(p.tg_user_id) as tg_ids
     FROM battles b
     JOIN participants p ON p.battle_id = b.id AND p.paid = 1
     LEFT JOIN user_settings us ON us.wallet = p.wallet
     WHERE b.status = 'live'
       AND b.ends_at BETWEEN $1 AND $2
       AND b.notified_5min IS NULL
     GROUP BY b.id`,
    [now + WARN_BEFORE - 15, now + WARN_BEFORE + 15]
  );

  for (const b of soonBattles.rows) {
    try {
      // Отмечаем что уведомление уже отправлено
      await q("UPDATE battles SET notified_5min=1 WHERE id=$1", [b.id]);

      // Получаем всех участников с tg_user_id
      const parts = await q(
        `SELECT p.wallet, us.tg_user_id FROM participants p
         LEFT JOIN user_settings us ON us.wallet = p.wallet
         WHERE p.battle_id = $1 AND p.paid = 1`,
        [b.id]
      );

      for (const p of parts.rows) {
        if (!p.tg_user_id) continue;
        const _l5 = await getUserLang(p.tg_user_id) || 'ru';
        await sendTg(p.tg_user_id,
          (MSG.warn_5min[_l5] || MSG.warn_5min['en'])(b.id),
          { reply_markup: { inline_keyboard: [[{ text: MSG.watch_race[_l5] || MSG.watch_race['en'], web_app: { url: `${CONFIG.FRONTEND_URL}?lang=${_l5}` } }]] } }
        );
      }
      console.log(`[notify] 5min warning sent for battle ${b.id}`);
    } catch (e) {
      console.error(`[notify] Error ${b.id}:`, e.message);
    }
  }

  // 1. Финализируем завершённые live битвы
  const exp = await q("SELECT id FROM battles WHERE status='live' AND ends_at<=$1", [now]);
  for (const b of exp.rows) {
    try { await fetch(`http://localhost:${CONFIG.PORT}/battles/${b.id}/finalize`, { method: 'POST' }); }
    catch (e) { console.error('[auto-finalize]', e.message); }
  }

  // 2. Автоотмена: open битвы старше 60 минут — отменяем и возвращаем ставки
  const CANCEL_TIMEOUT_SEC = 60 * 60; // 60 минут
  const stale = await q(
    "SELECT * FROM battles WHERE status='open' AND created_at <= $1",
    [now - CANCEL_TIMEOUT_SEC]
  );

  for (const b of stale.rows) {
    try {
      console.log(`[auto-cancel] Battle ${b.id} open for 2h+ — cancelling`);

      // Сначала отменяем чтобы никто новый не вошёл
      await q("UPDATE battles SET status='cancelled' WHERE id=$1", [b.id]);
      await q('DELETE FROM pending_payments WHERE battle_id=$1', [b.id]);

      // Возвращаем ставки всем кто уже оплатил
      const paid = await q(
        'SELECT p.wallet, us.tg_user_id FROM participants p LEFT JOIN user_settings us ON us.wallet=p.wallet WHERE p.battle_id=$1 AND p.paid=1',
        [b.id]
      );

      for (const p of paid.rows) {
        const result = await autoPayout(`REFUND:${b.id}`, p.wallet, parseInt(b.stake));
        if (result.ok) {
          console.log(`[auto-cancel] Refunded ${b.stake} SLUG → ${p.wallet} tx=${result.txHash}`);
        } else {
          console.error(`[auto-cancel] Refund failed ${p.wallet}: ${result.error}`);
        }

        if (p.tg_user_id) {
          const _lR = await getUserLang(p.tg_user_id) || 'ru';
          await sendTg(p.tg_user_id, (MSG.refund[_lR] || MSG.refund['en'])(b.id, b.stake));
        }
      }

      // Если никто не платил — просто уведомляем создателя
      if (paid.rows.length === 0) {
        const creator = await q('SELECT tg_user_id FROM user_settings WHERE wallet=$1', [b.creator]);
        const tgId = creator.rows[0]?.tg_user_id;
        if (tgId) {
          const _lC = await getUserLang(tgId) || 'ru';
          await sendTg(tgId, (MSG.cancel_empty[_lC] || MSG.cancel_empty['en'])(b.id));
        }
      }

    } catch (e) {
      console.error(`[auto-cancel] Error ${b.id}:`, e.message);
    }
  }

  } catch(e) { console.error('[autofinalize] error:', e.message); }
}, 30 * 1000);

async function setWebhook(baseUrl) {
  const res  = await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/setWebhook?url=${encodeURIComponent(baseUrl+'/webhook/'+CONFIG.BOT_TOKEN)}`);
  console.log('[tg] Webhook:', (await res.json()).ok ? 'OK' : 'failed');
}

async function start() {
  await initDB();
  await loadTokenContracts(); // загружаем контракты токенов из пулов
// Мониторинг памяти каждые 5 минут
setInterval(() => {
  const mem = process.memoryUsage();
  const mb  = (b) => Math.round(b / 1024 / 1024);
  console.log(`[memory] heap: ${mb(mem.heapUsed)}/${mb(mem.heapTotal)}MB rss: ${mb(mem.rss)}MB cache: ${_portfolioCache.size} rateLimit: ${_rateLimitMap.size}`);
}, 5 * 60 * 1000);

  app.listen(CONFIG.PORT, async () => {
    console.log(`🐌 SLUG Battle Server v3 (PostgreSQL) on port ${CONFIG.PORT}`);
    if (process.env.RAILWAY_STATIC_URL) await setWebhook(`https://${process.env.RAILWAY_STATIC_URL}`);
  });
  // Обновляем контракты раз в час (если появятся новые токены)
  setInterval(loadTokenContracts, 60 * 60 * 1000);
}

start().catch(e => { console.error('Failed to start:', e.message); process.exit(1); });
