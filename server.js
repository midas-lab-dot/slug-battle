// ============================================================
// CRYPTO PORTFOLIO BATTLE — Backend Server v3 (PostgreSQL)
// ============================================================

const express    = require('express');
const { Pool }   = require('pg');
const cors       = require('cors');
const crypto     = require('crypto');
const { DirectSecp256k1HdWallet, DirectSecp256k1Wallet } = require('@cosmjs/proto-signing');
const { SigningCosmWasmClient } = require('@cosmjs/cosmwasm-stargate');
const { GasPrice } = require('@cosmjs/stargate');
const { HttpBatchClient, Tendermint37Client } = require('@cosmjs/tendermint-rpc');

const app = express();
app.use(express.json());
app.use(cors());

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
  MIN_STAKE:       100,
  PAYMENT_TIMEOUT_SEC: 900,
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
});

async function q(sql, params = []) {
  const client = await pool.connect();
  try { return await client.query(sql, params); }
  finally { client.release(); }
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
  await q(`CREATE TABLE IF NOT EXISTS user_settings (
    tg_user_id TEXT PRIMARY KEY,
    lang       TEXT DEFAULT 'ru',
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

  console.log('[db] PostgreSQL ready');
}

// ── Axiome API ────────────────────────────────────────────────
async function getCW20Balance(wallet, contract) {
  try {
    const qb  = Buffer.from(JSON.stringify({ balance: { address: wallet } })).toString('base64');
    const res = await fetch(`${CONFIG.AXIOME_REST}/cosmwasm/wasm/v1/contract/${contract}/smart/${qb}`, { signal: AbortSignal.timeout(6000) });
    return parseInt((await res.json())?.data?.balance || '0');
  } catch { return 0; }
}

async function getNativeBalance(wallet) {
  try {
    const res  = await fetch(`${CONFIG.AXIOME_REST}/cosmos/bank/v1beta1/balances/${wallet}`, { signal: AbortSignal.timeout(6000) });
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

async function getFullPortfolio(wallet) {
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
async function getTokenPrices() {
  const prices = { AXM: 1 }; // AXM = 1 AXM

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

  return prices;
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
}


// ── Получаем цепочку рефералов для кошелька (до 3 уровней) ──
async function getReferralChain(wallet) {
  const chain = [];
  let current = wallet;

  for (let level = 1; level <= 3; level++) {
    // Ищем реферера через tg_user_id (привязка только через Telegram)
    const r = await q(
      `SELECT us2.wallet as ref_wallet
       FROM user_settings us1
       JOIN user_settings us2 ON us2.tg_user_id = us1.referred_by
       WHERE us1.wallet = $1
       AND us1.referred_by IS NOT NULL
       AND us2.wallet IS NOT NULL
       LIMIT 1`,
      [current]
    );
    const ref = r.rows[0]?.ref_wallet;
    if (!ref || ref === current) break;
    chain.push({ level, wallet: ref });
    current = ref;
  }

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
        `🏆 <b>Поздравляем! Ты победил!</b>\n\nБитва: <b>#${battle.id}</b>\nПриз: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 Токены уже отправлены на твой кошелёк!\nТранзакция: <code>${result.txHash}</code>`
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
        `🏆 <b>Ты победил!</b>\n\nБитва: <b>#${battle.id}</b>\nПриз: <b>${prize.toLocaleString()} SLUG</b>\n\nВыплата обрабатывается, скоро получишь токены.`
      );
    }
  }
}

async function notifyWinnerAuto(battle, winner, prize) {
  const p = (await q('SELECT tg_user_id FROM participants WHERE battle_id=$1 AND wallet=$2', [battle.id, winner])).rows[0];
  if (!p?.tg_user_id) return;
  await sendTg(p.tg_user_id, `🏆 <b>Ты победил!</b>\n\nБитва: <b>#${battle.id}</b>\nПриз: <b>${prize.toLocaleString()} SLUG</b>\n\n💰 Токены уже на твоём кошельке!`);
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
    const url = `${CONFIG.AXIOME_REST}/cosmos/tx/v1beta1/txs?query=wasm.action%3D${actionEnc}%20AND%20wasm.to%3D${walletEnc}&limit=50`;
    const data = await (await fetch(url, { signal: AbortSignal.timeout(8000) })).json();
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
  try { await q('INSERT INTO seen_txs (tx_hash) VALUES ($1)', [txHash]); } catch { return; }

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
  if (p?.tg_user_id) await sendTg(p.tg_user_id, `Ставка принята! Битва #${battleId}. Оплатили: ${paidCount}/${b.max_players}`);

  if (paidCount >= b.max_players && b.status === 'open') {
    const endsAt = Math.floor(Date.now() / 1000) + Math.floor(b.duration_ms / 1000);
    await q("UPDATE battles SET status='live', started_at=$1, ends_at=$2 WHERE id=$3", [Math.floor(Date.now()/1000), endsAt, battleId]);
    console.log(`[watcher] Battle ${battleId} started!`);
    const prize = Math.round(b.stake * b.max_players * (1 - CONFIG.FEE_PERCENT / 100));
    const parts = await q('SELECT tg_user_id FROM participants WHERE battle_id=$1 AND tg_user_id IS NOT NULL', [battleId]);
    for (const part of parts.rows) {
      await sendTg(part.tg_user_id, `🚀 Битва #${battleId} началась!\n\nБанк: ${(b.stake * b.max_players).toLocaleString()} SLUG\nПриз: ${prize.toLocaleString()} SLUG\n\nЧей портфель вырастет больше — забирает всё!`);
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

  const active = pending.filter(p => now - parseInt(p.created_at) < CONFIG.PAYMENT_TIMEOUT_SEC);
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
    const pend = active.find(p =>
      p.wallet === senderWallet &&
      paidAmount >= parseInt(p.amount)
    );
    if (!pend) continue;

    await confirmPayment(pend.battle_id, senderWallet, txHash, paidAmount);
  }
}

setInterval(checkPayments, 5000);

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
    const [total, paid] = await Promise.all([
      q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1', [b.id]),
      q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=1', [b.id]),
    ]);
    return { ...b, participants_count: +total.rows[0].c, paid_count: +paid.rows[0].c };
  }));
  res.json(result);
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

  res.json({ id, qr_string: buildJoinQR(id, stake, memo), memo, stake, message: `Битва #${id} создана!`, payment_timeout_sec: CONFIG.PAYMENT_TIMEOUT_SEC });
});

app.post('/battles/:id/join', requireWallet, async (req, res) => {
  const { tg_user_id, tg_username } = req.body;
  const b = (await q('SELECT * FROM battles WHERE id=$1', [req.params.id])).rows[0];
  if (!b) return res.status(404).json({ error: 'Not found' });
  if (b.status !== 'open') return res.status(400).json({ error: 'Битва не открыта' });

  const paidCnt = +(await q('SELECT COUNT(*) as c FROM participants WHERE battle_id=$1 AND paid=1', [req.params.id])).rows[0].c;
  if (paidCnt >= b.max_players) return res.status(400).json({ error: 'Битва заполнена' });

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

    // Считаем стоимость ТОЛЬКО тех токенов и в том количестве
    // которые были на кошельке в момент входа в битву.
    // Докупленные после старта токены НЕ учитываются.
    const startTokens = start.tokens || {};
    const startUsd    = start.total_axm || start.total_usd || 0; // в AXM

    // Считаем текущую стоимость стартового портфеля по новым ценам
    let curUsd = 0;
    for (const [symbol, data] of Object.entries(startTokens)) {
      const currentPrice = prices[symbol] || 0;
      curUsd += (data.amount || 0) * currentPrice;
    }

    const growth = startUsd > 0 ? ((curUsd - startUsd) / startUsd) * 100 : 0;

    await q('UPDATE participants SET portfolio_end=$1,growth_pct=$2 WHERE battle_id=$3 AND wallet=$4',
      [JSON.stringify({ tokens: startTokens, prices, total_usd: curUsd, ts: Date.now() }), growth, b.id, p.wallet]);
    if (growth > bestGrowth) { bestGrowth = growth; winner = p.wallet; }
  }

  const prize = Math.round(parseInt(b.stake) * parts.length * (1 - CONFIG.FEE_PERCENT / 100));
  const fee   = Math.round(parseInt(b.stake) * parts.length * CONFIG.FEE_PERCENT / 100);

  await q("UPDATE battles SET status='ended',winner=$1 WHERE id=$2", [winner, b.id]);
  console.log(`[finalize] Battle ${b.id} ended. Winner: ${winner} prize=${prize}`);

  if (CONFIG.ESCROW_CONTRACT) {
    try { await callEscrowFinalize(b.id, winner); await notifyWinnerAuto(b, winner, prize); }
    catch (e) { await notifyWinner(b, winner, prize); }
  } else {
    await notifyWinner(b, winner, prize);
  }

  for (const loser of parts.filter(p => p.wallet !== winner)) {
    if (!loser.tg_user_id) continue;
    await sendTg(loser.tg_user_id, `Битва #${b.id} завершена. Победитель: ${winner.slice(0,8)}... Твой результат: ${(parts.find(p=>p.wallet===loser.wallet)?.growth_pct||0).toFixed(2)}%`);
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
    const endPrices   = end.prices   || {};

    for (const [symbol, data] of Object.entries(startTokens)) {
      if (!data.amount || data.amount === 0) continue;
      const priceBefore = startPrices[symbol] || 0;
      const priceAfter  = endPrices[symbol]   || 0;
      const valBefore   = data.amount * priceBefore;
      const valAfter    = data.amount * priceAfter;
      const change      = priceBefore > 0 ? ((priceAfter - priceBefore) / priceBefore) * 100 : 0;

      tokenReport.push({
        symbol,
        amount:       data.amount,
        price_before: priceBefore,
        price_after:  priceAfter,
        val_before:   valBefore,
        val_after:    valAfter,
        change_pct:   parseFloat(change.toFixed(2)),
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
    no_battles:   'Активных битв нет. Создай первую!',
    battles_title:'⚔️ <b>Активные битвы:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
  },
  en: {
    welcome:      '🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\nBuild an Axiome token portfolio and battle other traders!\n\nWhose portfolio grows more — takes the entire prize pool 🏆\n\nStakes in $SLUG • Auto payouts • Real prices',
    open_game:    '⚔️ Open game',
    no_battles:   'No active battles. Create the first one!',
    battles_title:'⚔️ <b>Active battles:</b>\n\n',
    lang_select:  '🌍 Выбери язык / Choose language:',
  },
};

async function getUserLang(tgUserId) {
  const r = await q('SELECT lang FROM user_settings WHERE tg_user_id=$1', [String(tgUserId)]);
  return r.rows[0]?.lang || null;
}

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

    if (data === 'lang_ru' || data === 'lang_en') {
      const lang = data.replace('lang_', '');
      await setUserLang(userId, lang);
      const t = I18N[lang];

      // Редактируем сообщение с выбором языка
      await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: callback.id }),
      });

      await sendTg(chatId, t.welcome, {
        reply_markup: { inline_keyboard: [[{ text: t.open_game, web_app: { url: CONFIG.FRONTEND_URL + '?lang=' + lang } }]] }
      });
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
      if (refWallet.startsWith('axm1') && refWallet !== 'unknown') {
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
          inline_keyboard: [[
            { text: '🇷🇺 Русский', callback_data: 'lang_ru' },
            { text: '🇬🇧 English', callback_data: 'lang_en' },
          ]]
        }
      });
    } else {
      const t = I18N[lang] || I18N.ru;
      await sendTg(chatId, t.welcome, {
        reply_markup: { inline_keyboard: [[{ text: t.open_game, web_app: { url: CONFIG.FRONTEND_URL + '?lang=' + lang } }]] }
      });
    }
  }

  if (msg.text === '/lang') {
    // Смена языка в любой момент
    await sendTg(chatId, I18N.ru.lang_select, {
      reply_markup: {
        inline_keyboard: [[
          { text: '🇷🇺 Русский', callback_data: 'lang_ru' },
          { text: '🇬🇧 English', callback_data: 'lang_en' },
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
  const now = Math.floor(Date.now() / 1000);
  const exp = await q("SELECT id FROM battles WHERE status='live' AND ends_at<=$1", [now]);
  for (const b of exp.rows) {
    try { await fetch(`http://localhost:${CONFIG.PORT}/battles/${b.id}/finalize`, { method: 'POST' }); }
    catch (e) { console.error('[auto-finalize]', e.message); }
  }
}, 5 * 60 * 1000);

async function setWebhook(baseUrl) {
  const res  = await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/setWebhook?url=${encodeURIComponent(baseUrl+'/webhook/'+CONFIG.BOT_TOKEN)}`);
  console.log('[tg] Webhook:', (await res.json()).ok ? 'OK' : 'failed');
}

async function start() {
  await initDB();
  await loadTokenContracts(); // загружаем контракты токенов из пулов
  app.listen(CONFIG.PORT, async () => {
    console.log(`🐌 SLUG Battle Server v3 (PostgreSQL) on port ${CONFIG.PORT}`);
    if (process.env.RAILWAY_STATIC_URL) await setWebhook(`https://${process.env.RAILWAY_STATIC_URL}`);
  });
  // Обновляем контракты раз в час (если появятся новые токены)
  setInterval(loadTokenContracts, 60 * 60 * 1000);
}

start().catch(e => { console.error('Failed to start:', e.message); process.exit(1); });
