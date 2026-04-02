// ============================================================
// CRYPTO PORTFOLIO BATTLE — Backend Server v2
// Node.js + Express + better-sqlite3
// ============================================================

const express  = require('express');
const Database = require('better-sqlite3');
const cors     = require('cors');
const crypto   = require('crypto');

const app = express();
app.use(express.json());
app.use(cors());

// ============================================================
// КОНФИГ
// ============================================================
const CONFIG = {
  BOT_TOKEN:      process.env.BOT_TOKEN      || 'YOUR_BOT_TOKEN',
  ADMIN_KEY:      process.env.ADMIN_KEY       || 'slug-admin-2024',
  FRONTEND_URL:   process.env.FRONTEND_URL    || 'https://slug-token.netlify.app/battle',
  SLUG_CONTRACT:  'axm1nzvd5njkc0gqdezgzzqw00tu056rgxgwaqgq25g9ku0y5au3nwmqxqtr0l',
  PROJECT_WALLET: 'axm1fnhlykzpra0j6xzy0eenpfwn7anp5upm2jxm9k',
  AXIOME_REST:    'https://api-chain.axiomechain.org',
  NETWORK:        'axiome',
  FEE_PERCENT:    2,
  PORT:           process.env.PORT || 3000,

  // ── Escrow контракт ──────────────────────────────────────
  // Адрес задеплоенного battle-escrow контракта на Axiome Chain
  // Заполни после деплоя контракта (см. DEPLOY.md)
  ESCROW_CONTRACT: process.env.ESCROW_CONTRACT || '',

  // Минимальная ставка в SLUG
  MIN_STAKE: 100,

  // Таймаут оплаты ставки — 15 минут
  PAYMENT_TIMEOUT_SEC: 900,

  // Известные токены экосистемы Axiome (добавляй новые по мере появления)
  KNOWN_TOKENS: [
    { contract: 'axm1nzvd5njkc0gqdezgzzqw00tu056rgxgwaqgq25g9ku0y5au3nwmqxqtr0l', symbol: 'SLUG', decimals: 6 },
    // { contract: 'axm1...', symbol: 'PEPE', decimals: 6 },
  ],
};

// ============================================================
// DATABASE
// ============================================================
const db = new Database(process.env.DB_PATH || 'battles.db');
db.pragma('journal_mode = WAL');  // лучше для concurrent reads

db.exec(`
  CREATE TABLE IF NOT EXISTS battles (
    id          TEXT    PRIMARY KEY,
    creator     TEXT    NOT NULL,
    stake       INTEGER NOT NULL,
    max_players INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    status      TEXT    DEFAULT 'open',
    created_at  INTEGER DEFAULT (unixepoch()),
    started_at  INTEGER,
    ends_at     INTEGER,
    winner      TEXT,
    pool_total  INTEGER DEFAULT 0
  );

  -- Участники битвы
  CREATE TABLE IF NOT EXISTS participants (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    battle_id       TEXT    NOT NULL,
    wallet          TEXT    NOT NULL,
    tg_user_id      TEXT,
    tg_username     TEXT,
    joined_at       INTEGER DEFAULT (unixepoch()),
    portfolio_start TEXT,
    portfolio_end   TEXT,
    growth_pct      REAL,
    paid            INTEGER DEFAULT 0,
    UNIQUE(battle_id, wallet)
  );

  -- Ожидающие платежи (ставки)
  -- verified=0 → ждём транзакцию
  -- verified=1 → оплата подтверждена
  -- verified=2 → просрочено (15 мин истекли)
  CREATE TABLE IF NOT EXISTS pending_payments (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    battle_id  TEXT    NOT NULL,
    wallet     TEXT    NOT NULL,
    amount     INTEGER NOT NULL,
    memo       TEXT    NOT NULL  UNIQUE,
    created_at INTEGER DEFAULT (unixepoch()),
    verified   INTEGER DEFAULT 0
  );

  -- Уже обработанные tx_hash — чтобы не засчитывать дважды
  CREATE TABLE IF NOT EXISTS seen_txs (
    tx_hash TEXT    PRIMARY KEY,
    seen_at INTEGER DEFAULT (unixepoch())
  );

  -- Индексы для быстрых запросов
  CREATE INDEX IF NOT EXISTS idx_pending_verified   ON pending_payments(verified);
  CREATE INDEX IF NOT EXISTS idx_participants_paid  ON participants(battle_id, paid);
  CREATE INDEX IF NOT EXISTS idx_battles_status     ON battles(status);
`);

// ============================================================
// AXIOME CHAIN API
// ============================================================

async function getCW20Balance(wallet, contract) {
  try {
    const q   = Buffer.from(JSON.stringify({ balance: { address: wallet } })).toString('base64');
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
    const res  = await fetch(
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

  for (const t of CONFIG.KNOWN_TOKENS) {
    const raw = await getCW20Balance(wallet, t.contract);
    portfolio[t.symbol] = {
      symbol:   t.symbol,
      amount:   raw / Math.pow(10, t.decimals),
      contract: t.contract,
    };
  }

  return portfolio;
}

async function getTokenPrices() {
  const prices = { SLUG: 0.008 };
  try {
    const res  = await fetch(
      'https://api.coingecko.com/api/v3/simple/price?ids=axiome&vs_currencies=usd',
      { signal: AbortSignal.timeout(4000) }
    );
    const data = await res.json();
    prices['AXM'] = data?.axiome?.usd || 0.004;
  } catch {
    prices['AXM'] = 0.004;
  }
  return prices;
}

async function calcPortfolioUsd(portfolio, prices) {
  let total = 0;
  for (const [sym, d] of Object.entries(portfolio)) {
    total += d.amount * (prices[sym] || 0);
  }
  return total;
}

// ============================================================
// PAYMENT WATCHER
// Логика:
// 1. Каждые 5 сек берём pending_payments где verified=0
// 2. Запрашиваем последние 30 входящих CW20 транзакций на PROJECT_WALLET
// 3. Для каждой tx — проверяем memo: BATTLE:{id}:{wallet}
// 4. Если совпадает с pending — подтверждаем оплату
// 5. Если 15 минут прошло — помечаем просроченным и удаляем участника
// ============================================================

async function getRecentIncomingTxs() {
  try {
    // Ищем CW20 transfer транзакции где recipient = PROJECT_WALLET
    const url = [
      `${CONFIG.AXIOME_REST}/cosmos/tx/v1beta1/txs`,
      `?events=wasm._contract_address%3D%27${CONFIG.SLUG_CONTRACT}%27`,
      `&events=wasm.recipient%3D%27${CONFIG.PROJECT_WALLET}%27`,
      `&order_by=ORDER_DESC&limit=30`,
    ].join('');

    const res  = await fetch(url, { signal: AbortSignal.timeout(8000) });
    const data = await res.json();
    return data?.txs || [];
  } catch (e) {
    console.error('[watcher] getRecentIncomingTxs error:', e.message);
    return [];
  }
}

// Парсим memo формата BATTLE:{battleId}:{wallet}
// wallet может содержать двоеточия (axm1...) — берём всё после второго ':'
function parseBattleMemo(memo) {
  if (!memo?.startsWith('BATTLE:')) return null;
  const idx1 = memo.indexOf(':');
  const idx2 = memo.indexOf(':', idx1 + 1);
  if (idx2 === -1) return null;
  const battleId = memo.slice(idx1 + 1, idx2);
  const wallet   = memo.slice(idx2 + 1);
  if (!battleId || !wallet.startsWith('axm1')) return null;
  return { battleId, wallet };
}

// Извлекаем сумму перевода из wasm events транзакции
function extractTransferAmount(tx) {
  try {
    const events = tx.logs?.[0]?.events || [];
    for (const ev of events) {
      if (ev.type !== 'wasm') continue;
      const attrs = {};
      for (const a of ev.attributes) attrs[a.key] = a.value;
      if (attrs.recipient === CONFIG.PROJECT_WALLET && attrs.amount) {
        return parseInt(attrs.amount);
      }
    }
    return null;
  } catch { return null; }
}

async function confirmPayment(battleId, wallet, txHash, paidAmount) {
  // Помечаем tx как обработанную
  try {
    db.prepare('INSERT INTO seen_txs (tx_hash) VALUES (?)').run(txHash);
  } catch {
    return; // уже обработана
  }

  // Подтверждаем оплату
  db.prepare('UPDATE pending_payments SET verified=1 WHERE battle_id=? AND wallet=?')
    .run(battleId, wallet);
  db.prepare('UPDATE participants SET paid=1 WHERE battle_id=? AND wallet=?')
    .run(battleId, wallet);

  console.log(`[watcher] ✅ Payment confirmed: battle=${battleId} wallet=${wallet} tx=${txHash} amount=${paidAmount}`);

  // Уведомляем игрока в TG
  const p = db.prepare('SELECT tg_user_id FROM participants WHERE battle_id=? AND wallet=?')
    .get(battleId, wallet);
  const b = db.prepare('SELECT * FROM battles WHERE id=?').get(battleId);
  if (!b) return;

  const paidCount = db.prepare(
    'SELECT COUNT(*) as c FROM participants WHERE battle_id=? AND paid=1'
  ).get(battleId).c;

  if (p?.tg_user_id) {
    await sendTg(p.tg_user_id,
      `✅ <b>Ставка принята!</b>\n\n` +
      `Битва <b>#${battleId}</b>\n` +
      `Игроков оплатили: ${paidCount}/${b.max_players}\n\n` +
      `Ждём остальных игроков...`
    );
  }

  // Проверяем: все ли оплатили?
  // Если да — стартуем битву (меняем статус на live)
  if (paidCount >= b.max_players && b.status === 'open') {
    const endsAt = Math.floor(Date.now() / 1000) + Math.floor(b.duration_ms / 1000);
    db.prepare("UPDATE battles SET status='live', started_at=unixepoch(), ends_at=? WHERE id=?")
      .run(endsAt, battleId);

    console.log(`[watcher] 🚀 Battle ${battleId} started! Ends: ${new Date(endsAt * 1000).toISOString()}`);

    // Уведомляем ВСЕХ участников о старте
    const allParts = db.prepare(
      'SELECT tg_user_id, wallet FROM participants WHERE battle_id=? AND tg_user_id IS NOT NULL'
    ).all(battleId);

    const prize = Math.round(b.stake * b.max_players * (1 - CONFIG.FEE_PERCENT / 100));
    const durationLabel = formatDuration(b.duration_ms);

    for (const part of allParts) {
      await sendTg(part.tg_user_id,
        `🚀 <b>Битва #${battleId} началась!</b>\n\n` +
        `💰 Банк: <b>${(b.stake * b.max_players).toLocaleString()} SLUG</b>\n` +
        `🏆 Приз победителю: <b>${prize.toLocaleString()} SLUG</b>\n` +
        `⏱ Длительность: <b>${durationLabel}</b>\n\n` +
        `Портфели зафиксированы. Чей вырастет больше — забирает всё!\n` +
        `Результат в: ${new Date(endsAt * 1000).toLocaleString('ru-RU')}`
      );
    }
  }
}

async function checkPayments() {
  const pending = db.prepare(
    'SELECT * FROM pending_payments WHERE verified=0'
  ).all();

  if (!pending.length) return;

  // ── Шаг 1: Проверяем просроченные платежи ──────────────────
  const now = Math.floor(Date.now() / 1000);
  for (const p of pending) {
    const age = now - p.created_at;
    if (age < CONFIG.PAYMENT_TIMEOUT_SEC) continue;

    // Платёж просрочен — удаляем участника из битвы
    db.prepare('UPDATE pending_payments SET verified=2 WHERE id=?').run(p.id);
    db.prepare('DELETE FROM participants WHERE battle_id=? AND wallet=? AND paid=0')
      .run(p.battle_id, p.wallet);

    // Уменьшаем пул битвы
    const b = db.prepare('SELECT * FROM battles WHERE id=?').get(p.battle_id);
    if (b) {
      const newPool = Math.max(0, (b.pool_total || 0) - b.stake);
      db.prepare('UPDATE battles SET pool_total=? WHERE id=?').run(newPool, p.battle_id);
    }

    console.log(`[watcher] ⏰ Payment expired: battle=${p.battle_id} wallet=${p.wallet}`);

    // Уведомляем игрока
    const part = db.prepare(
      'SELECT tg_user_id FROM participants WHERE battle_id=? AND wallet=?'
    ).get(p.battle_id, p.wallet);

    if (part?.tg_user_id) {
      await sendTg(part.tg_user_id,
        `⏰ <b>Время оплаты истекло</b>\n\n` +
        `Ставка для битвы <b>#${p.battle_id}</b> не была получена в течение 15 минут.\n` +
        `Ты исключён из этой битвы.\n\n` +
        `Зайди снова чтобы участвовать.`
      );
    }
  }

  // ── Шаг 2: Ищем транзакции на блокчейне ────────────────────
  const activePending = pending.filter(p => {
    const age = now - p.created_at;
    return age < CONFIG.PAYMENT_TIMEOUT_SEC;
  });

  if (!activePending.length) return;

  const txs = await getRecentIncomingTxs();
  if (!txs.length) return;

  for (const tx of txs) {
    const txHash = tx.txhash;
    if (!txHash) continue;

    // Уже видели эту tx — пропускаем
    if (db.prepare('SELECT 1 FROM seen_txs WHERE tx_hash=?').get(txHash)) continue;

    // Транзакция провалилась — пропускаем
    if (tx.code !== undefined && tx.code !== 0) continue;

    const memo   = tx.body?.memo || '';
    const parsed = parseBattleMemo(memo);
    if (!parsed) continue;

    // Ищем соответствующий pending платёж
    const pend = activePending.find(
      p => p.battle_id === parsed.battleId && p.wallet === parsed.wallet
    );
    if (!pend) continue;

    // Проверяем сумму — должна быть >= ожидаемой
    const paidAmount = extractTransferAmount(tx);
    if (paidAmount !== null && paidAmount < pend.amount) {
      console.warn(
        `[watcher] ⚠️ Underpayment: battle=${parsed.battleId} ` +
        `expected=${pend.amount} got=${paidAmount} tx=${txHash}`
      );
      continue;
    }

    await confirmPayment(parsed.battleId, parsed.wallet, txHash, paidAmount);
  }
}

// Запускаем watcher каждые 5 секунд
setInterval(checkPayments, 5000);
console.log('[watcher] Payment watcher started (5s interval)');

// ============================================================
// TELEGRAM BOT
// ============================================================

async function sendTg(chatId, text, extra = {}) {
  if (!chatId || CONFIG.BOT_TOKEN === 'YOUR_BOT_TOKEN') return;
  try {
    await fetch(`https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/sendMessage`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML', ...extra }),
    });
  } catch (e) { console.error('[tg] send error:', e.message); }
}

// Уведомление победителю с QR для получения приза
async function notifyWinner(battle, winner, prize) {
  const p = db.prepare(
    'SELECT tg_user_id FROM participants WHERE battle_id=? AND wallet=?'
  ).get(battle.id, winner);
  if (!p?.tg_user_id) return;

  // Генерируем QR для выплаты победителю
  // Ты (владелец PROJECT_WALLET) сканируешь этот QR в Axiome Wallet
  const payload = {
    type:          'cosmwasm_execute',
    network:       CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT,
    funds:         [],
    msg: {
      transfer: {
        recipient: winner,
        amount:    String(prize * 1e6),
      }
    },
    memo: `WIN:${battle.id}`,
  };
  const qr = `axiomesign://${Buffer.from(JSON.stringify(payload)).toString('base64')}`;

  await sendTg(p.tg_user_id,
    `🏆 <b>Поздравляем! Ты победил!</b>\n\n` +
    `Битва: <b>#${battle.id}</b>\n` +
    `Приз: <b>${prize.toLocaleString()} SLUG</b>\n\n` +
    `Для получения приза — открой Axiome Wallet → Connect → вставь строку ниже:`
  );
  await sendTg(p.tg_user_id, `<code>${qr}</code>`);
}

// ── Вызов Escrow.Finalize через Axiome REST API ───────────────
// Сервер сам отправляет транзакцию — для этого нужен server_wallet
// с приватным ключом. Ключ задаётся через env ESCROW_SIGNER_MNEMONIC.
// Если ключа нет — используем fallback (QR для ручной подписи).
//
// ВАЖНО: для автоматической подписи используй axmd или cosmjs.
// Здесь генерируем QR который сервер логирует, а ты подписываешь
// через Axiome Wallet — до тех пор пока не настроишь автосайнинг.
async function callEscrowFinalize(battleId, winner) {
  const qr = buildEscrowFinalizeQR(battleId, winner);

  // Логируем QR — пока ручная подпись
  console.log(`[escrow] Finalize QR for battle ${battleId}:`);
  console.log(qr);

  // Отправляем QR в Telegram себе (admin) чтобы подписать
  const adminChatId = process.env.ADMIN_TG_ID;
  if (adminChatId && CONFIG.BOT_TOKEN !== 'YOUR_BOT_TOKEN') {
    await sendTg(adminChatId,
      `🔑 <b>Нужна подпись финализации!</b>\n\n` +
      `Битва: <b>#${battleId}</b>\n` +
      `Победитель: <code>${winner}</code>\n\n` +
      `Вставь в Axiome Connect:`
    );
    await sendTg(adminChatId, `<code>${qr}</code>`);
  }
}

// ── Уведомление победителя при автовыплате (без QR) ──────────
async function notifyWinnerAuto(battle, winner, prize) {
  const p = db.prepare(
    'SELECT tg_user_id FROM participants WHERE battle_id=? AND wallet=?'
  ).get(battle.id, winner);
  if (!p?.tg_user_id) return;

  await sendTg(p.tg_user_id,
    `🏆 <b>Поздравляем! Ты победил!</b>\n\n` +
    `Битва: <b>#${battle.id}</b>\n` +
    `Приз: <b>${prize.toLocaleString()} SLUG</b>\n\n` +
    `💰 Токены уже отправлены на твой кошелёк автоматически!\n` +
    `Проверь баланс в Axiome Wallet.`
  );
}

// TG webhook
app.post(`/webhook/${CONFIG.BOT_TOKEN}`, async (req, res) => {
  res.sendStatus(200);
  const msg = req.body?.message;
  if (!msg) return;

  const chatId = msg.chat.id;
  const text   = msg.text || '';

  if (text === '/start') {
    await sendTg(chatId,
      `🐌 <b>CRYPTO PORTFOLIO BATTLE</b>\n\n` +
      `Собери портфель мем-токенов в Axiome Wallet и сражайся с другими!\n\n` +
      `Чей портфель вырастет больше за время битвы — забирает весь банк 🏆\n\n` +
      `Нажми кнопку ниже 👇`,
      {
        reply_markup: {
          inline_keyboard: [[
            { text: '⚔️ Открыть игру', web_app: { url: CONFIG.FRONTEND_URL } }
          ]]
        }
      }
    );
  }

  if (text === '/battles') {
    const battles = db.prepare(
      "SELECT * FROM battles WHERE status IN ('open','live') ORDER BY created_at DESC LIMIT 5"
    ).all();

    if (!battles.length) {
      await sendTg(chatId, '😴 Активных битв нет. Создай первую!');
      return;
    }

    let txt = '⚔️ <b>Активные битвы:</b>\n\n';
    for (const b of battles) {
      const cnt = db.prepare(
        'SELECT COUNT(*) as c FROM participants WHERE battle_id=?'
      ).get(b.id);
      const paidCnt = db.prepare(
        'SELECT COUNT(*) as c FROM participants WHERE battle_id=? AND paid=1'
      ).get(b.id);
      txt += `🔸 <b>#${b.id}</b> | ${b.stake} SLUG | ${cnt.c}/${b.max_players} игроков | оплатили: ${paidCnt.c} | ${b.status.toUpperCase()}\n`;
    }
    await sendTg(chatId, txt);
  }
});

async function setWebhook(baseUrl) {
  const url = `${baseUrl}/webhook/${CONFIG.BOT_TOKEN}`;
  const res = await fetch(
    `https://api.telegram.org/bot${CONFIG.BOT_TOKEN}/setWebhook?url=${encodeURIComponent(url)}`
  );
  const data = await res.json();
  console.log('[tg] Webhook set:', data.ok ? 'OK' : data.description);
}

// ============================================================
// HELPERS
// ============================================================

function formatDuration(ms) {
  const h = ms / 3600000;
  if (h < 24)   return `${h}ч`;
  if (h < 72)   return `${h / 24}д`;
  if (h < 168)  return `${h / 24}д`;
  return '7д';
}

// ── QR для входа в битву через Escrow контракт ──────────────
function buildEscrowJoinQR(battleId, amountSlug) {
  const hookMsg = Buffer.from(
    JSON.stringify({ join_battle: { battle_id: battleId } })
  ).toString('base64');
  const payload = {
    type: 'cosmwasm_execute', network: CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT, funds: [],
    msg: { send: { contract: CONFIG.ESCROW_CONTRACT, amount: String(Math.round(amountSlug * 1e6)), msg: hookMsg } },
    memo: `JOIN:${battleId}`,
  };
  return `axiomesign://${Buffer.from(JSON.stringify(payload)).toString('base64')}`;
}

// ── QR для cancel через Escrow контракт ──────────────────────
function buildEscrowCancelQR(battleId) {
  const payload = {
    type: 'cosmwasm_execute', network: CONFIG.NETWORK,
    contract_addr: CONFIG.ESCROW_CONTRACT, funds: [],
    msg: { cancel_battle: { battle_id: battleId } },
    memo: `CANCEL:${battleId}`,
  };
  return `axiomesign://${Buffer.from(JSON.stringify(payload)).toString('base64')}`;
}

// ── Fallback QR (если escrow не задеплоен) ───────────────────
function buildQR(recipient, amountSlug, memo) {
  const payload = {
    type: 'cosmwasm_execute', network: CONFIG.NETWORK,
    contract_addr: CONFIG.SLUG_CONTRACT, funds: [],
    msg: { transfer: { recipient, amount: String(Math.round(amountSlug * 1e6)) } },
    memo,
  };
  return `axiomesign://${Buffer.from(JSON.stringify(payload)).toString('base64')}`;
}

// ── Выбираем нужный QR в зависимости от режима ───────────────
function buildJoinQR(battleId, amountSlug, memo) {
  if (CONFIG.ESCROW_CONTRACT) return buildEscrowJoinQR(battleId, amountSlug);
  return buildQR(CONFIG.PROJECT_WALLET, amountSlug, memo);
}


// ============================================================
// MIDDLEWARE
// ============================================================

function requireWallet(req, res, next) {
  const wallet = req.headers['x-wallet'] || req.body?.wallet;
  if (!wallet?.startsWith('axm1')) {
    return res.status(401).json({ error: 'Axiome wallet address required (axm1...)' });
  }
  req.wallet = wallet;
  next();
}

function requireAdmin(req, res, next) {
  const key = req.headers['x-admin-key'] || req.body?.admin_key;
  if (key !== CONFIG.ADMIN_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// ============================================================
// ROUTES
// ============================================================

app.get('/health', (_, res) => res.json({ ok: true, time: Date.now() }));

// ── Список битв ──────────────────────────────────────────────

app.get('/battles', (req, res) => {
  const { status } = req.query;
  const battles = status
    ? db.prepare('SELECT * FROM battles WHERE status=? ORDER BY created_at DESC LIMIT 50').all(status)
    : db.prepare('SELECT * FROM battles ORDER BY created_at DESC LIMIT 50').all();

  // Добавляем счётчики участников
  const result = battles.map(b => {
    const total = db.prepare('SELECT COUNT(*) as c FROM participants WHERE battle_id=?').get(b.id).c;
    const paid  = db.prepare('SELECT COUNT(*) as c FROM participants WHERE battle_id=? AND paid=1').get(b.id).c;
    return { ...b, participants_count: total, paid_count: paid };
  });

  res.json(result);
});

// ── Одна битва ───────────────────────────────────────────────

app.get('/battles/:id', (req, res) => {
  const b = db.prepare('SELECT * FROM battles WHERE id=?').get(req.params.id);
  if (!b) return res.status(404).json({ error: 'Battle not found' });

  const participants = db.prepare(
    'SELECT wallet, tg_username, joined_at, growth_pct, paid FROM participants WHERE battle_id=? ORDER BY growth_pct DESC'
  ).all(req.params.id);

  res.json({ ...b, participants });
});

// ── Создать битву ────────────────────────────────────────────

app.post('/battles', requireWallet, async (req, res) => {
  const { stake, max_players, duration_key, tg_user_id, tg_username } = req.body;

  if (!stake || stake < CONFIG.MIN_STAKE) {
    return res.status(400).json({ error: `Минимальная ставка: ${CONFIG.MIN_STAKE} SLUG` });
  }
  if (!max_players || max_players < 2 || max_players > 50) {
    return res.status(400).json({ error: 'Игроков: от 2 до 50' });
  }

  const DURATIONS = { '1h': 3_600_000, '24h': 86_400_000, '3d': 259_200_000, '7d': 604_800_000 };
  const duration_ms = DURATIONS[duration_key];
  if (!duration_ms) {
    return res.status(400).json({ error: 'Длительность: 1h | 24h | 3d | 7d' });
  }

  // Проверяем баланс SLUG создателя
  const slugBal = await getCW20Balance(req.wallet, CONFIG.SLUG_CONTRACT);
  if (slugBal < stake * 1e6) {
    return res.status(400).json({
      error: `Недостаточно SLUG. Нужно: ${stake}, есть: ${(slugBal / 1e6).toFixed(0)}`
    });
  }

  const id   = crypto.randomBytes(4).toString('hex').toUpperCase();
  const memo = `BATTLE:${id}:${req.wallet}`;

  // Фиксируем стартовый портфель создателя
  const portfolio = await getFullPortfolio(req.wallet);
  const prices    = await getTokenPrices();
  const usdVal    = await calcPortfolioUsd(portfolio, prices);
  const snap      = JSON.stringify({ tokens: portfolio, prices, total_usd: usdVal, ts: Date.now() });

  // Создаём битву
  db.prepare(`
    INSERT INTO battles (id, creator, stake, max_players, duration_ms, status, pool_total)
    VALUES (?, ?, ?, ?, ?, 'open', 0)
  `).run(id, req.wallet, stake, max_players, duration_ms);

  // Добавляем создателя как первого участника (unpaid)
  db.prepare(`
    INSERT INTO participants (battle_id, wallet, tg_user_id, tg_username, portfolio_start)
    VALUES (?, ?, ?, ?, ?)
  `).run(id, req.wallet, tg_user_id || null, tg_username || null, snap);

  // Регистрируем ожидающий платёж
  db.prepare(`
    INSERT INTO pending_payments (battle_id, wallet, amount, memo)
    VALUES (?, ?, ?, ?)
  `).run(id, req.wallet, stake * 1e6, memo);

  // Генерируем QR для оплаты ставки
  const qr = buildJoinQR(id, stake, memo);

  console.log(`[battle] Created: #${id} by ${req.wallet} stake=${stake} players=${max_players}`);

  res.json({
    id,
    qr_string: qr,
    memo,
    stake,
    message: `Битва #${id} создана! Оплати ставку ${stake} SLUG чтобы активировать.`,
    payment_timeout_sec: CONFIG.PAYMENT_TIMEOUT_SEC,
  });
});

// ── Войти в битву ────────────────────────────────────────────

app.post('/battles/:id/join', requireWallet, async (req, res) => {
  const { tg_user_id, tg_username } = req.body;

  const b = db.prepare('SELECT * FROM battles WHERE id=?').get(req.params.id);
  if (!b) return res.status(404).json({ error: 'Битва не найдена' });
  if (b.status !== 'open') return res.status(400).json({ error: 'Битва уже не открыта' });

  // Считаем только тех кто оплатил или ещё не просрочил
  const activeCnt = db.prepare(`
    SELECT COUNT(*) as c FROM participants p
    WHERE p.battle_id = ?
    AND (p.paid = 1 OR EXISTS (
      SELECT 1 FROM pending_payments pp
      WHERE pp.battle_id = p.battle_id
      AND pp.wallet = p.wallet
      AND pp.verified = 0
      AND (unixepoch() - pp.created_at) < ?
    ))
  `).get(req.params.id, CONFIG.PAYMENT_TIMEOUT_SEC).c;

  if (activeCnt >= b.max_players) {
    return res.status(400).json({ error: 'Битва заполнена' });
  }

  // Уже участвует?
  const existing = db.prepare(
    'SELECT paid FROM participants WHERE battle_id=? AND wallet=?'
  ).get(req.params.id, req.wallet);

  if (existing) {
    if (existing.paid) {
      return res.status(400).json({ error: 'Ты уже в этой битве и оплатил ставку' });
    }
    // Уже есть pending — возвращаем тот же QR
    const pend = db.prepare(
      'SELECT * FROM pending_payments WHERE battle_id=? AND wallet=? AND verified=0'
    ).get(req.params.id, req.wallet);
    if (pend) {
      const qr = buildJoinQR(req.params.id, b.stake, pend.memo);
      return res.json({
        message:  'Ты уже в очереди на оплату',
        qr_string: qr,
        memo:     pend.memo,
        stake:    b.stake,
        payment_timeout_sec: CONFIG.PAYMENT_TIMEOUT_SEC,
      });
    }
  }

  // Проверяем баланс SLUG
  const slugBal = await getCW20Balance(req.wallet, CONFIG.SLUG_CONTRACT);
  if (slugBal < b.stake * 1e6) {
    return res.status(400).json({
      error: `Недостаточно SLUG. Нужно: ${b.stake}, есть: ${(slugBal / 1e6).toFixed(0)}`
    });
  }

  // Фиксируем стартовый портфель
  const portfolio = await getFullPortfolio(req.wallet);
  const prices    = await getTokenPrices();
  const usdVal    = await calcPortfolioUsd(portfolio, prices);
  const snap      = JSON.stringify({ tokens: portfolio, prices, total_usd: usdVal, ts: Date.now() });

  const memo = `BATTLE:${req.params.id}:${req.wallet}`;

  // Добавляем участника (paid=0 до подтверждения платежа)
  try {
    db.prepare(`
      INSERT INTO participants (battle_id, wallet, tg_user_id, tg_username, portfolio_start)
      VALUES (?, ?, ?, ?, ?)
    `).run(req.params.id, req.wallet, tg_user_id || null, tg_username || null, snap);
  } catch (e) {
    if (e.message.includes('UNIQUE')) {
      return res.status(400).json({ error: 'Уже участвуешь в этой битве' });
    }
    throw e;
  }

  // Регистрируем ожидающий платёж
  db.prepare(`
    INSERT OR REPLACE INTO pending_payments (battle_id, wallet, amount, memo)
    VALUES (?, ?, ?, ?)
  `).run(req.params.id, req.wallet, b.stake * 1e6, memo);

  // Генерируем QR
  const qr = buildJoinQR(req.params.id, b.stake, memo);

  console.log(`[battle] Join: wallet=${req.wallet} battle=${req.params.id} stake=${b.stake}`);

  res.json({
    message:   `Войди в битву #${req.params.id}. Оплати ставку ${b.stake} SLUG чтобы подтвердить участие.`,
    qr_string:  qr,
    memo,
    stake:      b.stake,
    portfolio_usd: usdVal.toFixed(2),
    payment_timeout_sec: CONFIG.PAYMENT_TIMEOUT_SEC,
  });
});

// ── Статус оплаты (фронтенд поллит каждые 3 сек) ────────────

app.get('/battles/:id/payment-status/:wallet', (req, res) => {
  const p = db.prepare(
    'SELECT paid FROM participants WHERE battle_id=? AND wallet=?'
  ).get(req.params.id, req.params.wallet);

  if (!p) return res.status(404).json({ error: 'Участник не найден' });

  const pend = db.prepare(
    'SELECT verified, created_at FROM pending_payments WHERE battle_id=? AND wallet=?'
  ).get(req.params.id, req.params.wallet);

  const now         = Math.floor(Date.now() / 1000);
  const timeoutSec  = pend ? CONFIG.PAYMENT_TIMEOUT_SEC - (now - pend.created_at) : 0;
  const expired     = pend?.verified === 2;

  res.json({
    paid:        p.paid === 1,
    expired,
    timeout_sec: Math.max(0, timeoutSec),
    battle_id:   req.params.id,
    wallet:      req.params.wallet,
  });
});

// ── Финализировать битву ─────────────────────────────────────

app.post('/battles/:id/finalize', async (req, res) => {
  const b = db.prepare('SELECT * FROM battles WHERE id=?').get(req.params.id);
  if (!b) return res.status(404).json({ error: 'Не найдено' });
  if (b.status !== 'live') return res.status(400).json({ error: 'Битва не активна' });

  const now = Math.floor(Date.now() / 1000);
  if (b.ends_at && now < b.ends_at) {
    return res.status(400).json({ error: `Битва завершится через ${b.ends_at - now}с` });
  }

  // Только участники которые оплатили
  const parts = db.prepare(
    'SELECT * FROM participants WHERE battle_id=? AND paid=1'
  ).all(req.params.id);

  if (!parts.length) {
    db.prepare("UPDATE battles SET status='cancelled' WHERE id=?").run(req.params.id);
    // Если escrow задеплоен — отменяем на блокчейне (вернёт ставки)
    if (CONFIG.ESCROW_CONTRACT) {
      const cancelQr = buildEscrowCancelQR(req.params.id);
      console.log(`[finalize] Cancel QR for battle ${req.params.id}: ${cancelQr.slice(0,60)}...`);
    }
    return res.json({ message: 'Битва отменена — нет оплативших участников' });
  }

  const prices = await getTokenPrices();
  let winner = null, bestGrowth = -Infinity;

  for (const p of parts) {
    const start    = JSON.parse(p.portfolio_start || '{}');
    const startUsd = start.total_usd || 0;

    const cur    = await getFullPortfolio(p.wallet);
    const curUsd = await calcPortfolioUsd(cur, prices);

    const growth = startUsd > 0 ? ((curUsd - startUsd) / startUsd) * 100 : 0;

    db.prepare(
      'UPDATE participants SET portfolio_end=?, growth_pct=? WHERE battle_id=? AND wallet=?'
    ).run(
      JSON.stringify({ tokens: cur, prices, total_usd: curUsd, ts: Date.now() }),
      growth,
      b.id,
      p.wallet
    );

    if (growth > bestGrowth) {
      bestGrowth = growth;
      winner     = p.wallet;
    }
  }

  const paidCount = parts.length;
  const pool      = b.stake * paidCount;
  const fee       = Math.round(pool * CONFIG.FEE_PERCENT / 100);
  const prize     = pool - fee;

  db.prepare("UPDATE battles SET status='ended', winner=? WHERE id=?").run(winner, b.id);

  console.log(`[finalize] Battle ${b.id} ended. Winner: ${winner} prize=${prize} SLUG`);

  // ── АВТОВЫПЛАТА ──────────────────────────────────────────
  // Если Escrow контракт задеплоен — вызываем Finalize на блокчейне.
  // Контракт сам отправит приз победителю и комиссию fee_wallet.
  // Никакого ручного QR — всё автоматически.
  if (CONFIG.ESCROW_CONTRACT) {
    try {
      await callEscrowFinalize(b.id, winner);
      console.log(`[finalize] ✅ Escrow finalize called: battle=${b.id} winner=${winner}`);

      // Уведомляем победителя — без QR, деньги уже на кошельке
      await notifyWinnerAuto(b, winner, prize);
    } catch (e) {
      console.error(`[finalize] ❌ Escrow finalize failed: ${e.message}`);
      // Fallback: отправляем QR вручную
      await notifyWinner(b, winner, prize);
    }
  } else {
    // Старый режим — QR для ручной выплаты
    await notifyWinner(b, winner, prize);
  }

  // Уведомляем проигравших
  const losers = parts.filter(p => p.wallet !== winner);
  for (const loser of losers) {
    if (!loser.tg_user_id) continue;
    await sendTg(loser.tg_user_id,
      `😔 <b>Битва #${b.id} завершена</b>\n\n` +
      `Победитель: <code>${winner.slice(0,8)}...${winner.slice(-4)}</code>\n` +
      `Твой результат: <b>${parts.find(p => p.wallet === loser.wallet)?.growth_pct?.toFixed(2) || 0}%</b>\n\n` +
      `Попробуй снова! 💪`
    );
  }

  res.json({
    winner,
    growth_pct: bestGrowth.toFixed(2),
    prize_slug: prize,
    fee_slug:   fee,
    paid_players: paidCount,
    message: `Победитель: ${winner}. Приз: ${prize} SLUG. QR отправлен в Telegram.`,
  });
});

// ── Портфель игрока ──────────────────────────────────────────

app.get('/portfolio/:wallet', async (req, res) => {
  if (!req.params.wallet.startsWith('axm1')) {
    return res.status(400).json({ error: 'Неверный адрес' });
  }
  const [portfolio, prices] = await Promise.all([
    getFullPortfolio(req.params.wallet),
    getTokenPrices(),
  ]);
  const total_usd = await calcPortfolioUsd(portfolio, prices);
  res.json({ wallet: req.params.wallet, portfolio, prices, total_usd });
});

// ── Admin: ручная финализация всех просроченных ─────────────

app.post('/admin/finalize-all', requireAdmin, async (req, res) => {
  const now     = Math.floor(Date.now() / 1000);
  const expired = db.prepare(
    "SELECT id FROM battles WHERE status='live' AND ends_at<=?"
  ).all(now);

  const results = [];
  for (const b of expired) {
    try {
      const r = await fetch(`http://localhost:${CONFIG.PORT}/battles/${b.id}/finalize`, {
        method: 'POST'
      });
      results.push({ id: b.id, ok: r.ok });
    } catch (e) {
      results.push({ id: b.id, error: e.message });
    }
  }

  res.json({ processed: results.length, results });
});

// ── Admin: статистика ─────────────────────────────────────────

app.get('/admin/stats', requireAdmin, (req, res) => {
  const stats = {
    battles: {
      open:      db.prepare("SELECT COUNT(*) as c FROM battles WHERE status='open'").get().c,
      live:      db.prepare("SELECT COUNT(*) as c FROM battles WHERE status='live'").get().c,
      ended:     db.prepare("SELECT COUNT(*) as c FROM battles WHERE status='ended'").get().c,
      cancelled: db.prepare("SELECT COUNT(*) as c FROM battles WHERE status='cancelled'").get().c,
    },
    payments: {
      pending:   db.prepare('SELECT COUNT(*) as c FROM pending_payments WHERE verified=0').get().c,
      confirmed: db.prepare('SELECT COUNT(*) as c FROM pending_payments WHERE verified=1').get().c,
      expired:   db.prepare('SELECT COUNT(*) as c FROM pending_payments WHERE verified=2').get().c,
    },
    participants: {
      total: db.prepare('SELECT COUNT(*) as c FROM participants').get().c,
      paid:  db.prepare('SELECT COUNT(*) as c FROM participants WHERE paid=1').get().c,
    },
  };
  res.json(stats);
});

// ============================================================
// AUTO FINALIZE — каждые 5 минут
// ============================================================
setInterval(async () => {
  const now     = Math.floor(Date.now() / 1000);
  const expired = db.prepare(
    "SELECT id FROM battles WHERE status='live' AND ends_at<=?"
  ).all(now);

  for (const b of expired) {
    try {
      await fetch(`http://localhost:${CONFIG.PORT}/battles/${b.id}/finalize`, {
        method: 'POST'
      });
    } catch (e) {
      console.error(`[auto-finalize] Error for ${b.id}:`, e.message);
    }
  }
}, 5 * 60 * 1000);

// ============================================================
// START
// ============================================================
app.listen(CONFIG.PORT, async () => {
  console.log(`🐌 SLUG Battle Server v2 on port ${CONFIG.PORT}`);

  if (process.env.RAILWAY_STATIC_URL) {
    await setWebhook(`https://${process.env.RAILWAY_STATIC_URL}`);
  }
});
