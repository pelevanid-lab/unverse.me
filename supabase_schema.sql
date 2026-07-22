-- ============================================================
--  Supabase schema for the trading agent
--  Paste this whole file into: Supabase -> SQL Editor -> Run
--  Safe to run more than once (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
-- ============================================================

-- 1) pending_signals: add the new columns the engine now writes
--    (score = raw signed conviction, features = the market snapshot,
--     sl_price / tp_price = structure-based bracket suggested by the strategy)
alter table if exists public.pending_signals
    add column if not exists score      double precision,
    add column if not exists features   jsonb,
    add column if not exists sl_price   double precision,
    add column if not exists tp_price   double precision;

-- If pending_signals does not exist yet, create it:
create table if not exists public.pending_signals (
    id          uuid primary key default gen_random_uuid(),
    symbol      text        not null,
    action      text        not null,          -- LONG | SHORT
    confidence  double precision,
    reasoning   text,
    score       double precision,
    features    jsonb,
    sl_price    double precision,
    tp_price    double precision,
    status      text        not null default 'PENDING',  -- PENDING|APPROVED|EXECUTED
    created_at  timestamptz not null default now()
);

-- 2) trade_journal: the learning dataset (one row per trade, updated on close)
create table if not exists public.trade_journal (
    id            bigserial primary key,
    trade_id      text        not null unique,
    symbol        text        not null,
    action        text        not null,          -- LONG | SHORT
    entry_price   double precision,
    exit_price    double precision,
    quantity      double precision,
    leverage      integer,
    sl_price      double precision,
    tp_price      double precision,
    realized_pnl  double precision,              -- from exchange fills
    commission    double precision,              -- total fees paid
    net_pnl       double precision,              -- realized_pnl - commission
    outcome       text,                          -- WIN | LOSS
    duration_s    double precision,
    features      jsonb,                          -- entry-time market snapshot
    signal        jsonb,                          -- what the engine decided
    status        text        not null default 'OPEN',  -- OPEN | CLOSED
    created_at    timestamptz not null default now()
);

create index if not exists idx_trade_journal_status  on public.trade_journal (status);
create index if not exists idx_trade_journal_symbol  on public.trade_journal (symbol);

-- 3) active_trades: make sure it exists with the columns the code uses
create table if not exists public.active_trades (
    id          bigserial primary key,
    symbol      text        not null,
    side        text        not null,          -- LONG | SHORT
    entry_price double precision,
    leverage    integer,
    quantity    double precision,
    status      text        not null default 'OPEN',   -- OPEN | CLOSED
    created_at  timestamptz not null default now()
);

-- 4) trade_history: closed trades with REAL pnl (no more fake zeros)
--    Dashboard orders/displays by closed_at, so that is the canonical column.
create table if not exists public.trade_history (
    id          bigserial primary key,
    symbol      text        not null,
    side        text,
    entry_price double precision,
    exit_price  double precision,
    pnl         double precision,
    closed_at   timestamptz not null default now()
);
-- If the table already existed with the old `date` column, add closed_at.
alter table if exists public.trade_history
    add column if not exists closed_at timestamptz default now();

-- 5) agent_logs: live reasoning feed for the dashboard
create table if not exists public.agent_logs (
    id          bigserial primary key,
    agent_name  text,
    action      text,
    message     text,
    created_at  timestamptz not null default now()
);

-- 6) wallets: balance widget on the dashboard
create table if not exists public.wallets (
    id          uuid primary key,
    wallet_name text,
    network     text,
    balance     double precision,
    updated_at  timestamptz not null default now()
);

-- 7) narrative_trends: the Gemini "hot sectors" oracle output (editable in UI)
create table if not exists public.narrative_trends (
    id          uuid primary key,
    grounded    boolean,                 -- true = from live search, false = stale/training
    sectors     jsonb,                   -- [{sector, heat, tokens:[...]}, ...]
    bonus_map   jsonb,                   -- {SYMBOL: bonus}
    updated_at  timestamptz not null default now()
);


-- 8) htf_levels: the HTF (multi-month) support/resistance zones level_agent
--    publishes per symbol, and the polarity role (support/resistance) each
--    currently implies. Dashboard-visible mirror of what's cached in Redis.
create table if not exists public.htf_levels (
    symbol      text primary key,
    zones       jsonb,      -- [{price_low, price_high, touches, last_touch_ts, role}, ...]
    updated_at  timestamptz not null default now()
);

-- 9) learning_state: the learning agent's current calibration derived from
--    the trade journal (per-trigger-type stats, bounded confidence deltas,
--    disabled trigger types, trailing multiplier). Single-row upsert; the
--    live copy htf_agent actually reads is the Redis key "learn:adjustments".
create table if not exists public.learning_state (
    id          integer primary key,
    state       jsonb,
    updated_at  timestamptz not null default now()
);
