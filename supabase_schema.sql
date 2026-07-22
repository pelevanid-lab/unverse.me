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

-- 10) Dashboard read (+ approve/reject write) access for the PUBLIC/
--     publishable key (unverse-dashboard/src/utils/supabase/client.ts).
--     The backend (execution_engine, htf_agent, learning_agent, ...) uses
--     the SECRET key, which bypasses RLS entirely -- that is why the bot ran
--     fine while the dashboard showed nothing: every one of these tables had
--     RLS enabled with no permissive policy for the public key, so
--     PostgREST silently returned 200 OK / zero rows for every query
--     (2026-07-22 diagnosis). The Telegram chat-ID check in page.tsx is a
--     client-side UX gate only -- it does NOT protect this data, since the
--     public key is baked into the shipped JS bundle and works from any
--     plain HTTP client. Anyone with that key can already read/approve
--     signals directly against the REST API. If that's not acceptable,
--     the real fix is server-side auth (a Next.js API route that checks the
--     Telegram identity before touching Supabase), not tighter RLS -- these
--     policies just restore the dashboard to working as designed.
alter table public.agent_logs      enable row level security;
alter table public.active_trades   enable row level security;
alter table public.trade_history   enable row level security;
alter table public.wallets         enable row level security;
alter table public.pending_signals enable row level security;
alter table public.narrative_trends enable row level security;

drop policy if exists "dashboard read" on public.agent_logs;
create policy "dashboard read" on public.agent_logs for select using (true);

drop policy if exists "dashboard read" on public.active_trades;
create policy "dashboard read" on public.active_trades for select using (true);

drop policy if exists "dashboard read" on public.trade_history;
create policy "dashboard read" on public.trade_history for select using (true);

drop policy if exists "dashboard read" on public.wallets;
create policy "dashboard read" on public.wallets for select using (true);

drop policy if exists "dashboard read" on public.narrative_trends;
create policy "dashboard read" on public.narrative_trends for select using (true);

-- pending_signals additionally needs UPDATE: the dashboard's own
-- Approve/Reject buttons (handleApprove/handleReject in page.tsx) write
-- status directly, separately from Telegram's approve/reject flow.
drop policy if exists "dashboard read" on public.pending_signals;
create policy "dashboard read" on public.pending_signals for select using (true);
drop policy if exists "dashboard approve reject" on public.pending_signals;
create policy "dashboard approve reject" on public.pending_signals for update using (true) with check (true);
