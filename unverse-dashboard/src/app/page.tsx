"use client";

import { useEffect, useState } from "react";
import { supabase } from "@/utils/supabase/client";
import {
  Activity,
  Briefcase,
  History,
  Wallet,
  TrendingUp,
  TerminalSquare,
  CheckCircle,
  XCircle,
  Clock,
  Settings,
  Sparkles,
  Save
} from "lucide-react";
import { format } from "date-fns";

export default function Dashboard() {
  const [logs, setLogs] = useState<any[]>([]);
  const [activeTrades, setActiveTrades] = useState<any[]>([]);
  const [history, setHistory] = useState<any[]>([]);
  const [wallets, setWallets] = useState<any[]>([]);
  const [pendingSignals, setPendingSignals] = useState<any[]>([]);
  const [narrative, setNarrative] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  // Editable bot config (seeded once so the 10s refresh doesn't clobber typing).
  const [cfgSymbols, setCfgSymbols] = useState("");
  const [cfgStrategy, setCfgStrategy] = useState("confluence");
  const [cfgRisk, setCfgRisk] = useState("0.015");
  const [cfgTpR, setCfgTpR] = useState("2.0");
  const [cfgLeverage, setCfgLeverage] = useState("10");
  const [cfgSeeded, setCfgSeeded] = useState(false);
  const [cfgSaving, setCfgSaving] = useState(false);
  const [cfgSavedMsg, setCfgSavedMsg] = useState("");

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 10000);
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    try {
      const [logsRes, tradesRes, historyRes, walletsRes, pendingRes, configRes, narrativeRes] = await Promise.all([
        supabase.from("agent_logs").select("*").order("created_at", { ascending: false }).limit(20),
        supabase.from("active_trades").select("*").eq("status", "OPEN").order("created_at", { ascending: false }),
        supabase.from("trade_history").select("*").order("closed_at", { ascending: false }).limit(50),
        supabase.from("wallets").select("*").order("updated_at", { ascending: false }),
        supabase.from("pending_signals").select("*").eq("status", "PENDING").order("created_at", { ascending: false }),
        supabase.from("bot_config").select("*"),
        supabase.from("narrative_trends").select("*").order("updated_at", { ascending: false }).limit(1)
      ]);

      if (logsRes.data) setLogs(logsRes.data);
      if (tradesRes.data) setActiveTrades(tradesRes.data);
      if (historyRes.data) setHistory(historyRes.data);
      if (walletsRes.data) setWallets(walletsRes.data);
      if (pendingRes.data) setPendingSignals(pendingRes.data);
      if (narrativeRes.data && narrativeRes.data.length) setNarrative(narrativeRes.data[0]);

      // Seed the config form once from bot_config.
      if (configRes.data && !cfgSeeded) {
        const cfg: Record<string, any> = {};
        configRes.data.forEach((row: any) => { cfg[row.key] = row.value; });
        if (Array.isArray(cfg.symbols)) setCfgSymbols(cfg.symbols.join(", "));
        if (cfg.strategy) setCfgStrategy(String(cfg.strategy));
        if (cfg.risk_per_trade_pct != null) setCfgRisk(String(cfg.risk_per_trade_pct));
        if (cfg.tp_r_multiple != null) setCfgTpR(String(cfg.tp_r_multiple));
        if (cfg.leverage != null) setCfgLeverage(String(cfg.leverage));
        setCfgSeeded(true);
      }
    } catch (error) {
      console.error("Data fetch error:", error);
    } finally {
      setLoading(false);
    }
  };

  const handleSaveConfig = async () => {
    setCfgSaving(true);
    setCfgSavedMsg("");
    try {
      const symbols = cfgSymbols.split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
      const rows = [
        { key: "symbols", value: symbols },
        { key: "strategy", value: cfgStrategy },
        { key: "risk_per_trade_pct", value: Number(cfgRisk) },
        { key: "tp_r_multiple", value: Number(cfgTpR) },
        { key: "leverage", value: Number(cfgLeverage) },
      ];
      await supabase.from("bot_config").upsert(rows, { onConflict: "key" });
      setCfgSavedMsg("Kaydedildi. Ajanlar bir sonraki başlangıçta uygular.");
    } catch (error) {
      console.error("Error saving config:", error);
      setCfgSavedMsg("Kaydetme hatası.");
    } finally {
      setCfgSaving(false);
    }
  };

  const handleApprove = async (id: string) => {
    try {
      await supabase.from("pending_signals").update({ status: "APPROVED" }).eq("id", id);
      fetchData(); // refresh list
    } catch (error) {
      console.error("Error approving signal:", error);
    }
  };

  const handleReject = async (id: string) => {
    try {
      await supabase.from("pending_signals").update({ status: "REJECTED" }).eq("id", id);
      fetchData(); // refresh list
    } catch (error) {
      console.error("Error rejecting signal:", error);
    }
  };

  // Toplam PnL Hesaplaması
  const totalPnL = history.reduce((acc, trade) => acc + (Number(trade.pnl) || 0), 0);

  if (loading) {
    return <div className="flex h-screen items-center justify-center text-slate-400">Yükleniyor...</div>;
  }

  return (
    <div className="flex min-h-screen bg-[#0f111a] font-sans">
      
      {/* SIDEBAR */}
      <div className="w-64 bg-[#1a1d2d] border-r border-slate-800 p-6 flex flex-col gap-8 hidden md:flex shrink-0">
        <div className="flex items-center gap-3 text-white font-bold text-xl tracking-wider">
          <Activity className="text-blue-500" />
          UNVERSE.ME
        </div>
        <nav className="flex flex-col gap-4 text-slate-400 font-medium">
          <a href="#" className="flex items-center gap-3 text-blue-400 bg-blue-500/10 p-3 rounded-lg cursor-pointer transition">
            <TrendingUp size={20} />
            Dashboard
          </a>
          <a href="#active-positions" className="flex items-center gap-3 hover:text-slate-200 p-3 rounded-lg cursor-pointer transition">
            <Briefcase size={20} />
            Active Trades
          </a>
          <a href="#trade-history" className="flex items-center gap-3 hover:text-slate-200 p-3 rounded-lg cursor-pointer transition">
            <History size={20} />
            Trade History
          </a>
          <a href="#wallets" className="flex items-center gap-3 hover:text-slate-200 p-3 rounded-lg cursor-pointer transition">
            <Wallet size={20} />
            Wallets
          </a>
          <a href="#narratives" className="flex items-center gap-3 hover:text-slate-200 p-3 rounded-lg cursor-pointer transition">
            <Sparkles size={20} />
            Narratives
          </a>
          <a href="#settings" className="flex items-center gap-3 hover:text-slate-200 p-3 rounded-lg cursor-pointer transition">
            <Settings size={20} />
            Settings
          </a>
        </nav>
      </div>

      {/* MAIN CONTENT */}
      <div className="flex-1 p-6 lg:p-10 overflow-y-auto max-h-screen">
        <header className="mb-10 flex justify-between items-end flex-wrap gap-4">
          <div>
            <h1 className="text-3xl font-bold text-white">Trading Dashboard</h1>
            <p className="text-slate-400 mt-2">Real-time AI execution metrics & overview.</p>
          </div>
          {/* TOTAL PNL CARD */}
          <div className="bg-[#1a1d2d] border border-slate-800 rounded-xl p-6 min-w-[200px] shadow-lg">
            <p className="text-slate-400 text-sm font-medium mb-1">Total PnL</p>
            <p className={`text-3xl font-bold ${totalPnL >= 0 ? "text-emerald-500" : "text-rose-500"}`}>
              {totalPnL >= 0 ? "+" : ""}{totalPnL.toFixed(2)} USDT
            </p>
          </div>
        </header>

        <div className="grid grid-cols-1 xl:grid-cols-3 gap-8">
          
          {/* LEFT COLUMN: ACTIVE TRADES & WALLETS */}
          <div className="xl:col-span-2 flex flex-col gap-8">
            
            {/* PENDING APPROVALS */}
            {pendingSignals.length > 0 && (
              <div className="bg-gradient-to-r from-amber-500/10 to-orange-500/10 border border-amber-500/30 rounded-xl p-6 shadow-[0_0_15px_rgba(245,158,11,0.1)]">
                <div className="flex items-center gap-2 mb-6">
                  <Clock className="text-amber-500" size={24}/>
                  <h2 className="text-xl font-bold text-amber-500">Pending Approvals</h2>
                  <span className="ml-2 bg-amber-500 text-black text-xs font-bold px-2 py-1 rounded-full">{pendingSignals.length}</span>
                </div>
                <div className="grid grid-cols-1 gap-4">
                  {pendingSignals.map(signal => (
                    <div key={signal.id} className="bg-[#0f111a] border border-amber-500/20 rounded-lg p-5 flex flex-col md:flex-row justify-between md:items-center gap-4">
                      <div>
                        <div className="flex items-center gap-3 mb-2">
                          <span className="text-lg font-bold text-white">{signal.symbol}</span>
                          <span className={`px-2 py-1 rounded text-xs font-bold ${signal.action === 'LONG' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-rose-500/20 text-rose-400'}`}>
                            {signal.action}
                          </span>
                          <span className="text-xs text-amber-400 font-bold border border-amber-400/30 px-2 py-1 rounded">
                            AI Confidence: {Math.round(signal.confidence * 100)}%
                          </span>
                        </div>
                        <p className="text-slate-400 text-sm">{signal.reasoning}</p>
                      </div>
                      <div className="flex items-center gap-3 shrink-0">
                        <button onClick={() => handleReject(signal.id)} className="flex items-center gap-2 px-4 py-2 bg-rose-500/10 hover:bg-rose-500/20 text-rose-500 border border-rose-500/30 rounded-lg font-bold transition">
                          <XCircle size={18} />
                          Reject
                        </button>
                        <button onClick={() => handleApprove(signal.id)} className="flex items-center gap-2 px-4 py-2 bg-emerald-500 hover:bg-emerald-600 text-white shadow-lg shadow-emerald-500/20 rounded-lg font-bold transition">
                          <CheckCircle size={18} />
                          Approve Trade
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* ACTIVE TRADES */}
            <div id="active-positions" className="bg-[#1a1d2d] border border-slate-800 rounded-xl p-6 scroll-mt-6">
              <div className="flex items-center gap-2 mb-6">
                <Briefcase className="text-blue-500" size={24}/>
                <h2 className="text-xl font-bold text-white">Active Positions</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left">
                  <thead className="text-slate-500 border-b border-slate-800">
                    <tr>
                      <th className="pb-3 font-medium">Symbol</th>
                      <th className="pb-3 font-medium">Side</th>
                      <th className="pb-3 font-medium">Entry Price</th>
                      <th className="pb-3 font-medium">Quantity</th>
                      <th className="pb-3 font-medium">Leverage</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800">
                    {activeTrades.length === 0 ? (
                      <tr><td colSpan={5} className="py-4 text-slate-500 text-center">No active positions.</td></tr>
                    ) : (
                      activeTrades.map(trade => (
                        <tr key={trade.id} className="text-slate-300">
                          <td className="py-4 font-bold">{trade.symbol}</td>
                          <td className="py-4">
                            <span className={`px-2 py-1 rounded text-xs font-bold ${trade.side === 'LONG' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-rose-500/20 text-rose-400'}`}>
                              {trade.side}
                            </span>
                          </td>
                          <td className="py-4">${trade.entry_price}</td>
                          <td className="py-4">{trade.quantity}</td>
                          <td className="py-4">{trade.leverage}x</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            {/* TRADE HISTORY */}
            <div id="trade-history" className="bg-[#1a1d2d] border border-slate-800 rounded-xl p-6 scroll-mt-6">
              <div className="flex items-center gap-2 mb-6">
                <History className="text-purple-500" size={24}/>
                <h2 className="text-xl font-bold text-white">Trade History</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="text-slate-500 border-b border-slate-800">
                    <tr>
                      <th className="pb-3 font-medium">Symbol</th>
                      <th className="pb-3 font-medium">Side</th>
                      <th className="pb-3 font-medium">Entry</th>
                      <th className="pb-3 font-medium">Exit</th>
                      <th className="pb-3 font-medium">PnL</th>
                      <th className="pb-3 font-medium">Date</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800">
                    {history.length === 0 ? (
                      <tr><td colSpan={6} className="py-4 text-slate-500 text-center">No history yet.</td></tr>
                    ) : (
                      history.map(trade => (
                        <tr key={trade.id} className="text-slate-300 hover:bg-slate-800/50 transition">
                          <td className="py-3 font-medium">{trade.symbol}</td>
                          <td className="py-3 text-xs">{trade.side}</td>
                          <td className="py-3">${trade.entry_price}</td>
                          <td className="py-3">${trade.exit_price}</td>
                          <td className={`py-3 font-bold ${Number(trade.pnl) >= 0 ? "text-emerald-500" : "text-rose-500"}`}>
                            {Number(trade.pnl) > 0 ? "+" : ""}{trade.pnl}
                          </td>
                          <td className="py-3 text-slate-500">{trade.closed_at ? format(new Date(trade.closed_at), "MMM d, HH:mm") : "-"}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>

          </div>

          {/* RIGHT COLUMN: AGENT LOGS & WALLETS */}
          <div className="flex flex-col gap-8">
            
            {/* AGENT LOGS */}
            <div className="bg-[#1a1d2d] border border-slate-800 rounded-xl p-6 flex-1 max-h-[500px] flex flex-col">
              <div className="flex items-center gap-2 mb-6 shrink-0">
                <TerminalSquare className="text-amber-500" size={24}/>
                <h2 className="text-xl font-bold text-white">Live AI Stream</h2>
              </div>
              <div className="overflow-y-auto pr-2 flex-1 space-y-4">
                {logs.length === 0 ? (
                  <p className="text-slate-500">No logs found.</p>
                ) : (
                  logs.map(log => (
                    <div key={log.id} className="bg-[#0f111a] p-4 rounded-lg border border-slate-800">
                      <div className="flex justify-between items-center mb-2">
                        <span className="text-xs font-bold text-amber-500">{log.agent_name}</span>
                        <span className="text-xs text-slate-500">{log.created_at ? format(new Date(log.created_at), "HH:mm:ss") : "-"}</span>
                      </div>
                      <span className="inline-block px-2 py-1 bg-slate-800 rounded text-xs text-slate-300 mb-2 font-medium">
                        {log.action}
                      </span>
                      <p className="text-sm text-slate-400 leading-relaxed">
                        {log.message}
                      </p>
                    </div>
                  ))
                )}
              </div>
            </div>

            {/* WALLETS */}
            <div id="wallets" className="bg-[#1a1d2d] border border-slate-800 rounded-xl p-6 scroll-mt-6">
              <div className="flex items-center gap-2 mb-6">
                <Wallet className="text-emerald-500" size={24}/>
                <h2 className="text-xl font-bold text-white">Wallets</h2>
              </div>
              <div className="space-y-4">
                {wallets.length === 0 ? (
                  <p className="text-slate-500 text-sm">No wallets configured.</p>
                ) : (
                  wallets.map(wallet => (
                    <div key={wallet.id} className="flex justify-between items-center p-4 bg-[#0f111a] rounded-lg border border-slate-800">
                      <div>
                        <p className="text-white font-medium">{wallet.wallet_name}</p>
                        <p className="text-xs text-slate-500 mt-1">{wallet.network}</p>
                      </div>
                      <div className="text-right">
                        <p className="text-emerald-400 font-bold text-lg">${wallet.balance}</p>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>

            {/* HOT NARRATIVES (from the Gemini narrative agent) */}
            <div id="narratives" className="bg-[#1a1d2d] border border-slate-800 rounded-xl p-6 scroll-mt-6">
              <div className="flex items-center gap-2 mb-6">
                <Sparkles className="text-fuchsia-500" size={24}/>
                <h2 className="text-xl font-bold text-white">Hot Narratives</h2>
                {narrative && (
                  <span className={`ml-2 text-xs font-bold px-2 py-1 rounded ${narrative.grounded ? "bg-emerald-500/20 text-emerald-400" : "bg-amber-500/20 text-amber-400"}`}>
                    {narrative.grounded ? "LIVE" : "STALE"}
                  </span>
                )}
              </div>
              {(!narrative || !Array.isArray(narrative.sectors) || narrative.sectors.length === 0) ? (
                <p className="text-slate-500 text-sm">Narrative agent henüz veri üretmedi.</p>
              ) : (
                <div className="space-y-3">
                  {narrative.sectors.map((s: any, i: number) => (
                    <div key={i} className="p-4 bg-[#0f111a] rounded-lg border border-slate-800">
                      <div className="flex justify-between items-center mb-2">
                        <span className="text-white font-medium">{s.sector}</span>
                        <span className="text-fuchsia-400 text-xs font-bold">heat {Number(s.heat).toFixed(2)}</span>
                      </div>
                      <div className="flex flex-wrap gap-2">
                        {(s.tokens || []).map((t: string, j: number) => (
                          <span key={j} className="px-2 py-1 bg-slate-800 rounded text-xs text-slate-300 font-medium">{t}</span>
                        ))}
                      </div>
                    </div>
                  ))}
                  {!narrative.grounded && (
                    <p className="text-amber-400/80 text-xs mt-2">
                      Uyarı: Bu liste canlı aramadan değil, modelin eğitim verisinden geldi — güncel olmayabilir.
                    </p>
                  )}
                </div>
              )}
            </div>

            {/* SETTINGS (UI-editable bot config) */}
            <div id="settings" className="bg-[#1a1d2d] border border-slate-800 rounded-xl p-6 scroll-mt-6">
              <div className="flex items-center gap-2 mb-6">
                <Settings className="text-blue-400" size={24}/>
                <h2 className="text-xl font-bold text-white">Bot Settings</h2>
              </div>
              <div className="space-y-4">
                <div>
                  <label className="block text-xs text-slate-400 mb-1">Watchlist (coinler, virgülle)</label>
                  <input
                    value={cfgSymbols}
                    onChange={(e) => setCfgSymbols(e.target.value)}
                    placeholder="btcusdt, ondousdt, dogeusdt"
                    className="w-full bg-[#0f111a] border border-slate-800 rounded-lg px-3 py-2 text-slate-200 text-sm focus:outline-none focus:border-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-xs text-slate-400 mb-1">Strateji</label>
                  <select
                    value={cfgStrategy}
                    onChange={(e) => setCfgStrategy(e.target.value)}
                    className="w-full bg-[#0f111a] border border-slate-800 rounded-lg px-3 py-2 text-slate-200 text-sm focus:outline-none focus:border-blue-500"
                  >
                    <option value="confluence">confluence (5 katman + narratif)</option>
                    <option value="sweep_reversal">sweep_reversal</option>
                    <option value="flow">flow</option>
                  </select>
                </div>
                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <label className="block text-xs text-slate-400 mb-1">Risk / işlem</label>
                    <input value={cfgRisk} onChange={(e) => setCfgRisk(e.target.value)}
                      className="w-full bg-[#0f111a] border border-slate-800 rounded-lg px-3 py-2 text-slate-200 text-sm focus:outline-none focus:border-blue-500" />
                  </div>
                  <div>
                    <label className="block text-xs text-slate-400 mb-1">TP (R)</label>
                    <input value={cfgTpR} onChange={(e) => setCfgTpR(e.target.value)}
                      className="w-full bg-[#0f111a] border border-slate-800 rounded-lg px-3 py-2 text-slate-200 text-sm focus:outline-none focus:border-blue-500" />
                  </div>
                  <div>
                    <label className="block text-xs text-slate-400 mb-1">Kaldıraç</label>
                    <input value={cfgLeverage} onChange={(e) => setCfgLeverage(e.target.value)}
                      className="w-full bg-[#0f111a] border border-slate-800 rounded-lg px-3 py-2 text-slate-200 text-sm focus:outline-none focus:border-blue-500" />
                  </div>
                </div>
                <button onClick={handleSaveConfig} disabled={cfgSaving}
                  className="flex items-center gap-2 px-4 py-2 bg-blue-500 hover:bg-blue-600 disabled:opacity-50 text-white rounded-lg font-bold transition">
                  <Save size={18} />
                  {cfgSaving ? "Kaydediliyor..." : "Ayarları Kaydet"}
                </button>
                {cfgSavedMsg && <p className="text-xs text-slate-400">{cfgSavedMsg}</p>}
              </div>
            </div>

          </div>
        </div>
      </div>
    </div>
  );
}
