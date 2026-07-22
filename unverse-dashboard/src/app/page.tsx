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
  Sparkles,
  RefreshCw,
  Search
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
  const [authError, setAuthError] = useState("");

  // Manual scan trigger + on-demand symbol analysis (same commands
  // Telegram's /tara and plain-text-symbol flow use, routed through
  // Supabase's dashboard_commands table since Redis isn't reachable from
  // the dashboard's serverless functions).
  const [scanTriggering, setScanTriggering] = useState(false);
  const [scanMsg, setScanMsg] = useState("");
  const [analyzeSymbol, setAnalyzeSymbol] = useState("");
  const [analyzing, setAnalyzing] = useState(false);
  const [analyzeMsg, setAnalyzeMsg] = useState("");

  useEffect(() => {
    const tg = (window as any).Telegram?.WebApp;
    
    // Güvenlik Kilidi: Telegram dışından gelenleri reddet
    if (!tg || !tg.initDataUnsafe?.user) {
      setAuthError("🚫 Yetkisiz Erişim. Lütfen paneli Telegram üzerinden açın.");
      setLoading(false);
      return;
    }
    
    // Web App'i tam ekrana genişlet
    tg.expand();

    // Güvenlik Kilidi: Sadece Enes'in ID'sine izin ver (1495511765)
    const userId = tg.initDataUnsafe.user.id;
    if (userId !== 1495511765) {
      setAuthError(`🚫 Yetkisiz Erişim. Bu panele erişim izniniz yok. (ID: ${userId})`);
      setLoading(false);
      return;
    }

    fetchData();
    const interval = setInterval(fetchData, 10000);
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    try {
      const [logsRes, tradesRes, historyRes, walletsRes, pendingRes, narrativeRes] = await Promise.all([
        supabase.from("agent_logs").select("*").order("created_at", { ascending: false }).limit(20),
        supabase.from("active_trades").select("*").eq("status", "OPEN").order("created_at", { ascending: false }),
        supabase.from("trade_history").select("*").order("closed_at", { ascending: false }).limit(50),
        supabase.from("wallets").select("*").order("updated_at", { ascending: false }),
        supabase.from("pending_signals").select("*").eq("status", "PENDING").order("created_at", { ascending: false }),
        supabase.from("narrative_trends").select("*").order("updated_at", { ascending: false }).limit(1)
      ]);

      if (logsRes.data) setLogs(logsRes.data);
      if (tradesRes.data) setActiveTrades(tradesRes.data);
      if (historyRes.data) setHistory(historyRes.data);
      if (walletsRes.data) setWallets(walletsRes.data);
      if (pendingRes.data) setPendingSignals(pendingRes.data);
      if (narrativeRes.data && narrativeRes.data.length) setNarrative(narrativeRes.data[0]);
    } catch (error) {
      console.error("Data fetch error:", error);
    } finally {
      setLoading(false);
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

  const handleManualScan = async () => {
    setScanTriggering(true);
    setScanMsg("");
    try {
      await supabase.from("dashboard_commands").insert({ type: "manual_scan" });
      setScanMsg("Tetiklendi. Sonuçlar Live AI Stream'de görünecek.");
    } catch (error) {
      console.error("Error triggering manual scan:", error);
      setScanMsg("Tetikleme hatası.");
    } finally {
      setScanTriggering(false);
    }
  };

  const handleAnalyzeSymbol = async () => {
    const symbol = analyzeSymbol.trim().toUpperCase().replace(/^\$/, "");
    if (!symbol) return;
    setAnalyzing(true);
    setAnalyzeMsg("");
    try {
      await supabase.from("dashboard_commands").insert({ type: "analyze_symbol", symbol });
      setAnalyzeMsg(`${symbol} analiz ediliyor — sonuç Live AI Stream'de görünecek.`);
      setAnalyzeSymbol("");
    } catch (error) {
      console.error("Error requesting symbol analysis:", error);
      setAnalyzeMsg("Analiz isteği hatası.");
    } finally {
      setAnalyzing(false);
    }
  };

  // Toplam PnL Hesaplaması
  const totalPnL = history.reduce((acc, trade) => acc + (Number(trade.pnl) || 0), 0);

  if (authError) {
    return (
      <div className="flex h-screen items-center justify-center bg-[#0f111a] p-6">
        <div className="bg-[#1a1d2d] border border-rose-500/30 rounded-xl p-8 max-w-md w-full text-center shadow-[0_0_30px_rgba(244,63,94,0.1)]">
          <XCircle className="text-rose-500 mx-auto mb-4" size={48} />
          <h2 className="text-xl font-bold text-white mb-2">Erişim Reddedildi</h2>
          <p className="text-slate-400">{authError}</p>
        </div>
      </div>
    );
  }

  if (loading) {
    return <div className="flex h-screen items-center justify-center text-slate-400 bg-[#0f111a]">Yükleniyor...</div>;
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

            {/* MANUAL CONTROLS: scan trigger + on-demand symbol analysis */}
            <div className="bg-[#1a1d2d] border border-slate-800 rounded-xl p-6">
              <div className="flex items-center gap-2 mb-4">
                <Search className="text-blue-400" size={24}/>
                <h2 className="text-xl font-bold text-white">Manuel Kontroller</h2>
              </div>
              <div className="space-y-4">
                <div>
                  <button
                    onClick={handleManualScan}
                    disabled={scanTriggering}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-500 hover:bg-blue-600 disabled:opacity-50 text-white rounded-lg font-bold transition"
                  >
                    <RefreshCw size={18} className={scanTriggering ? "animate-spin" : ""} />
                    {scanTriggering ? "Tetikleniyor..." : "Manuel Tarama Başlat"}
                  </button>
                  {scanMsg && <p className="text-xs text-slate-400 mt-2">{scanMsg}</p>}
                </div>
                <div>
                  <label className="block text-xs text-slate-400 mb-1">Sembol analiz et (örn. BTC, ETHUSDT)</label>
                  <div className="flex gap-2">
                    <input
                      value={analyzeSymbol}
                      onChange={(e) => setAnalyzeSymbol(e.target.value)}
                      onKeyDown={(e) => { if (e.key === "Enter") handleAnalyzeSymbol(); }}
                      placeholder="BTC"
                      className="flex-1 bg-[#0f111a] border border-slate-800 rounded-lg px-3 py-2 text-slate-200 text-sm focus:outline-none focus:border-blue-500"
                    />
                    <button
                      onClick={handleAnalyzeSymbol}
                      disabled={analyzing || !analyzeSymbol.trim()}
                      className="flex items-center gap-2 px-4 py-2 bg-blue-500 hover:bg-blue-600 disabled:opacity-50 text-white rounded-lg font-bold transition shrink-0"
                    >
                      <Search size={16} />
                      Analiz Et
                    </button>
                  </div>
                  {analyzeMsg && <p className="text-xs text-slate-400 mt-2">{analyzeMsg}</p>}
                </div>
              </div>
            </div>

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

          </div>
        </div>
      </div>
    </div>
  );
}
