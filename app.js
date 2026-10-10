/* Binance Radar Pro - 前端主逻辑
 * 合约 / 现货 走 Binance 官方公共 WS + REST；Alpha 走本地后端 /api/alpha（后端每 10s 全量采集）
 */
(() => {
  "use strict";

  // 全局错误捕获（便于排查）
  window.addEventListener("error", (e) => {
    window.__lastError = (e.message || "") + " @" + (e.filename || "").split("/").pop() + ":" + (e.lineno || 0);
  });
  window.addEventListener("unhandledrejection", (e) => {
    window.__lastError = "promise: " + ((e.reason && e.reason.message) || e.reason);
  });

  /* 数据源线路（无代理可用性）：
   *  - proxy ：币安官方域名（fapi / api / fstream / stream），合约 + 现货 + Alpha 全量，国内多数网络需代理 / VPN
   *  - direct：币安官方公共镜像（REST data-api.binance.vision / WS data-stream.binance.vision），国内可直连，但仅提供【现货】数据
   * 启动时自动探测两条线路并择优切换；合约（fapi）与 Alpha 无镜像，无代理时界面标注「当前需代理」且不报错、不卡死。 */
  const DOMAINS = {
    proxy: {
      REST_FUT: "https://fapi.binance.com",
      REST_SPOT: "https://api.binance.com",
      WS_FUT: "wss://fstream.binance.com/stream?streams=",
      WS_SPOT: "wss://stream.binance.com:9443/stream?streams="
    },
    direct: {
      REST_FUT: "https://fapi.binance.com",
      REST_SPOT: "https://data-api.binance.vision",
      WS_FUT: "wss://fstream.binance.com/stream?streams=",
      WS_SPOT: "wss://data-stream.binance.vision/stream?streams="
    }
  };
  const NET = {
    prefer: localStorage.getItem("radar.netPrefer") || "auto",   // auto | direct | proxy
    mode: "proxy",        // 当前生效线路：direct | proxy
    probing: false,
    lastProbe: 0,
    proxyOk: null,        // 官方域名（api.binance.com）是否可达
    directOk: null,       // 官方公共镜像（data-api.binance.vision）是否可达
    futOk: null,          // 合约接口（fapi）是否可达
    alphaOk: null,        // Alpha 是否可用（依赖后端 /api/alpha + 可访问币安主站）
    detail: {}
  };
  const NET_PROBE_TIMEOUT = 5000;   // 单次探测超时
  const NET_REPROBE_MS = 60000;     // 线路巡检 / 重探间隔

  let REST_FUT = DOMAINS.proxy.REST_FUT;
  let REST_SPOT = DOMAINS.proxy.REST_SPOT;
  let WS_FUT = DOMAINS.proxy.WS_FUT;
  let WS_SPOT = DOMAINS.proxy.WS_SPOT;

  /* Alpha 数据源：由本地/自建后端（app.py，提供 /api/alpha）供给。
   * 网页版可在 config.js 中设置 window.RADAR_API_BASE（如 "http://192.168.1.8:8765"）；
   * 留空则先试同源，再试本机 127.0.0.1:8765，最后尝试直连币安公共接口兜底。 */
  const IS_PUBLIC = !!window.RADAR_PUBLIC;   // 公网在线版：无本地后端，行情由浏览器直连公共接口
  const API_BASE = String(window.RADAR_API_BASE || "").replace(/\/+$/, "");
  const API_CANDIDATES = IS_PUBLIC ? [] : (API_BASE ? [API_BASE] : ["", "http://127.0.0.1:8765"]);
  const ALPHA_DIRECT_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list";

  const MAX_HIST_MS = 65 * 60 * 1000;   // 每条价格序列最多保留 65 分钟
  const SHARD_SIZE = 110;               // 大单分片大小
  const SHARD_SWITCH_MS = 60000;        // 分片切换周期
  const PAGE_SIZE = 100;

  // REST 兜底轮询节奏（官方 WS 推送不可用时自动启用）
  const REST_FUT_PRICE_MS = 3000;
  const REST_FUT_24H_MS = 60000;
  const REST_SPOT_MS = 15000;
  const WS_DEAD_MS = 8000;              // 超过该时长无推送即判定 WS 失效
  const AGG_DEAD_MS = 12000;            // 逐笔流超过该时长无消息即判定不可用

  const state = {
    market: "futures",
    params: { windowMin: 5, upPct: 5, downPct: -5, largeUsd: 250000, liqUsd: 100000, alphaPct: 8 },
    markets: {
      futures: { name: "合约永续", records: new Map(), symbols: [], ws: null, wsStatus: "init", shards: [], shardIdx: 0, shardText: "-", aggWs: null, lastMsgTs: 0, aggLastMsgTs: 0, aggOpenAt: 0, aggSupported: null, source: "ws", nextPrice: 0, next24h: 0, symbolCount: 0 },
      spot:    { name: "现货",     records: new Map(), symbols: [], ws: null, wsStatus: "init", shards: [], shardIdx: 0, shardText: "-", aggWs: null, lastMsgTs: 0, aggLastMsgTs: 0, aggOpenAt: 0, aggSupported: null, source: "ws", nextPrice: 0, symbolCount: 0 },
      alpha:   { name: "Alpha",    records: new Map(), symbols: [], ws: null, wsStatus: "init", shards: [], shardIdx: 0, shardText: "-", aggWs: null, lastMsgTs: 0, aggLastMsgTs: 0, aggOpenAt: 0, aggSupported: null, source: "server", nextPrice: 0, symbolCount: 0 }
    },
    bursts: new Map(),      // key: market|symbol -> burst
    liquidations: [],
    largeTrades: [],
    alerts: [],
    deriv: new Map(),       // futures: symbol -> {funding, mark, ts}
    oi: new Map(),          // futures: symbol -> {value, changePct, ts}
    allPage: 1,
    alertSeq: 0
  };

  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const MKT_TAG = { futures: '<span class="tag f">合约</span>', spot: '<span class="tag s">现货</span>', alpha: '<span class="tag a">Alpha</span>' };

  /* ---------- 格式化 ---------- */
  function fmtPrice(v) {
    v = Number(v);
    if (!Number.isFinite(v) || v === 0) return "—";
    const a = Math.abs(v);
    if (a >= 1000) return v.toLocaleString("en-US", { maximumFractionDigits: 2 });
    if (a >= 1) return v.toFixed(4);
    if (a >= 0.01) return v.toFixed(6);
    return v.toPrecision(4);
  }
  function fmtPct(v) {
    if (v === null || v === undefined || !Number.isFinite(Number(v))) return "—";
    const n = Number(v);
    return (n >= 0 ? "+" : "") + n.toFixed(2) + "%";
  }
  function fmtMoney(v) {
    v = Number(v) || 0;
    if (v >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (v >= 1e6) return (v / 1e6).toFixed(2) + "M";
    if (v >= 1e3) return (v / 1e3).toFixed(1) + "K";
    return v.toFixed(0);
  }
  function clock(ts) {
    return new Date(ts).toLocaleTimeString("zh-CN", { hour12: false });
  }
  function pushLimited(arr, item, n) { arr.unshift(item); if (arr.length > n) arr.length = n; }

  async function getJSON(url, timeout = 15000) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeout);
    try {
      const r = await fetch(url, { cache: "no-store", signal: ctrl.signal });
      if (!r.ok) throw new Error(url + " HTTP " + r.status);
      return await r.json();
    } finally { clearTimeout(t); }
  }

  function setStatus(market, text, cls) {
    if (market === "net") {
      const dn = $("dotNet"), tn = $("netText");
      if (dn) dn.className = "dot " + (cls || "");
      if (tn) tn.textContent = text;
      return;
    }
    const dot = $({ futures: "dotFut", spot: "dotSpot", alpha: "dotAlpha" }[market]);
    const tx = $({ futures: "futText", spot: "spotText", alpha: "alphaText" }[market]);
    if (dot) dot.className = "dot " + (cls || "");
    if (tx) tx.textContent = text + lineSuffix(market);
  }
  function log(msg) { $("lastMsg").textContent = msg + " · " + new Date().toLocaleString(); }

  /* ---------- 网络线路：启动探测 / 择优切换 / 状态展示（无代理可用性） ---------- */
  function lineName(mode) { return mode === "direct" ? "直连（官方公共镜像）" : "代理 / VPN（官方域名）"; }
  function lineSuffix(market) {
    if (!state.__netApplied) return "";
    if (market === "spot") return NET.mode === "direct" ? " · 直连" : " · 代理";
    if (market === "futures") return NET.futOk ? " · 代理" : "";
    return "";
  }
  function probe(url, timeout) {
    const t0 = Date.now();
    return new Promise((resolve) => {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), timeout || NET_PROBE_TIMEOUT);
      fetch(url, { cache: "no-store", signal: ctrl.signal })
        .then((r) => { clearTimeout(timer); resolve({ ok: r.ok, status: r.status, ms: Date.now() - t0 }); })
        .catch(() => { clearTimeout(timer); resolve({ ok: false, status: 0, ms: Date.now() - t0 }); });
    });
  }
  function applyLine() {
    const d = DOMAINS[NET.mode] || DOMAINS.proxy;
    REST_FUT = d.REST_FUT; REST_SPOT = d.REST_SPOT;
    WS_FUT = d.WS_FUT; WS_SPOT = d.WS_SPOT;
    state.__netApplied = true;
  }
  async function probeNetwork(force) {
    if (NET.probing) return;
    if (!force && NET.lastProbe && Date.now() - NET.lastProbe < 8000) return;
    NET.probing = true;
    setStatus("net", "探测中", "");
    let restart = false;
    try {
      const r = await Promise.all([
        probe("https://api.binance.com/api/v3/ping"),          // 官方现货域名（有代理才通）
        probe("https://data-api.binance.vision/api/v3/ping"),  // 官方公共镜像（国内可直连，仅现货）
        probe("https://fapi.binance.com/fapi/v1/ping")         // 合约接口（无代理不可达）
      ]);
      const prevMode = NET.mode, prevFut = NET.futOk;
      NET.detail = { proxy: r[0], direct: r[1], fut: r[2] };
      NET.proxyOk = r[0].ok; NET.directOk = r[1].ok; NET.futOk = r[2].ok;
      NET.lastProbe = Date.now();
      // 择优：auto 优先走官方域名（全量市场），官方不可达时切公共镜像直连（仅现货）
      NET.mode = NET.prefer === "direct" ? "direct"
        : NET.prefer === "proxy" ? "proxy"
          : (NET.proxyOk ? "proxy" : (NET.directOk ? "direct" : "proxy"));
      applyLine();
      renderNetNotice();
      if (NET.mode === "direct" && NET.futOk === false && !state.__userPickedMarket && state.market === "futures") {
        setMarket("spot");
        log("未检测到可用代理：已自动切换到「现货」市场（合约需代理，可随时手动切回）。");
      }
      restart = !!state.__netApplied && (NET.mode !== prevMode || NET.futOk !== prevFut);
      log("网络探测完成：官方域名" + (NET.proxyOk ? "可达" : "不可达") + "、公共镜像" + (NET.directOk ? "可达" : "不可达")
        + "、合约 fapi " + (NET.futOk ? "可达" : "不可达") + " → 当前线路：" + lineName(NET.mode));
    } finally {
      NET.probing = false;
    }
    if (restart) restartMarkets();
  }
  function netHealth() {
    if (NET.probing) return;
    if (Date.now() - NET.lastProbe < NET_REPROBE_MS) return;
    const sp = state.markets.spot;
    const spotDead = sp.symbols.length > 0 && sp.source === "ws" && !(sp.lastMsgTs > 0 && Date.now() - sp.lastMsgTs < WS_DEAD_MS);
    // 代理恢复 / 断开、合约接口状态变化、现货推送中断时重新探测，实现自动择优切换
    if (NET.mode === "direct" || !NET.futOk || !NET.proxyOk || !NET.directOk || spotDead) probeNetwork(true);
  }
  function probeText(name, r) {
    if (!r) return name + " 未探测";
    return name + (r.ok ? " 可达（" + r.ms + "ms）" : " 不可达（超时 / 被拦，耗时 " + r.ms + "ms）");
  }
  /* 无代理（走官方公共镜像直连）时，合约相关面板统一给出「当前需代理」占位文案，避免空白或误导性的“正在积累” */
  function emptyHint(fallback) {
    if (NET.futOk === false && state.market === "futures") {
      return "合约（fapi）当前需代理：本网络下合约行情不可达，请切换到「现货」标签查看可用数据，或开启代理 / VPN 后点顶栏「重新探测」。";
    }
    return fallback;
  }
  function renderNetNotice() {
    const d = NET.detail || {};
    setStatus("net", NET.mode === "direct" ? "直连" : "代理", NET.mode === "direct" ? "warn" : "ok");
    const el = $("netNotice");
    if (!el) return;
    const lines = [];
    lines.push("<b>当前线路：" + lineName(NET.mode) + "</b> · " + probeText("官方 api", d.proxy)
      + " · " + probeText("公共镜像", d.direct) + " · " + probeText("合约 fapi", d.fut));
    if (NET.mode === "direct") {
      lines.push("未检测到可用代理 / VPN：现货已自动切换到币安官方公共镜像直连（REST data-api.binance.vision · WS data-stream.binance.vision），现货行情、异动检测、即时提醒与历史记录均正常。");
      lines.push("<b>合约（fapi）与 Alpha 接口在当前网络下不可达：</b>合约强平 / 大单 / 衍生品 / 价格·OI 结构与 Alpha 行情会显示「当前需代理」，程序不会报错、卡死或空白；开启代理或 VPN 后点击右侧「重新探测」，程序会自动切回官方域名并恢复全量功能。");
    } else {
      lines.push("已检测到可用代理 / VPN：全部市场走币安官方域名（fapi / api / fstream / stream），合约与现货全量可用。");
      if (NET.alphaOk === false) lines.push("Alpha 行情另需本地后端（/api/alpha）采集，后端未运行时显示无数据源，不影响其余功能。");
    }
    el.className = "notice" + (NET.mode === "direct" && !NET.futOk ? " warnline" : "");
    el.innerHTML = lines.join("<br/>");
  }
  function markFuturesUnavailable() {
    state.markets.futures.unavailable = true;
    setStatus("futures", "当前需代理", "warn");
    const fc = $("futCount"); if (fc) fc.textContent = "—";
    const mf = $("mFut"); if (mf) mf.textContent = "—";
  }
  async function restartMarkets() {
    log("线路已切换为「" + lineName(NET.mode) + "」，正在重连行情源…");
    for (const mk of ["futures", "spot"]) {
      const st = state.markets[mk];
      if (st.ws) { try { st.ws.close(); } catch (e) { /* 忽略 */ } st.ws = null; }
      if (st.aggWs) { try { st.aggWs.close(); } catch (e) { /* 忽略 */ } st.aggWs = null; }
      st.aggSupported = null; st.aggOpenAt = 0; st.aggLastMsgTs = 0; st.lastMsgTs = 0;
      st.source = "ws"; st.nextPrice = 0; st.next24h = 0;
    }
    const fu = state.markets.futures;
    fu.unavailable = NET.futOk === false;
    try {
      if (NET.futOk === false) {
        state.liquidations.length = 0;
        markFuturesUnavailable();
      } else if (!fu.symbols.length) {
        await initFutures();
      } else {
        connectFuturesWS();
      }
      await initSpot();
      tickShards();
      if (NET.futOk === false) warmup("spot");
      else warmup("futures").then(() => warmup("spot"));
      pollAlpha();
    } catch (e) { log("线路切换后重连失败：" + (e.message || e)); }
  }

  /* ---------- 记录维护 ---------- */
  function ensureRecord(market, symbol) {
    const st = state.markets[market];
    let rec = st.records.get(symbol);
    if (!rec) {
      rec = { market, symbol, price: null, open: null, high: null, low: null, vol24: 0, quoteVol: 0, chg24: null, ts: 0, hist: [], extra: {} };
      st.records.set(symbol, rec);
    }
    return rec;
  }

  function updateRecord(rec, price, ts, patch) {
    const p = Number(price);
    if (!Number.isFinite(p) || p <= 0) return;
    rec.price = p; rec.ts = ts;
    if (patch) Object.assign(rec, patch);
    if (rec.open) rec.chg24 = (p / rec.open - 1) * 100;
    const h = rec.hist;
    const last = h.length ? h[h.length - 1] : null;
    if (!last || ts - last[0] >= 900) h.push([ts, p]);   // 约每秒一个点
    const cutoff = ts - MAX_HIST_MS;
    while (h.length > 2 && h[0][0] < cutoff) h.shift();
  }

  /* 计算窗口涨跌：返回 {chg, coveredMs, ready} */
  function windowChange(rec, winMs) {
    const h = rec.hist;
    if (!rec.price || h.length < 2) return { chg: null, coveredMs: 0, ready: false };
    const now = h[h.length - 1][0];
    const target = now - winMs;
    let base = h[0][1];
    for (let i = h.length - 1; i >= 0; i--) {
      if (h[i][0] <= target) { base = h[i][1]; break; }
    }
    const coveredMs = now - h[0][0];
    const ready = coveredMs >= Math.min(winMs, 30000);
    return { chg: base > 0 ? (rec.price / base - 1) * 100 : null, coveredMs, ready };
  }

  /* ---------- 评分 / 结构 ---------- */
  function scoreBurst(chg, volRatio, oiPct) {
    let s = 35;
    const a = Math.abs(chg);
    if (a >= 30) s += 25; else if (a >= 15) s += 20; else if (a >= 8) s += 14; else if (a >= 5) s += 8;
    if (volRatio !== null && volRatio !== undefined) {
      if (volRatio >= 20) s += 25; else if (volRatio >= 10) s += 20; else if (volRatio >= 5) s += 12; else if (volRatio >= 2) s += 6;
    }
    if (oiPct !== null && oiPct !== undefined) {
      const o = Math.abs(oiPct);
      if (o >= 5) s += 20; else if (o >= 2) s += 12; else if (o >= 0.8) s += 6;
    }
    return Math.max(0, Math.min(100, s));
  }
  const levelOf = (s) => s >= 75 ? "S" : s >= 60 ? "A" : s >= 45 ? "B" : "C";
  function quadrant(chg, oi) {
    if (oi === null || oi === undefined) return "数据不足";
    if (chg >= 0 && oi >= 0) return "上涨 + 增仓";
    if (chg >= 0 && oi < 0) return "上涨 + 减仓";
    if (chg < 0 && oi >= 0) return "下跌 + 增仓";
    return "下跌 + 减仓";
  }

  /* ---------- 异动即时提醒（提示音 + 系统通知 + 应用内浮层） ---------- */
  const NOTIFY_DEFAULTS = { enabled: 1, sound: 1, system: 1, gapSec: 3, maxPerMin: 20 };
  const notify = Object.assign({}, NOTIFY_DEFAULTS);
  try { Object.assign(notify, JSON.parse(localStorage.getItem("radar.notify") || "{}") || {}); } catch (e) {}
  const NOTIFY_KEY_COOLDOWN_MS = 300000;      // 同一币种同一类型 5 分钟内不重复提醒（防刷屏）
  const notiState = { lastAt: 0, minute: [], keyAt: new Map(), audio: null, sysAsked: false };

  function saveNotify() {
    try { localStorage.setItem("radar.notify", JSON.stringify(notify)); } catch (e) {}
  }

  /* 提示音：WebAudio 实时合成，无需任何音频文件 */
  function beep(up) {
    try {
      const Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      if (!notiState.audio) notiState.audio = new Ctx();
      const ctx = notiState.audio;
      if (ctx.state === "suspended") ctx.resume();
      const t0 = ctx.currentTime + 0.01;
      (up ? [880, 1320] : [760, 480]).forEach((f, i) => {
        const o = ctx.createOscillator(), g = ctx.createGain();
        o.type = "sine";
        o.frequency.setValueAtTime(f, t0 + i * 0.15);
        g.gain.setValueAtTime(0.0001, t0 + i * 0.15);
        g.gain.exponentialRampToValueAtTime(0.22, t0 + i * 0.15 + 0.02);
        g.gain.exponentialRampToValueAtTime(0.0001, t0 + i * 0.15 + 0.14);
        o.connect(g); g.connect(ctx.destination);
        o.start(t0 + i * 0.15); o.stop(t0 + i * 0.15 + 0.16);
      });
    } catch (e) {}
  }

  /* 系统通知（浏览器 API 兜底，桌面 WebView2 / Edge 内可用；不可用时由应用内浮层兜底） */
  function systemNotify(title, body) {
    try {
      if (!("Notification" in window)) return false;
      if (Notification.permission === "granted") {
        new Notification(title, { body, silent: true });
        return true;
      }
      if (Notification.permission !== "denied") {
        Notification.requestPermission().then((p) => {
          if (p === "granted") { try { new Notification(title, { body, silent: true }); } catch (e) {} }
        }).catch(() => {});
      }
    } catch (e) {}
    return false;
  }

  /* v1.5 系统通知主通道：桌面版优先走本地后端 /api/toast（Windows 右下角 Toast，
     全屏游戏也能看到，点击可把监控窗口带到前台）；网页版/无后端时回退浏览器通知 */
  async function nativeNotify(title, body) {
    if (HISTORY.base) {
      try {
        await postJSON(HISTORY.base + "/api/toast", { title, body }, 5000);
        return true;
      } catch (e) {}
    }
    return systemNotify(title, body);
  }

  /* 应用内浮层提醒（窗口内可见，带自动消失） */
  function toast(entry) {
    const box = $("toasts");
    if (!box) return;
    const el = document.createElement("div");
    el.className = "toast " + (entry.dir === "DOWN" ? "down" : "up");
    el.innerHTML = '<div class="tt">' + esc(entry.title) + '</div><div class="tb">' + esc(entry.body) + '</div>' +
                   '<button class="tx" type="button" aria-label="关闭">×</button>';
    el.querySelector(".tx").onclick = () => el.remove();
    box.appendChild(el);
    setTimeout(() => { el.classList.add("out"); setTimeout(() => el.remove(), 420); }, 9000);
    while (box.children.length > 6) box.removeChild(box.firstChild);
  }

  const KIND_LABEL = {
    BURST_UP: "暴涨异动", BURST_DOWN: "暴跌异动",
    ALPHA_UP: "Alpha 暴涨", ALPHA_DOWN: "Alpha 暴跌",
    LIQ: "大额强平", BIG: "大额成交"
  };

  function fireNotify(rec) {
    if (!Number(notify.enabled)) return;
    const now = Date.now();
    if (now - notiState.lastAt < Math.max(0, Number(notify.gapSec) || 0) * 1000) return;      // 防刷屏：最小间隔
    notiState.minute = notiState.minute.filter(t => now - t < 60000);
    if (notiState.minute.length >= Math.max(1, Number(notify.maxPerMin) || 20)) return;        // 防刷屏：每分钟上限
    const kkey = rec.market + "|" + rec.symbol + "|" + rec.kind;
    if (now - (notiState.keyAt.get(kkey) || 0) < NOTIFY_KEY_COOLDOWN_MS) return;              // 防刷屏：同币种冷却
    notiState.lastAt = now;
    notiState.minute.push(now);
    notiState.keyAt.set(kkey, now);
    const mktName = (state.markets[rec.market] || {}).name || rec.market;
    const title = (KIND_LABEL[rec.kind] || "异动") + " · " + rec.symbol;
    const body = [mktName,
                  rec.windowPct === null || rec.windowPct === undefined ? "" : "窗口 " + fmtPct(rec.windowPct),
                  rec.info || "",
                  rec.price ? "@" + fmtPrice(rec.price) : ""].filter(Boolean).join(" · ");
    if (Number(notify.sound)) beep(rec.dir !== "DOWN");
    if (Number(notify.system)) nativeNotify(title, body);
    toast({ dir: rec.dir, title, body });
  }

  /* ---------- 异动历史（后端 SQLite 持久化，无后端时降级浏览器本地存储） ---------- */
  function scoreNotional(v) {
    v = Number(v) || 0;
    if (v >= 1e7) return 95;
    if (v >= 5e6) return 88;
    if (v >= 2e6) return 80;
    if (v >= 1e6) return 70;
    if (v >= 5e5) return 60;
    if (v >= 2.5e5) return 52;
    return 45;
  }

  /* 把报警事件整理成可持久化的历史记录（时间/市场/币种/窗口涨跌/24h/成交额/评分/等级/触发类型） */
  function buildRecord(a) {
    const rec = (state.markets[a.market] && state.markets[a.market].records.get(a.symbol)) || {};
    let kind = a.kind;
    if (!kind) {
      if (a.type === "BURST") {
        kind = a.market === "alpha"
          ? (a.dir === "UP" ? "ALPHA_UP" : "ALPHA_DOWN")
          : (a.dir === "UP" ? "BURST_UP" : "BURST_DOWN");
      } else {
        kind = a.type || "BURST_UP";
      }
    }
    const ts = a.ts || Date.now();
    const vol = Number(a.vol) || 0;
    const score = Number(a.score) || ((a.type === "LIQ" || a.type === "BIG") ? scoreNotional(vol) : 0);
    return {
      key: a.market + "|" + a.symbol + "|" + kind + "|" + Math.floor(ts / 300000),
      ts,
      market: a.market,
      symbol: a.symbol,
      kind,
      windowPct: (a.chg === null || a.chg === undefined) ? null : Number(a.chg),
      chg24: (rec.chg24 === null || rec.chg24 === undefined) ? null : Number(rec.chg24),
      quoteVol: vol || (Number(rec.quoteVol) || null),
      price: Number(a.price) || (Number(rec.price) || null),
      score,
      level: a.level || levelOf(score),
      info: a.info || ""
    };
  }

  const HISTORY = {
    mode: "local",        // server=后端 SQLite 持久化；local=浏览器本地存储兜底
    base: "", db: "", queue: [], timer: null, local: [],
    LS_KEY: "radar.history.local", error: "", lastSync: 0
  };

  async function postJSON(url, body, timeout = 12000) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeout);
    try {
      const r = await fetch(url, {
        method: "POST", cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body), signal: ctrl.signal
      });
      if (!r.ok) throw new Error(url + " HTTP " + r.status);
      return await r.json();
    } finally { clearTimeout(t); }
  }

  async function initHistory() {
    for (const base of API_CANDIDATES) {
      try {
        const d = await getJSON(base + "/api/history?limit=1", 6000);
        if (d && Array.isArray(d.items)) {
          HISTORY.mode = "server";
          HISTORY.base = base;
          HISTORY.db = d.db || "";
          return;
        }
      } catch (e) { /* 试下一个候选地址 */ }
    }
    HISTORY.mode = "local";
    try { HISTORY.local = JSON.parse(localStorage.getItem(HISTORY.LS_KEY) || "[]") || []; } catch (e) { HISTORY.local = []; }
    log("未检测到后端，历史记录暂存于浏览器本地存储");
  }

  function historyPush(rec) {
    if (HISTORY.mode === "server") {
      HISTORY.queue.push(rec);
      if (HISTORY.queue.length > 500) HISTORY.queue.splice(0, HISTORY.queue.length - 500);
      if (!HISTORY.timer) HISTORY.timer = setTimeout(flushHistory, 1500);
    } else {
      HISTORY.local.unshift(rec);
      if (HISTORY.local.length > 2000) HISTORY.local.length = 2000;
      try { localStorage.setItem(HISTORY.LS_KEY, JSON.stringify(HISTORY.local)); } catch (e) {}
    }
  }

  async function flushHistory() {
    HISTORY.timer = null;
    if (HISTORY.mode !== "server" || !HISTORY.queue.length) return;
    const items = HISTORY.queue.splice(0, 100);
    try {
      const d = await postJSON(HISTORY.base + "/api/history", { items });
      if (d && d.db) HISTORY.db = d.db;
      HISTORY.error = (d && d.ok === false) ? (d.error || "后端写入失败") : "";
      HISTORY.lastSync = Date.now();
    } catch (e) {
      HISTORY.error = e.message || String(e);      // 失败回队，下次重试，避免丢记录
      HISTORY.queue = items.concat(HISTORY.queue);
      if (HISTORY.queue.length > 500) HISTORY.queue.splice(0, HISTORY.queue.length - 500);
    }
    if (HISTORY.queue.length) HISTORY.timer = setTimeout(flushHistory, 4000);
  }

  async function listHistory(opts) {
    if (HISTORY.mode === "server") {
      const qs = new URLSearchParams();
      qs.set("limit", String(opts.limit || 200));
      if (opts.kind) qs.set("kind", opts.kind);
      if (opts.market) qs.set("market", opts.market);
      if (opts.q) qs.set("q", opts.q);
      const d = await getJSON(HISTORY.base + "/api/history?" + qs.toString(), 12000);
      if (d && d.db) HISTORY.db = d.db;
      return { items: (d && d.items) || [], total: (d && d.total) || 0 };
    }
    let arr = HISTORY.local.slice();
    if (opts.kind) arr = arr.filter(x => x.kind === opts.kind);
    if (opts.market) arr = arr.filter(x => x.market === opts.market);
    if (opts.q) {
      const q = opts.q.toUpperCase();
      arr = arr.filter(x => String(x.symbol || "").toUpperCase().indexOf(q) >= 0 ||
                            String(x.info || "").toUpperCase().indexOf(q) >= 0);
    }
    return { items: arr.slice(0, opts.limit || 200), total: arr.length };
  }

  async function clearHistory() {
    if (HISTORY.mode === "server") {
      const d = await postJSON(HISTORY.base + "/api/history/clear", {});
      return (d && d.cleared) || 0;
    }
    const n = HISTORY.local.length;
    HISTORY.local = [];
    try { localStorage.removeItem(HISTORY.LS_KEY); } catch (e) {}
    return n;
  }

  let histBusy = false, histCache = { items: [], total: 0 };

  function renderHistory() {
    const items = histCache.items || [];
    $("histBody").innerHTML = items.map(r => {
      const wCls = (r.windowPct === null || r.windowPct === undefined) ? "" : (Number(r.windowPct) >= 0 ? "green" : "red");
      const cCls = (r.chg24 === null || r.chg24 === undefined) ? "" : (Number(r.chg24) >= 0 ? "green" : "red");
      return `<tr><td class="muted">${esc(new Date(r.ts).toLocaleString("zh-CN", { hour12: false }))}</td>` +
        `<td>${esc(KIND_LABEL[r.kind] || r.kind || "—")}</td>` +
        `<td>${MKT_TAG[r.market] || esc(r.market || "")}</td>` +
        `<td><b>${esc(r.symbol || "")}</b></td>` +
        `<td class="${wCls}">${fmtPct(r.windowPct)}</td>` +
        `<td class="${cCls}">${fmtPct(r.chg24)}</td>` +
        `<td>${r.quoteVol ? fmtMoney(r.quoteVol) : "—"}</td>` +
        `<td class="yellow">${r.score || "—"}</td>` +
        `<td>${esc(r.level || "—")}</td>` +
        `<td class="muted">${esc(r.info || "")}</td></tr>`;
    }).join("") || `<tr><td colspan="10" class="empty">暂无历史记录</td></tr>`;
    $("histMeta").textContent = "共 " + (histCache.total || 0) + " 条 · 当前显示 " + items.length +
      (HISTORY.mode === "server" ? " · 待写入 " + HISTORY.queue.length : "");
    const nt = $("histNotice");
    if (nt) {
      nt.textContent = HISTORY.mode === "server"
        ? "历史记录由本地后端持久化保存（SQLite）：" + (HISTORY.db || "—") + "，软件重启后仍可查看。" +
          (HISTORY.error ? "（最近一次写入异常：" + HISTORY.error + "）" : "")
        : "未检测到本地后端，历史记录暂存于浏览器本地存储（localStorage），仅本设备可见；" +
          "运行 app.py 启动后端后会自动切换为后端持久化。";
    }
  }

  async function refreshHistory() {
    if (histBusy) return;
    histBusy = true;
    try {
      histCache = await listHistory({
        limit: Number(($("histLimit") || {}).value) || 200,
        kind: ($("histKind") || {}).value || "",
        market: ($("histMarket") || {}).value || "",
        q: (($("histSearch") || {}).value || "").trim()
      });
      renderHistory();
    } catch (e) {
      const m = $("histMeta");
      if (m) m.textContent = "读取历史失败：" + (e.message || e);
    } finally { histBusy = false; }
  }

  /* ---------- 报警 ---------- */
  function addAlert(a) {
    a.id = ++state.alertSeq;
    pushLimited(state.alerts, a, 300);
    const rec = buildRecord(a);
    historyPush(rec);       // 后台持久化
    fireNotify(rec);        // 即时提醒
  }

  /* ---------- 异动检测 ---------- */
  function scanBursts() {
    const winMs = state.params.windowMin * 60000;
    const now = Date.now();
    for (const mk of ["futures", "spot", "alpha"]) {
      const st = state.markets[mk];
      for (const rec of st.records.values()) {
        if (!rec.price) continue;
        const w = windowChange(rec, winMs);
        if (w.chg === null || !w.ready) continue;
        const hit = w.chg >= state.params.upPct || w.chg <= state.params.downPct || (mk === "alpha" && Math.abs(w.chg) >= state.params.alphaPct);
        const key = mk + "|" + rec.symbol;
        if (hit) {
          const dir = w.chg >= 0 ? "UP" : "DOWN";
          state.bursts.set(key, { market: mk, symbol: rec.symbol, name: rec.name, chg: w.chg, price: rec.price, dir, ts: now, score: scoreBurst(w.chg, null, mk === "futures" ? (state.oi.get(rec.symbol) || {}).changePct : null) });
          const prev = (st.lastAlert || {})[key] || 0;
          if (now - prev > 300000) {
            st.lastAlert = st.lastAlert || {};
            st.lastAlert[key] = now;
            const oi = mk === "futures" ? (state.oi.get(rec.symbol) || {}).changePct : null;
            const sc = scoreBurst(w.chg, null, oi);
            addAlert({ type: "BURST", market: mk, symbol: rec.symbol, dir, chg: w.chg, price: rec.price, score: sc, level: levelOf(sc), info: "窗口 " + state.params.windowMin + "m · " + quadrant(w.chg, oi), ts: now });
          }
        } else if (state.bursts.has(key)) {
          const b = state.bursts.get(key);
          if (now - b.ts > winMs) state.bursts.delete(key);
        }
      }
    }
    // burst 过期清理
    for (const [k, b] of state.bursts) {
      if (now - b.ts > Math.max(winMs * 2, 600000)) state.bursts.delete(k);
    }
  }

  /* ---------- 合约 ---------- */
  async function initFutures() {
    // 无代理 / VPN 时合约接口不可达：直接标注状态并跳过，避免长时间卡在拉取列表
    if (NET.futOk === false) { markFuturesUnavailable(); return; }
    setStatus("futures", "拉取列表…", "");
    try {
      const tick = await getJSON(REST_FUT + "/fapi/v1/ticker/24hr", 25000);
      const set = new Set();
      for (const x of tick) {
        if (!x.symbol || !x.symbol.endsWith("USDT") || x.symbol.indexOf("_") >= 0) continue;
        set.add(x.symbol);
      }
      const syms = [...set];
      const st = state.markets.futures;
      st.symbols = syms;
      st.symbolSet = set;
      st.symbolCount = syms.length;
      $("futCount").textContent = syms.length;
      $("mFut").textContent = syms.length;

      const now = Date.now();
      for (const x of tick) {
        if (!set.has(x.symbol)) continue;
        const rec = ensureRecord("futures", x.symbol);
        updateRecord(rec, x.lastPrice, now, {
          open: Number(x.openPrice), high: Number(x.highPrice), low: Number(x.lowPrice),
          vol24: Number(x.volume), quoteVol: Number(x.quoteVolume),
          chg24: Number(x.priceChangePercent)
        });
      }
      buildShards("futures");
      connectFuturesWS();
      loadPremium();
      log("合约市场初始化完成：" + syms.length + " 个 USDT 永续");
    } catch (e) {
      setStatus("futures", "REST 失败", "bad");
      log("合约初始化失败：" + e.message);
    }
  }

  function connectFuturesWS() {
    const st = state.markets.futures;
    if (st.ws) { try { st.ws.close(); } catch (e) {} }
    const ws = new WebSocket(WS_FUT + "!miniTicker@arr/!forceOrder@arr");
    st.ws = ws;
    ws.onopen = () => { setStatus("futures", "实时", "ok"); };
    ws.onclose = () => { setStatus("futures", "断开·重连", "bad"); setTimeout(connectFuturesWS, 4000); };
    ws.onerror = () => { setStatus("futures", "异常", "bad"); };
    ws.onmessage = (ev) => {
      let m; try { m = JSON.parse(ev.data); } catch (e) { return; }
      st.lastMsgTs = Date.now();
      const d = m.data || m;
      if (Array.isArray(d)) { for (const x of d) onMiniTicker("futures", x); }
      else if (d.e === "24hrMiniTicker") onMiniTicker("futures", d);
      else if (d.e === "forceOrder") onForceOrder(d);
    };
  }

  function onMiniTicker(market, x) {
    if (!x || !x.s || !x.c) return;
    if (market === "futures") {
      const st = state.markets.futures;
      if (st.symbols.length && !st.symbolSet) st.symbolSet = new Set(st.symbols);
      if (st.symbolSet && !st.symbolSet.has(x.s)) return;
    } else if (market === "spot") {
      if (!x.s.endsWith("USDT")) return;
      const st = state.markets.spot;
      if (st.symbolSet && !st.symbolSet.has(x.s)) return;
    }
    const ts = Number(x.E) || Date.now();
    const rec = ensureRecord(market, x.s);
    updateRecord(rec, x.c, ts, {
      open: Number(x.o) || rec.open,
      high: Number(x.h) || rec.high,
      low: Number(x.l) || rec.low,
      vol24: Number(x.v) || rec.vol24,
      quoteVol: Number(x.q) || rec.quoteVol
    });
  }

  function onForceOrder(d) {
    const o = d.o || {};
    if (!o.s || !o.p || !o.q) return;
    const notional = Number(o.p) * Number(o.q);
    if (!(notional >= state.params.liqUsd)) return;
    const item = { ts: Number(d.E) || Date.now(), symbol: o.s, side: o.S, price: Number(o.p), notional };
    pushLimited(state.liquidations, item, 300);
    if (notional >= state.params.liqUsd * 5) {
      addAlert({ type: "LIQ", market: "futures", symbol: o.s, dir: o.S === "SELL" ? "DOWN" : "UP", chg: null, price: item.price, score: 0, vol: notional, info: (o.S === "SELL" ? "多单爆仓 " : "空单爆仓 ") + fmtMoney(notional) + " USDT", ts: item.ts });
    }
  }

  async function loadPremium() {
    try {
      const rows = await getJSON(REST_FUT + "/fapi/v1/premiumIndex");
      const ts = Date.now();
      for (const x of rows) {
        if (!x.symbol || !x.symbol.endsWith("USDT")) continue;
        state.deriv.set(x.symbol, { funding: Number(x.lastFundingRate), mark: Number(x.markPrice), ts });
      }
    } catch (e) { /* ignore */ }
  }

  async function pollOI(list) {
    const arr = list.slice(0, 20);
    for (const s of arr) {
      try {
        const x = await getJSON(REST_FUT + "/fapi/v1/openInterest?symbol=" + s, 8000);
        const cur = Number(x.openInterest);
        const prev = state.oi.get(s);
        const changePct = prev && prev.value ? (cur / prev.value - 1) * 100 : null;
        state.oi.set(s, { value: cur, changePct, ts: Date.now() });
      } catch (e) { /* ignore */ }
      await new Promise(r => setTimeout(r, 40));
    }
  }

  /* ---------- REST 兜底轮询（官方 WS 推送不可用时自动接管，保证数据不断档） ---------- */
  let futPriceBusy = false, fut24hBusy = false, spotPriceBusy = false, futRestErr = 0;

  function applyTick(market, x) {
    const rec = ensureRecord(market, x.symbol);
    rec.high = Number(x.highPrice) || rec.high;
    rec.low = Number(x.lowPrice) || rec.low;
    rec.vol24 = Number(x.volume) || rec.vol24;
    rec.quoteVol = Number(x.quoteVolume) || rec.quoteVol;
    rec.trades = Number(x.count) || rec.trades;
    const cp = Number(x.priceChangePercent);
    if (Number.isFinite(cp)) rec.chg24 = cp;
    updateRecord(rec, x.lastPrice, Date.now(), null);
  }

  async function pollFuturesPrice() {
    if (futPriceBusy) return;
    futPriceBusy = true;
    try {
      const arr = await getJSON(REST_FUT + "/fapi/v1/ticker/price", 10000);
      const ts = Date.now();
      let n = 0;
      for (const t of arr) {
        if (!t.symbol.endsWith("USDT") || t.symbol.indexOf("_") >= 0) continue;
        updateRecord(ensureRecord("futures", t.symbol), t.price, ts, null);
        n++;
      }
      state.markets.futures.symbolCount = n;
      futRestErr = 0;
    } catch (e) {
      futRestErr++;
      if (futRestErr % 10 === 1) log("合约价格 REST 轮询失败：" + e.message);
    } finally { futPriceBusy = false; }
  }

  async function pollFutures24h() {
    if (fut24hBusy) return;
    fut24hBusy = true;
    const st = state.markets.futures;
    try {
      const arr = await getJSON(REST_FUT + "/fapi/v1/ticker/24hr", 25000);
      let n = 0;
      for (const x of arr) {
        if (!x.symbol.endsWith("USDT") || x.symbol.indexOf("_") >= 0) continue;
        applyTick("futures", x); n++;
      }
      st.symbolCount = n;
      st.symbols = [...st.records.keys()];
      st.symbolSet = new Set(st.symbols);
      $("futCount").textContent = n; $("mFut").textContent = n;
      log("合约 24h 快照同步完成：" + n + " 个 USDT 永续");
    } catch (e) {
      log("合约 24h 快照失败：" + e.message);
    } finally { fut24hBusy = false; }
  }

  async function pollSpotPrice() {
    if (spotPriceBusy) return;
    spotPriceBusy = true;
    const st = state.markets.spot;
    try {
      const arr = await getJSON(REST_SPOT + "/api/v3/ticker/24hr", 30000);
      let n = 0;
      for (const x of arr) {
        if (!x.symbol.endsWith("USDT")) continue;
        applyTick("spot", x); n++;
      }
      st.symbolCount = n;
    } catch (e) { /* 静默重试 */ }
    finally { spotPriceBusy = false; }
  }

  /* 启动预热：用 1 分钟 K 线补齐窗口历史，避免刚打开时要等 N 分钟才出现窗口涨跌 */
  async function warmup(market) {
    const st = state.markets[market];
    const isFut = market === "futures";
    const base = (isFut ? REST_FUT + "/fapi/v1/klines" : REST_SPOT + "/api/v3/klines");
    const list = st.symbols.slice();
    let idx = 0, ok = 0, bad = 0;
    async function worker() {
      while (idx < list.length) {
        const s = list[idx++];
        try {
          const ks = await getJSON(base + "?symbol=" + s + "&interval=1m&limit=6", 8000);
          const rec = st.records.get(s);
          if (!rec || !ks || ks.length < 2) { bad++; continue; }
          const pts = [];
          for (const k of ks) {
            const t = Number(k[0]), c = Number(k[4]);
            if (t > 0 && c > 0) pts.push([t, c]);
          }
          if (pts.length < 2) { bad++; continue; }
          rec.hist = pts;
          if (rec.price) rec.hist.push([Date.now(), rec.price]);
          ok++;
        } catch (e) { bad++; }
      }
    }
    await Promise.all(Array.from({ length: 10 }, worker));
    log((isFut ? "合约" : "现货") + "窗口历史预热：" + ok + " 个币种" + (bad ? "，" + bad + " 个无数据" : ""));
  }

  /* 数据源自适应：WS 有推送就走 WS，静默超过阈值即切换 REST 轮询，恢复后自动切回 */
  function adaptSources() {
    const now = Date.now();
    netHealth();                                   // 线路巡检：代理恢复 / 断开时自动择优切换
    const fu = state.markets.futures, sp = state.markets.spot;

    if (fu.symbols.length && NET.futOk !== false) {
      const alive = fu.lastMsgTs > 0 && (now - fu.lastMsgTs) < WS_DEAD_MS;
      if (alive) {
        if (fu.source !== "ws") { fu.source = "ws"; setStatus("futures", "实时", "ok"); }
      } else {
        fu.source = "rest";
        setStatus("futures", "轮询 " + (REST_FUT_PRICE_MS / 1000) + "s", "");
        if (now >= fu.nextPrice) { fu.nextPrice = now + REST_FUT_PRICE_MS; pollFuturesPrice(); }
        if (now >= fu.next24h) { fu.next24h = now + REST_FUT_24H_MS; pollFutures24h(); }
      }
    }

    if (sp.symbols.length) {
      const alive = sp.lastMsgTs > 0 && (now - sp.lastMsgTs) < WS_DEAD_MS;
      if (alive) {
        if (sp.source !== "ws") { sp.source = "ws"; setStatus("spot", "实时", "ok"); }
      } else {
        sp.source = "rest";
        setStatus("spot", "轮询 " + (REST_SPOT_MS / 1000) + "s", "");
        if (now >= sp.nextPrice) { sp.nextPrice = now + REST_SPOT_MS; pollSpotPrice(); }
      }
    }

    // 逐笔流（大单）健康检查：长时间无消息即判定该市场不可用，停止轮换
    for (const mk of ["futures", "spot"]) {
      const st = state.markets[mk];
      if (st.aggSupported === false) continue;
      if (st.aggWs && st.aggOpenAt && now - st.aggOpenAt > AGG_DEAD_MS && now - st.aggLastMsgTs > AGG_DEAD_MS) {
        st.aggSupported = false;
        try { st.aggWs.close(); } catch (e) {}
        st.aggWs = null;
        log((mk === "futures" ? "合约" : "现货") + "逐笔成交流无推送，已停用该源的大单订阅");
      }
    }
    // 合约逐笔流不可用时，把大单订阅资源全部让给现货，缩短轮换周期以扩大覆盖
    const fuAgg = state.markets.futures.aggSupported === false;
    $("bigSub").textContent = fuAgg ? "现货分片 " + state.markets.spot.shardText : "分片 " + state.markets.futures.shardText;
  }

  /* ---------- 现货 ---------- */
  async function initSpot() {
    setStatus("spot", "拉取列表…", "");
    try {
      const tick = await getJSON(REST_SPOT + "/api/v3/ticker/24hr", 30000);
      const syms = [...new Set(tick.map(x => x.symbol).filter(s => s && s.endsWith("USDT")))];
      const st = state.markets.spot;
      st.symbols = syms;
      st.symbolSet = new Set(syms);
      st.symbolCount = syms.length;
      $("spotCount").textContent = syms.length;
      $("mSpot").textContent = syms.length;

      const now = Date.now();
      for (const x of tick) {
        if (!x.symbol.endsWith("USDT") || !st.symbolSet.has(x.symbol)) continue;
        const rec = ensureRecord("spot", x.symbol);
        updateRecord(rec, x.lastPrice, now, {
          open: Number(x.openPrice), high: Number(x.highPrice), low: Number(x.lowPrice),
          vol24: Number(x.volume), quoteVol: Number(x.quoteVolume)
        });
      }
      buildShards("spot");
      connectSpotWS();
      log("现货市场初始化完成：" + syms.length + " 个 USDT 交易对");
    } catch (e) {
      setStatus("spot", "REST 失败", "bad");
      log("现货初始化失败：" + e.message);
    }
  }

  function connectSpotWS() {
    const st = state.markets.spot;
    if (st.ws) { try { st.ws.close(); } catch (e) {} }
    const ws = new WebSocket(WS_SPOT + "!miniTicker@arr");
    st.ws = ws;
    ws.onopen = () => setStatus("spot", "实时", "ok");
    ws.onclose = () => { setStatus("spot", "断开·重连", "bad"); setTimeout(connectSpotWS, 4000); };
    ws.onerror = () => setStatus("spot", "异常", "bad");
    ws.onmessage = (ev) => {
      let m; try { m = JSON.parse(ev.data); } catch (e) { return; }
      st.lastMsgTs = Date.now();
      const d = m.data || m;
      if (Array.isArray(d)) { for (const x of d) onMiniTicker("spot", x); }
      else if (d.e === "24hrMiniTicker") onMiniTicker("spot", d);
    };
  }

  /* ---------- 大单（分片轮换） ---------- */
  function buildShards(market) {
    const st = state.markets[market];
    const list = st.symbols.slice();
    list.sort((a, b) => {
      const ra = st.records.get(a), rb = st.records.get(b);
      return ((rb && rb.quoteVol) || 0) - ((ra && ra.quoteVol) || 0);
    });
    const shards = [];
    for (let i = 0; i < list.length; i += SHARD_SIZE) shards.push(list.slice(i, i + SHARD_SIZE));
    st.shards = shards;
    st.shardIdx = 0;
  }

  function connectAggShard(market) {
    const st = state.markets[market];
    if (st.aggSupported === false) return;     // 该市场的逐笔流不可用，跳过
    if (!st.shards.length) return;
    const idx = st.shardIdx % st.shards.length;
    const list = st.shards[idx];
    st.shardIdx++;
    st.shardText = (idx + 1) + "/" + st.shards.length;
    if (market === "futures") $("bigSub").textContent = "分片 " + st.shardText;
    if (st.aggWs) { try { st.aggWs.close(); } catch (e) {} }
    const base = market === "futures" ? WS_FUT : WS_SPOT;
    const url = base + list.map(s => s.toLowerCase() + "@aggTrade").join("/");
    let ws;
    try { ws = new WebSocket(url); } catch (e) { return; }
    st.aggWs = ws;
    st.aggOpenAt = Date.now();
    st.aggLastMsgTs = 0;
    ws.onmessage = (ev) => {
      st.aggLastMsgTs = Date.now();
      let m; try { m = JSON.parse(ev.data); } catch (e) { return; }
      const d = m.data || m;
      if (!d || d.e !== "aggTrade") return;
      const price = Number(d.p), qty = Number(d.q);
      const notional = price * qty;
      if (!(notional >= state.params.largeUsd)) return;
      const side = d.m ? "SELL" : "BUY";
      const ts = Number(d.T) || Date.now();
      pushLimited(state.largeTrades, { ts, market, symbol: d.s, price, notional, side }, 300);
      if (notional >= state.params.largeUsd * 8) {
        addAlert({ type: "BIG", market, symbol: d.s, dir: side === "BUY" ? "UP" : "DOWN", chg: null, price, score: 0, vol: notional, info: (side === "BUY" ? "主动买 " : "主动卖 ") + fmtMoney(notional) + " USDT", ts });
      }
    };
  }

  /* ---------- Alpha（本地/自建后端，必要时直连兜底） ---------- */
  let alphaTimer = null;

  async function getAlpha(win) {
    let lastErr = null;
    for (const base of API_CANDIDATES) {
      try {
        const d = await getJSON(base + "/api/alpha?win=" + win, 12000);
        if (d && (d.tokens || d.count !== undefined)) { d.__src = base || "local"; return d; }
      } catch (e) { lastErr = e; }
    }
    // 降级：直连币安 Alpha 公共接口（浏览器跨域策略允许时可用，无后端也能看到部分数据）
    const raw = await getJSON(ALPHA_DIRECT_URL, 15000);
    const rows = (raw && raw.data) || [];
    const best = new Map();
    const now = Date.now();
    for (const x of rows) {
      const sym = String(x.symbol || "").trim();
      const price = Number(x.price) || 0;
      if (!sym || !(price > 0)) continue;
      const vol = Number(x.volume24h) || 0;
      const item = {
        symbol: sym, name: String(x.name || ""), chain: String(x.chainName || ""),
        price, chg24: Number(x.percentChange24h) || 0, vol24: vol,
        mcap: Number(x.marketCap) || 0, cex: !!x.listingCex, ts: now,
      };
      const prev = best.get(sym);
      if (!prev || vol > prev.vol24) best.set(sym, item);
    }
    const tokens = [...best.values()];
    if (!tokens.length) throw (lastErr || new Error("alpha empty"));
    return { tokens, count: tokens.length, ts: now, win, __src: "direct" };
  }

  async function pollAlpha() {
    try {
      const d = await getAlpha(state.params.windowMin);
      const st = state.markets.alpha;
      const now = Date.now();
      const chains = new Set();
      for (const t of (d.tokens || [])) {
        const rec = ensureRecord("alpha", t.symbol);
        rec.name = t.name; rec.chain = t.chain;
        rec.open = t.chg24 !== null && t.chg24 !== undefined ? null : rec.open;
        rec.chg24 = t.chg24;
        rec.quoteVol = t.vol24 || 0;
        rec.extra.mcap = t.mcap; rec.extra.cex = t.cex; rec.extra.wChg = t.wChg;
        if (t.chain) chains.add(t.chain);
        if (t.price) updateRecord(rec, t.price, now, { chg24: t.chg24 !== null && t.chg24 !== undefined ? t.chg24 : ((t.price && t.open) ? (t.price / t.open - 1) * 100 : rec.chg24) });
      }
      st.symbols = [...st.records.keys()];
      st.win = d.win;
      st.updatedAt = d.ts;
      const sel = $("alphaChain");
      if (sel && sel.options.length <= 1 && chains.size) {
        [...chains].sort().forEach(c => { const o = document.createElement("option"); o.value = c; o.textContent = c; sel.appendChild(o); });
      }
      $("alphaCount").textContent = (d.count || st.records.size);
      $("mAlpha").textContent = (d.count || st.records.size);
      $("alphaSub").textContent = "更新 " + clock(d.ts);
      const nAlpha = (d.count || st.records.size);
      const nt = $("alphaNotice");
      if (nt) nt.textContent = (d.__src === "direct")
        ? "Alpha 数据直连币安公共接口（未检测到后端，窗口涨跌不可用），共 " + nAlpha + " 个代币。"
        : "Alpha 数据由后端每 10 秒全量采集（当前 " + nAlpha + " 个代币，覆盖币安 Alpha 全部已上线代币），窗口涨跌基于后端采样的价格序列计算。";
      setStatus("alpha", "实时", "ok");
    } catch (e) {
      setStatus("alpha", "无数据源", "bad");
      $("alphaSub").textContent = "连接失败";
      const nt = $("alphaNotice");
      if (nt) nt.textContent = "Alpha 数据源不可用：浏览器直连被跨域策略拦截，且未检测到后端。请在 config.js 中把 window.RADAR_API_BASE 指向后端地址（桌面程序局域网模式启动时会打印该地址）。";
    }
  }

  /* ---------- 渲染 ---------- */
  const cur = () => state.markets[state.market];

  /* ---------- 策略位置（参考）：机械计算，仅供参考 ---------- */
  const strat = Object.assign({ enabled: 0, expandAll: 0 }, (() => {
    try { return JSON.parse(localStorage.getItem("radar.strat") || "{}"); } catch (e) { return {}; }
  })());
  function saveStrat() { localStorage.setItem("radar.strat", JSON.stringify({ enabled: strat.enabled, expandAll: strat.expandAll })); }
  const stratOpen = new Set();          // 已展开依据的异动条目
  const KL = new Map();                 // symbolKey -> { at, k:[{t,c,h,l}], src }
  const KL_TTL = 180000;
  let klBusy = 0;

  function fetchKlines(market, symbol) {
    const useFut = (market === "futures" && NET.futOk !== false);
    const url = useFut ? REST_FUT + "/fapi/v1/klines?symbol=" + symbol + "&interval=1m&limit=120"
      : REST_SPOT + "/api/v3/klines?symbol=" + symbol + "&interval=1m&limit=120";
    const src = useFut ? "合约 1 分钟 K 线"
      : (market === "futures" ? "现货 1 分钟 K 线（合约接口当前不可达，以现货同币种替代参考）" : "现货 1 分钟 K 线");
    return getJSON(url, 10000).then((ks) => ({
      src: src,
      k: (ks || []).map(x => ({ t: Number(x[0]), h: Number(x[2]), l: Number(x[3]), c: Number(x[4]) })).filter(x => x.c > 0)
    }));
  }
  function ensureStrategyData() {
    if (!Number(strat.enabled)) return;
    const list = [...state.bursts.values()]
      .filter(b => b.market === state.market && b.market !== "alpha")
      .sort((a, b) => Math.abs(b.chg) - Math.abs(a.chg)).slice(0, 40);
    const now = Date.now();
    for (const b of list) {
      if (klBusy >= 4) break;
      const key = b.market + "|" + b.symbol;
      const cur = KL.get(key);
      if (cur && (now - cur.at < KL_TTL || (cur.loading && now - cur.at < 20000))) continue;
      KL.set(key, { at: now, loading: true, k: (cur && cur.k) || [], src: (cur && cur.src) || "K 线加载中" });
      klBusy++;
      fetchKlines(b.market, b.symbol).then((r) => {
        KL.set(key, r.k.length >= 20 ? { at: Date.now(), k: r.k, src: r.src } : { at: Date.now(), k: [], src: "K 线数据不足" });
      }).catch(() => {
        KL.set(key, { at: Date.now(), k: [], src: "K 线接口不可用" });
      }).then(() => { klBusy--; });
    }
  }
  /* 展开「依据」时按需补拉该币种 K 线：后台预热只覆盖异动最强的 40 个币种 */
  function stratEnsureForKey(key) {
    const parts = String(key).split("|");
    const market = parts[0], symbol = parts[1];
    if (!market || !symbol || market === "alpha") return;
    const k = market + "|" + symbol;
    const cur = KL.get(k);
    if (cur && cur.k && cur.k.length >= 20) return;
    if (cur && cur.loading && Date.now() - cur.at < 20000) return;
    KL.set(k, { at: Date.now(), loading: true, k: (cur && cur.k) || [], src: (cur && cur.src) || "K 线加载中" });
    fetchKlines(market, symbol).then((r) => {
      KL.set(k, r.k.length >= 20 ? { at: Date.now(), k: r.k, src: r.src } : { at: Date.now(), k: [], src: "K 线数据不足" });
    }).catch(() => {
      KL.set(k, { at: Date.now(), k: [], src: "K 线接口不可用" });
    }).then(() => { renderAll(); });
  }
  function avgOf(arr) { return arr.length ? arr.reduce((a, x) => a + x, 0) / arr.length : null; }
  function spKey(b) { return b.market + "|" + b.symbol + "|" + b.dir; }

  /* 推荐买入位置：以回踩（急涨）/ 支撑（急跌）锚点为中枢，按平均分钟波动限幅成区间 */
  function strategyPosition(b, rec) {
    const p = Number(rec.price || b.price);
    if (!Number.isFinite(p) || p <= 0) return null;
    const winMin = Math.max(1, state.params.windowMin);
    const winPct = Number(b.chg) || 0;
    const base = (1 + winPct / 100) > 0 ? p / (1 + winPct / 100) : p;
    const kl = KL.get(b.market + "|" + b.symbol);
    let series = [], srcText = "";
    if (kl && kl.k && kl.k.length >= 20) { series = kl.k.map(x => x.c); srcText = kl.src; }
    else if (rec.hist && rec.hist.length >= 30) { series = rec.hist.map(x => x[1]); srcText = "会话实时采样价格（K 线尚未返回）"; }
    else { series = []; srcText = "仅现价与窗口涨跌（数据不足，位置为粗略推算）"; }
    const ma20 = series.length >= 10 ? avgOf(series.slice(Math.max(0, series.length - 20))) : null;
    const ma60 = series.length >= 30 ? avgOf(series.slice(Math.max(0, series.length - 60))) : null;
    const seg = series.slice(Math.max(0, series.length - 60));
    const lo60 = seg.length ? Math.min.apply(null, seg) : null;
    const hi60 = seg.length ? Math.max.apply(null, seg) : null;
    const volPerMin = Math.max(0.3, Math.min(6, Math.abs(winPct) / winMin));
    const band = volPerMin / 100;
    const w = Math.max(0.004, Math.min(0.02, band * 1.2));
    const oi = b.market === "futures" ? ((state.oi.get(b.symbol) || {}).changePct ?? null) : null;
    let low, high, stop, batch, reason;
    if (b.dir === "UP") {
      let anchor = (ma20 || lo60) ? Math.max(ma20 || 0, lo60 || 0) : p * (1 - 3 * band);
      anchor = Math.min(anchor, p * 0.995);
      anchor = Math.max(anchor, p * (1 - Math.min(0.08, Math.max(0.015, 3 * band))));
      low = anchor * (1 - w); high = anchor * (1 + w * 0.7);
      stop = Math.min(low * (1 - Math.max(0.006, band * 1.5)), (lo60 || low) * 0.997);
      batch = "首笔 40% 在 " + fmtPrice(high) + " 附近（回踩触及区间上沿），第二笔 30% 在 " + fmtPrice(low)
        + " 附近（区间下沿），剩余 30% 留待回踩不破锚点且放量转强后再补；触及失效位不补仓。";
      reason = "急涨 " + fmtPct(winPct) + "（" + winMin + " 分钟），窗口起点价约 " + fmtPrice(base) + "；回踩锚点取 1 分钟 MA20 "
        + (ma20 ? fmtPrice(ma20) : "—") + " 与近 60 分钟低点 " + (lo60 ? fmtPrice(lo60) : "—")
        + " 中较高者，按平均分钟波动 " + volPerMin.toFixed(2) + "% 限幅（距现价不超过 8%）；区间为锚点上下 " + (w * 100).toFixed(2) + "% 内。"
        + (oi !== null ? " 合约 OI 变化 " + fmtPct(oi) + "。" : "");
    } else {
      let anchor = lo60 ? Math.min(lo60, p * 0.99) : p * (1 - 2 * band);
      anchor = Math.max(anchor, p * (1 - 0.12));
      low = anchor * (1 - w); high = anchor * (1 + w * 0.7);
      stop = Math.min(low * (1 - Math.max(0.008, band * 1.5)), (lo60 || low) * 0.99);
      batch = "首笔 30% 在 " + fmtPrice(high) + " 附近（支撑首次触碰并出现止跌信号时），第二笔 30% 在 " + fmtPrice(low)
        + " 附近，剩余 40% 留待 24h 低点附近或次日确认后再分批；跌破失效位立即离场，不摊平。";
      reason = "急跌 " + fmtPct(winPct) + "（" + winMin + " 分钟），窗口起点价约 " + fmtPrice(base) + "；支撑锚取近 60 分钟低点 "
        + (lo60 ? fmtPrice(lo60) : "—（数据不足，按平均分钟波动推算）") + " 与现价下方 1% 中较低者，按平均分钟波动 "
        + volPerMin.toFixed(2) + "% 限幅（距现价不超过 12%）；区间为支撑锚上 / 下 " + (w * 100).toFixed(2) + "% 内。"
        + (oi !== null ? " 合约 OI 变化 " + fmtPct(oi) + "。" : "");
    }
    if (!(low > 0) || !(high > low)) return null;
    return {
      low: low, high: high, stop: stop, ts: Date.now(),
      distLow: (low / p - 1) * 100, distHigh: (high / p - 1) * 100, stopPct: (stop / p - 1) * 100,
      reason: reason, batch: batch,
      basis: srcText + " · " + winMin + " 分钟窗口 · 现价 " + fmtPrice(p) + " · MA60 " + (ma60 ? fmtPrice(ma60) : "—")
        + " · 近 60 分钟高/低 " + (hi60 ? fmtPrice(hi60) : "—") + " / " + (lo60 ? fmtPrice(lo60) : "—")
        + " · 24h 高/低 " + (rec.high ? fmtPrice(rec.high) : "—") + " / " + (rec.low ? fmtPrice(rec.low) : "—")
        + " · 平均分钟波动 " + volPerMin.toFixed(2) + "%"
    };
  }
  function stratCellHtml(b, sp) {
    if (!sp) return '<span class="muted">—</span>';
    return '<div class="spzone ' + (b.dir === "UP" ? "up" : "down") + '">' + fmtPrice(sp.low) + " ~ " + fmtPrice(sp.high) + "</div>"
      + '<div class="spdist">距现价 ' + fmtPct(sp.distLow) + " ~ " + fmtPct(sp.distHigh) + "</div>"
      + '<button class="spbtn" data-sp="' + esc(spKey(b)) + '">依据</button>';
  }
  function stratDetailHtml(b, sp, cs) {
    return '<tr class="spdetail"><td colspan="' + cs + '">'
      + '<div class="sptitle">策略位置（参考）· ' + esc(b.symbol) + " · " + (b.dir === "UP" ? "急涨回踩场景" : "急跌支撑场景") + "</div>"
      + '<div class="spgrid">'
      + "<div><b>建议买入区间</b>" + fmtPrice(sp.low) + " ~ " + fmtPrice(sp.high) + "（距现价 " + fmtPct(sp.distLow) + " ~ " + fmtPct(sp.distHigh) + "）</div>"
      + "<div><b>失效 / 止损参考</b>" + fmtPrice(sp.stop) + "（距现价 " + fmtPct(sp.stopPct) + "）</div>"
      + "<div><b>判断依据</b>" + esc(sp.reason) + "</div>"
      + "<div><b>分批建议</b>" + esc(sp.batch) + "</div>"
      + "</div>"
      + '<div class="muted">数据基准：' + esc(sp.basis) + " · 计算时间 " + clock(sp.ts) + " · 以上为公开行情数据的机械推算，仅供参考，不构成投资建议。</div>"
      + "</td></tr>";
  }

  function renderBursts() {
    const winMs = state.params.windowMin * 60000;
    const list = [...state.bursts.values()].filter(b => b.market === state.market);
    const up = list.filter(b => b.dir === "UP").sort((a, b) => Math.abs(b.chg) - Math.abs(a.chg)).slice(0, 60);
    const dn = list.filter(b => b.dir === "DOWN").sort((a, b) => Math.abs(b.chg) - Math.abs(a.chg)).slice(0, 60);
    const on = !!Number(strat.enabled);
    const cs = on ? 8 : 7;
    for (const id of ["spThUp", "spThDown"]) { const th = $(id); if (th) th.style.display = on ? "" : "none"; }
    const row = (b) => {
      const rec = state.markets[b.market].records.get(b.symbol) || {};
      const cls = b.dir === "UP" ? "green" : "red";
      const sp = on ? strategyPosition(b, rec) : null;
      let html = `<tr><td>${MKT_TAG[b.market]}</td><td class="${cls}">${esc(b.symbol)}</td><td>${clock(b.ts)}</td>
        <td class="${cls}">${fmtPct(b.chg)}</td><td>${fmtPrice(b.price)}</td><td>${fmtMoney(rec.quoteVol)}</td>
        <td class="yellow">${b.score}</td>`;
      if (on) html += `<td class="spcell">${stratCellHtml(b, sp)}</td>`;
      html += "</tr>";
      if (on && sp && (stratOpen.has(spKey(b)) || Number(strat.expandAll))) html += stratDetailHtml(b, sp, cs);
      return html;
    };
    $("upBody").innerHTML = up.map(row).join("") || `<tr><td colspan="${cs}" class="empty">${emptyHint("暂无暴涨触发，持续扫描中…")}</td></tr>`;
    $("downBody").innerHTML = dn.map(row).join("") || `<tr><td colspan="${cs}" class="empty">${emptyHint("暂无暴跌触发，持续扫描中…")}</td></tr>`;
    $("upBadge").textContent = up.length;
    $("downBadge").textContent = dn.length;
    $("upCount").textContent = up.length;
    $("downCount").textContent = dn.length;
    const au = list.filter(b => b.dir === "UP").length, ad = list.filter(b => b.dir === "DOWN").length;
    $("upSub").textContent = `${cur().name} 涨 ${au}`;
    $("downSub").textContent = `${cur().name} 跌 ${ad}`;
  }

  function renderHeat() {
    const st = cur();
    const rows = [];
    for (const rec of st.records.values()) {
      if (!rec.price) continue;
      const chg24 = (rec.chg24 === null || rec.chg24 === undefined) ? null : Number(rec.chg24);
      if (chg24 === null) continue;
      rows.push({ rec, chg: chg24, vol: rec.quoteVol || 0 });
    }
    let up = 0, dn = 0;
    for (const r of rows) { if (r.chg > 0) up++; else if (r.chg < 0) dn++; }
    $("breadth").textContent = up + " / " + dn;
    $("breadth").className = "v " + (up >= dn ? "green" : "red");
    $("breadthSub").textContent = st.name + " · 覆盖 " + rows.length + " 币种";

    // v1.5：固定取 24h 涨跌幅绝对值前 40（集合固定，仅排序随下拉变化）
    const top = [...rows].sort((a, b) => Math.abs(b.chg) - Math.abs(a.chg)).slice(0, 40);
    const sort = $("heatSort").value;
    if (sort === "chgDesc") top.sort((a, b) => b.chg - a.chg);
    else if (sort === "chgAsc") top.sort((a, b) => a.chg - b.chg);
    else top.sort((a, b) => b.vol - a.vol);

    $("heatTitle").textContent = st.name;
    $("heatMeta").textContent = "（24h 涨跌幅前 40 · 色块随行情实时刷新 · 全部数据见「全市场列表」）";
    $("heat").innerHTML = top.map(r => {
      const c = r.chg >= 0 ? "green" : "red";
      return `<div class="tile" title="${esc(r.rec.symbol)} ${fmtPct(r.chg)}">
        <div class="s ${c}">${esc(r.rec.symbol)}</div>
        <div class="m ${c}">${fmtPct(r.chg)}</div>
        <div class="p">${fmtPrice(r.rec.price)}</div></div>`;
    }).join("") || `<div class="empty">${emptyHint("正在积累行情数据…")}</div>`;
  }

  function renderAllTable() {
    const mkFilter = $("allMarket").value;
    const kw = ($("allSearch").value || "").trim().toUpperCase();
    const sort = $("allSort").value;
    const winMs = state.params.windowMin * 60000;
    const rows = [];
    const src = mkFilter === "all" ? ["futures", "spot", "alpha"] : [mkFilter];
    for (const mk of src) {
      for (const rec of state.markets[mk].records.values()) {
        if (!rec.price) continue;
        if (kw && !rec.symbol.toUpperCase().includes(kw) && !(rec.name || "").toUpperCase().includes(kw)) continue;
        const w = windowChange(rec, winMs);
        rows.push({ mk, rec, wchg: (mk === "alpha" && typeof rec.extra.wChg === "number") ? rec.extra.wChg : w.chg, chg24: rec.chg24 });
      }
    }
    const maxAbs = (a) => a;
    if (sort === "chgDesc") rows.sort((a, b) => (b.wchg ?? -1e9) - (a.wchg ?? -1e9));
    else if (sort === "chgAsc") rows.sort((a, b) => (a.wchg ?? 1e9) - (b.wchg ?? 1e9));
    else if (sort === "chg24Desc") rows.sort((a, b) => (b.chg24 ?? -1e9) - (a.chg24 ?? -1e9));
    else if (sort === "chg24Asc") rows.sort((a, b) => (a.chg24 ?? 1e9) - (b.chg24 ?? 1e9));
    else rows.sort((a, b) => (b.rec.quoteVol || 0) - (a.rec.quoteVol || 0));

    const pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
    if (state.allPage > pages) state.allPage = pages;
    const start = (state.allPage - 1) * PAGE_SIZE;
    const show = rows.slice(start, start + PAGE_SIZE);
    $("allBody").innerHTML = show.map((r, i) => {
      const c = r.wchg >= 0 ? "green" : "red";
      const c24 = r.chg24 >= 0 ? "green" : "red";
      return `<tr><td class="muted">${start + i + 1}</td><td>${MKT_TAG[r.mk]}</td>
        <td><b>${esc(r.rec.symbol)}</b>${r.rec.name ? ' <span class="muted">' + esc(r.rec.name) + '</span>' : ''}</td>
        <td>${fmtPrice(r.rec.price)}</td><td class="${c}">${fmtPct(r.wchg)}</td><td class="${c24}">${fmtPct(r.chg24)}</td>
        <td>${fmtMoney(r.rec.quoteVol)}</td><td class="muted">${fmtPrice(r.rec.high)} / ${fmtPrice(r.rec.low)}</td>
        <td class="muted">${r.rec.ts ? clock(r.rec.ts) : "—"}</td></tr>`;
    }).join("") || `<tr><td colspan="9" class="empty">无匹配数据</td></tr>`;
    $("allPageInfo").textContent = state.allPage + " / " + pages;
    $("allCount").textContent = "共 " + rows.length + " 条";
    $("allPrev").disabled = state.allPage <= 1;
    $("allNext").disabled = state.allPage >= pages;
  }

  function renderLiq() {
    const cutoff = Date.now() - 300000;
    const recent = state.liquidations.filter(x => x.ts >= cutoff);
    $("liqTotal").textContent = fmtMoney(recent.reduce((a, x) => a + x.notional, 0));
    $("liqBody").innerHTML = state.liquidations.slice(0, 60).map(x =>
      `<tr><td>${clock(x.ts)}</td><td>${esc(x.symbol)}</td>
       <td class="${x.side === "SELL" ? "red" : "green"}">${x.side === "SELL" ? "多单爆仓" : "空单爆仓"}</td>
       <td>${fmtMoney(x.notional)}</td><td>${fmtPrice(x.price)}</td></tr>`
    ).join("") || `<tr><td colspan="5" class="empty">暂无强平</td></tr>`;

    const m = {};
    for (const x of recent) {
      if (!m[x.symbol]) m[x.symbol] = { total: 0, long: 0, short: 0 };
      m[x.symbol].total += x.notional;
      if (x.side === "SELL") m[x.symbol].long += x.notional; else m[x.symbol].short += x.notional;
    }
    $("liqRankBody").innerHTML = Object.entries(m).sort((a, b) => b[1].total - a[1].total).slice(0, 20).map(([s, x]) =>
      `<tr><td>${esc(s)}</td><td>${fmtMoney(x.total)}</td><td class="red">${fmtMoney(x.long)}</td><td class="green">${fmtMoney(x.short)}</td></tr>`
    ).join("") || `<tr><td colspan="4" class="empty">暂无排行</td></tr>`;

    const notice = $("liqNotice");
    if (notice) {
      notice.textContent = NET.futOk === false
        ? "合约（fapi）当前需代理：本网络下合约推送流不可达，强平事件暂不可用；现货数据与现货异动不受影响，开启代理 / VPN 后点顶栏「重新探测」即可恢复。"
        : state.markets.futures.source === "rest"
          ? "当前网络未能接收币安合约推送流（fstream 无数据返回），强平事件不可用；合约行情已改为 REST 轮询，涨跌异动 / 热力图 / 衍生品面板均正常工作。"
          : "数据来源：币安合约强平订单推送流（!forceOrder@arr），仅统计单笔名义价值达阈值的强平。";
    }
  }

  function renderBig() {
    const cutoff = Date.now() - 300000;
    const arr = state.largeTrades.filter(x => x.ts >= cutoff);
    $("bigTotal").textContent = fmtMoney(arr.reduce((a, x) => a + x.notional, 0));
    $("bigBody").innerHTML = state.largeTrades.slice(0, 60).map(x =>
      `<tr><td>${clock(x.ts)}</td><td>${MKT_TAG[x.market] || ""}</td><td>${esc(x.symbol)}</td>
       <td class="${x.side === "BUY" ? "green" : "red"}">${x.side === "BUY" ? "主动买" : "主动卖"}</td>
       <td>${fmtMoney(x.notional)}</td><td>${fmtPrice(x.price)}</td></tr>`
    ).join("") || `<tr><td colspan="6" class="empty">暂无大单</td></tr>`;
    const m = {};
    for (const x of arr) { if (!m[x.symbol]) m[x.symbol] = { sum: 0, n: 0 }; m[x.symbol].sum += x.notional; m[x.symbol].n++; }
    $("bigRankBody").innerHTML = Object.entries(m).sort((a, b) => b[1].sum - a[1].sum).slice(0, 20).map(([s, x]) =>
      `<tr><td>${esc(s)}</td><td>${fmtMoney(x.sum)}</td><td>${x.n}</td></tr>`
    ).join("") || `<tr><td colspan="3" class="empty">暂无排行</td></tr>`;
    const fu = state.markets.futures, sp = state.markets.spot;
    const parts = [];
    if (fu.aggSupported === false) {
      parts.push((NET.futOk === false ? "合约（fapi）当前需代理，合约逐笔成交流不可用" : "合约逐笔成交流在本机网络下无数据返回，已停用")
        + `（合约分片 ${fu.shards.length} 片未订阅）`);
    } else {
      parts.push(`合约 ${fu.shards.length} 片（当前 ${fu.shardText}）`);
    }
    if (sp.aggSupported === false) {
      parts.push(`现货逐笔成交流无数据返回，已停用`);
    } else {
      parts.push(`现货 ${sp.shards.length} 片（当前 ${sp.shardText}）`);
    }
    $("bigNotice").textContent = `大单采用分片轮换订阅，每 ${SHARD_SWITCH_MS / 1000} 秒切换一片，逐批覆盖全部币种。${parts.join(" · ")}。已发生的大单保留在列表中，不受分片切换影响。`;
  }

  function renderDeriv() {
    const kw = ($("derivSearch").value || "").trim().toUpperCase();
    const sort = $("derivSort").value;
    let rows = [...state.deriv.entries()].filter(([s]) => !kw || s.includes(kw));
    if (sort === "abs") rows.sort((a, b) => Math.abs(b[1].funding || 0) - Math.abs(a[1].funding || 0));
    else if (sort === "hi") rows.sort((a, b) => (b[1].funding || 0) - (a[1].funding || 0));
    else rows.sort((a, b) => (a[1].funding || 0) - (b[1].funding || 0));
    $("derivBody").innerHTML = rows.slice(0, 400).map(([s, x]) => {
      const f = x.funding;
      const cls = f > 0 ? "green" : f < 0 ? "red" : "";
      return `<tr><td>${esc(s)}</td><td class="${cls}">${(f * 100).toFixed(4)}%</td>
        <td class="${cls}">${(f * 100 * 3 * 365).toFixed(1)}%</td><td>${fmtPrice(x.mark)}</td><td class="muted">${clock(x.ts)}</td></tr>`;
    }).join("") || `<tr><td colspan="5" class="empty">${NET.futOk === false ? "合约（fapi）当前需代理，资金费率暂不可用" : "正在采集资金费率…"}</td></tr>`;
  }

  function renderStruct() {
    const rows = [...state.bursts.values()].filter(b => b.market === "futures").map(b => {
      const oi = (state.oi.get(b.symbol) || {}).changePct ?? null;
      return { ...b, oi, quadrant: quadrant(b.chg, oi), score: scoreBurst(b.chg, null, oi) };
    }).sort((a, b) => b.score - a.score).slice(0, 60);
    $("structBody").innerHTML = rows.map(x =>
      `<tr><td>${esc(x.symbol)}</td><td>${esc(x.quadrant)}</td>
       <td class="${x.chg >= 0 ? "green" : "red"}">${fmtPct(x.chg)}</td>
       <td class="${x.oi !== null ? (x.oi >= 0 ? "green" : "red") : ""}">${fmtPct(x.oi)}</td>
       <td class="yellow">${x.score}</td><td>${levelOf(x.score)}</td></tr>`
    ).join("") || `<tr><td colspan="6" class="empty">${NET.futOk === false ? "合约（fapi）当前需代理，价格·OI 结构暂不可用" : "等待结构数据…"}</td></tr>`;
  }

  function renderAlpha() {
    const st = state.markets.alpha;
    const kw = ($("alphaSearch").value || "").trim().toUpperCase();
    const chain = $("alphaChain").value;
    const sort = $("alphaSort").value;
    let rows = [...st.records.values()].filter(r => r.price);
    if (kw) rows = rows.filter(r => r.symbol.toUpperCase().includes(kw) || (r.name || "").toUpperCase().includes(kw));
    if (chain) rows = rows.filter(r => r.chain === chain);
    const winMs = state.params.windowMin * 60000;
    const wc = (r) => (typeof r.extra.wChg === "number" ? r.extra.wChg : windowChange(r, winMs).chg);
    if (sort === "volDesc") rows.sort((a, b) => (b.quoteVol || 0) - (a.quoteVol || 0));
    else if (sort === "chgDesc") rows.sort((a, b) => (b.chg24 ?? -1e9) - (a.chg24 ?? -1e9));
    else if (sort === "chgAsc") rows.sort((a, b) => (a.chg24 ?? 1e9) - (b.chg24 ?? 1e9));
    else if (sort === "wDesc") rows.sort((a, b) => (wc(b) ?? -1e9) - (wc(a) ?? -1e9));
    else if (sort === "wAsc") rows.sort((a, b) => (wc(a) ?? 1e9) - (wc(b) ?? 1e9));
    else rows.sort((a, b) => (b.extra.mcap || 0) - (a.extra.mcap || 0));
    $("alphaBody").innerHTML = rows.slice(0, 500).map((r, i) => {
      const w = wc(r);
      return `<tr><td class="muted">${i + 1}</td><td><b>${esc(r.symbol)}</b> <span class="muted">${esc(r.name || "")}</span></td>
        <td class="muted">${esc(r.chain || "")}</td><td>${fmtPrice(r.price)}</td>
        <td class="${w >= 0 ? "green" : "red"}">${fmtPct(w)}</td>
        <td class="${r.chg24 >= 0 ? "green" : "red"}">${fmtPct(r.chg24)}</td>
        <td>${fmtMoney(r.quoteVol)}</td><td>${fmtMoney(r.extra.mcap)}</td>
        <td>${r.extra.cex ? '<span class="tag s">已上CEX</span>' : '<span class="muted">—</span>'}</td></tr>`;
    }).join("") || `<tr><td colspan="9" class="empty">等待 Alpha 数据…</td></tr>`;
    $("alphaMeta").textContent = "共 " + rows.length + " 个代币 · 显示前 500";
  }

  function renderAlerts() {
    const f = $("alertFilter").value;
    const list = state.alerts.filter(a => !f || a.type === f).slice(0, 200);
    $("alertMeta").textContent = "共 " + state.alerts.length + " 条";
    $("alertBody").innerHTML = list.map(a =>
      `<tr><td>${clock(a.ts)}</td><td>${esc(a.type)}</td><td>${MKT_TAG[a.market] || ""}</td><td>${esc(a.symbol)}</td>
       <td class="${a.dir === "UP" ? "green" : a.dir === "DOWN" ? "red" : ""}">${a.dir || "—"}</td>
       <td>${fmtPct(a.chg)}</td><td>${fmtPrice(a.price)}</td><td class="yellow">${a.score || "—"}</td>
       <td class="muted">${esc(a.info || (a.level ? "等级 " + a.level : ""))}</td></tr>`
    ).join("") || `<tr><td colspan="9" class="empty">暂无报警</td></tr>`;
  }

  function renderKpi() {
    const alphaBurst = [...state.bursts.values()].filter(b => b.market === "alpha").length;
    $("alphaBurst").textContent = alphaBurst;
  }

  function renderAll() {
    renderKpi(); renderBursts(); renderHeat();
    if ($("tab-all").classList.contains("active")) renderAllTable();
    renderLiq(); renderBig();
    if ($("tab-deriv").classList.contains("active")) renderDeriv();
    if ($("tab-struct").classList.contains("active")) renderStruct();
    if ($("tab-alpha").classList.contains("active")) renderAlpha();
    if ($("tab-alerts").classList.contains("active")) renderAlerts();
    // 历史记录按需刷新（切换页签 / 15s 定时），避免每秒重复请求后端
    if ($("tab-history").classList.contains("active")) renderHistory();
  }

  /* ---------- 周期性任务 ---------- */
  function periodic() {
    adaptSources();
    scanBursts();
    renderAll();
  }

  let lastDeriv = 0;
  function slowLoop() {
    const now = Date.now();
    ensureStrategyData();                        // 策略位置：按需拉取 1 分钟 K 线（限流）
    if (now - lastDeriv > 45000) {
      lastDeriv = now;
      if (NET.futOk !== false) {
        loadPremium();
        const hot = [...state.bursts.values()].filter(b => b.market === "futures").slice(0, 12).map(b => b.symbol);
        if (hot.length) pollOI([...new Set(hot)]);
      }
    }
  }

  /* ---------- 交互 ---------- */
  $("applyBtn").onclick = () => {
    const p = state.params;
    p.windowMin = Math.max(1, Math.min(60, Number($("windowMin").value) || 5));
    p.upPct = Number($("upPct").value) || 5;
    p.downPct = Number($("downPct").value) || -5;
    p.largeUsd = Number($("largeUsd").value) || 250000;
    p.liqUsd = Number($("liqUsd").value) || 100000;
    p.alphaPct = Number($("alphaPct").value) || 8;
    $("windowMin").value = p.windowMin;
    pollAlpha();                 // 窗口变化后立即按新窗口重算 Alpha 涨跌
    log("参数已应用：窗口 " + p.windowMin + " 分钟 · 涨 " + p.upPct + "% / 跌 " + p.downPct + "%");
  };

  function setMarket(mk) {
    document.querySelectorAll(".mbtn[data-market]").forEach(b => b.classList.toggle("active", b.dataset.market === mk));
    state.market = mk;
    state.allPage = 1;
    renderAll();
  }
  document.querySelectorAll(".mbtn[data-market]").forEach(btn => {
    btn.addEventListener("click", () => {
      state.__userPickedMarket = true;   // 用户手动选过市场后，不再自动切换
      setMarket(btn.dataset.market);
    });
  });

  document.querySelectorAll(".tabbtn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tabbtn").forEach(b => b.classList.remove("active"));
      document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
      btn.classList.add("active");
      $("tab-" + btn.dataset.tab).classList.add("active");
      renderAll();
      if (btn.dataset.tab === "history") refreshHistory();
    });
  });

  $("clearAlerts").onclick = () => { state.alerts.length = 0; renderAlerts(); };

  /* 策略位置：功能开关 / 依据查看（行内展开） / 网络重新探测 */
  function syncStratUI() {
    const en = $("stratEnabled"), ea = $("stratExpandAll");
    if (en) en.checked = !!Number(strat.enabled);
    if (ea) { ea.checked = !!Number(strat.expandAll); ea.disabled = !Number(strat.enabled); }
    const ss = $("stratState");
    if (ss) ss.textContent = Number(strat.enabled) ? "已开启（仅供参考）" : "已关闭";
    renderAll();
  }
  if ($("stratEnabled")) $("stratEnabled").addEventListener("change", () => {
    strat.enabled = $("stratEnabled").checked ? 1 : 0;
    saveStrat(); syncStratUI();
    log(strat.enabled ? "策略位置已开启（机械推算，仅供参考，不构成投资建议）" : "策略位置已关闭");
  });
  if ($("stratExpandAll")) $("stratExpandAll").addEventListener("change", () => {
    strat.expandAll = $("stratExpandAll").checked ? 1 : 0;
    saveStrat(); syncStratUI();
  });
  document.addEventListener("click", (ev) => {
    const t = ev.target;
    if (t && t.dataset && t.dataset.sp) {
      const k = t.dataset.sp;
      if (stratOpen.has(k)) stratOpen.delete(k); else { stratOpen.add(k); stratEnsureForKey(k); }
      renderAll();
    }
  });
  if ($("netReprobe")) $("netReprobe").onclick = () => { KL.clear(); stratOpen.clear(); probeNetwork(true); };
  $("heatSort").addEventListener("change", renderHeat);
  ["allMarket", "allSort"].forEach(id => $(id).addEventListener("change", () => { state.allPage = 1; renderAllTable(); }));
  $("allSearch").addEventListener("input", () => { state.allPage = 1; renderAllTable(); });
  $("allPrev").onclick = () => { state.allPage--; renderAllTable(); };
  $("allNext").onclick = () => { state.allPage++; renderAllTable(); };
  ["alphaSearch", "alphaSort", "alphaChain"].forEach(id => $(id).addEventListener("input", renderAlpha));
  $("derivSearch").addEventListener("input", renderDeriv);
  $("derivSort").addEventListener("change", renderDeriv);
  $("alertFilter").addEventListener("change", renderAlerts);

  /* 提醒设置：开关 / 提示音 / 系统通知 / 防刷屏参数 */
  function syncNotifyUI() {
    $("notifyEnabled").checked = !!Number(notify.enabled);
    $("notifySound").checked = !!Number(notify.sound);
    $("notifySystem").checked = !!Number(notify.system);
    $("notifyGap").value = notify.gapSec;
    $("notifyMax").value = notify.maxPerMin;
    $("notifyState").textContent = Number(notify.enabled)
      ? ("已开启" + (Number(notify.sound) ? " · 提示音" : "") + (Number(notify.system) ? " · 桌面右下角通知" : ""))
      : "已关闭";
    // 桌面版（有本地后端）系统通知由后端 Toast 弹出，任何环境都可用；纯网页版才依赖浏览器权限
    $("notifySystem").disabled = !HISTORY.base && !("Notification" in window);
    $("notifyPerm").textContent = HISTORY.base
      ? "桌面右下角通知：由本地后端弹出（Windows Toast，全屏游戏也能看到，点击可把监控窗口带到前台）"
      : ("Notification" in window)
        ? ("系统通知权限：" + (Notification.permission === "granted" ? "已授权" : Notification.permission === "denied" ? "已拒绝（可在浏览器/系统设置中开启）" : "未授权，开启后将自动申请"))
        : "当前环境不支持系统通知（将以应用内浮层提醒）";
  }
  $("notifyEnabled").addEventListener("change", () => {
    notify.enabled = $("notifyEnabled").checked ? 1 : 0;
    if (Number(notify.enabled) && Number(notify.sound)) beep(true);   // 开启时试听一次
    saveNotify(); syncNotifyUI();
    log(Number(notify.enabled) ? "异动提醒已开启" : "异动提醒已关闭");
  });
  $("notifySound").addEventListener("change", () => {
    notify.sound = $("notifySound").checked ? 1 : 0;
    if (Number(notify.sound)) beep(true);
    saveNotify(); syncNotifyUI();
  });
  $("notifySystem").addEventListener("change", () => {
    notify.system = $("notifySystem").checked ? 1 : 0;
    if (Number(notify.system) && Number(notify.enabled)) nativeNotify("币安异动监控", "系统通知已开启，有新异动时会在此提醒");
    saveNotify(); setTimeout(syncNotifyUI, 400);
  });
  $("notifyGap").addEventListener("change", () => {
    notify.gapSec = Math.max(0, Math.min(600, Number($("notifyGap").value) || 0));
    $("notifyGap").value = notify.gapSec; saveNotify();
  });
  $("notifyMax").addEventListener("change", () => {
    notify.maxPerMin = Math.max(1, Math.min(600, Number($("notifyMax").value) || 20));
    $("notifyMax").value = notify.maxPerMin; saveNotify();
  });
  $("notifyTest").onclick = () => {
    beep(true);
    nativeNotify("币安异动监控 · 测试", "这是一条测试提醒，看到它说明提醒功能正常");
    toast({ dir: "UP", title: "测试提醒 · BTCUSDT", body: "提示音 + 系统通知 + 应用内浮层均正常" });
  };

  /* ---------- 邮箱推送（本地后端 SMTP，含策略，防刷屏） ---------- */
  const MAIL_DEFAULTS = { enabled: 0, host: "smtp.qq.com", port: 465, user: "", password: "", to: "", gapSec: 30, maxPerMin: 3 };
  const mailUI = { cfg: Object.assign({}, MAIL_DEFAULTS), base: "", hasPassword: false, lastSent: "", lastError: "", db: "" };

  function fillMailUI() {
    const c = mailUI.cfg;
    $("mailEnabled").checked = !!Number(c.enabled);
    $("mailHost").value = c.host || "";
    $("mailPort").value = c.port || 465;
    $("mailUser").value = c.user || "";
    $("mailPass").value = "";
    const tos = (c.to || "").split(/[,;；，]/).map(s => s.trim()).filter(Boolean);
    $("mailTo1").value = tos[0] || "";
    $("mailTo2").value = tos[1] || "";
    $("mailTo3").value = tos[2] || "";
    $("mailGapSec").value = c.gapSec ?? 30;
    $("mailMaxPerMin").value = c.maxPerMin ?? 3;
    const offline = IS_PUBLIC ? "（公网在线版，无本地后端）" : (!mailUI.base ? "（未连接后端，显示上次保存状态）" : "");
    $("mailState").textContent = (Number(c.enabled) ? "已启用" : "未启用") + offline + (mailUI.lastSent ? " · 上次发送 " + mailUI.lastSent : "");
    const nt = $("mailNotice");
    if (nt) {
      nt.textContent = mailUI.base
        ? (mailUI.lastError ? "⚠ 最近发送失败：" + mailUI.lastError + "；请检查 SMTP 配置后点击「测试发送」。" : "配置保存在本机，重启软件不丢失；点「测试发送」验证 SMTP 是否可用。")
        : (IS_PUBLIC ? "公网在线版不含本地邮箱推送。请在桌面版 BinanceRadarPro.exe（本机运行）或云端监控（GitHub Actions）中配置邮箱，可同时推送 3 个收件邮箱。" : "正在连接本地后端…连接成功后即可保存与测试（若长时间未连接，请确认是从桌面版窗口打开的页面）");
    }
  }

  const MAIL_LS_KEY = "radar.mailCfg";   // 本地缓存：后端未连时回显上次保存的开关状态，杜绝"刷新回弹"

  function mailCfgCache() {
    try { return JSON.parse(localStorage.getItem(MAIL_LS_KEY) || "{}") || {}; } catch (e) { return {}; }
  }
  function mailCfgCacheSave() {
    try { localStorage.setItem(MAIL_LS_KEY, JSON.stringify(mailUI.cfg)); } catch (e) {}
  }

  function scheduleMailRetry() {
    if (mailUI.retryTimer) return;
    mailUI.retryTimer = setInterval(() => {
      if (mailUI.base) { clearInterval(mailUI.retryTimer); mailUI.retryTimer = null; return; }
      loadMail();
    }, 8000);
  }

  async function loadMail() {
    for (const base of API_CANDIDATES) {
      try {
        const d = await getJSON(base + "/api/mail/config", 4000);
        if (d && d.ok && d.cfg) {
          mailUI.base = base;
          mailUI.cfg = Object.assign({}, MAIL_DEFAULTS, d.cfg);
          mailUI.hasPassword = !!d.cfg.hasPassword;
          mailUI.lastSent = d.cfg.lastSent || "";
          mailUI.lastError = d.cfg.lastError || "";
          if (mailUI.retryTimer) { clearInterval(mailUI.retryTimer); mailUI.retryTimer = null; }
          mailCfgCacheSave();
          fillMailUI();
          return;
        }
      } catch (e) {}
    }
    // 连不上后端：用上次保存的配置回显（开关保持用户上次设定，刷新不回弹），并每 8 秒自动重连
    const cached = mailCfgCache();
    mailUI.base = "";
    mailUI.cfg = Object.assign({}, MAIL_DEFAULTS, cached);
    mailUI.hasPassword = !!cached.hasPassword;
    fillMailUI();
    scheduleMailRetry();
  }

  $("mailSave").onclick = async () => {
    if (!mailUI.base) { $("mailNotice").textContent = "未连接本地后端，无法保存邮箱配置。"; return; }
    const body = {
      enabled: $("mailEnabled").checked ? 1 : 0,
      host: $("mailHost").value.trim(),
      port: Number($("mailPort").value) || 465,
      user: $("mailUser").value.trim(),
      to: [$("mailTo1").value.trim(), $("mailTo2").value.trim(), $("mailTo3").value.trim()].filter(Boolean).join(","),
      gapSec: Math.max(5, Number($("mailGapSec").value) || 30),
      maxPerMin: Math.max(1, Number($("mailMaxPerMin").value) || 3)
    };
    const pw = $("mailPass").value;
    if (pw.trim()) body.password = pw.trim();
    try {
      const d = await postJSON(mailUI.base + "/api/mail/config", body, 10000);
      if (d && d.ok) {
        Object.assign(mailUI.cfg, d.cfg); mailUI.hasPassword = !!d.cfg.hasPassword;
        $("mailPass").value = "";
        $("mailState").textContent = "已保存" + (Number(d.cfg.enabled) ? "，推送已启用" : "，未启用");
        log("邮箱推送配置已保存");
      } else {
        $("mailNotice").textContent = "保存失败：" + (d && d.error ? d.error : "未知错误");
      }
    } catch (e) { $("mailNotice").textContent = "保存失败：" + (e.message || e); }
  };

  $("mailTest").onclick = async () => {
    if (!mailUI.base) { $("mailNotice").textContent = "未连接本地后端，无法测试发送。"; return; }
    $("mailTest").disabled = true; $("mailTest").textContent = "发送中…";
    try {
      const d = await postJSON(mailUI.base + "/api/mail/test", {}, 30000);
      if (d && d.ok) { $("mailState").textContent = "测试邮件已发送，请查收收件箱"; log("邮件测试发送成功"); }
      else $("mailNotice").textContent = "测试发送失败：" + (d && d.error ? d.error : "未知错误");
    } catch (e) { $("mailNotice").textContent = "测试发送失败：" + (e.message || e); }
    finally { $("mailTest").disabled = false; $("mailTest").textContent = "测试发送"; }
  };

  $("mailEnabled").addEventListener("change", () => {
    // 拨动即保存：不再依赖「保存配置」按钮，刷新后不回弹
    if (!mailUI.base) { $("mailState").textContent = "未连接后端"; return; }
    $("mailState").textContent = $("mailEnabled").checked ? "正在启用…" : "正在关闭…";
    saveMailConfig(true);
  });

  /* 历史记录：筛选 / 刷新 / 清空 */
  ["histKind", "histMarket", "histLimit"].forEach(id => $(id).addEventListener("change", refreshHistory));
  $("histSearch").addEventListener("input", () => { clearTimeout(window.__histT); window.__histT = setTimeout(refreshHistory, 350); });
  $("histRefresh").onclick = refreshHistory;
  $("histClear").onclick = async () => {
    const btn = $("histClear");
    if (btn.dataset.armed !== "1") {                 // 二次确认，避免误清空
      btn.dataset.armed = "1";
      btn.textContent = "确认清空？";
      btn.classList.add("danger");
      setTimeout(() => { btn.dataset.armed = "0"; btn.textContent = "清空历史"; btn.classList.remove("danger"); }, 4000);
      return;
    }
    btn.dataset.armed = "0"; btn.textContent = "清空历史"; btn.classList.remove("danger");
    try {
      const n = await clearHistory();
      await refreshHistory();
      log("已清空历史记录：" + n + " 条");
    } catch (e) { log("清空历史失败：" + (e.message || e)); }
  };

  /* ---------- 启动 ---------- */
  setInterval(periodic, 1000);
  setInterval(slowLoop, 5000);

  /* 大单分片轮换：合约逐笔流不可用时，把订阅资源让给现货并加快轮换 */
  function tickShards() {
    const now = Date.now();
    const fuDead = state.markets.futures.aggSupported === false;
    for (const mk of ["futures", "spot"]) {
      const st = state.markets[mk];
      if (!st.shards.length || st.aggSupported === false) continue;
      const every = (mk === "spot" && fuDead) ? Math.round(SHARD_SWITCH_MS / 2) : SHARD_SWITCH_MS;
      if (now - (st.aggAt || 0) >= every) { st.aggAt = now; connectAggShard(mk); }
    }
  }
  setInterval(tickShards, 5000);

  async function boot() {
    window.__radar = state;      // 调试入口
    renderAll();
    syncNotifyUI();
    await initHistory();         // 探测后端历史接口：可用则走后端 SQLite，否则本地存储兜底
    log(HISTORY.mode === "server"
      ? "异动历史后端已连接：" + (HISTORY.db || "")
      : "异动历史未连接后端，暂存浏览器本地存储");
    // 历史页签打开时每 15 秒自动刷新一次，保证新记录及时出现
    setInterval(() => {
      if ($("tab-history").classList.contains("active")) refreshHistory();
    }, 15000);
    if ($("stratEnabled")) syncStratUI();      // 恢复「策略位置」开关状态
    log("正在探测网络线路（直连 / 代理自动择优）…");
    await loadMail();                          // 邮箱推送配置（本地后端 SMTP）
    try {
      const h = await getJSON((HISTORY.base || API_CANDIDATES[0]) + "/api/health", 5000);
      if (h && h.lanUrl) { $("lanNotice").style.display = ""; $("lanUrlText").textContent = h.lanUrl; }
    } catch (e) {}
    if (IS_PUBLIC) {
      if ($("lanNotice")) $("lanNotice").style.display = "none";
      const pn = $("pubNotice");
      if (pn) pn.style.display = "";
    }
    await probeNetwork(true);                  // 启动探测：官方域名不可达时自动切换官方公共镜像直连
    await Promise.allSettled([initFutures(), initSpot()]);
    tickShards();
    pollAlpha();
    alphaTimer = setInterval(pollAlpha, 8000);
    log("正在预热全市场窗口历史…");
    warmup("futures").then(() => warmup("spot")).then(() => log("全市场窗口历史预热完成，异动检测已就绪"));
  }
  boot();
})();
