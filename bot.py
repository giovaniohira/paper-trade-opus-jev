"""
Crypto paper-trading agent. $5,000 simulated, one hour, live Binance prices.

  engine (every ~2s)   four deterministic setups per coin, risk sizing, stops/targets/trailing
  Jev    (System One)  typed, calibrated gate: every engine entry must pass Jev; Jev also exits stalled trades
  Opus   (System Two)  Claude Code headless every ~60s: sets the engine policy and trades on its own

Nothing is ever sent to an exchange. Fills cross the live bid/ask, plus slippage and a 0.1% fee per side.

  python bot.py           run the 60-minute session, dashboard at http://localhost:8765
  python bot.py 10        run a 10-minute session
  python bot.py test      self-check of the money path
"""
import json, os, queue, shutil, subprocess, sys, threading, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
API = "https://data-api.binance.vision/api/v3"  # public market data, no key
JEV_URL = "https://api.typesafe.ai/v1/systemone"
START_CASH = 5000.0
FEE = 0.001          # 0.1% per side, Binance spot taker
SLIP = 0.0002        # 0.02% slippage on every fill, on top of crossing the spread
MAX_POS = 0.8        # max notional per position, x equity
MAX_GROSS = 2.5      # max total notional, x equity (simulated cross margin, shorts allowed)
MIN_STOP = 0.005     # stops closer than 0.5% just pay fees
N_COINS = 12
TICK = 2.0
PORT = 8765
OPUS_EVERY = 60
JEV_EXIT = 0.8       # Jev "close it" probability that closes a position
NO_ENTRY_TAIL = 180  # no new trades in the last 3 minutes
STABLES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "USD1", "RLUSD", "USDE", "EUR", "EURI", "AEUR", "XUSD",
           "BFUSD", "PAXG", "WBTC", "WBETH", "BUSD", "USDS", "PYUSD", "USDF"}

# ATR multiples (1-minute ATR). trail=True: breakeven at +1R, then trail 1R behind the best price.
# Backtest, 50h x 12 coins: 20-bar breakouts, EMA pullbacks and Bollinger reversion all lost about the
# fee on every trade (no edge at 1m), so they were deleted. Fewer, bigger, trend-aligned trades lose least.
PARAMS = {
    "breakout": dict(stop=3.0, tp=12.0, trail=True, tmax=30),
    "leadlag":  dict(stop=2.0, tp=5.0, trail=False, tmax=10),
}
STRAT_DESC = {
    "breakout": "breakout of the 60-minute range on high volume, in the direction of the 1-hour trend",
    "leadlag": "catch-up move following a sharp Bitcoin move",
    "opus": "discretionary trade",
}
POLICY = dict(risk_pct=1.0, max_positions=4, jev_min_prob=0.5, allow_long=True, allow_short=True,
              weights=dict(breakout=0.0, leadlag=0.0), blacklist=[])  # engine entries off until Opus turns them on


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


# ---------------------------------------------------------------- market data

def get(path, **params):
    url = f"{API}/{path}" + ("?" + urllib.parse.urlencode(params) if params else "")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                return json.load(r)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(0.5)


def klines(sym, limit=120):
    return [dict(t=r[0], o=float(r[1]), h=float(r[2]), l=float(r[3]), c=float(r[4]), v=float(r[5]), tb=float(r[9]))
            for r in get("klines", symbol=sym, interval="1m", limit=limit)]


def quotes(syms):
    rows = get("ticker/bookTicker", symbols=json.dumps(syms, separators=(",", ":")))
    return {r["symbol"]: (float(r["bidPrice"]), float(r["askPrice"])) for r in rows}


POOL = ThreadPoolExecutor(16)


def all_klines(syms, limit=120):
    return dict(zip(syms, POOL.map(lambda s: klines(s, limit), syms)))


# ---------------------------------------------------------------- indicators

def ema(xs, n):
    k, e, out = 2 / (n + 1), xs[0], []
    for x in xs:
        e += k * (x - e)
        out.append(e)
    return out


def rsi(cl, n):
    d = [b - a for a, b in zip(cl[-n - 1:-1], cl[-n:])]
    g, l = sum(x for x in d if x > 0), -sum(x for x in d if x < 0)
    return 100.0 if l == 0 else 100 - 100 / (1 + g / l)


def atr(b, n=14):
    return sum(max(x["h"], p["c"]) - min(x["l"], p["c"]) for p, x in zip(b[-n - 1:-1], b[-n:])) / n


def candle(x, a):
    body = (x["c"] - x["o"]) / a
    return "big green" if body > 1 else "green" if body > 0.25 else "big red" if body < -1 else "red" if body < -0.25 else "doji"


def features(bars, px, now):
    """bars: 1m klines, last one still open. px: live mid price."""
    b, cur = bars[:-1], bars[-1]
    cl = [x["c"] for x in b] + [px]
    a = atr(b)
    e9, e21, e55 = ema(cl, 9)[-1], ema(cl, 21)[-1], ema(cl, 55)[-1]
    avgv = sum(x["v"] for x in b[-31:-1]) / 30 or 1e-9
    frac = clamp((now * 1000 - cur["t"]) / 60000, 0.33, 1)
    last4 = b[-3:] + [cur]
    vol = sum(x["v"] for x in last4)
    moves = sum(abs(cl[i] - cl[i - 1]) for i in range(-20, 0))
    w = b[-60:] + [cur]
    return dict(
        px=px, atr=a, atrp=a / px, e9=e9, e21=e21, e55=e55,
        up=e9 > e21 > e55, dn=e9 < e21 < e55,
        hi20=max(x["h"] for x in b[-20:]), lo20=min(x["l"] for x in b[-20:]),
        hi60=max(x["h"] for x in b[-60:]), lo60=min(x["l"] for x in b[-60:]),
        volr=max(b[-1]["v"], cur["v"] / frac) / avgv,
        flow=sum(x["tb"] for x in last4) / vol if vol else 0.5,
        er=abs(cl[-1] - cl[-21]) / moves if moves else 0,
        vwap=sum((x["h"] + x["l"] + x["c"]) / 3 * x["v"] for x in w) / (sum(x["v"] for x in w) or 1e-9),
        rsi14=rsi(cl, 14),
        r1=px / cl[-2] - 1, r3=px / cl[-4] - 1, r5=px / cl[-6] - 1, r15=px / cl[-16] - 1, r60=px / cl[-61] - 1,
        candles=[candle(x, a) for x in b[-5:]],
    )


def signals(f, btc, is_btc=False):
    """Engine setups: list of (side, strategy, tp_frac or None)."""
    out = []
    # 1. 60-minute range breakout on volume, taker flow agreeing, 1-hour trend agreeing
    if f["px"] > f["hi60"] and f["volr"] > 1.5 and f["flow"] > 0.52 and f["r60"] > 0.005:
        out.append((1, "breakout", None))
    if f["px"] < f["lo60"] and f["volr"] > 1.5 and f["flow"] < 0.48 and f["r60"] < -0.005:
        out.append((-1, "breakout", None))
    # 2. lead-lag: BTC just moved hard, this coin has not followed yet
    if not is_btc:
        if btc["r3"] > 0.003 and f["r3"] < 0.3 * btc["r3"] and not f["dn"] and f["flow"] > 0.5:
            out.append((1, "leadlag", None))
        if btc["r3"] < -0.003 and f["r3"] > 0.3 * btc["r3"] and not f["up"] and f["flow"] < 0.5:
            out.append((-1, "leadlag", None))
    return out


def bucket(x, cuts, labels):
    return labels[sum(x > c for c in cuts)]


def words(f, btc):
    """Jev is not a calculator: describe the chart in words, not numbers."""
    a = f["atrp"]
    trend = ("strong uptrend" if f["up"] and f["er"] > 0.4 else "uptrend" if f["up"] else
             "strong downtrend" if f["dn"] and f["er"] > 0.4 else "downtrend" if f["dn"] else "sideways / choppy")
    rng = ("breaking above the 20-minute high" if f["px"] > f["hi20"] else
           "breaking below the 20-minute low" if f["px"] < f["lo20"] else
           "upper half of the 20-minute range" if f["px"] > (f["hi20"] + f["lo20"]) / 2 else
           "lower half of the 20-minute range")
    move = ["sharp drop", "drop", "flat", "rise", "sharp rise"]
    return {
        "trend": trend,
        "last 5 minutes": bucket(f["r5"] / a, [-4, -1.5, 1.5, 4], move),
        "last 15 minutes": bucket(f["r15"] / a, [-7, -2.5, 2.5, 7], move),
        "last hour": bucket(f["r60"] / a, [-14, -5, 5, 14], move),
        "volume": bucket(f["volr"], [0.7, 1.5, 3], ["below normal", "normal", "high", "extreme"]),
        "order flow": bucket(f["flow"], [0.4, 0.47, 0.53, 0.6], ["sellers dominate", "sellers slightly ahead",
                                                               "balanced", "buyers slightly ahead", "buyers dominate"]),
        "RSI": bucket(f["rsi14"], [25, 40, 60, 75], ["oversold", "weak", "neutral", "strong", "overbought"]),
        "price vs VWAP": bucket((f["px"] / f["vwap"] - 1) / a, [-3, -1, 1, 3],
                                ["far below", "below", "near", "above", "far above"]),
        "20-minute range": rng,
        "last 5 one-minute candles, oldest first": ", ".join(f["candles"]),
        "Bitcoin last 5 minutes": bucket(btc["r5"] / btc["atrp"], [-4, -1.5, 1.5, 4],
                                         ["falling hard", "falling", "flat", "rising", "rising hard"]),
    }


# ---------------------------------------------------------------- simulated broker

def better(old, new, side):
    return max(old, new) if side > 0 else min(old, new)


class Broker:
    """Cross-margin paper account: balance moves only on fees and realized PnL."""

    def __init__(self):
        self.balance, self.pos, self.trades, self.fees, self.n = START_CASH, {}, [], 0.0, 0

    @staticmethod
    def mark(p, q):
        return q[0] if p["side"] > 0 else q[1]  # what closing right now would get, before slippage

    def upnl(self, p, q):
        return p["side"] * (self.mark(p, q) - p["entry"]) * p["qty"]

    def equity(self, Q):
        return self.balance + sum(self.upnl(p, Q[s]) for s, p in self.pos.items())

    def gross(self, Q):
        return sum(self.mark(p, Q[s]) * p["qty"] for s, p in self.pos.items())

    def open(self, sym, side, notional, q, stop_frac, tp_frac, strat, why, trail, now, tmax):
        px = q[1] * (1 + SLIP) if side > 0 else q[0] * (1 - SLIP)
        fee = notional * FEE
        self.balance -= fee
        self.fees += fee
        self.n += 1
        p = dict(id=self.n, sym=sym, side=side, qty=notional / px, entry=px, stop=px * (1 - side * stop_frac),
                 tp=px * (1 + side * tp_frac) if tp_frac else None, R=px * stop_frac, strat=strat, why=why,
                 trail=trail, opened=now, tmax=tmax, peak=0.0, fee=fee)
        self.pos[sym] = p
        return p

    def close(self, sym, q, why, now):
        p = self.pos.pop(sym)
        px = q[0] * (1 - SLIP) if p["side"] > 0 else q[1] * (1 + SLIP)
        gross = p["side"] * (px - p["entry"]) * p["qty"]
        fee = px * p["qty"] * FEE
        self.balance += gross - fee
        self.fees += fee
        t = dict(id=p["id"], sym=p["sym"], side="long" if p["side"] > 0 else "short", strat=p["strat"],
                 entry=p["entry"], exit=px, qty=p["qty"], notional=p["entry"] * p["qty"],
                 pnl=gross - fee - p["fee"], pnl_pct=100 * (gross - fee - p["fee"]) / (p["entry"] * p["qty"]),
                 fees=fee + p["fee"], opened=p["opened"], closed=now, why_open=p["why"], why_close=why)
        self.trades.append(t)
        return t


def manage(p, q, now):
    """Ratchet the stop, return an exit reason or None."""
    s = p["side"]
    px = q[0] if s > 0 else q[1]
    gain = s * (px - p["entry"])
    p["peak"] = max(p["peak"], gain)
    if p["trail"]:
        if p["peak"] >= p["R"]:  # +1R: lock in breakeven plus fees
            p["stop"] = better(p["stop"], p["entry"] * (1 + s * 0.0025), s)
        if p["peak"] >= 1.5 * p["R"]:  # +1.5R: trail 1R behind the best price
            p["stop"] = better(p["stop"], p["entry"] + s * (p["peak"] - p["R"]), s)
    if s * (px - p["stop"]) <= 0:
        return "stop" if s * (p["stop"] - p["entry"]) < 0 else "trailing"
    if p["tp"] and s * (px - p["tp"]) >= 0:
        return "alvo"
    if now - p["opened"] > p["tmax"] * 60 and gain < 0.3 * p["R"]:
        return "tempo"
    return None


# ---------------------------------------------------------------- Jev (System One)

def load_key():
    k = os.environ.get("TYPESAFE_API_KEY", "")
    env = os.path.join(ROOT, ".env")
    if not k and os.path.exists(env):
        for line in open(env, encoding="utf-8"):
            if line.strip().startswith("TYPESAFE_API_KEY="):
                k = line.split("=", 1)[1].strip().strip('"').strip("'")
    return k


class Jev:
    def __init__(self, key):
        self.key, self.calls, self.errors, self.ms = key, 0, 0, 0
        self.status = "online" if key else "sem TYPESAFE_API_KEY"

    def ask(self, state, questions):
        if not self.key:
            return None
        body = json.dumps({"state": state, "model": "jev-latest", "questions": questions}).encode()
        req = urllib.request.Request(JEV_URL, data=body, headers={
            "Authorization": f"Bearer {self.key}", "Content-Type": "application/json"})
        t = time.time()
        try:
            with urllib.request.urlopen(req, timeout=6) as r:
                ans = json.load(r)["answers"]
            self.calls += 1
            self.ms = int((time.time() - t) * 1000)
            self.status = "online"
            return ans
        except Exception as e:
            self.errors += 1
            self.status = f"erro: {e}"[:120]
            return None


DIRECTION = {"type": "choice", "instructions": "Where is this coin's price most likely to go over the next 15 minutes?",
             "criteria": {"up": "Rises meaningfully", "down": "Falls meaningfully", "sideways": "Chops around, no meaningful move"}}
EXHAUSTED = {"type": "noul", "instructions": "The current move is overextended and likely to reverse soon."}
GATE_Q = {
    "direction": DIRECTION,
    "quality": {"type": "score", "instructions": "How clean and high-probability is the proposed trade setup?",
                "criteria": ["Very poor, likely to fail", "Weak", "Average", "Good", "Excellent, textbook setup"]},
    "exhausted": EXHAUSTED,
}
SCAN_Q = {"direction": DIRECTION, "exhausted": EXHAUSTED}
EXIT_Q = {"exit": {"type": "noul", "instructions":
                   "This open position should be closed now because the move has stalled or is turning against it."}}


# ---------------------------------------------------------------- Opus (System Two) via Claude Code

OPUS_SYSTEM = """You are the portfolio manager (System Two) of a crypto paper-trading agent.
Goal: maximize NET profit in USD by the end of a timed session (see minutes_left) that started with $5,000.
Trading is simulated on live Binance prices. Every fill crosses the spread, pays 0.02% slippage and a 0.1% fee,
so a round trip costs about 0.25%. Shorts are allowed. Gross exposure is capped at 2.5x equity, 0.8x per position.

How the system works:
- A deterministic engine runs every 2s. Per coin it detects two setups: "breakout" (60-minute range breakout on
  volume in the direction of the 1-hour trend) and "leadlag" (coin lagging a sharp BTC move). It sizes them by
  risk_pct and your strategy weight and manages stops, targets and trailing stops for every position
  (breakeven at +1R, then trail 1R behind the best price after +1.5R).
- Evidence: on the last 50 hours of these coins, mechanical 1-minute setups (short breakouts, EMA pullbacks,
  Bollinger mean reversion) lost roughly the round-trip fee on every trade: no edge at 5-30 minute horizons.
  Profit has to come from selectivity: few trades, larger moves, trend-aligned, winners allowed to run.
  Engine entries start switched off (weights 0); turn a setup on only if the regime suits it.
  Daily time-series momentum is the best-documented crypto edge: r24h and pos_in_24h_range show it.
- Jev, a fast System One model, gates EVERY entry, yours included: its probability for the trade's direction
  must be >= jev_min_prob and its exhaustion probability <= 0.7, else the trade is vetoed (see
  recent_jev_vetoes). Jev's setup-quality score scales position size 0.6x-1.4x. It also closes positions
  it judges stalled. Each coin row shows Jev's current read
  (jev_up / jev_down / jev_sideways / jev_exhausted, all probabilities).
- You run about every 60s. Your policy governs the engine until your next call, and you can open or close
  positions yourself with actions.

Actions: "open" needs side, stop_pct and take_pct (percent distance from entry, e.g. 0.9 = 0.9%) and size_mult
(0.3-1.5, 1 = standard risk sizing). Your opens also trail once in profit. "close" exits a position now.
Open when you see an edge the engine is missing: relative strength or weakness across timeframes, momentum
leaders on high volume, BTC-driven moves. Close positions whose thesis is broken. Empty actions is fine.

Principles:
- Fees punish small moves: only take trades where you expect at least ~1% in your favor. Overtrading is the
  main way this account loses money; doing nothing is often right.
- Look at strategy_stats: set weight 0 for setups losing money this session, raise weights that work.
  Blacklist coins that keep stopping out.
- Trade with the dominant intraday direction; coins with high relative volume move most.
- Protect gains: with little time left, cut new risk. The engine stops opening 3 minutes before the end
  and closes everything at the end.
Be decisive. Keep "view" to one or two sentences."""

OPUS_SCHEMA = {
    "type": "object",
    "properties": {
        "view": {"type": "string"},
        "risk_pct": {"type": "number", "description": "equity percent risked per engine trade, 0.25 to 2.5"},
        "max_positions": {"type": "integer", "description": "0 to 6"},
        "jev_min_prob": {"type": "number", "description": "0.35 to 0.9"},
        "allow_long": {"type": "boolean"},
        "allow_short": {"type": "boolean"},
        "weights": {"type": "object", "properties": {k: {"type": "number"} for k in PARAMS}, "required": list(PARAMS)},
        "blacklist": {"type": "array", "items": {"type": "string"}},
        "actions": {"type": "array", "items": {"type": "object", "properties": {
            "op": {"type": "string", "enum": ["open", "close"]},
            "symbol": {"type": "string"},
            "side": {"type": "string", "enum": ["long", "short"]},
            "stop_pct": {"type": "number"},
            "take_pct": {"type": "number"},
            "size_mult": {"type": "number"},
            "why": {"type": "string"}}, "required": ["op", "symbol", "why"]}},
    },
    "required": ["view", "risk_pct", "max_positions", "jev_min_prob", "allow_long", "allow_short",
                 "weights", "blacklist", "actions"],
}


def ask_opus(snapshot):
    cmd = [shutil.which("claude") or "claude", "-p", "--model", "opus", "--tools", "", "--safe-mode",
           "--no-session-persistence", "--output-format", "json",
           "--system-prompt", OPUS_SYSTEM, "--json-schema", json.dumps(OPUS_SCHEMA)]
    env = {k: v for k, v in os.environ.items() if k not in ("CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT")}
    r = subprocess.run(cmd, input=json.dumps(snapshot), capture_output=True, text=True, encoding="utf-8",
                       timeout=150, env=env, cwd=ROOT)
    out = json.loads(r.stdout)
    if out.get("is_error"):
        raise RuntimeError(str(out.get("result"))[:200])
    return out.get("structured_output") or json.loads(out["result"]), out.get("total_cost_usd", 0)


# ---------------------------------------------------------------- agent

class Agent:
    def __init__(self, minutes):
        self.B, self.jev = Broker(), Jev(load_key())
        self.lock, self.actions, self.done = threading.Lock(), queue.SimpleQueue(), threading.Event()
        self.minutes = minutes
        self.curve, self.decisions, self.vetoes, self.cool, self.scan, self.day = [], [], [], {}, {}, {}
        self.F, self.Q, self.KL = {}, {}, {}
        self.opus = dict(status="aguardando", calls=0, cost=0.0, secs=0, view="")
        self.state = "{}"
        os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
        self.log_path = os.path.join(ROOT, "logs", time.strftime("run-%Y%m%d-%H%M%S.jsonl"))

    def log(self, kind, **kw):
        with open(self.log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(dict(t=round(time.time(), 2), kind=kind, **kw)) + "\n")

    # -- universe: liquid, tight spread, most volatile per minute
    def pick_universe(self):
        t = get("ticker/24hr", type="MINI")
        c = [x for x in t if x["symbol"].endswith("USDT") and x["symbol"][:-4] not in STABLES
             and float(x["quoteVolume"]) > 15e6]
        syms = [x["symbol"] for x in sorted(c, key=lambda x: -float(x["quoteVolume"]))[:30]]
        Q, KL = quotes(syms), all_klines(syms)

        def score(s):
            bid, ask = Q[s]
            if len(KL[s]) < 120 or (ask - bid) / bid > 0.0005:
                return -1
            return atr(KL[s][:-1], 60) / KL[s][-1]["c"]

        ranked = [s for s in sorted(syms, key=score, reverse=True) if score(s) > 0 and s not in ("BTCUSDT", "ETHUSDT")]
        self.uni = ["BTCUSDT", "ETHUSDT"] + ranked[:N_COINS - 2]
        self.KL = {s: KL[s] for s in self.uni}
        self.log("universe", coins=self.uni)

    def size(self, eq, stop_frac, mult):
        n = eq * POLICY["risk_pct"] / 100 / stop_frac * mult
        return min(n, eq * MAX_POS, eq * MAX_GROSS - self.B.gross(self.Q))

    def close(self, s, why, now):
        t = self.B.close(s, self.Q[s], why, now)
        self.cool[s] = now + 120
        self.log("close", **t)

    def open(self, s, side, strat, stop_frac, tp_frac, mult, why, now, trail, tmax):
        n = self.size(self.B.equity(self.Q), stop_frac, mult)
        if n < 100:
            return
        p = self.B.open(s, side, n, self.Q[s], stop_frac, tp_frac, strat, why, trail, now, tmax)
        self.log("open", **{k: v for k, v in p.items()})

    # -- main loop
    def run(self):
        self.pick_universe()
        self.start = time.time()
        self.end = self.start + self.minutes * 60
        threading.Thread(target=self.opus_loop, daemon=True).start()
        last_kl = last_exit = 0
        print(f"sessao {self.minutes} min | moedas: {', '.join(s[:-4] for s in self.uni)} | Jev: {self.jev.status}")
        print(f"dashboard: http://localhost:{PORT}   log: {self.log_path}")
        while True:
            t0 = time.time()
            try:
                Q = quotes(self.uni)
                if t0 - last_kl >= 10:
                    self.KL.update(all_klines(self.uni))
                    last_kl = t0
            except Exception as e:
                self.log("data_error", err=str(e))
                time.sleep(TICK)
                continue
            now = time.time()
            with self.lock:
                self.Q = Q
                for s in self.uni:
                    mid = sum(Q[s]) / 2
                    cur = self.KL[s][-1]
                    cur["c"], cur["h"], cur["l"] = mid, max(cur["h"], mid), min(cur["l"], mid)
                    self.F[s] = features(self.KL[s], mid, now)
                if now >= self.end:
                    for s in list(self.B.pos):
                        self.close(s, "fim da sessao", now)
                    self.publish(now)
                    break
                try:
                    self.step(now, run_exit_monitor=now - last_exit >= 20)
                except Exception as e:
                    self.log("step_error", err=repr(e))
                if now - last_exit >= 20:
                    last_exit = now
                self.publish(now)
            time.sleep(max(0.2, TICK - (time.time() - t0)))
        self.done.set()
        eq = self.B.equity(self.Q)
        self.log("end", equity=eq, pnl=eq - START_CASH, trades=len(self.B.trades), fees=self.B.fees)
        print(f"FIM  equity ${eq:,.2f}  PnL ${eq - START_CASH:+,.2f} ({100 * (eq / START_CASH - 1):+.2f}%)  "
              f"trades {len(self.B.trades)}  taxas ${self.B.fees:,.2f}")

    def step(self, now, run_exit_monitor):
        B, F, btc = self.B, self.F, self.F["BTCUSDT"]
        # 1. stops / targets / trailing / time
        for s, p in list(B.pos.items()):
            why = manage(p, self.Q[s], now)
            if why:
                self.close(s, why, now)
        # 2. Jev exit monitor
        if run_exit_monitor and self.jev.key:
            olds = [s for s, p in B.pos.items() if now - p["opened"] >= 180]
            for s, ans in zip(olds, POOL.map(lambda s: self.jev.ask(self.exit_state(s, now), EXIT_Q), olds)):
                if ans and ans.get("exit", {}).get("noul", 0) >= JEV_EXIT and s in B.pos:
                    self.close(s, f"jev saida {ans['exit']['noul']:.2f}", now)
        # 3. Opus actions
        while not self.actions.empty():
            self.apply_action(self.actions.get(), now)
        # 4. engine entries through the Jev gate
        if now > self.end - NO_ENTRY_TAIL:
            return
        room = POLICY["max_positions"] - len(B.pos)
        if room <= 0:
            return
        cands = []
        for s in self.uni:
            if s in B.pos or s[:-4] in POLICY["blacklist"] or self.cool.get(s, 0) > now:
                continue
            for side, strat, tp_frac in signals(F[s], btc, s == "BTCUSDT"):
                w = POLICY["weights"].get(strat, 0)
                if w > 0 and POLICY["allow_long" if side > 0 else "allow_short"]:
                    cands.append((w * min(F[s]["volr"], 5), s, side, strat, tp_frac))
                    break
        cands = sorted(cands, reverse=True)[:room]
        gates = list(POOL.map(lambda c: self.gate(*c[1:4]), cands))
        for (w, s, side, strat, tp_frac), g in zip(cands, gates):
            pdir, q, ex = g if g else (None, 0.5, 0)
            if g and (pdir < POLICY["jev_min_prob"] or ex > 0.7):
                self.veto(s, side, strat, pdir, q, ex)
                continue
            a, P = F[s]["atrp"], PARAMS[strat]
            stop = max(P["stop"] * a, MIN_STOP)
            tp = max(tp_frac or P["tp"] * a, 1.5 * stop)
            mult = POLICY["weights"][strat] * (0.6 + 0.8 * q)
            why = f"{strat} | jev p={pdir:.2f} q={q:.2f} exh={ex:.2f}" if g else f"{strat} | jev offline"
            self.open(s, side, strat, stop, tp, min(mult, 1.5), why, now, P["trail"], P["tmax"])

    def gate(self, s, side, strat):
        try:
            return self._gate(s, side, strat)
        except Exception as e:  # unexpected answer shape: treat as Jev offline for this call
            self.log("jev_error", err=str(e))
            return None

    def _gate(self, s, side, strat):
        state = {"coin": s[:-4], "market": words(self.F[s], self.F["BTCUSDT"]),
                 "proposed trade": f"{'LONG' if side > 0 else 'SHORT'}: {STRAT_DESC[strat]}, held 2 to 15 minutes"}
        a = self.jev.ask(state, GATE_Q)
        if not a:
            return None
        return (a["direction"]["probabilities"]["up" if side > 0 else "down"],
                a["quality"]["score"] / 4, a["exhausted"]["noul"])

    def exit_state(self, s, now):
        p = self.B.pos[s]
        u = 100 * self.B.upnl(p, self.Q[s]) / (p["entry"] * p["qty"])
        return {"coin": s[:-4], "market": words(self.F[s], self.F["BTCUSDT"]),
                "open position": f"{'LONG' if p['side'] > 0 else 'SHORT'} opened {int((now - p['opened']) / 60)} minutes ago, "
                                 f"currently {'in profit' if u > 0 else 'at a loss'} ({u:+.1f}%)"}

    def apply_action(self, a, now):
        s = a.get("symbol", "").upper().replace("/", "")
        s = s if s.endswith("USDT") else s + "USDT"
        if s not in self.uni:
            return
        if a["op"] == "close" and s in self.B.pos:
            self.close(s, "opus: " + a.get("why", "")[:80], now)
        elif (a["op"] == "open" and s not in self.B.pos and a.get("side") in ("long", "short")
              and now < self.end - NO_ENTRY_TAIL and len(self.B.pos) < 6):
            side = 1 if a["side"] == "long" else -1
            g = self.gate(s, side, "opus")
            pdir, q, ex = g if g else (None, 0.5, 0)
            if g and (pdir < POLICY["jev_min_prob"] or ex > 0.7):
                self.veto(s, side, "opus", pdir, q, ex, a.get("why", ""))
                return
            stop = clamp(a.get("stop_pct", 1.0) / 100, MIN_STOP, 0.03)
            self.open(s, side, "opus", stop, clamp(a.get("take_pct", 2.0) / 100, 1.5 * stop, 0.08),
                      clamp(a.get("size_mult", 1.0), 0.3, 1.5) * (0.6 + 0.8 * q),
                      f"opus: {a.get('why', '')[:120]} | " + (f"jev p={pdir:.2f} q={q:.2f}" if g else "jev offline"),
                      now, True, 30)

    def veto(self, s, side, strat, pdir, q, ex, why=""):
        v = dict(t=round(time.time()), coin=s[:-4], side="long" if side > 0 else "short", strat=strat,
                 jev_p_dir=round(pdir, 2), jev_quality=round(q, 2), jev_exhausted=round(ex, 2), why=why[:120])
        self.vetoes.append(v)
        self.log("jev_veto", **v)

    # -- Opus thread
    def opus_loop(self):
        while len(self.F) < len(self.uni):  # wait for the first market tick
            time.sleep(0.5)
        while not self.done.is_set():
            t0 = time.time()
            try:
                self.opus["status"] = "pensando"
                self.jev_scan()
                try:
                    for x in get("ticker/24hr", symbols=json.dumps(self.uni, separators=(",", ":")), type="MINI"):
                        o, c, h, l = (float(x[k]) for k in ("openPrice", "lastPrice", "highPrice", "lowPrice"))
                        self.day[x["symbol"]] = dict(r24h=round(100 * (c / o - 1), 2), pos_in_24h_range=round((c - l) / (h - l or 1), 2))
                except Exception as e:
                    self.log("data_error", err=str(e))
                with self.lock:
                    snap = self.snapshot(time.time())
                d, cost = ask_opus(snap)
                secs = round(time.time() - t0, 1)
                POLICY.update(
                    risk_pct=clamp(d["risk_pct"], 0.25, 2.5), max_positions=int(clamp(d["max_positions"], 0, 6)),
                    jev_min_prob=clamp(d["jev_min_prob"], 0.35, 0.9), allow_long=bool(d["allow_long"]),
                    allow_short=bool(d["allow_short"]), blacklist=[x.upper().replace("USDT", "") for x in d["blacklist"]],
                    weights={k: clamp(float(d["weights"].get(k, 0)), 0, 2) for k in PARAMS})
                for a in d["actions"]:
                    self.actions.put(a)
                self.opus.update(status="ok", calls=self.opus["calls"] + 1, cost=self.opus["cost"] + cost,
                                 secs=secs, view=d["view"])
                self.decisions.append(dict(t=round(time.time()), secs=secs, view=d["view"], actions=d["actions"],
                                           policy=json.loads(json.dumps(POLICY))))
                self.log("opus", secs=secs, cost=cost, decision=d)
            except Exception as e:
                self.opus["status"] = f"erro: {e}"[:160]
                self.log("opus_error", err=str(e)[:500])
            self.done.wait(max(5, OPUS_EVERY - (time.time() - t0)))

    def jev_scan(self):
        with self.lock:
            states = {s: {"coin": s[:-4], "market": words(self.F[s], self.F["BTCUSDT"])} for s in self.uni if s in self.F}
        for s, a in zip(states, POOL.map(lambda s: self.jev.ask(states[s], SCAN_Q), states)):
            if a:
                self.scan[s] = dict(up=a["direction"]["probabilities"]["up"], down=a["direction"]["probabilities"]["down"],
                                    sideways=a["direction"]["probabilities"]["sideways"], exhausted=a["exhausted"]["noul"])

    def snapshot(self, now):
        B, r = self.B, lambda x, n=2: round(x, n)
        eq = B.equity(self.Q)
        coins = []
        for s in self.uni:
            f, j = self.F[s], self.scan.get(s, {})
            sig = signals(f, self.F["BTCUSDT"], s == "BTCUSDT")
            coins.append(dict(
                coin=s[:-4], px=f["px"], r1m=r(100 * f["r1"]), r5m=r(100 * f["r5"]), r15m=r(100 * f["r15"]),
                r60m=r(100 * f["r60"]), atr1m_pct=r(100 * f["atrp"], 3), vol_x=r(f["volr"], 1),
                buy_flow=r(f["flow"]), rsi14=round(f["rsi14"]), trend=words(f, self.F["BTCUSDT"])["trend"],
                vs_vwap_pct=r(100 * (f["px"] / f["vwap"] - 1)),
                engine_signal=[f"{'long' if sd > 0 else 'short'} {st}" for sd, st, _ in sig], **self.day.get(s, {}),
                **{f"jev_{k}": r(v) for k, v in j.items()}))
        stats = {}
        for t in B.trades:
            x = stats.setdefault(t["strat"], dict(n=0, wins=0, pnl=0.0))
            x["n"] += 1
            x["wins"] += t["pnl"] > 0
            x["pnl"] = r(x["pnl"] + t["pnl"])
        return dict(
            minutes_left=r((self.end - now) / 60, 1), equity=r(eq), pnl_usd=r(eq - START_CASH), fees_paid=r(B.fees),
            open_positions=[dict(coin=s[:-4], side="long" if p["side"] > 0 else "short", strat=p["strat"],
                                 entry=p["entry"], px=B.mark(p, self.Q[s]), upnl_usd=r(B.upnl(p, self.Q[s])),
                                 notional=r(p["entry"] * p["qty"]), stop=p["stop"], tp=p["tp"],
                                 age_min=r((now - p["opened"]) / 60, 1)) for s, p in B.pos.items()],
            recent_trades=[dict(coin=t["sym"][:-4], side=t["side"], strat=t["strat"], pnl_usd=r(t["pnl"]),
                                pnl_pct=r(t["pnl_pct"]), mins=r((t["closed"] - t["opened"]) / 60, 1),
                                exit=t["why_close"]) for t in B.trades[-12:]],
            strategy_stats=stats, current_policy=POLICY, coins=coins, recent_jev_vetoes=self.vetoes[-6:],
            your_last_view=self.opus["view"])

    # -- dashboard state
    def publish(self, now):
        B = self.B
        eq = B.equity(self.Q)
        if not self.curve or now - self.curve[-1][0] >= 4:
            self.curve.append([round(now, 1), round(eq, 2)])
        wins = sum(t["pnl"] > 0 for t in B.trades)
        self.state = json.dumps(dict(
            start=self.start, end=self.end, now=now, done=now >= self.end, start_cash=START_CASH, equity=eq,
            pnl=eq - START_CASH, fees=B.fees, wins=wins, universe=[s[:-4] for s in self.uni],
            positions=[dict(sym=s[:-4], side="long" if p["side"] > 0 else "short", strat=p["strat"], entry=p["entry"],
                            px=B.mark(p, self.Q[s]), upnl=B.upnl(p, self.Q[s]), notional=p["entry"] * p["qty"],
                            stop=p["stop"], tp=p["tp"], opened=p["opened"]) for s, p in B.pos.items()],
            trades=[dict(t, sym=t["sym"][:-4]) for t in B.trades], curve=self.curve, decisions=self.decisions,
            policy=POLICY, opus=self.opus,
            jev=dict(status=self.jev.status, calls=self.jev.calls, errors=self.jev.errors, ms=self.jev.ms)))


# ---------------------------------------------------------------- dashboard server

def serve(agent):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/state"):
                body, ctype = agent.state.encode(), "application/json"
            elif self.path.startswith("/log"):
                body, ctype = open(agent.log_path, "rb").read() if os.path.exists(agent.log_path) else b"", "application/x-ndjson"
            else:
                body, ctype = open(os.path.join(ROOT, "dashboard.html"), "rb").read(), "text/html; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()


def selftest():
    b = Broker()
    p = b.open("XUSDT", 1, 1000, (99.99, 100.0), 0.01, 0.02, "t", "", True, 0, 15)
    assert abs(p["entry"] - 100.02) < 1e-9 and abs(b.balance - 4999) < 1e-9 and abs(p["stop"] - 99.0198) < 1e-9
    assert manage(p, (101.1, 101.11), 10) is None and abs(p["stop"] - 100.02 * 1.0025) < 1e-9  # +1R -> breakeven
    assert manage(p, (101.6, 101.61), 20) is None and p["stop"] > 100.02 * 1.0025               # +1.5R -> trailing
    assert manage(p, (100.5, 100.51), 30) == "trailing"
    t = b.close("XUSDT", (100.5, 100.51), "x", 30)
    exit_px = 100.5 * (1 - SLIP)
    assert abs(t["pnl"] - ((exit_px - 100.02) * p["qty"] - exit_px * p["qty"] * FEE - 1)) < 1e-9
    assert abs(b.balance - (START_CASH + t["pnl"])) < 1e-9 and not b.pos
    s = b.open("YUSDT", -1, 1000, (50.0, 50.01), 0.01, 0.02, "t", "", False, 0, 15)
    assert manage(s, (48.9, 48.99), 1) == "alvo" and b.upnl(s, (48.9, 48.99)) > 0
    assert manage(s, (50.49, 50.51), 2) == "stop"
    assert manage(s, (49.9, 49.91), 16 * 60) == "tempo"
    print("selftest ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["test"]:
        selftest()
        sys.exit()
    agent = Agent(float(sys.argv[1]) if len(sys.argv) > 1 else 60)
    agent.start = agent.end = time.time()
    threading.Thread(target=serve, args=(agent,), daemon=True).start()
    try:
        agent.run()
        print("sessao encerrada; dashboard segue no ar. Ctrl+C para sair.")
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
