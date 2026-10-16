"""
Pump.fun Mayhem Mode: recorder + multi-strategy paper trader. Read-only: no keys, no wallet, no orders.

  data    Solana public RPC websocket (logsSubscribe on the pump program); Anchor events decoded from the logs
  record  every Mayhem-coin create / trade / graduation -> SQLite (data/mayhem.db)
  paper   strategies trade the same stream side by side, each with its own paper account, next to random-entry
          controls that use the same exits. Fills walk the bonding curve with the coin's real fee (1.25%/side),
          1.5s latency, price impact, a slippage limit and a per-transaction fee.
  replay  the same engine over the recorded tape, with per-strategy statistics against the controls

  python mayhem.py           record + paper trade live, dashboard at http://localhost:8766
  python mayhem.py replay    backtest the strategies on everything recorded so far
  python mayhem.py test      self-check of the curve math and the paper broker
"""
import asyncio, base64, collections, heapq, json, os, random, sqlite3, statistics, struct, sys, threading, time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
DB = os.path.join(DATA, "mayhem.db")
PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
# ponytail: the public RPC can lag or drop messages under load; point RPC_WS at a private node if coverage matters
RPC_WS = os.environ.get("RPC_WS", "wss://api.mainnet-beta.solana.com")
PORT = 8766
LAMPORTS = 1e9
START_SOL = 10.0       # paper bankroll per strategy
SIZE_SOL = 0.2         # SOL per entry, fees included
MAX_OPEN = 8           # open positions per strategy
LATENCY = 1.5          # seconds from decision to landing on chain
TX_FEE_SOL = 0.0003    # base + priority fee per transaction
MAX_SLIP = 0.15        # a buy landing 15% above the decision price fails (the tx fee is still paid)
DEFAULT_FEE_BPS = 125  # 0.95% protocol + 0.30% creator per side, as observed on Mayhem coins
WINDOW = 24 * 3600     # the Mayhem agent trades a coin for its first 24h
COOLDOWN = 300         # seconds before the same strategy re-enters the same coin
MIN_REAL_SOL = 1.0     # entries need real SOL in the curve: exits can never take out more than is really there


# ---------------------------------------------------------------- event decoding

B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58(b):
    n, s = int.from_bytes(b, "big"), ""
    while n:
        n, r = divmod(n, 58)
        s = B58[r] + s
    return "1" * (len(b) - len(b.lstrip(b"\0"))) + s


# Field order from pump-fun/pump-public-docs idl/pump.json; fields after the last one listed are ignored.
S, P, U, I, B = "string", "pubkey", "u64", "i64", "bool"
CREATE = [("name", S), ("symbol", S), ("uri", S), ("mint", P), ("bonding_curve", P), ("user", P), ("creator", P),
          ("timestamp", I), ("vtok", U), ("vsol", U), ("rtok", U), ("supply", U), ("token_program", P), ("mayhem", B)]
TRADE = [("mint", P), ("sol", U), ("tok", U), ("buy", B), ("user", P), ("timestamp", I), ("vsol", U), ("vtok", U),
         ("rsol", U), ("rtok", U), ("fee_recipient", P), ("fee_bps", U), ("fee", U), ("creator", P), ("cfee_bps", U),
         ("cfee", U), ("track_volume", B), ("unclaimed", U), ("claimed", U), ("sol_volume", U), ("last_update", I),
         ("ix", S), ("mayhem", B)]
COMPLETE = [("user", P), ("mint", P), ("bonding_curve", P), ("timestamp", I)]
EVENTS = {bytes([27, 114, 169, 77, 222, 235, 99, 118]): ("create", CREATE),
          bytes([189, 219, 127, 211, 78, 230, 97, 238]): ("trade", TRADE),
          bytes([95, 114, 97, 156, 212, 46, 152, 8]): ("complete", COMPLETE)}


def decode(layout, b):
    out, o = {}, 8
    for name, t in layout:
        if t == P:
            v, o = b58(b[o:o + 32]), o + 32
        elif t == S:
            n = struct.unpack_from("<I", b, o)[0]
            v, o = b[o + 4:o + 4 + n].decode("utf-8", "replace"), o + 4 + n
        elif t == B:
            v, o = b[o] != 0, o + 1
        else:
            v, o = struct.unpack_from("<Q" if t == U else "<q", b, o)[0], o + 8
        out[name] = v
    return out


def events(logs):
    """Pump events in a transaction's logs: (kind, fields)."""
    for line in logs:
        if line.startswith("Program data: "):
            raw = base64.b64decode(line[14:])
            ev = EVENTS.get(raw[:8])
            if ev:
                yield ev[0], decode(ev[1], raw)


# ---------------------------------------------------------------- bonding curve

class Coin:
    def __init__(self, t, e):
        self.mint, self.sym, self.name, self.dev = e["mint"], e["symbol"][:12], e["name"][:40], e["creator"]
        self.t0, self.vsol, self.vtok, self.rsol = t, e["vsol"], e["vtok"], 0
        self.fee_bps, self.done, self.dev_sold = DEFAULT_FEE_BPS, False, False
        self.tape = collections.deque()           # (t, agent, buy, lamports, user), last 10 minutes
        self.px = collections.deque()             # (t, price), last 10 minutes
        self.bal = collections.defaultdict(int)   # organic token balances, from the trades we saw
        self.n = [0, 0]                           # organic, agent trades

    @property
    def price(self):  # lamports per raw token unit; x1e-3 = SOL per token
        return self.vsol / self.vtok

    def update(self, t, e, agent):
        self.vsol, self.vtok, self.rsol = e["vsol"], e["vtok"], e["rsol"]
        if not agent:
            self.fee_bps = e["fee_bps"] + e["cfee_bps"]
            self.bal[e["user"]] += e["tok"] if e["buy"] else -e["tok"]
            self.dev_sold |= e["user"] == self.dev and not e["buy"]
        self.n[agent] += 1
        self.tape.append((t, agent, e["buy"], e["sol"], e["user"]))
        self.px.append((t, self.price))
        while self.tape[0][0] < t - 600:
            self.tape.popleft()
        while self.px[0][0] < t - 600:
            self.px.popleft()

    def ret(self, now, w):
        """Price change over the last w seconds."""
        for t, p in self.px:
            if t >= now - w and p > 0:  # the agent can walk the virtual reserve all the way to zero
                return self.price / p - 1
        return 0.0

    def flow(self, now, w, agent):
        """SOL bought, SOL sold and buyer wallets over the last w seconds, agent or organic side."""
        buy = sell = 0
        users = set()
        for t, a, b, sol, u in reversed(self.tape):
            if t < now - w:
                break
            if a == agent:
                if b:
                    buy += sol
                    users.add(u)
                else:
                    sell += sol
        return buy / LAMPORTS, sell / LAMPORTS, users

    def top10(self):
        held = sorted((v for v in self.bal.values() if v > 0), reverse=True)
        return sum(held[:10]) / sum(held) if held else 0.0


def buy_tokens(c, sol):
    """Raw tokens received for `sol` SOL spent (fee included), and the lamports that entered the curve."""
    x = sol * LAMPORTS / (1 + c.fee_bps / 1e4)
    return c.vtok - c.vsol * c.vtok / (c.vsol + x), x


def sell_sol(c, tok, x=0.0):
    """SOL received for selling `tok` raw tokens, after the fee. x: lamports our own buy left in the curve.
    Paper trades never touch the real reserves, so the exit is priced on the curve shifted by our entry;
    otherwise every round trip would pay the entry's price impact twice."""
    vsol = c.vsol + x
    vtok = c.vsol * c.vtok / vsol
    out = min(vsol - vsol * vtok / (vtok + tok), c.rsol + x)
    return out * (1 - c.fee_bps / 1e4) / LAMPORTS


# ---------------------------------------------------------------- strategies (hypotheses, all long-only)

def momentum(c, now, rng):
    """Organic buyers piling in, early in the move, holders not concentrated, dev still in."""
    if now - c.t0 < 60 or c.dev_sold:
        return None
    b, s, users = c.flow(now, 60, agent=False)
    r = c.ret(now, 60)
    if b - s >= 1.5 and len(users) >= 4 and 0.05 <= r <= 0.5 and c.top10() < 0.35:
        return f"fluxo org +{b - s:.1f} SOL, {len(users)} compradores, {r:+.0%}/60s"


def agent_dip(c, now, rng):
    """A drop driven by the agent's (random, fee-free) selling while organic flow holds: bet on a rebound."""
    if now - c.t0 < 300:
        return None
    r = c.ret(now, 180)
    if r > -0.10:
        return None
    ab, asl, _ = c.flow(now, 180, agent=True)
    ob, osl, _ = c.flow(now, 180, agent=False)
    if asl > ab and asl >= 0.6 * (asl + osl) and ob - osl >= -0.2:
        return f"queda {r:.0%}/3min puxada pelo agente (venda liq {asl - ab:.2f} SOL)"


def control(p):
    def entry(c, now, rng):
        return "aleatorio" if now - c.t0 >= 60 and rng.random() < p else None
    return entry


MOM_EXIT = dict(sl=0.15, tp=None, arm=0.12, trail=0.10, tmax=900)
DIP_EXIT = dict(sl=0.12, tp=0.08, arm=None, trail=None, tmax=600)
STRATEGIES = [  # name, label, entry, exits. Each controle shares the exits of the strategy above it.
    ("momentum", "Momentum orgânico", momentum, MOM_EXIT),
    ("ctrl_momentum", "Controle momentum", control(0.0004), MOM_EXIT),
    ("agent_dip", "Queda do agente", agent_dip, DIP_EXIT),
    ("ctrl_dip", "Controle queda", control(0.0004), DIP_EXIT),
]


# ---------------------------------------------------------------- paper engine

class Account:
    def __init__(self, name, label, entry, exits):
        self.name, self.label, self.entry, self.x = name, label, entry, exits
        self.cash, self.pos, self.trades, self.curve, self.cool, self.fails = START_SOL, {}, [], [], {}, 0

    def value(self, coins):
        return self.cash + sum(max(sell_sol(coins[m], p["tok"], p["x"]) - TX_FEE_SOL, 0) for m, p in self.pos.items())


class Engine:
    def __init__(self, log=None, seed=1):
        self.coins, self.pending, self.now = {}, [], 0.0
        self.log = log or (lambda **kw: None)
        self.rng = random.Random(seed)
        self.accts = [Account(*s) for s in STRATEGIES]
        self.agents = collections.Counter()

    def create(self, t, e):
        self.advance(t)
        if e.get("mayhem"):
            self.coins[e["mint"]] = Coin(t, e)

    def trade(self, t, e):
        self.advance(t)
        c = self.coins.get(e["mint"])
        if not c or c.done:
            return
        agent = e["fee_bps"] == 0 and e["cfee_bps"] == 0  # the agent trades fee-free; everyone else pays
        if agent:
            self.agents[e["user"]] += 1
        c.update(t, e, agent)
        young = t - c.t0 < WINDOW - 3600 and c.rsol >= MIN_REAL_SOL * LAMPORTS and c.vsol >= LAMPORTS
        for a in self.accts:
            p = a.pos.get(c.mint)
            if p:
                self.check_exit(a, c, p, t)
            elif (young and len(a.pos) < MAX_OPEN and a.cool.get(c.mint, 0) <= t and a.cash >= SIZE_SOL + TX_FEE_SOL
                  and not any(o["a"] is a and o["mint"] == c.mint for o in self.pending)):
                why = a.entry(c, t, self.rng)
                if why:
                    self.order(a, "buy", c, t, why)

    def complete(self, t, e):
        self.advance(t)
        c = self.coins.get(e["mint"])
        if c:
            for a in self.accts:
                if c.mint in a.pos:
                    self.fill_sell(a, c, t, "graduou")
            c.done = True

    def order(self, a, side, c, t, why):
        if side == "sell":
            a.pos[c.mint]["closing"] = True
        self.pending.append(dict(t=t + LATENCY, a=a, side=side, mint=c.mint, px=c.price, why=why))

    def advance(self, t):
        self.now = t
        if any(o["t"] <= t for o in self.pending):
            due = [o for o in self.pending if o["t"] <= t]
            self.pending = [o for o in self.pending if o["t"] > t]
            for o in due:
                c = self.coins[o["mint"]]
                if o["side"] == "buy":
                    self.fill_buy(o["a"], c, o["t"], o["why"], o["px"])
                else:
                    self.fill_sell(o["a"], c, o["t"], o["why"])
        for a in self.accts:
            for m, p in a.pos.items():
                if not p.get("closing") and t - p["t"] >= a.x["tmax"]:
                    self.order(a, "sell", self.coins[m], t, "tempo")

    def fill_buy(self, a, c, t, why, px0):
        if c.done:
            return
        if c.price > px0 * (1 + MAX_SLIP):
            a.cash -= TX_FEE_SOL
            a.fails += 1
            self.log(kind="fail", strat=a.name, mint=c.mint, sym=c.sym, t=t, slip=c.price / px0 - 1, why=why)
            return
        a.cash -= SIZE_SOL + TX_FEE_SOL
        tok, x = buy_tokens(c, SIZE_SOL)
        a.pos[c.mint] = dict(tok=tok, x=x, cost=SIZE_SOL + TX_FEE_SOL, t=t, mcap=mcap(c), peak=0.0, why=why, sym=c.sym)
        self.log(kind="open", strat=a.name, mint=c.mint, sym=c.sym, t=t, mcap=mcap(c), why=why)

    def fill_sell(self, a, c, t, why):
        p = a.pos.pop(c.mint, None)
        if not p:
            return
        got = sell_sol(c, p["tok"], p["x"]) - TX_FEE_SOL
        a.cash += got
        tr = dict(strat=a.name, mint=c.mint, sym=c.sym, opened=p["t"], closed=t, cost=p["cost"], got=got,
                  pnl=got - p["cost"], pct=100 * (got / p["cost"] - 1), mcap_in=p["mcap"], mcap_out=mcap(c),
                  why_open=p["why"], why_close=why)
        a.trades.append(tr)
        a.cool[c.mint] = t + COOLDOWN
        self.log(kind="close", **tr)

    def check_exit(self, a, c, p, t):
        if p.get("closing"):
            return
        r = (sell_sol(c, p["tok"], p["x"]) - TX_FEE_SOL) / p["cost"] - 1
        p["peak"] = max(p["peak"], r)
        x = a.x
        why = ("stop" if r <= -x["sl"] else
               "alvo" if x["tp"] and r >= x["tp"] else
               "trailing" if x["trail"] and p["peak"] >= x["arm"] and r <= p["peak"] - x["trail"] else None)
        if why:
            self.order(a, "sell", c, t, f"{why} {r:+.0%}")

    def prune(self, t):
        held = {m for a in self.accts for m in a.pos} | {o["mint"] for o in self.pending}
        for m in [m for m, c in self.coins.items() if t - c.t0 > WINDOW + 3600 and m not in held]:
            del self.coins[m]


def mcap(c):  # market cap in SOL on a 1B-token basis
    return c.price * 1e-3 * 1e9


# ---------------------------------------------------------------- recorder

SCHEMA = """
CREATE TABLE IF NOT EXISTS coins(mint TEXT PRIMARY KEY, t REAL, ts INTEGER, sig TEXT, name TEXT, symbol TEXT,
    creator TEXT, vsol INTEGER, vtok INTEGER, rtok INTEGER, supply INTEGER);
CREATE TABLE IF NOT EXISTS trades(t REAL, ts INTEGER, slot INTEGER, sig TEXT, mint TEXT, user TEXT, buy INTEGER,
    sol INTEGER, tok INTEGER, vsol INTEGER, vtok INTEGER, rsol INTEGER, fee_bps INTEGER, cfee_bps INTEGER, ix TEXT);
CREATE INDEX IF NOT EXISTS trades_t ON trades(t);
CREATE TABLE IF NOT EXISTS completes(mint TEXT, t REAL, ts INTEGER);
CREATE TABLE IF NOT EXISTS gaps(t0 REAL, t1 REAL);
"""


class Recorder:
    def __init__(self, path=DB):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)
        self.rows = collections.defaultdict(list)

    def add(self, table, row):
        self.rows[table].append(row)

    def flush(self):
        for table, rows in self.rows.items():
            if rows:
                verb = "INSERT OR IGNORE" if table == "coins" else "INSERT"
                self.db.executemany(f"{verb} INTO {table} VALUES ({','.join('?' * len(rows[0]))})", rows)
        self.db.commit()
        self.rows.clear()


# ---------------------------------------------------------------- live

class Live:
    def __init__(self):
        self.start = time.time()
        self.rec, self.st = Recorder(), collections.Counter()
        self.log_path = os.path.join(DATA, time.strftime("paper-%Y%m%d-%H%M%S.jsonl"))
        self.eng = Engine(log=self.log)
        self.lag, self.seen, self.seen_q = collections.deque(maxlen=500), set(), collections.deque()
        self.sol_usd, self.state, self.gap_since = 0.0, "{}", None

    def log(self, **kw):
        with open(self.log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(kw) + "\n")

    def handle(self, msg):
        res = json.loads(msg).get("params", {}).get("result", {})
        v = res.get("value")
        if not v:
            return
        self.st["tx"] += 1
        if v["err"]:
            self.st["failed"] += 1
            return
        sig, slot, t = v["signature"], res["context"]["slot"], time.time()
        if sig in self.seen:
            return
        self.seen.add(sig)
        self.seen_q.append(sig)
        if len(self.seen_q) > 20000:
            self.seen.discard(self.seen_q.popleft())
        self.st["truncated"] += any(line.startswith("Log truncated") for line in v["logs"])
        try:
            evs = list(events(v["logs"]))
        except Exception:
            self.st["decode_errors"] += 1
            return
        for kind, e in evs:
            if kind == "create":
                self.st["creates"] += 1
                if e.get("mayhem"):
                    self.st["m_creates"] += 1
                    self.rec.add("coins", (e["mint"], t, e["timestamp"], sig, e["name"], e["symbol"], e["creator"],
                                           e["vsol"], e["vtok"], e["rtok"], e["supply"]))
                    self.eng.create(t, e)
            elif kind == "trade" and e.get("mayhem"):
                agent = e["fee_bps"] == 0 and e["cfee_bps"] == 0
                self.st["m_trades"] += 1
                self.st["agent_trades"] += agent
                self.lag.append(t - e["timestamp"])
                self.rec.add("trades", (t, e["timestamp"], slot, sig, e["mint"], e["user"], int(e["buy"]), e["sol"],
                                        e["tok"], e["vsol"], e["vtok"], e["rsol"], e["fee_bps"], e["cfee_bps"], e["ix"]))
                self.eng.trade(t, e)
            elif kind == "complete" and e["mint"] in self.eng.coins:
                self.rec.add("completes", (e["mint"], t, e["timestamp"]))
                self.eng.complete(t, e)

    async def stream(self):
        import websockets
        sub = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                          "params": [{"mentions": [PUMP]}, {"commitment": "processed"}]})
        backoff = 1
        while True:
            try:
                async with websockets.connect(RPC_WS, max_size=None, ping_interval=20, ping_timeout=30) as ws:
                    await ws.send(sub)
                    await ws.recv()
                    if self.gap_since:
                        self.rec.add("gaps", (self.gap_since, time.time()))
                        self.st["gap_secs"] += time.time() - self.gap_since
                        self.gap_since = None
                    backoff = 1
                    async for msg in ws:
                        try:
                            self.handle(msg)
                        except Exception as e:  # a bug in one message must not drop the stream
                            self.st["handle_errors"] += 1
                            self.log(kind="handle_error", t=time.time(), err=repr(e)[:300])
            except Exception as e:
                self.gap_since = self.gap_since or time.time()
                self.st["reconnects"] += 1
                self.log(kind="ws_error", t=time.time(), err=repr(e)[:200])
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def ticker(self):
        n = 0
        while True:
            await asyncio.sleep(0.25)
            now = time.time()
            self.eng.advance(now)
            n += 1
            if n % 8 == 0:  # every 2s
                self.rec.flush()
                self.publish(now)
            if n % 40 == 0:  # every 10s
                for a in self.eng.accts:
                    a.curve.append([round(now), round(a.value(self.eng.coins), 5)])
                    if len(a.curve) > 4000:
                        a.curve = a.curve[::2]
            if n % 2400 == 0:
                self.eng.prune(now)

    def sol_price(self):
        while True:
            try:
                with urllib.request.urlopen("https://data-api.binance.vision/api/v3/ticker/price?symbol=SOLUSDT", timeout=10) as r:
                    self.sol_usd = float(json.load(r)["price"])
            except Exception:
                pass
            time.sleep(60)

    def publish(self, now):
        eng, st = self.eng, self.st
        accts, pos, trades = [], [], []
        for a in eng.accts:
            n = len(a.trades)
            accts.append(dict(name=a.name, label=a.label, value=a.value(eng.coins), trades=n,
                              wins=sum(t["pnl"] > 0 for t in a.trades), fails=a.fails, open=len(a.pos),
                              avg=sum(t["pct"] for t in a.trades) / n if n else 0.0, curve=a.curve))
            for m, p in a.pos.items():
                c = eng.coins[m]
                v = sell_sol(c, p["tok"], p["x"]) - TX_FEE_SOL
                pos.append(dict(strat=a.name, sym=p["sym"], mint=m, opened=p["t"], cost=p["cost"], value=v,
                                pct=100 * (v / p["cost"] - 1), mcap_in=p["mcap"], mcap=mcap(c), why=p["why"]))
            trades += a.trades[-150:]
        active = sum(1 for c in eng.coins.values() if now - c.t0 < WINDOW and not c.done)
        self.state = json.dumps(dict(
            now=now, start=self.start, start_sol=START_SOL, size_sol=SIZE_SOL, sol_usd=self.sol_usd, accounts=accts,
            positions=pos, trades=sorted(trades, key=lambda t: t["closed"])[-300:],
            rec=dict(tx=st["tx"], failed=st["failed"], creates=st["creates"], m_creates=st["m_creates"],
                     m_trades=st["m_trades"], agent_trades=st["agent_trades"], coins=active,
                     lag=statistics.median(self.lag) if self.lag else None, reconnects=st["reconnects"],
                     gap_secs=round(st["gap_secs"] + (now - self.gap_since if self.gap_since else 0)),
                     truncated=st["truncated"], decode_errors=st["decode_errors"], handle_errors=st["handle_errors"],
                     agents=[[u, n] for u, n in eng.agents.most_common(3)])))

    def run(self):
        threading.Thread(target=self.sol_price, daemon=True).start()
        threading.Thread(target=serve, args=(self,), daemon=True).start()
        print(f"gravando em {DB}\npaper log {self.log_path}\ndashboard http://localhost:{PORT}")

        async def main():
            await asyncio.gather(self.stream(), self.ticker())
        try:
            asyncio.run(main())
        finally:
            self.rec.flush()


def serve(live):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/state"):
                body, ctype = live.state.encode(), "application/json"
            elif self.path.startswith("/log"):
                body = open(live.log_path, "rb").read() if os.path.exists(live.log_path) else b""
                ctype = "application/x-ndjson"
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


# ---------------------------------------------------------------- replay

def tape(db):
    """All recorded events in time order: (t, kind, fields)."""
    coins = ((r[0], "create", dict(mint=r[1], name=r[2], symbol=r[3], creator=r[4], vsol=r[5], vtok=r[6], mayhem=True))
             for r in db.execute("SELECT t, mint, name, symbol, creator, vsol, vtok FROM coins ORDER BY t"))
    trades = ((r[0], "trade", dict(mint=r[1], user=r[2], buy=bool(r[3]), sol=r[4], tok=r[5], vsol=r[6], vtok=r[7],
                                   rsol=r[8], fee_bps=r[9], cfee_bps=r[10]))
              for r in db.execute("SELECT t, mint, user, buy, sol, tok, vsol, vtok, rsol, fee_bps, cfee_bps "
                                  "FROM trades ORDER BY t"))
    done = ((r[0], "complete", dict(mint=r[1])) for r in db.execute("SELECT t, mint FROM completes ORDER BY t"))
    return heapq.merge(coins, trades, done, key=lambda x: x[0])


def replay(path=DB, seed=1):
    db = sqlite3.connect(path)
    eng, n, t = Engine(seed=seed), 0, 0.0
    for t, kind, e in tape(db):
        getattr(eng, kind)(t, e)
        n += 1
    eng.advance(t + LATENCY + 1)
    for a in eng.accts:  # close what is still open at the last known price
        for m in list(a.pos):
            eng.fill_sell(a, eng.coins[m], t, "fim do replay")
    t0 = db.execute("SELECT min(t) FROM trades").fetchone()[0] or t
    gaps = db.execute("SELECT coalesce(sum(t1 - t0), 0) FROM gaps").fetchone()[0]
    print(f"tape: {n} eventos, {len(eng.coins)} coins Mayhem com criação gravada, {(t - t0) / 3600:.1f}h, "
          f"{gaps / 60:.0f} min de buraco no stream")
    print(f"{'estratégia':<20}{'n':>5}{'win':>7}{'média':>9}{'mediana':>9}{'t':>7}{'soma SOL':>10}{'top5':>7}"
          f"{'1a met.':>9}{'2a met.':>9}")
    mid = (t0 + t) / 2
    for a in eng.accts:
        r = [x["pct"] for x in a.trades]
        if len(r) < 2:
            print(f"{a.label:<20}{len(r):>5}  poucos trades")
            continue
        m, sd = statistics.mean(r), statistics.stdev(r) or 1
        pnl = sorted((x["pnl"] for x in a.trades), reverse=True)
        tot = sum(pnl)
        h1 = [x["pct"] for x in a.trades if x["opened"] < mid] or [0]
        h2 = [x["pct"] for x in a.trades if x["opened"] >= mid] or [0]
        print(f"{a.label:<20}{len(r):>5}{100 * sum(x > 0 for x in r) / len(r):>6.0f}%{m:>+8.2f}%"
              f"{statistics.median(r):>+8.2f}%{m / sd * len(r) ** 0.5:>+7.2f}{tot:>+10.3f}"
              f"{(sum(pnl[:5]) / tot if tot > 0 else float('nan')):>7.0%}"
              f"{statistics.mean(h1):>+8.2f}%{statistics.mean(h2):>+8.2f}%")
    print("média = retorno líquido por trade (taxas, impacto, latência e tx fee incluídos); t > 2 ~ significativo")
    return eng


# ---------------------------------------------------------------- self-check

def selftest():
    e0 = dict(mint="M", symbol="T", name="T", creator="DEV", vsol=30_000_000_000, vtok=1_073_000_000_000_000, mayhem=True)
    eng = Engine(seed=1)
    eng.create(0.0, e0)
    c = eng.coins["M"]

    def tr(t, sol, buy=True, user="U", agent=False):
        k = c.vsol * c.vtok
        vsol = c.vsol + sol if buy else c.vsol - sol
        e = dict(mint="M", sol=sol, tok=abs(c.vtok - k // vsol), buy=buy, user=user, vsol=vsol, vtok=k // vsol,
                 rsol=vsol - 30_000_000_000, fee_bps=0 if agent else 95, cfee_bps=0 if agent else 30)
        eng.trade(t, e)

    tr(1, 5_000_000_000)                                        # someone buys 5 SOL: real reserves exist
    tok, x = buy_tokens(c, 1.0)
    back = sell_sol(c, tok, x)                                  # round trip at unchanged reserves
    assert abs(back - 0.9875 / 1.0125) < 1e-9, back             # exactly the two 1.25% fees, no double impact
    a = eng.accts[0]
    eng.order(a, "buy", c, 10, "t")
    tr(10.5, 1_000_000_000)                                     # lands during the latency: moves the price up
    expected = buy_tokens(c, SIZE_SOL)[0]
    tr(12, 1_000_000_000, agent=True)                           # the fill (t=11.5) uses the t=10.5 reserves
    assert abs(a.pos["M"]["tok"] - expected) < 1e-6
    assert abs(a.cash - (START_SOL - SIZE_SOL - TX_FEE_SOL)) < 1e-9
    assert eng.agents and c.n == [2, 1]
    for i in range(12):                                         # agent dumps: price falls through the 15% stop
        tr(20 + i, 600_000_000, buy=False, agent=True)
    tr(40, 1, agent=True)
    assert "M" not in a.pos and a.trades[-1]["why_close"].startswith("stop") and a.trades[-1]["pnl"] < 0
    assert abs(a.cash + 0 - (START_SOL + a.trades[-1]["pnl"])) < 1e-9
    b = eng.accts[1]
    eng.order(b, "buy", c, 50, "t")
    eng.advance(60)
    assert "M" in b.pos
    eng.advance(60 + 901)                                       # time stop fires, sells after latency
    eng.advance(60 + 903)
    assert "M" not in b.pos and b.trades[-1]["why_close"] == "tempo"
    print("selftest ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["test"]:
        selftest()
    elif sys.argv[1:] == ["replay"]:
        replay()
    else:
        Live().run()
