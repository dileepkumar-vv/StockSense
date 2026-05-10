"""
StockSense  NSE Watchlist Analyzer - Flask Backend
Run: pip install flask flask-cors requests yfinance niftystocks pandas
Then: python app.py
"""
from flask import Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS
import requests, json, time, os, csv, io, threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

# Allow GitHub Pages frontend (and localhost for dev)
# Replace YOUR_GITHUB_USERNAME with your actual GitHub username
CORS(app, origins=[
    "https://dileepkumar-vv.github.io",
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "null",  # local file:// access during dev
])

WATCHLIST_FILE = "watchlist.json"
DEFAULT_WATCHLIST = [
    "IRCTC","INOXWIND","HDFCBANK","ONGC","TMPV","TMCV",
    "PIIND","BEL","RELIANCE","INDUSINDBK","SBIN"
]
THRESHOLDS = {
    "cirp_series":              ["BZ","BE","XT","IL"],
    "min_market_cap_cr":        100,
    "max_impact_cost":          1.5,
    "min_delivery_pct":         25.0,
    "sell_buy_imbalance_ratio": 5,
    "max_annual_volatility":    80.0,
}
SCREENER_STATUS = {"running":False,"done":0,"total":0,"symbol":"","workers":1}

# ── Helpers ───────────────────────────────────────────────
def load_watchlist():
    if os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE) as f: return json.load(f)
    return DEFAULT_WATCHLIST[:]

def save_watchlist(s):
    with open(WATCHLIST_FILE,"w") as f: json.dump(s,f)

def safe_float(v, d=0.0):
    try:
        if v is None or str(v).strip() in ("","-","N/A","NA","--"): return d
        return float(str(v).replace(",","").replace("%","").strip())
    except: return d

def safe_int(v, d=0):
    try:
        if v is None or str(v).strip() in ("","-","N/A","NA","--"): return d
        return int(float(str(v).replace(",","").strip()))
    except: return d

def safe_bool(v):
    if isinstance(v,bool): return v
    return str(v).lower() in ("true","1","yes")

# ── NSE Session ───────────────────────────────────────────
def create_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Accept":"application/json,text/html,*/*",
        "Accept-Language":"en-US,en;q=0.9",
        "Referer":"https://www.nseindia.com/",
    })
    try:
        s.get("https://www.nseindia.com",timeout=10)
        time.sleep(0.5)
        s.get("https://www.nseindia.com/market-data/live-equity-market",timeout=10)
        time.sleep(0.5)
    except: pass
    return s

def refresh_session(session):
    try:
        session.get("https://www.nseindia.com",timeout=8)
        time.sleep(1)
    except: pass

# ── Fetch Quote ───────────────────────────────────────────
def fetch_quote(session, symbol, retries=3):
    from urllib.parse import quote as uq
    # Some symbols have NSE quirks
    aliases = {
        "M&M": ["M%26M"],
    }
    sym_list = aliases.get(symbol.upper(), [symbol.upper()])

    for sym in sym_list:
        encoded = uq(sym, safe="")
        url = f"https://www.nseindia.com/api/quote-equity?symbol={encoded}"
        for attempt in range(retries+1):
            try:
                r = session.get(url, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    if data and ("priceInfo" in data or "securityInfo" in data):
                        # Fetch trade_info (delivery%, volatility, impact cost, market cap)
                        # Retry up to 3 times -- NSE can 401 during live market on first hit
                        ti_url = f"https://www.nseindia.com/api/quote-equity?symbol={encoded}&section=trade_info"
                        for ti_attempt in range(3):
                            try:
                                if ti_attempt > 0: time.sleep(1.5)
                                tr = session.get(ti_url, timeout=12)
                                if tr.status_code == 200:
                                    td = tr.json()
                                    mdo = data.setdefault("marketDeptOrderBook",{})
                                    ti  = mdo.setdefault("tradeInfo",{})
                                    src = td.get("marketDeptOrderBook",{}).get("tradeInfo",{})
                                    if src: ti.update(src)
                                    if "tradeInfo" in td: ti.update(td["tradeInfo"])
                                    break
                                elif tr.status_code in (401, 403):
                                    refresh_session(session)
                                    continue
                            except: pass
                        return data
                    if attempt == 0:
                        refresh_session(session)
                        continue
                    break
                if r.status_code in (401,403):
                    refresh_session(session)
                    continue
                if r.status_code == 404: break
            except requests.Timeout:
                if attempt < retries: time.sleep(2); continue
            except Exception:
                if attempt < retries: time.sleep(1); continue
    return {"error": f"Could not fetch {symbol}"}

# ── Extract Fields ────────────────────────────────────────
def extract_fields(symbol, raw):
    if "error" in raw:
        return {"symbol":symbol,"error":raw["error"]}

    pi  = raw.get("priceInfo",{})
    si  = raw.get("securityInfo",{})
    nfo = raw.get("info",{})          # NSE puts isSuspended, isDelisted here
    md  = raw.get("metadata",{})
    mdo = raw.get("marketDeptOrderBook",{})
    ti  = mdo.get("tradeInfo",{})
    whl = pi.get("weekHighLow",{})
    idh = pi.get("intraDayHighLow",{})

    last = safe_float(pi.get("lastPrice",0))
    high = safe_float(whl.get("max",0)) or safe_float(whl.get("yearHigh",0)) or 1
    low  = safe_float(whl.get("min",0)) or safe_float(whl.get("yearLow",0))  or 1
    fv   = safe_float(si.get("faceValue",1)) or 1

    # ── Market cap: try every possible location ───────────
    # NSE totalMarketCap / marketCap fields are already in CRORES
    # Only the price*shares fallback produces raw rupees needing /1e7
    mktcap_cr = (
        safe_float(md.get("totalMarketCap"))
        or safe_float(md.get("marketCap"))
        or safe_float(ti.get("totalMarketCap"))
        or safe_float(ti.get("marketCap"))
        or safe_float(raw.get("industryInfo",{}).get("marketCap"))
        or 0
    )
    shares = (
        safe_int(md.get("issuedSize"))
        or safe_int(si.get("issuedSize"))
        or safe_int(ti.get("issuedSize"))
        or 0
    )
    # Fallback: price × shares gives rupees → convert to Crores
    if mktcap_cr == 0 and shares > 0 and last > 0:
        mktcap_cr = round(last * shares / 1e7, 2)
    else:
        mktcap_cr = round(mktcap_cr, 2)

    # ── Free float ────────────────────────────────────────
    # ffmc from NSE is also already in Crores
    ffmc_cr = round(safe_float(ti.get("ffmc") or md.get("ffmc") or 0), 2)

    # ── Liquidity (type-safe) ─────────────────────────────
    impact_cost  = safe_float(ti.get("impactCost",0))
    daily_vol    = safe_float(ti.get("cmDailyVolatility",0))
    annual_vol   = safe_float(ti.get("cmAnnualVolatility",0))
    delivery_pct = safe_float(ti.get("deliveryToTradedQuantity",0))
    delivery_qty = safe_int(ti.get("deliveryQuantity",0))
    total_vol    = safe_int(ti.get("totalTradedVolume",0))
    total_val    = safe_float(ti.get("totalTradedValue",0))

    # ── Fundamentals ──────────────────────────────────────
    fin       = raw.get("financialData",{})
    debt_cr   = safe_float(fin.get("totalDebt", fin.get("debt",0)))
    equity_cr = safe_float(fin.get("totalEquity", fin.get("equity",0)))
    debt_eq   = round(debt_cr/equity_cr,2) if equity_cr > 0 else 0
    pe_ratio  = safe_float(fin.get("pe", md.get("pdSymbolPe",
                  pi.get("pe", si.get("pe",0)))))
    book_val  = safe_float(fin.get("bookValue",0))
    pb_ratio  = round(last/book_val,2) if book_val > 0 else 0
    roe       = safe_float(fin.get("roe",0))
    eps       = safe_float(fin.get("eps",0))

    # ── Industry / sector ─────────────────────────────────
    ind_info = raw.get("industryInfo",{})
    industry = (nfo.get("industry") or si.get("industry")
                or ind_info.get("basicIndustry")
                or ind_info.get("industry") or "")
    sector   = (si.get("sector") or ind_info.get("macroSector")
                or ind_info.get("sector") or "")

    # company_name: NSE puts it in info.companyName, si.companyName is often just the symbol
    company_name = (nfo.get("companyName") or si.get("companyName") or symbol)
    series_val   = (si.get("series") or nfo.get("series") or "EQ")
    isin_val     = (si.get("isin")   or nfo.get("isin")   or "")

    return {
        "symbol":symbol, "company_name":company_name,
        "industry":str(industry), "sector":str(sector),
        "series":series_val, "face_value":fv,
        "isin":isin_val, "listing_date":si.get("listingDate",""),
        "issued_shares":shares,
        "is_suspended":safe_bool(nfo.get("isSuspended", si.get("isSuspended",False))),
        "is_delisted" :safe_bool(nfo.get("isDelisted",  si.get("isDelisted", False))),
        "is_fno"      :safe_bool(nfo.get("isFNOSec",    si.get("isFNOSec",   False))),
        "last_price":last, "open":safe_float(pi.get("open",0)),
        "close":safe_float(pi.get("close",0)),
        "prev_close":safe_float(pi.get("previousClose",0)),
        "change":round(safe_float(pi.get("change",0)),2),
        "pchange":round(safe_float(pi.get("pChange",0)),2),
        "vwap":safe_float(pi.get("vwap",0)),
        "lower_circuit":safe_float(pi.get("lowerCP",0)),
        "upper_circuit":safe_float(pi.get("upperCP",0)),
        "price_band":str(pi.get("pPriceBand","")),
        "intraday_low":safe_float(idh.get("min",0)),
        "intraday_high":safe_float(idh.get("max",0)),
        "week52_low":low, "week52_high":high,
        "week52_low_date":str(whl.get("minDate","")),
        "week52_high_date":str(whl.get("maxDate","")),
        "total_volume":total_vol,
        "total_value_lakh":round(total_val/1e5,2),
        "market_cap_cr":mktcap_cr, "free_float_cr":ffmc_cr,
        "impact_cost":impact_cost, "daily_volatility":daily_vol,
        "annual_volatility":annual_vol,
        "delivery_qty":delivery_qty, "delivery_pct":delivery_pct,
        "total_buy_qty":safe_int(mdo.get("totalBuyQuantity",0)),
        "total_sell_qty":safe_int(mdo.get("totalSellQuantity",0)),
        "debt_cr":debt_cr, "equity_cr":equity_cr,
        "debt_to_equity":debt_eq, "pe_ratio":pe_ratio,
        "pb_ratio":pb_ratio, "roe":roe, "eps":eps, "book_value":book_val,
        "price_to_fv":round(last/fv,2),
        "pct_from_52w_high":round((last-high)/high*100,2) if high>0 else 0,
        "pct_from_52w_low":round((last-low)/low*100,2)   if low>0  else 0,
        "fetched_at":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

# ── Cancelled stock detector ──────────────────────────────
def is_cancelled_stock(raw):
    if not isinstance(raw,dict): return False
    pi  = raw.get("priceInfo",{})
    si  = raw.get("securityInfo",{})
    nfo = raw.get("info",{})
    last      = safe_float(pi.get("lastPrice",-1))
    series    = str(si.get("series",""))
    suspended = safe_bool(nfo.get("isSuspended", si.get("isSuspended", False)))
    delisted  = safe_bool(nfo.get("isDelisted",  si.get("isDelisted",  False)))
    if delisted: return True
    if suspended: return True          # suspended alone is enough
    if last == 0 and series in ["BZ","BE","XT","IL"]: return True
    if last == 0 and safe_float(pi.get("previousClose",-1)) == 0: return True
    return False

# ── Demerger detector ─────────────────────────────────────
def is_demerger_drop(data, raw):
    if not isinstance(raw,dict): return False
    anns = raw.get("corporateInfo",{}).get("latestAnnouncements",[])
    KW = ["demerger","merger","amalgamation","scheme of arrangement",
          "record date","ex-date","rights issue","bonus","split","buyback"]
    for a in anns:
        if any(k in str(a.get("subject","")).lower() for k in KW): return True
    try:
        from datetime import datetime as dt
        hd = dt.strptime(str(raw.get("priceInfo",{}).get("weekHighLow",{}).get("maxDate","")),'%d-%b-%Y')
        if (dt.now()-hd).days <= 30: return True
    except: pass
    return False

# ── CIRP Risk Scorer ──────────────────────────────────────
def score_risk(data, raw=None):
    if "error" in data:
        return {"cirp_score":-1,"risk_level":"UNKNOWN","risk_color":"gray","flags":[data["error"]]}
    # Read isSuspended directly from raw (NSE puts this in "info", not "securityInfo")
    _raw_si   = (raw or {}).get("securityInfo", {}) if raw else {}
    _raw_info = (raw or {}).get("info",          {}) if raw else {}
    _raw_pi   = (raw or {}).get("priceInfo",     {}) if raw else {}
    _is_susp  = safe_bool(
                    _raw_info.get("isSuspended",
                    _raw_si.get("isSuspended",
                    _raw_si.get("suspended",
                    (raw or {}).get("isSuspended", False)))))
    _is_delist= safe_bool(
                    _raw_info.get("isDelisted",
                    _raw_si.get("isDelisted",
                    (raw or {}).get("isDelisted", False))))
    _series   = str(_raw_si.get("series", data.get("series", "")))
    _last_raw = safe_float(_raw_pi.get("lastPrice", data.get("last_price", -1)))

    # Cancelled stock -- force EXTREME (check raw directly, bypasses extract_fields parsing)
    if is_cancelled_stock(raw) if raw else False:
        return {"cirp_score":100,"risk_level":"EXTREME","risk_color":"#ef4444",
                "flags":["Share capital extinguished under NCLT/CIRP",
                         "Price is zero -- investors lost 100%",
                         f"Series {_series} -- Graded Surveillance",
                         "Stock suspended -- no trading possible"]}
    if _is_susp or _is_delist:
        return {"cirp_score":100,"risk_level":"EXTREME","risk_color":"#ef4444",
                "flags":["Stock is suspended / delisted",
                         f"Series {_series}",
                         "Do NOT invest -- no active trading"]}
    # Force EXTREME for price=0 regardless of flags
    last_p = data.get("last_price", 0)
    series_val = data.get("series", _series)
    if last_p == 0:
        return {"cirp_score":100,"risk_level":"EXTREME","risk_color":"#ef4444",
                "flags":["Price is zero -- capital likely extinguished",
                         f"Series: {series_val}",
                         "No active trading -- do NOT invest"]}
    flags=[]; score=0
    # Near-zero price (>0 but <1) is a critical danger signal
    if last_p > 0 and last_p < 1.0:
        flags.append(f"Price Rs.{last_p} is near-zero -- capital destruction risk"); score += 50
    if data.get("series","") in THRESHOLDS["cirp_series"]:
        flags.append(f"Danger series: {data['series']}"); score+=35
    if data.get("is_suspended",False):
        flags.append("Stock is suspended"); score+=35
    if data.get("is_delisted",False):
        flags.append("Stock is delisted"); score+=40
    if 0 < data.get("last_price",0) < data.get("face_value",0):
        flags.append(f"Price Rs.{data['last_price']} below face value Rs.{data['face_value']}"); score+=20
    mc = data.get("market_cap_cr",0)
    if 0 < mc < THRESHOLDS["min_market_cap_cr"]:
        flags.append(f"Micro cap: Rs.{mc} Cr"); score+=10
    if not data.get("is_fno",False) and score>20:
        flags.append("Not in F&O segment"); score+=5
    if data.get("impact_cost",0) > THRESHOLDS["max_impact_cost"]:
        flags.append(f"High impact cost: {data['impact_cost']}%"); score+=10
    bq = data.get("total_buy_qty",1) or 1
    sq = data.get("total_sell_qty",0)
    if sq > bq*THRESHOLDS["sell_buy_imbalance_ratio"]:
        flags.append(f"Sell/Buy imbalance: {round(sq/bq,1)}x"); score+=15
    if data.get("annual_volatility",0) > THRESHOLDS["max_annual_volatility"]:
        flags.append(f"High volatility: {data['annual_volatility']}%"); score+=10
    if 0 < data.get("delivery_pct",100) < THRESHOLDS["min_delivery_pct"]:
        flags.append(f"Low delivery: {data['delivery_pct']}%"); score+=5
    score=min(score,100)
    if   score>=80: lv,cl="EXTREME","#ef4444"
    elif score>=50: lv,cl="HIGH","#f97316"
    elif score>=25: lv,cl="MEDIUM","#eab308"
    else:           lv,cl="LOW","#22c55e"
    return {"cirp_score":score,"risk_level":lv,"risk_color":cl,"flags":flags}

# ── IIFL Opportunity Scorer ───────────────────────────────
def score_iifl_opportunity(data, raw=None):
    if "error" in data:
        return {"iifl_score":0,"iifl_level":"UNKNOWN","iifl_color":"gray",
                "iifl_points":[],"iifl_verdict":"Cannot evaluate"}
    if raw and is_cancelled_stock(raw) or data.get("last_price",-1)==0:
        return {"iifl_score":0,"iifl_level":"DISQUALIFIED","iifl_color":"#6b7280",
                "iifl_points":[],"iifl_verdict":"Disqualified -- shares cancelled under NCLT. Do NOT invest."}
    if data.get("is_suspended",False) or data.get("series","") in THRESHOLDS["cirp_series"]:
        return {"iifl_score":0,"iifl_level":"DISQUALIFIED","iifl_color":"#6b7280",
                "iifl_points":[],"iifl_verdict":f"Disqualified -- {data.get('series','')} series / suspended"}
    if raw and is_demerger_drop(data,raw):
        return {"iifl_score":0,"iifl_level":"DISQUALIFIED","iifl_color":"#6b7280",
                "iifl_points":[],"iifl_verdict":"Disqualified -- drop linked to corporate action (demerger/bonus/split)"}
    pts=[]; score=0
    ph  = data.get("pct_from_52w_high",0)
    pl  = data.get("pct_from_52w_low",0)
    dp  = data.get("delivery_pct",0)
    av  = data.get("annual_volatility",0)
    ic  = data.get("impact_cost",0)
    mc  = data.get("market_cap_cr",0)
    pr  = data.get("last_price",0)
    fv  = data.get("face_value",1) or 1
    bq  = data.get("total_buy_qty",1) or 1
    sq  = data.get("total_sell_qty",0)
    fno = data.get("is_fno",False)
    # Signal 1: Drop from peak
    if ph<=-60: pts.append({"label":f"Deep panic fall: {ph:.1f}% from 52W high","detail":"Extreme fear -- classic IIFL zone","weight":25}); score+=25
    elif ph<=-40: pts.append({"label":f"Strong correction: {ph:.1f}% from 52W high","detail":"Fear likely exceeding fundamentals","weight":20}); score+=20
    elif ph<=-25: pts.append({"label":f"Good correction: {ph:.1f}% from 52W high","detail":"Meaningful dip worth watching","weight":12}); score+=12
    # Signal 2: Near 52W low
    if pl<=5: pts.append({"label":f"Near 52W low: +{pl:.1f}% above bottom","detail":"Maximum pessimism -- risk/reward flipped","weight":20}); score+=20
    elif pl<=15: pts.append({"label":f"Close to 52W low: +{pl:.1f}% above bottom","detail":"In distress zone -- accumulation range","weight":12}); score+=12
    # Signal 3: Price vs face value
    if pr > fv*5: pts.append({"label":f"Price Rs.{pr} well above face value Rs.{fv}","detail":"Real book value -- not a zombie stock","weight":10}); score+=10
    elif pr > fv: pts.append({"label":f"Price Rs.{pr} above face value Rs.{fv}","detail":"Structural floor exists","weight":5}); score+=5
    # Signal 4: Delivery
    if dp>=60: pts.append({"label":f"Very high delivery: {dp:.1f}%","detail":"Strong hands accumulating","weight":15}); score+=15
    elif dp>=40: pts.append({"label":f"Good delivery: {dp:.1f}%","detail":"Genuine buying interest","weight":10}); score+=10
    elif dp>=25: pts.append({"label":f"Decent delivery: {dp:.1f}%","detail":"Some genuine buying","weight":5}); score+=5
    # Signal 5: Market cap (SEBI thresholds)
    if mc>=33000: pts.append({"label":f"Large cap: Rs.{mc:,.0f} Cr","detail":"SEBI Top 100 -- FIIs will return","weight":15}); score+=15
    elif mc>=10500: pts.append({"label":f"Mid cap: Rs.{mc:,.0f} Cr","detail":"SEBI 101-250 -- meaningful scale","weight":12}); score+=12
    elif mc>=2000: pts.append({"label":f"Small cap: Rs.{mc:,.0f} Cr","detail":"Real business -- higher risk/reward","weight":8}); score+=8
    elif mc>=500: pts.append({"label":f"Micro cap: Rs.{mc:,.0f} Cr","detail":"Limited liquidity","weight":4}); score+=4
    # Signal 6: F&O
    if fno: pts.append({"label":"In F&O segment","detail":"SEBI quality threshold passed","weight":10}); score+=10
    # Signal 7: Liquidity
    if 0<ic<=0.3: pts.append({"label":f"Excellent liquidity: {ic}% impact cost","detail":"Large buy/sell without slippage","weight":8}); score+=8
    elif 0<ic<=0.7: pts.append({"label":f"Good liquidity: {ic}% impact cost","detail":"Adequate for retail investors","weight":5}); score+=5
    # Signal 8: Buy-side order book
    if bq>0 and sq>0:
        br = bq/(bq+sq)*100
        if br>=40: pts.append({"label":f"Buy-side: {br:.0f}% of order book","detail":"Smart money stepping in","weight":8}); score+=8
        elif br>=25: pts.append({"label":f"Some buy-side: {br:.0f}% bids","detail":"Partial recovery in buyers","weight":4}); score+=4
    # Signal 9: Volatility sweet spot
    if 20<=av<=55: pts.append({"label":f"Healthy volatility: {av:.1f}%","detail":"Normal range for beaten-down stock","weight":5}); score+=5
    score=min(score,100)
    if   score>=75: lv,cl,vd="STRONG OPPORTUNITY","#f59e0b","Closely resembles IIFL-at-Rs75 setup. High conviction accumulation zone -- hold 2-3 years."
    elif score>=55: lv,cl,vd="GOOD OPPORTUNITY","#84cc16","Several IIFL-type signals present. Build position in tranches. Monitor quarterly results."
    elif score>=35: lv,cl,vd="WATCH","#60a5fa","Some signals but not strong enough. Keep on radar -- may improve if stock corrects further or delivery % improves."
    elif score>=15: lv,cl,vd="WEAK","#94a3b8","Few signals present. Not a compelling IIFL-type setup at current levels."
    else:           lv,cl,vd="NO OPPORTUNITY","#6b7280","Does not resemble IIFL opportunity setup at current price levels."
    return {"iifl_score":score,"iifl_level":lv,"iifl_color":cl,"iifl_points":pts,"iifl_verdict":vd}

# ── Signal Generator ──────────────────────────────────────
def get_signal(data, risk, iifl=None):
    if risk["cirp_score"]>=50: return {"label":"AVOID","color":"#ef4444","icon":"X"}
    ph   = data.get("pct_from_52w_high",0)
    dp   = data.get("delivery_pct",0)
    iscore = (iifl or {}).get("iifl_score", 0)

    # Delivery available -- use combined signal
    if dp > 0:
        if ph<-30 and dp>40: return {"label":"STRONG BUY","color":"#16a34a","icon":"++"}
        if ph<-20 and dp>30: return {"label":"BUY","color":"#22c55e","icon":"+"}

    # Delivery unavailable (after market hours) -- fall back to IIFL score + drop
    if ph<-40 and iscore>=75: return {"label":"STRONG BUY","color":"#16a34a","icon":"++"}
    if ph<-25 and iscore>=55: return {"label":"BUY","color":"#22c55e","icon":"+"}
    if ph<-15 and iscore>=35: return {"label":"BUY","color":"#22c55e","icon":"+"}

    if ph<-10: return {"label":"WATCH","color":"#eab308","icon":"~"}
    if ph>=-5: return {"label":"WAIT","color":"#f97316","icon":".."}
    return            {"label":"HOLD","color":"#60a5fa","icon":"="}

# ── Market cap classifier (SEBI 2024 thresholds) ──────────
def classify_market_cap(mc):
    if mc<=0:      return {"label":"Unknown",   "color":"#6b7280","icon":"?","order":3}
    if mc>=33000:  return {"label":"Large Cap",  "color":"#3b82f6","icon":"L","order":0}
    if mc>=10500:  return {"label":"Mid Cap",    "color":"#a78bfa","icon":"M","order":1}
    return                {"label":"Small Cap",  "color":"#f59e0b","icon":"S","order":2}

# ── NSE 500 symbol list ───────────────────────────────────
def fetch_nse500_symbols(session):
    try:
        r = session.get("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",timeout=15)
        if r.status_code==200:
            syms = [i["symbol"] for i in r.json().get("data",[]) if i.get("symbol") and i["symbol"]!="NIFTY 500"]
            if syms: return syms
    except: pass
    return [
        "HDFCBANK","ICICIBANK","SBIN","KOTAKBANK","AXISBANK","INDUSINDBK","BANDHANBNK",
        "FEDERALBNK","IDFCFIRSTB","RBLBANK","BAJFINANCE","BAJAJFINSV","CHOLAFIN",
        "MUTHOOTFIN","MANAPPURAM","HDFCLIFE","SBILIFE","ICICIGI","LICI",
        "TCS","INFY","HCLTECH","WIPRO","TECHM","LTIM","PERSISTENT","COFORGE",
        "MPHASIS","OFSS","KPITTECH","TATAELXSI","ZENSARTECH","NIITTECH",
        "TMPV","TMCV","MARUTI","BAJAJ-AUTO","EICHERMOT","HEROMOTOCO","M&M",
        "ASHOKLEY","TVSMOTOR","MOTHERSON","BOSCHLTD","EXIDEIND","AMARAJABAT",
        "SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","BIOCON","AUROPHARMA","LUPIN",
        "TORNTPHARM","ALKEM","IPCALAB","GLENMARK","NATCOPHARM","GRANULES",
        "RELIANCE","ONGC","BPCL","IOC","HINDPETRO","GAIL","PETRONET",
        "ADANIGREEN","ADANIPOWER","TATAPOWER","NTPC","POWERGRID","CESC","TORNTPOWER",
        "TATASTEEL","JSWSTEEL","HINDALCO","VEDL","COALINDIA","NMDC","SAIL","JINDALSTEL",
        "HINDUNILVR","ITC","NESTLEIND","BRITANNIA","DABUR","MARICO","GODREJCP","COLPAL",
        "LT","SIEMENS","ABB","BHEL","BEL","HAL","CUMMINSIND","THERMAX","VOLTAS",
        "HAVELLS","POLYCAB","KALPATPOWR","APLAPOLLO","IRCTC","CONCOR","BLUEDART",
        "ULTRACEMCO","SHREECEM","AMBUJACEM","ACC","RAMCOCEM","JKCEMENT",
        "DLF","GODREJPROP","PRESTIGE","OBEROIRLTY","PHOENIXLTD","BRIGADE",
        "INOXWIND","WAAREEENER","SUZLON","PIIND","SRF","DEEPAKNTR","AARTIIND",
        "DMART","TRENT","NYKAA","ZOMATO","PAYTM","JUBLFOOD","DEVYANI",
        "MAXHEALTH","FORTIS","APOLLOHOSP","METROPOLIS","DRPATH","LALPATHLAB",
        "BHARTIARTL","IDEA","DIXON","PGEL","AMBER","KAYNES","SYRMA",
        "SWIGGY","ZOMATO","PAYTM","POLICYBZR","NYKAA",
        "INDUSINDBK","YESBANK","DCBBANK","CSBBANK","SOUTHINDBANK",
        "MFSL","CANFINHOME","LICHSGFIN","PNBHOUSING","REPCO",
        "TATACHEM","UPL","BAYER","RALLIS","DHANUKA",
        "BERGEPAINT","ASIANPAINT","PIDILITIND","FINOLEXIND",
        "OMKARCHEM","GACM",
    ]

# ── API Routes ────────────────────────────────────────────
@app.route("/api/watchlist",methods=["GET"])
def get_watchlist(): return jsonify(load_watchlist())

@app.route("/api/watchlist",methods=["POST"])
def update_watchlist():
    b = request.get_json()
    s = [x.strip().upper() for x in b.get("symbols",[]) if x.strip()]
    save_watchlist(s)
    return jsonify({"status":"ok","symbols":s})

@app.route("/api/analyze",methods=["GET"])
def analyze():
    wl = load_watchlist()
    workers = min(len(wl), 5)
    sessions = [create_session() for _ in range(workers)]
    results_map = {}
    lock = threading.Lock()

    def fetch_one(args):
        idx, sym = args
        sess = sessions[idx % workers]
        raw  = fetch_quote(sess, sym)
        data = extract_fields(sym, raw)
        risk = score_risk(data, raw)
        iifl = score_iifl_opportunity(data, raw)
        signal = get_signal(data, risk, iifl)
        with lock:
            results_map[sym] = {"symbol":sym,"data":data,"risk":risk,"iifl":iifl,"signal":signal}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        ex.map(fetch_one, enumerate(wl))

    # Preserve original watchlist order
    results = [results_map[sym] for sym in wl if sym in results_map]
    return jsonify({"results":results,"analyzed_at":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"total":len(results)})

@app.route("/api/quote/<symbol>",methods=["GET"])
def single_quote(symbol):
    session=create_session(); raw=fetch_quote(session,symbol.upper())
    data=extract_fields(symbol.upper(),raw); risk=score_risk(data,raw)
    iifl=score_iifl_opportunity(data,raw); signal=get_signal(data,risk,iifl)
    # Flat portfolio-friendly format (used by portfolio tab refresh)
    flat = {
        "symbol":         symbol.upper(),
        "company_name":   data.get("company_name",""),
        "sector":         data.get("sector",""),
        "cmp":            data.get("last_price",0),
        "mktcap_cr":      data.get("market_cap_cr",0),
        "week52_high":    data.get("week52_high",0),
        "week52_low":     data.get("week52_low",0),
        "discount_pct":   data.get("pct_from_52w_high",0),
        "pe":             round(data.get("pe_ratio",0),1),
        "roe_pct":        round(data.get("roe",0)*100,1) if data.get("roe",0) and data.get("roe",0)<10 else round(data.get("roe",0),1),
        "yoy_growth_pct": 0,  # NSE API doesn't provide this directly
        "debt_free":      data.get("debt_cr",1)==0 or data.get("debt_to_equity",1)<0.1,
        "total_debt_cr":  data.get("debt_cr",0),
        "cirp_score":     risk.get("cirp_score", 0),   # raw 0-100 scale from backend
        "cirp_rating":    risk.get("risk_level", ""),   # "EXTREME"/"HIGH"/"MEDIUM"/"LOW"
        "is_suspended":   data.get("is_suspended", False),
        "is_delisted":    data.get("is_delisted", False),
        "iifl_score":     iifl.get("iifl_score",0),
        "signal":         signal.get("label",""),
        # Also include full nested for backward compat
        "data":data,"risk":risk,"iifl":iifl,"signal_full":signal,
    }
    return jsonify(flat)

@app.route("/api/raw/<symbol>",methods=["GET"])
def raw_quote(symbol):
    session=create_session(); raw=fetch_quote(session,symbol.upper())
    si = raw.get("securityInfo",{})
    pi = raw.get("priceInfo",{})
    return jsonify({
        "symbol":symbol.upper(),"top_level_keys":list(raw.keys()),
        "security_info_full": si,
        "suspension_fields":{
            "info.isSuspended":         raw.get("info",{}).get("isSuspended"),
            "info.isDelisted":          raw.get("info",{}).get("isDelisted"),
            "info.isFNOSec":            raw.get("info",{}).get("isFNOSec"),
            "info.tempSuspendedSeries": raw.get("info",{}).get("tempSuspendedSeries"),
            "securityInfo.isSuspended": si.get("isSuspended"),
            "securityInfo.series":      si.get("series"),
            "priceInfo.lastPrice":      pi.get("lastPrice"),
        },
        "market_cap_locations":{
            "metadata.totalMarketCap":raw.get("metadata",{}).get("totalMarketCap"),
            "metadata.marketCap":raw.get("metadata",{}).get("marketCap"),
            "metadata.issuedSize":raw.get("metadata",{}).get("issuedSize"),
            "securityInfo.issuedSize":si.get("issuedSize"),
            "tradeInfo.totalMarketCap":raw.get("marketDeptOrderBook",{}).get("tradeInfo",{}).get("totalMarketCap"),
            "tradeInfo.ffmc":raw.get("marketDeptOrderBook",{}).get("tradeInfo",{}).get("ffmc"),
            "industryInfo.marketCap":raw.get("industryInfo",{}).get("marketCap"),
        },"raw":raw})

@app.route("/api/screener/status",methods=["GET"])
def screener_status(): return jsonify(SCREENER_STATUS)

@app.route("/api/screener/nse500",methods=["GET"])
def screen_nse500():
    global SCREENER_STATUS
    min_score = int(request.args.get("min_score",55))
    top_n     = int(request.args.get("top_n",5))
    min_drop  = float(request.args.get("min_drop",15))
    workers   = min(int(request.args.get("workers",5)),10)

    wl = load_watchlist(); wl_set = set(wl)
    session = create_session()
    symbols = fetch_nse500_symbols(session)
    symbols = list(dict.fromkeys([s.strip().upper() for s in symbols if s.strip()]))
    priority = [s for s in wl if s not in symbols]
    symbols = priority + symbols

    SCREENER_STATUS.update({"running":True,"done":0,"total":len(symbols),"symbol":"","workers":workers})
    opportunities=[]; lock=threading.Lock(); done_count=[0]
    sessions=[create_session() for _ in range(workers)]

    def process(args):
        idx,sym = args
        sess = sessions[idx % workers]
        with lock:
            done_count[0]+=1
            SCREENER_STATUS["done"]=done_count[0]
            SCREENER_STATUS["symbol"]=sym
        raw=fetch_quote(sess,sym); data=extract_fields(sym,raw)
        if "error" in data: return
        ph = data.get("pct_from_52w_high",0)
        is_wl = sym in wl_set
        if ph > -(10 if is_wl else min_drop): return
        risk=score_risk(data,raw); iifl=score_iifl_opportunity(data,raw)
        signal=get_signal(data,risk,iifl); cap=classify_market_cap(data.get("market_cap_cr",0))
        eff_min = 35 if is_wl else min_score
        if (iifl["iifl_score"]>=eff_min
                and iifl["iifl_level"] not in ["DISQUALIFIED","UNKNOWN"]
                and risk["cirp_score"]<50
                and cap["label"] in ["Large Cap","Mid Cap","Small Cap"]):
            with lock:
                opportunities.append({"symbol":sym,"data":data,"risk":risk,
                    "iifl":iifl,"signal":signal,"cap":cap,"is_watchlist":is_wl})

    with ThreadPoolExecutor(max_workers=workers) as ex:
        ex.map(process, enumerate(symbols))

    SCREENER_STATUS.update({"running":False,"symbol":"Complete"})

    buckets={"Large Cap":[],"Mid Cap":[],"Small Cap":[]}
    for o in opportunities:
        bl = o["cap"]["label"]
        if bl in buckets: buckets[bl].append(o)

    # Sort purely by IIFL score descending -- watchlist items are tagged visually, not ranked above
    def sk(x): return -x["iifl"]["iifl_score"]
    result={}
    for k,v in buckets.items():
        v.sort(key=sk); result[k]=v[:top_n]

    flat = result.get("Large Cap",[]) + result.get("Mid Cap",[]) + result.get("Small Cap",[])
    return jsonify({
        "opportunities":flat, "by_cap":result,
        "total_scanned":len(symbols), "total_found":len(opportunities),
        "returned":len(flat), "watchlist_count":sum(1 for o in flat if o["is_watchlist"]),
        "cap_counts":{"large":len(result.get("Large Cap",[])),"mid":len(result.get("Mid Cap",[])),"small":len(result.get("Small Cap",[]))},
        "filters":{"min_iifl_score":min_score,"min_drop_pct":min_drop,"top_n":top_n,"workers":workers},
        "scanned_at":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })

@app.route("/api/screener/export-csv",methods=["GET"])
def export_screener_csv():
    min_score=int(request.args.get("min_score",55))
    min_drop=float(request.args.get("min_drop",15))
    top_n=int(request.args.get("top_n",5))
    # Re-run a quick screener to get fresh data
    wl=load_watchlist(); wl_set=set(wl)
    session=create_session()
    symbols=fetch_nse500_symbols(session)
    symbols=list(dict.fromkeys([s.strip().upper() for s in symbols if s.strip()]))
    priority=[s for s in wl if s not in symbols]
    symbols=priority+symbols
    rows=[]
    for sym in symbols:
        raw=fetch_quote(session,sym); data=extract_fields(sym,raw)
        if "error" in data: time.sleep(0.3); continue
        ph=data.get("pct_from_52w_high",0); is_wl=sym in wl_set
        if ph > -(10 if is_wl else min_drop): time.sleep(0.3); continue
        risk=score_risk(data,raw); iifl=score_iifl_opportunity(data,raw)
        signal=get_signal(data,risk,iifl); cap=classify_market_cap(data.get("market_cap_cr",0))
        if (iifl["iifl_score"]>=(35 if is_wl else min_score)
                and iifl["iifl_level"] not in ["DISQUALIFIED","UNKNOWN"]
                and risk["cirp_score"]<50):
            rows.append({
                "Symbol":sym,"Company":data.get("company_name",""),
                "Industry":data.get("industry",""),"Sector":data.get("sector",""),
                "Cap Category":cap["label"],"Market Cap (Cr)":data.get("market_cap_cr",0),
                "Price":data.get("last_price",0),"Change %":data.get("pchange",0),
                "52W High":data.get("week52_high",0),"52W Low":data.get("week52_low",0),
                "From Peak %":data.get("pct_from_52w_high",0),
                "Delivery %":data.get("delivery_pct",0),
                "Volatility %":data.get("annual_volatility",0),
                "Impact Cost %":data.get("impact_cost",0),
                "PE Ratio":data.get("pe_ratio",0),"PB Ratio":data.get("pb_ratio",0),
                "Debt/Equity":data.get("debt_to_equity",0),"ROE %":data.get("roe",0),
                "IIFL Score":iifl["iifl_score"],"IIFL Level":iifl["iifl_level"],
                "CIRP Score":risk["cirp_score"],"Signal":signal["label"],
                "In Watchlist":"Yes" if is_wl else "No",
            })
        time.sleep(0.4)
    rows.sort(key=lambda x:(-x["IIFL Score"],x["From Peak %"]))
    out=io.StringIO()
    if rows:
        w=csv.DictWriter(out,fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    return Response(out.getvalue(),mimetype="text/csv",
        headers={"Content-Disposition":"attachment; filename=iifl_opportunities.csv"})

# ═══════════════════════════════════════════════════════════
# ── GEM SCANNER (yfinance fundamentals) ───────────────────
# Uses: yfinance, niftystocks  (pip install yfinance niftystocks)
# Scans for quality stocks: ROE>15%, PE 1-38, cash-rich
# Completely separate from NSE real-time scanner above
# ═══════════════════════════════════════════════════════════

GEM_STATUS = {"running":False,"done":0,"total":0,"symbol":"","last_run":None}
_gem_results = []          # cached last scan results

def get_gem_cirp_rating(score):
    if score <= 1.5: return "Low Risk"
    if score <= 3.5: return "Moderate Risk"
    return "High Risk"

def process_gem(symbol):
    """Process one ticker through yfinance fundamentals filter."""
    try:
        import yfinance as yf
        t   = yf.Ticker(symbol)
        inf = t.info

        # ROE with manual fallback
        roe = inf.get("returnOnEquity", 0) or 0
        if not roe:
            try:
                net_inc = t.financials.loc["Net Income"].iloc[0]
                equity  = t.balance_sheet.loc["Stockholders Equity"].iloc[0]
                roe = net_inc / equity if equity > 0 else 0
            except: roe = 0

        debt = inf.get("totalDebt", 0)   or 0
        cash = inf.get("totalCash", 0)   or 0
        pe   = inf.get("trailingPE", 0)  or 0

        # Quality + value filter (unchanged from original script)
        if not (roe > 0.15 and 1 < pe < 38 and cash > debt * 0.7):
            return None

        cmp  = inf.get("currentPrice", 0)
        high = inf.get("fiftyTwoWeekHigh", 1) or 1

        ebitda  = inf.get("ebitda", 1)          or 1
        int_exp = inf.get("interestExpense", 1)  or 1
        int_cover = ebitda / max(int_exp, 1)
        cirp_val  = round(max(1.0, min(10.0,
            ((debt / max(1, cash)) * 5) + (10 / max(0.1, int_cover)))), 1)

        mktcap_cr = round(inf.get("marketCap", 0) / 1e7, 1)

        return {
            "symbol":      symbol.replace(".NS",""),
            "company_name": inf.get("longName", "N/A"),
            "sector":      inf.get("sector", "N/A"),
            "cmp":         round(cmp, 2),
            "mktcap_cr":   mktcap_cr,
            "discount_pct": round((cmp - high) / high * 100, 1),
            "pe":          round(pe, 1),
            "roe_pct":     round(roe * 100, 1),
            "yoy_growth_pct": round((inf.get("earningsQuarterlyGrowth", 0) or 0) * 100, 1),
            "debt_free":   cash > debt,
            "total_debt_cr": round(debt / 1e7, 2),
            "cirp_score":  cirp_val,
            "cirp_rating": get_gem_cirp_rating(cirp_val),
        }
    except: return None

def _run_gem_scan():
    """Background thread: scan all NSE 500 tickers via yfinance."""
    global _gem_results
    try:
        from niftystocks import ns
        tickers = ns.get_nifty500_with_ns()
    except Exception as e:
        GEM_STATUS["running"] = False
        GEM_STATUS["symbol"]  = f"Error loading tickers: {e}"
        return

    GEM_STATUS.update({"running":True,"done":0,"total":len(tickers),"symbol":""})
    results = []
    lock = threading.Lock()

    def _process(sym):
        GEM_STATUS["symbol"] = sym
        res = process_gem(sym)
        with lock:
            GEM_STATUS["done"] += 1
            if res: results.append(res)

    with ThreadPoolExecutor(max_workers=5) as ex:
        ex.map(_process, tickers)

    # Sort: ROE desc, PE asc — matches original script
    results.sort(key=lambda x: (-x["roe_pct"], x["pe"]))
    _gem_results = results[:100]        # keep top 100 gems
    GEM_STATUS.update({"running":False,"symbol":"Scan complete!",
                        "last_run":datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

@app.route("/api/gems/scan", methods=["POST"])
def gems_scan():
    if GEM_STATUS["running"]:
        return jsonify({"error":"Scan already running"}), 409
    threading.Thread(target=_run_gem_scan, daemon=True).start()
    return jsonify({"message":"Gem scan started"})

@app.route("/api/gems/status", methods=["GET"])
def gems_status():
    return jsonify(GEM_STATUS)

@app.route("/api/gems/results", methods=["GET"])
def gems_results():
    top_n   = int(request.args.get("top_n", 50))
    sector  = request.args.get("sector", "all")
    df_free = request.args.get("debt_free", "all")   # all | yes | no
    data    = _gem_results[:top_n]
    if sector != "all":
        data = [r for r in data if r["sector"].lower() == sector.lower()]
    if df_free == "yes":
        data = [r for r in data if r["debt_free"]]
    elif df_free == "no":
        data = [r for r in data if not r["debt_free"]]
    sectors = sorted({r["sector"] for r in _gem_results if r["sector"] != "N/A"})
    return jsonify({"results":data,"total":len(data),"sectors":sectors,
                    "last_run":GEM_STATUS.get("last_run")})

@app.route("/api/gems/export", methods=["GET"])
def gems_export():
    if not _gem_results:
        return jsonify({"error":"No results yet — run scan first"}), 404
    out = io.StringIO()
    fields = ["symbol","company_name","sector","cmp","mktcap_cr","discount_pct",
              "pe","roe_pct","yoy_growth_pct","debt_free","total_debt_cr",
              "cirp_score","cirp_rating"]
    w = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
    w.writeheader(); w.writerows(_gem_results)
    return Response(out.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition":"attachment; filename=gem_scanner_results.csv"})


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "StockSense"})

@app.route("/")
def serve_frontend():
    # Serve the HTML file from the same folder as app.py
    import os
    folder = os.path.dirname(os.path.abspath(__file__))
    # Try common filenames
    for name in ["stocksense.html", "stocksense-5.html", "index.html"]:
        if os.path.exists(os.path.join(folder, name)):
            return send_from_directory(folder, name)
    return "Frontend HTML not found in same folder as app.py", 404

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"
    print(f"StockSense NSE Analyzer -- http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
