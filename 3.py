import asyncio
import json
import aiohttp
import websockets
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from decimal import Decimal, ROUND_DOWN
import ntplib
from datetime import datetime
import math
import threading

# =========================
# TIME SYNC CORRECTION
# =========================
TIME_OFFSET = 0  # Will be set after NTP sync
# MANUAL ADJUSTMENT: Add seconds here if your code is still late
# Example: If phone shows 06:55:30 but code shows 06:55:25, set MANUAL_ADJUST = -5
MANUAL_ADJUST = 0  # Change this value to match your phone!

# =========================
# PNL THRESHOLD OVERRIDE
# =========================
user_pnl_threshold = 0.0  # Default: exit at break-even
terminal_paused = False   # Flag to pause terminal output

# 🆕 EVENT-DRIVEN BBO QUEUES
bbo_queues = {
    "binance": asyncio.Queue(),
    "bybit": asyncio.Queue()
}

def get_accurate_time():
    """Get accurate time using NTP/Exchange sync offset + Manual Adjustment"""
    return time.time() + TIME_OFFSET + MANUAL_ADJUST

def pnl_threshold_listener(state):
    """Background thread listening for 'p' key press to override PNL threshold"""
    global user_pnl_threshold, terminal_paused
    
    while state.get("running", True):
        try:
            # Wait for user input (blocking, but in separate thread)
            user_input = input()
            
            # Check if user typed 'p'
            if user_input.strip().lower() == 'p':
                # Pause terminal
                terminal_paused = True
                
                # Ask for new threshold
                print("\n" + "="*60)
                print("🎯 PNL THRESHOLD OVERRIDE")
                print("="*60)
                threshold_input = input("Enter PNL threshold to exit (e.g., +5, -3, 0): ")
                
                try:
                    # Parse input (handle +/- signs)
                    new_threshold = float(threshold_input)
                    user_pnl_threshold = new_threshold
                    
                    audit_log(f"🎯 PNL threshold changed to: ${user_pnl_threshold:+.2f}")
                    print(f"✅ PNL threshold updated to: ${user_pnl_threshold:+.2f}")
                    print(f"✅ Terminal paused. Background continues...")
                    print("="*60 + "\n")
                    
                except ValueError:
                    print("❌ Invalid input. Threshold unchanged.")
                    audit_log(f"⚠️ Invalid PNL threshold input: {threshold_input}")
                    terminal_paused = False  # Resume terminal on error
                    
        except Exception as e:
            audit_log(f"PNL threshold listener error: {e}")
            break

# =========================
# LOGGING SETUP
# =========================
LOG_FILE = "trade_audit.log"

TRACE_FILE = "bbo_trace.log"

def trace_log(msg):
    """Write BBO trace to separate file"""
    ts = datetime.fromtimestamp(get_accurate_time()).strftime('%H:%M:%S.%f')[:-3]
    try:
        with open(TRACE_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception as e:
        pass  # Silent fail

CHASE_LOG_FILE = "chase_debug.log"

def chase_log(msg):
    ts = datetime.fromtimestamp(get_accurate_time()).strftime('%H:%M:%S.%f')[:-3]
    try:
        with open(CHASE_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except:
        pass


def audit_log(msg, also_print=True):
    """Write message to file and optionally print to console"""
    curr_time = get_accurate_time()
    ts_full = datetime.fromtimestamp(curr_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    ts_short = datetime.fromtimestamp(curr_time).strftime('%H:%M:%S.%f')[:-3]
    
    log_entry = f"[{ts_full}] {msg}"
    
    # Always append to file
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
    except Exception as e:
        print(f"Error writing to log file: {e}")

    # Optionally print to terminal
    if also_print:
        print(f"[{ts_short}] 📝 AUDIT: {msg}")

def drain_bbo_queue(exchange):
    """Drain queue and return count of skipped updates"""
    drained = 0
    while not bbo_queues[exchange].empty():
        try:
            bbo_queues[exchange].get_nowait()
            drained += 1
        except asyncio.QueueEmpty:
            break
    return drained

def sync_time_with_ntp():
    """Sync system time with NTP servers and calculate offset"""
    global TIME_OFFSET
    try:
        ntp_client = ntplib.NTPClient()
        # Try multiple NTP servers
        ntp_servers = ['time.google.com', 'pool.ntp.org', 'time.windows.com']
        
        for server in ntp_servers:
            try:
                response = ntp_client.request(server, version=3, timeout=5)
                ntp_time = response.tx_time
                system_time = time.time()
                TIME_OFFSET = ntp_time - system_time
                
                audit_log(f"✅ Time synced with {server}")
                audit_log(f"   System time: {datetime.fromtimestamp(system_time).strftime('%H:%M:%S.%f')[:-3]}")
                audit_log(f"   NTP time:    {datetime.fromtimestamp(ntp_time).strftime('%H:%M:%S.%f')[:-3]}")
                audit_log(f"   Offset:      {TIME_OFFSET:.3f}s {'(AHEAD)' if TIME_OFFSET > 0 else '(BEHIND)'}")
                
                if abs(TIME_OFFSET) > 1:
                    audit_log(f"⚠️  WARNING: Your system clock is {abs(TIME_OFFSET):.1f} seconds {'ahead' if TIME_OFFSET > 0 else 'behind'}!")
                
                return True
            except Exception as e:
                print(f"Failed to sync with {server}: {e}")
                continue
        
        print("❌ Could not sync with any NTP server, using system time")
        TIME_OFFSET = 0
        return False
    except Exception as e:
        print(f"❌ NTP sync error: {e}")
        TIME_OFFSET = 0
        return False

def get_binance_server_time():
    """Get accurate server time from Binance"""
    global TIME_OFFSET, MANUAL_ADJUST
    try:
        # Get Binance server time
        response = requests.get("https://fapi.binance.com/fapi/v1/time", timeout=3)
        data = response.json()
        server_time = data["serverTime"] / 1000  # Convert to seconds
        
        # Get local time
        local_time = time.time()
        
        # Calculate offset
        TIME_OFFSET = server_time - local_time
        
        # 🆕 AUTO-ADJUST: If offset is too big, apply it to MANUAL_ADJUST
        if abs(TIME_OFFSET) > 5:
            print(f"⚠️  CRITICAL: Your clock is {TIME_OFFSET:.1f}s off!")
            print(f"🔧 AUTO-ADJUSTING MANUAL_ADJUST from {MANUAL_ADJUST} to {int(TIME_OFFSET)}")
            MANUAL_ADJUST = int(TIME_OFFSET)
            TIME_OFFSET = 0  # Reset offset since we moved it to manual adjust
            print(f"✅ Auto-correction applied! New timestamps will be accurate.")
        
        print(f"✅ Synced with Binance server")
        print(f"   Server time offset: {TIME_OFFSET:.3f} seconds")
        print(f"   Manual adjustment: {MANUAL_ADJUST:+d} seconds")
        if abs(TIME_OFFSET) > 1:
            print(f"   ⚠️  WARNING: Your clock is {abs(TIME_OFFSET):.1f}s {'ahead' if TIME_OFFSET < 0 else 'behind'}")
        
        return True
    except Exception as e:
        print(f"❌ Failed to sync with Binance: {e}")
        return False

def get_bybit_server_time():
    """Get accurate server time from Bybit"""
    global TIME_OFFSET, MANUAL_ADJUST
    try:
        # Get Bybit server time
        response = requests.get("https://api.bybit.com/v5/market/time", timeout=3)
        data = response.json()
        server_time = int(data["result"]["timeSecond"])  # Already in seconds
        
        # Get local time
        local_time = time.time()
        
        # Calculate offset
        TIME_OFFSET = server_time - local_time
        
        # 🆕 AUTO-ADJUST: If offset is too big, apply it to MANUAL_ADJUST
        if abs(TIME_OFFSET) > 5:
            print(f"⚠️  CRITICAL: Your clock is {TIME_OFFSET:.1f}s off!")
            print(f"🔧 AUTO-ADJUSTING MANUAL_ADJUST from {MANUAL_ADJUST} to {int(TIME_OFFSET)}")
            MANUAL_ADJUST = int(TIME_OFFSET)
            TIME_OFFSET = 0  # Reset offset since we moved it to manual adjust
            print(f"✅ Auto-correction applied! New timestamps will be accurate.")
        
        print(f"✅ Synced with Bybit server")
        print(f"   Server time offset: {TIME_OFFSET:.3f} seconds")
        print(f"   Manual adjustment: {MANUAL_ADJUST:+d} seconds")
        if abs(TIME_OFFSET) > 1:
            print(f"   ⚠️  WARNING: Your clock is {abs(TIME_OFFSET):.1f}s {'ahead' if TIME_OFFSET < 0 else 'behind'}")
        
        return True
    except Exception as e:
        print(f"❌ Failed to sync with Bybit: {e}")
        return False

def continuous_time_sync(state, exchanges):
    """Re-sync time with exchange servers every 2 minutes"""
    global TIME_OFFSET, MANUAL_ADJUST
    
    while state.get("running", True):
        try:
            # Alternate between exchanges for redundancy
            if "binance" in exchanges:
                success = get_binance_server_time()
                if success:
                    audit_log("🔄 Time re-synced with Binance")
            elif "bybit" in exchanges:
                success = get_bybit_server_time()
                if success:
                    audit_log("🔄 Time re-synced with Bybit")
            
            # Wait 2 minutes before next sync
            time.sleep(120)
            
        except Exception as e:
            audit_log(f"⚠️ Time sync error: {e}")
            time.sleep(120)  # Still wait 2 min even on error


# =========================
# BINANCE WS (MARK + FUNDING)
# =========================
async def binance_ws(symbol, state):
    # Combined stream: bookTicker for BBO + markPrice for Funding/TimeSync
    base_url = "wss://stream.binancefuture.com/stream?streams="  # ✅ TESTNET
    streams = f"{symbol.lower()}@bookTicker/{symbol.lower()}@markPrice"
    url = base_url + streams
    
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                print(f"✅ Binance Combined WS connected ({symbol})")
                async for msg in ws:
                    res = json.loads(msg)
                    stream = res.get("stream")
                    d = res.get("data")
                    
                    if "bookticker" in stream.lower():
                        # Update BBO for calculations
                        state["binance"]["bid"] = float(d["b"])
                        state["binance"]["ask"] = float(d["a"])

                        chase_log(f"[BBO_IN] BINANCE bid={float(d['b']):.8f} ask={float(d['a']):.8f}")

                        # 🆕 LOG BBO TO TRACE FILE
                        bid = float(d["b"])
                        ask = float(d["a"])
                        order_status = state["binance"].get("trace_order_status", "None")
                        trace_log(f"BINANCE BBO: Bid={bid:.8f}, Ask={ask:.8f} | Order: {order_status}")

                        # 🆕 SIGNAL BBO CHANGE EVENT
                        try:
                            bbo_queues["binance"].put_nowait({
                                "bid": float(d["b"]),
                                "ask": float(d["a"]),
                                "timestamp": time.time()
                            })
                        except asyncio.QueueFull:
                            pass  # Queue full, skip
                    
                    elif "markprice" in stream.lower():
                        # Update Display Price & Funding & Time
                        state["binance"]["price"] = float(d["p"])
                        state["binance"]["funding"] = float(d["r"]) * 100
                        state["binance"]["next_ts"] = int(d["T"])
                        
                        global TIME_OFFSET
                        ws_server_time = int(d["E"]) / 1000
                        TIME_OFFSET = ws_server_time - time.time()
        except Exception as e:
            print("Binance WS error:", e)
            await asyncio.sleep(0.5)

# =========================
# BYBIT WS (MARK + FUNDING)
# =========================
async def bybit_ws(symbol, state):
    url = "wss://stream.bybit.com/v5/public/linear"
    sub = {"op": "subscribe", "args": [f"tickers.{symbol}"]}
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                print("✅ Bybit BBO WS connected")
                async for msg in ws:
                    d = json.loads(msg)
                    if "data" in d:
                        data = d["data"]
                        if "bid1Price" in data: state["bybit"]["bid"] = float(data["bid1Price"])
                        if "ask1Price" in data: state["bybit"]["ask"] = float(data["ask1Price"])

                        chase_log(f"[BBO_IN] BYBIT bid={state['bybit']['bid']:.8f} ask={state['bybit']['ask']:.8f}")

                        # 🆕 LOG BBO TO TRACE FILE
                        if "bid1Price" in data or "ask1Price" in data:
                            bid = state["bybit"]["bid"]
                            ask = state["bybit"]["ask"]
                            order_status = state["bybit"].get("trace_order_status", "None")
                            trace_log(f"BYBIT BBO: Bid={bid:.8f}, Ask={ask:.8f} | Order: {order_status}")

                        # 🆕 SIGNAL BBO CHANGE EVENT
                        if "bid1Price" in data or "ask1Price" in data:
                            try:
                                bbo_queues["bybit"].put_nowait({
                                    "bid": state["bybit"]["bid"],
                                    "ask": state["bybit"]["ask"],
                                    "timestamp": time.time()
                                })
                            except asyncio.QueueFull:
                                pass
                        if "markPrice" in data: state["bybit"]["price"] = float(data["markPrice"])
                        if "fundingRate" in data: state["bybit"]["funding"] = float(data["fundingRate"]) * 100
                        if "nextFundingTime" in data: state["bybit"]["next_ts"] = int(data["nextFundingTime"])
        except Exception as e:
            print("Bybit WS error:", e)
            await asyncio.sleep(0.5)

# =========================
# BINANCE POSITION WEBSOCKET
# =========================
async def binance_position_ws(symbol, state):
    """Real-time position updates via WebSocket - NO POLLING!"""
    
    while state.get("running", True):
        try:
            # Get listen key for user data stream
            params = binance_sign({"timestamp": int(get_accurate_time() * 1000)})
            r = requests.post(
                BINANCE_URL + "/fapi/v1/listenKey",
                headers={"X-MBX-APIKEY": BINANCE_KEY},
                timeout=5
            )
            listen_key_data = r.json()
            
            if "listenKey" not in listen_key_data:
                audit_log(f"❌ Failed to get Binance listen key: {listen_key_data}")
                await asyncio.sleep(5)
                continue
            
            listen_key = listen_key_data["listenKey"]
            url = f"wss://stream.binancefuture.com/ws/{listen_key}"
            
            audit_log(f"🔌 Connecting to Binance Position WebSocket...")
            
            async with websockets.connect(url, ping_interval=20) as ws:
                audit_log(f"✅ Binance Position WebSocket CONNECTED")
                
                # Keep-alive task for listen key (refresh every 30 min)
                last_keepalive = time.time()
                
                async for msg in ws:
                    # Keep-alive (refresh listen key every 30 minutes)
                    if time.time() - last_keepalive > 1800:  # 30 minutes
                        params = binance_sign({"timestamp": int(get_accurate_time() * 1000)})
                        requests.put(
                            BINANCE_URL + "/fapi/v1/listenKey",
                            headers={"X-MBX-APIKEY": BINANCE_KEY},
                            timeout=5
                        )
                        last_keepalive = time.time()
                        audit_log(f"🔄 Binance listen key refreshed")
                    
                    data = json.loads(msg)
                    event_type = data.get("e")
                    
                    # ==========================================
                    # EVENT 1: ACCOUNT_UPDATE (Position Changes)
                    # ==========================================
                    if event_type == "ACCOUNT_UPDATE":
                        audit_log(f"📡 BINANCE ACCOUNT_UPDATE EVENT RECEIVED")
                        
                        for position in data.get("a", {}).get("P", []):
                            if position["s"] == symbol:
                                pos_amt = float(position["pa"])  # Position amount
                                entry_price = float(position["ep"])  # Entry price
                                unrealized_pnl = float(position["up"])  # Unrealized PNL
                                
                                # Determine side
                                if pos_amt > 0:
                                    side = "LONG"
                                elif pos_amt < 0:
                                    side = "SHORT"
                                else:
                                    side = "FLAT"
                                
                                # Update state
                                old_size = state["binance"].get("position_size", 0)
                                state["binance"]["position_size"] = abs(pos_amt)
                                state["binance"]["position_entry"] = entry_price
                                state["binance"]["position_pnl"] = unrealized_pnl
                                state["binance"]["position_side"] = side
                                
                                # LOG DETAILS
                                audit_log(f"   Symbol: {symbol}")
                                audit_log(f"   Position: {side} {abs(pos_amt):.1f} coins @ ${entry_price:.8f}")
                                audit_log(f"   Unrealized PNL: ${unrealized_pnl:+.6f}")
                                audit_log(f"   Change: {old_size:.1f} → {abs(pos_amt):.1f} ({abs(pos_amt) - old_size:+.1f})")
                    
                    # ==========================================
                    # EVENT 2: ORDER_TRADE_UPDATE (Fills)
                    # ==========================================
                    elif event_type == "ORDER_TRADE_UPDATE":
                        order = data.get("o", {})
                        
                        if order.get("s") == symbol:
                            order_id = order.get("i")
                            status = order.get("X")  # Order status
                            order_price = float(order.get("p", 0))
                            if status in ["NEW", "PARTIALLY_FILLED"]:
                                state["binance"]["ws_active_order_price"] = order_price
                            elif status in ["FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH"]:
                                state["binance"]["ws_active_order_price"] = None
                            filled_qty = float(order.get("z", 0))  # Filled quantity
                            total_qty = float(order.get("q", 0))  # Total quantity
                            avg_price = float(order.get("ap", 0))  # Average price
                            
                            audit_log(f"📡 BINANCE ORDER_TRADE_UPDATE EVENT")
                            audit_log(f"   Order ID: {order_id}")
                            audit_log(f"   Status: {status}")
                            audit_log(f"   Filled: {filled_qty}/{total_qty} @ ${avg_price:.8f}")
                            
                            if status == "PARTIALLY_FILLED":
                                audit_log(f"   ⚡ PARTIAL FILL DETECTED!")
                            elif status == "FILLED":
                                audit_log(f"   ✅ ORDER FULLY FILLED!")
                            elif status == "CANCELED":
                                audit_log(f"   ❌ ORDER CANCELED")
        
        except Exception as e:
            audit_log(f"💥 Binance Position WebSocket error: {e}")
            await asyncio.sleep(2)

# =========================
# BYBIT POSITION WEBSOCKET
# =========================
async def bybit_position_ws(symbol, state):
    """Real-time position updates via WebSocket - NO POLLING!"""
    
    while state.get("running", True):
        try:
            url = "wss://stream.bybit.com/v5/private"
            
            # Authentication
            expires = int((time.time() + 10) * 1000)
            signature = hmac.new(
                BYBIT_SECRET.encode(),
                f"GET/realtime{expires}".encode(),
                hashlib.sha256
            ).hexdigest()
            
            auth_msg = {
                "op": "auth",
                "args": [BYBIT_KEY, expires, signature]
            }
            
            sub_msg = {
                "op": "subscribe",
                "args": ["position.linear", "order"] # ✅ FIXED: Subscribe to all linear positions
            }
            
            audit_log(f"🔌 Connecting to Bybit Position WebSocket...")
            
            async with websockets.connect(url, ping_interval=20) as ws:
                # Authenticate
                await ws.send(json.dumps(auth_msg))
                auth_resp = json.loads(await ws.recv())
                
                if auth_resp.get("success"):
                    audit_log(f"✅ Bybit Position WebSocket AUTHENTICATED")
                else:
                    audit_log(f"❌ Bybit auth failed: {auth_resp}")
                    await asyncio.sleep(5)
                    continue
                
                # Subscribe to position updates
                await ws.send(json.dumps(sub_msg))
                sub_resp = json.loads(await ws.recv())
                
                if sub_resp.get("success"):
                    audit_log(f"✅ Bybit Position WebSocket SUBSCRIBED to linear positions")
                else:
                    audit_log(f"❌ Bybit subscription failed: {sub_resp}")
                    await asyncio.sleep(5)
                    continue
                
                async for msg in ws:
                    data = json.loads(msg)
                    
                    # Heartbeat response
                    if data.get("op") == "pong":
                        continue
                    
                    # Position update - FILTER BY SYMBOL
                    if data.get("topic") == "order":
                        for order in data.get("data", []):
                            if order.get("symbol") == symbol:
                                status = order.get("orderStatus")
                                order_price = float(order.get("price", 0))
                                if status in ["New", "PartiallyFilled"]:
                                    state["bybit"]["ws_active_order_price"] = order_price
                                elif status in ["Filled", "Cancelled", "Rejected", "Deactivated"]:
                                    state["bybit"]["ws_active_order_price"] = None
                    if data.get("topic") == "position.linear":  # ✅ UPDATED TOPIC NAME
                        audit_log(f"📡 BYBIT POSITION UPDATE EVENT RECEIVED")
                        
                        for position in data.get("data", []):
                            if position.get("symbol") == symbol:  # ✅ Filter for our symbol
                                pos_size = float(position.get("size", 0))
                                entry_price = float(position.get("avgPrice", 0))
                                unrealized_pnl = float(position.get("unrealisedPnl", 0))
                                side = position.get("side")
                                
                                # Update state
                                old_size = state["bybit"].get("position_size", 0)
                                state["bybit"]["position_size"] = pos_size
                                state["bybit"]["position_entry"] = entry_price
                                state["bybit"]["position_pnl"] = unrealized_pnl
                                state["bybit"]["position_side"] = side
                                
                                # LOG DETAILS
                                audit_log(f"   Symbol: {symbol}")
                                audit_log(f"   Position: {side} {pos_size:.1f} coins @ ${entry_price:.8f}")
                                audit_log(f"   Unrealized PNL: ${unrealized_pnl:+.6f}")
                                audit_log(f"   Change: {old_size:.1f} → {pos_size:.1f} ({pos_size - old_size:+.1f})")
        
        except Exception as e:
            audit_log(f"💥 Bybit Position WebSocket error: {e}")
            await asyncio.sleep(2)
            
# =========================
# MEXC REST (MARK + FUNDING)
# =========================
async def mexc_rest(symbol, state):
    price_url = f"https://contract.mexc.com/api/v1/contract/fair_price/{symbol}"
    funding_url = f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(price_url) as r:
                    j = await r.json()
                    state["mexc"]["price"] = float(j["data"]["fairPrice"])
                async with session.get(funding_url) as r:
                    j = await r.json()
                    state["mexc"]["funding"] = float(j["data"]["fundingRate"]) * 100
                    state["mexc"]["next_ts"] = int(j["data"]["nextSettleTime"])
            except Exception as e:
                print("MEXC REST error:", e)
            await asyncio.sleep(1)

# =========================
# TIME LEFT (REAL EXCHANGE TIME)
# =========================
def time_left(ts):
    if not ts:
        return "--:--:--"
    
    # Convert timestamp from milliseconds to seconds
    ts_seconds = ts / 1000
    current_time = get_accurate_time()  # Use NTP-synced time
    diff = int(ts_seconds - current_time)
    
    # Debug: Print if time difference is unusual
    if diff < -3600 or diff > 28800:  # Less than -1 hour or more than 8 hours
        print(f"⚠️ WARNING: Unusual time difference: {diff}s (ts={ts}, current={current_time})")
    
    if diff <= 0:
        return "00:00:00"
    
    h = diff // 3600
    m = (diff % 3600) // 60
    s = diff % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

# =========================
# FUNDING ARB SIGNAL
# =========================
def funding_arbitrage_signal(state, exchanges, min_spread):
    rates = {ex: state[ex]["funding"] for ex in exchanges if state[ex]["funding"] is not None}
    if len(rates) < 2:
        return None
    short_ex = max(rates, key=rates.get)
    long_ex = min(rates, key=rates.get)
    spread = rates[short_ex] - rates[long_ex]
    if spread < min_spread:
        return None
    return {"long": long_ex, "short": short_ex, "spread": spread}

# =========================
# BINANCE TESTNET EXECUTION
# =========================
BINANCE_KEY = "20o5eMr269hIU1Tej94iZUBRoubmfODMeYoGmy60uYaUKydUyeRJdJfzlY3IHq0t"
BINANCE_SECRET = "djPGrUCip9ITZdsVxHoc6SWCnfQK6LcrNn7GiVOLHTbynsHYUMORVM28BhzmF6r7"
BINANCE_URL = "https://testnet.binancefuture.com"

def binance_sign(params):
    qs = urlencode(params)
    sig = hmac.new(BINANCE_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    params["signature"] = sig
    return params

def binance_set_leverage(symbol, lev):
    try:
        params = binance_sign({
            "symbol": symbol,
            "leverage": lev,
            "timestamp": int(get_accurate_time()*1000)  # Use NTP-synced time
        })
        r = requests.post(
            BINANCE_URL + "/fapi/v1/leverage",
            headers={"X-MBX-APIKEY": BINANCE_KEY},
            params=params,
            timeout=5
        )
        print(f"Binance leverage response: {r.status_code} - {r.text}")
        return r.json()
    except Exception as e:
        print(f"Binance leverage error: {e}")
        return {}

def get_binance_symbol_filters(symbol):
    """Fetch LOT_SIZE stepSize, PRICE_FILTER tickSize, and MIN_NOTIONAL from Binance API"""
    try:
        url = BINANCE_URL + "/fapi/v1/exchangeInfo"
        r = requests.get(url, timeout=3)
        data = r.json()
        
        filters = {"step_size": "1", "tick_size": "0.01", "min_notional": "5.0"}
        for s in data.get("symbols", []):
            if s["symbol"] == symbol:
                for f in s.get("filters", []):
                    if f["filterType"] == "LOT_SIZE":
                        filters["step_size"] = f["stepSize"]
                        filters["min_qty"] = f.get("minQty", f["stepSize"])  
                    if f["filterType"] == "PRICE_FILTER":
                        filters["tick_size"] = f["tickSize"]
                    if f["filterType"] == "MIN_NOTIONAL":
                        filters["min_notional"] = f["notional"]
                
                print(f"📏 Binance {symbol}: step={filters['step_size']}, tick={filters['tick_size']}, min_notional=${filters['min_notional']}")
                return filters
        return filters
    except Exception as e:
        print(f"⚠️ CRITICAL: Failed to fetch Binance filters: {e}")
        return None

def round_step_size(quantity, step_size):
    """Round quantity to match stepSize precision (Floor)"""
    step_size = Decimal(str(step_size))
    quantity = Decimal(str(quantity))
    rounded = float(quantity.quantize(step_size, rounding=ROUND_DOWN))
    return rounded

def round_step_size_nearest(quantity, step_size):
    """Round quantity to match stepSize precision (Nearest) - Best for Hedging Parity"""
    from decimal import ROUND_HALF_UP
    step_size = Decimal(str(step_size))
    quantity = Decimal(str(quantity))
    rounded = float(quantity.quantize(step_size, rounding=ROUND_HALF_UP))
    return rounded

def round_price(price, tick_size):
    """Round price to match tickSize precision"""
    tick_size = Decimal(str(tick_size))
    price = Decimal(str(price))
    rounded = float(price.quantize(tick_size, rounding=ROUND_DOWN))
    return rounded

# =========================
# BINANCE ERROR HANDLER
# =========================
def handle_binance_min_notional_error(res, qty_to_order, target_price):
    """
    Specifically handle Binance's minimum notional error (-4164).
    Returns dict if it's a dust order we should accept.
    Returns None for other -4164 errors (should retry).
    """
    if "code" in res and res["code"] == -4164:
        msg = res.get("msg", "").lower()
        
        # Check if this is specifically a minimum notional error
        if "notional" in msg and "no smaller than" in msg:
            # Extract the minimum value from error message
            import re
            match = re.search(r'no smaller than ([\d\.]+)', msg)
            min_notional = float(match.group(1)) if match else None
            
            audit_log(f"💡 Minimum order value error: {res.get('msg')}")
            
            if min_notional:
                audit_log(f"📊 Binance minimum: ${min_notional:.2f}, Our order: ${qty_to_order * target_price:.2f}")
                
                # Check if we're below minimum
                if qty_to_order * target_price < min_notional:
                    audit_log(f"💎 Accepting dust order (below minimum ${min_notional:.2f})")
                    return {
                        "code": -4164,
                        "msg": "Dust order accepted",
                        "is_min_notional": True,
                        "min_notional": min_notional,
                        "our_notional": qty_to_order * target_price
                    }
            
            # Even if we can't parse the exact value, if it's a notional error, accept it
            audit_log(f"💎 Accepting dust mismatch")
            return {
                "code": -4164,
                "msg": "Dust order accepted",
                "is_min_notional": True
            }
        else:
            # Other type of -4164 error (could be position, reduce-only, etc.)
            audit_log(f"⚠️ Other -4164 error: {res.get('msg')}. Will retry...")
            return None  # Signal to retry
    
    return None  # Not a handled error

def binance_limit_order(symbol, side, qty, price, step_size, tick_size):
    for attempt in range(3):
        try:
            qty = round_step_size(qty, step_size)
            price = round_price(price, tick_size)
            params = binance_sign({
                "symbol": symbol,
                "side": side,
                "type": "LIMIT",
                "quantity": qty,
                "price": price,
                "timeInForce": "GTX",  # Post-Only
                "timestamp": int(get_accurate_time()*1000)
            })
            audit_log(f"Binance Limit Req: {side} {qty} @ {price:.8f} (Post-Only) | Attempt {attempt+1}")
            r = requests.post(BINANCE_URL + "/fapi/v1/order", headers={"X-MBX-APIKEY": BINANCE_KEY}, params=params, timeout=5)
            res = r.json()
            
            # ✅ NEW: Check for minimum notional dust order error
            dust_check = handle_binance_min_notional_error(res, qty, price)
            if dust_check and dust_check.get("is_min_notional"):
                # This was a dust order below minimum, accept it as filled
                audit_log(f"✅ Accepting dust order as filled: ${qty * price:.8f}")
                return dust_check  # Return the dust check result
            
            # Check for transient errors
            if "code" in res and res["code"] in [-1001, -1003, -1007, -1021]:
                audit_log(f"⚠️ Binance transient error {res['code']}: {res.get('msg')}. Retrying...")
                time.sleep(1)
                continue
                
            audit_log(f"Binance Limit Res: {json.dumps(res)}")
            return res
        except Exception as e:
            audit_log(f"💥 Binance limit order exception (Attempt {attempt+1}): {e}")
            if attempt < 2: time.sleep(1)
    return {}

def binance_cancel_order(symbol, order_id):
    try:
        params = binance_sign({
            "symbol": symbol,
            "orderId": order_id,
            "timestamp": int(get_accurate_time()*1000)
        })
        r = requests.delete(BINANCE_URL + "/fapi/v1/order", headers={"X-MBX-APIKEY": BINANCE_KEY}, params=params, timeout=5)
        return r.json()
    except Exception as e:
        print(f"Binance cancel error: {e}")
        return {}

def binance_get_order(symbol, order_id):
    try:
        params = binance_sign({
            "symbol": symbol,
            "orderId": order_id,
            "timestamp": int(get_accurate_time()*1000)
        })
        r = requests.get(BINANCE_URL + "/fapi/v1/order", headers={"X-MBX-APIKEY": BINANCE_KEY}, params=params, timeout=5)
        return r.json()
    except Exception as e:
        print(f"Binance get order error: {e}")
        return {}

def binance_order(symbol, side, qty, step_size):
    for attempt in range(3):
        try:
            qty = round_step_size(qty, step_size)
            params = binance_sign({
                "symbol": symbol,
                "side": side,
                "type": "MARKET",
                "quantity": qty,
                "timestamp": int(get_accurate_time()*1000)
            })
            r = requests.post(BINANCE_URL + "/fapi/v1/order", headers={"X-MBX-APIKEY": BINANCE_KEY}, params=params, timeout=5)
            res = r.json()
            
            # ✅ NEW: Check for minimum notional dust order error (for market orders too)
            # We need the price for calculation - use current market price
            if "code" in res and res["code"] == -4164:
                msg = res.get("msg", "").lower()
                if "notional" in msg and "no smaller than" in msg:
                    audit_log(f"💡 Binance MARKET dust order: {res.get('msg')}")
                    audit_log(f"💎 Accepting dust order as filled")
                    return {
                        "code": -4164,
                        "msg": "Dust order accepted",
                        "is_min_notional": True
                    }
            
            if "code" in res and res["code"] in [-1001, -1003, -1007, -1021]:
                audit_log(f"⚠️ Binance Market transient error {res['code']}: {res.get('msg')}. Retrying...")
                time.sleep(1)
                continue
                
            return res
        except Exception as e:
            print(f"Binance order error (Attempt {attempt+1}): {e}")
            if attempt < 2: time.sleep(1)
    return {}

# =========================
# BINANCE PNL FETCHER (NEW)
# =========================

async def get_binance_entry_info(session, symbol):
    """Fetch entry price and position size from Binance (Async)"""
    try:
        params = binance_sign({
            "symbol": symbol,
            "timestamp": int(get_accurate_time()*1000)
        })
        async with session.get(BINANCE_URL + "/fapi/v2/positionRisk", params=params, timeout=3) as r:
            if r.status == 200:
                data = await r.json()
                for pos in data:
                    if pos["symbol"] == symbol:
                        size = float(pos.get("positionAmt", 0))
                        if size != 0:
                            entry = float(pos.get("entryPrice", 0))
                            side = "LONG" if size > 0 else "SHORT"
                            return {"entry_price": entry, "size": abs(size), "side": side}
            return None
    except Exception as e:
        # audit_log(f"Binance entry info error: {e}")
        return None

async def get_bybit_entry_info(session, symbol):
    """Fetch entry price and position size from Bybit (Async)"""
    try:
        timestamp = str(int(get_accurate_time() * 1000))
        params = {"category": "linear", "symbol": symbol}
        signature = bybit_sign(params, timestamp, "")
        queryString = urlencode(sorted(params.items()))
        url = f"{BYBIT_URL}/v5/position/list?{queryString}"
        
        headers = {
            "X-BAPI-API-KEY": BYBIT_KEY,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": "5000",
        }
        
        async with session.get(url, headers=headers, timeout=5) as r:
            if r.status != 200: return None
            data = await r.json()
            if data.get("retCode") == 0:
                positions = data.get("result", {}).get("list", [])
                for pos in positions:
                    if pos.get("symbol") == symbol:
                        size = float(pos.get("size", 0))
                        if size != 0:
                            entry = float(pos.get("avgPrice") or pos.get("entryPrice", 0))
                            side = pos.get("side")
                            return {"entry_price": entry, "size": size, "side": side}
            return None
    except Exception as e:
        # audit_log(f"Bybit entry info exception: {e}")
        return None

# =========================
# BYBIT DEMO EXECUTION
# =========================
BYBIT_KEY = "hfE8R6aHfeEdGX18w7"
BYBIT_SECRET = "AYuExy9gfspxpTBWTZkyHIiyKz8vkdqVqnso"
BYBIT_URL = "https://api-demo.bybit.com"

def bybit_sign(params, timestamp, request_body=""):
    """CORRECT V5 signature for Bybit"""
    # Sort and stringify parameters alphabetically for GET requests
    param_str = ""
    if params:
        param_str = urlencode(sorted(params.items()))
    
    # Build signature payload
    recv_window = "5000"
    
    if request_body:  # POST request
        signature_payload = timestamp + BYBIT_KEY + recv_window + request_body
    else:  # GET request
        signature_payload = timestamp + BYBIT_KEY + recv_window + param_str
    
    # Generate HMAC-SHA256
    signature = hmac.new(
        bytes(BYBIT_SECRET, "utf-8"),
        bytes(signature_payload, "utf-8"),
        hashlib.sha256
    ).hexdigest()
    
    return signature
def bybit_set_leverage(symbol, lev):
    try:
        timestamp = str(int(get_accurate_time() * 1000))
        signature = bybit_sign({}, timestamp, "")
        
        headers = {
            "X-BAPI-API-KEY": BYBIT_KEY,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": "5000",
        }
        
        r = requests.get(
            BYBIT_URL + "/v5/account/info",
            headers=headers,
            timeout=5
        )
        
        if r.status_code == 200:
            response = r.json()
            if response.get("retCode") == 0:
                print("✅ Bybit Demo API keys are valid")
                
                params = {
                    "category": "linear",
                    "symbol": symbol,
                    "buyLeverage": str(lev),
                    "sellLeverage": str(lev)
                }
                
                signature = bybit_sign({}, timestamp, json.dumps(params))
                headers["X-BAPI-SIGN"] = signature
                headers["Content-Type"] = "application/json"
                
                r = requests.post(
                    BYBIT_URL + "/v5/position/set-leverage",
                    headers=headers,
                    json=params,
                    timeout=5
                )
                print(f"Bybit leverage response: {r.status_code} - {r.text}")
            else:
                print(f"❌ Bybit API error: {response.get('retMsg', 'Unknown error')}")
                print("⚠️ Get new Demo API keys from: https://www.bybit.com (Demo Trading)")
        else:
            print(f"❌ Bybit API connection failed: {r.status_code}")
            
        return {}
    except Exception as e:
        print(f"Bybit leverage error: {e}")
        return {}

def get_bybit_symbol_filters(symbol):
    """Fetch Bybit instrument info for qty step, tick size, and min notional"""
    try:
        url = BYBIT_URL + f"/v5/market/instruments-info?category=linear&symbol={symbol}"
        r = requests.get(url, timeout=3)
        data = r.json()
        
        filters = {"step_size": "1", "tick_size": "0.01", "min_notional": "10.0"}
        if data.get("retCode") == 0:
            result = data.get("result", {}).get("list", [])
            if result:
                instrument = result[0]
                filters["step_size"] = instrument.get("lotSizeFilter", {}).get("qtyStep", "1")
                filters["min_qty"] = instrument.get("lotSizeFilter", {}).get("minOrderQty", filters["step_size"])  
                filters["tick_size"] = instrument.get("priceFilter", {}).get("tickSize", "0.01")
                
                # Bybit's minimum order value
                min_order_amt = instrument.get("lotSizeFilter", {}).get("minOrderAmt", None)
                if min_order_amt:
                    filters["min_notional"] = min_order_amt
                
                print(f"📏 Bybit {symbol}: step={filters['step_size']}, tick={filters['tick_size']}, min_notional=${filters['min_notional']}")
                return filters
        return filters
    except Exception as e:
        print(f"⚠️ CRITICAL: Failed to fetch Bybit filters: {e}")
        return None

def bybit_limit_order(symbol, side, qty, price, step_size, tick_size):
    for attempt in range(3):
        try:
            qty = round_step_size(qty, step_size)
            price = round_price(price, tick_size)
            timestamp = str(int(get_accurate_time() * 1000))
            params = {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": "Limit",
                "qty": str(qty),
                "price": str(price),
                "timeInForce": "PostOnly"
            }
            signature = bybit_sign({}, timestamp, json.dumps(params))
            headers = {"X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-SIGN": signature, "X-BAPI-RECV-WINDOW": "5000", "Content-Type": "application/json"}
            audit_log(f"Bybit Limit Req: {side} {qty} @ {price:.8f} (Post-Only) | Attempt {attempt+1}")
            r = requests.post(BYBIT_URL + "/v5/order/create", headers=headers, json=params, timeout=5)
            res = r.json()
            
            # Bybit transient codes: 10002 (Expired), 10006 (Rate limit), 10016 (System error)
            if res.get("retCode") in [10002, 10006, 10016]:
                audit_log(f"⚠️ Bybit transient error {res['retCode']}: {res.get('retMsg')}. Retrying...")
                time.sleep(1)
                continue

            audit_log(f"Bybit Limit Res: {json.dumps(res)}")
            return res
        except Exception as e:
            audit_log(f"💥 Bybit limit order exception (Attempt {attempt+1}): {e}")
            if attempt < 2: time.sleep(1)
    return {}

def bybit_cancel_order(symbol, order_id):
    try:
        timestamp = str(int(get_accurate_time() * 1000))
        params = {"category": "linear", "symbol": symbol, "orderId": order_id}
        signature = bybit_sign({}, timestamp, json.dumps(params))
        headers = {"X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-SIGN": signature, "X-BAPI-RECV-WINDOW": "5000", "Content-Type": "application/json"}
        r = requests.post(BYBIT_URL + "/v5/order/cancel", headers=headers, json=params, timeout=5)
        return r.json()
    except Exception as e:
        print(f"Bybit cancel error: {e}")
        return {}

def bybit_get_order(symbol, order_id):
    try:
        timestamp = str(int(get_accurate_time() * 1000))
        params = {"category": "linear", "symbol": symbol, "orderId": order_id}
        signature = bybit_sign(params, timestamp, "")
        
        # Manual construction to ensure signature match
        queryString = urlencode(sorted(params.items()))
        url = f"{BYBIT_URL}/v5/order/realtime?{queryString}"
        
        headers = {
            "X-BAPI-API-KEY": BYBIT_KEY, 
            "X-BAPI-TIMESTAMP": timestamp, 
            "X-BAPI-SIGN": signature, 
            "X-BAPI-RECV-WINDOW": "5000"
        }
        
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code != 200:
            print(f"Bybit order fetch error: {r.status_code} - {r.text}")
            return {}
            
        return r.json()
    except Exception as e:
        print(f"Bybit get order error: {e}")
        return {}

def bybit_order(symbol, side, qty, step_size):
    for attempt in range(3):
        try:
            qty = round_step_size(qty, step_size)
            timestamp = str(int(get_accurate_time() * 1000))
            params = {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": "Market",
                "qty": str(qty),
                "timeInForce": "IOC"
            }
            signature = bybit_sign({}, timestamp, json.dumps(params))
            headers = {"X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-SIGN": signature, "X-BAPI-RECV-WINDOW": "5000", "Content-Type": "application/json"}
            r = requests.post(BYBIT_URL + "/v5/order/create", headers=headers, json=params, timeout=5)
            res = r.json()
            
            if res.get("retCode") in [10002, 10006, 10016]:
                audit_log(f"⚠️ Bybit Market transient error {res['retCode']}: {res.get('retMsg')}. Retrying...")
                time.sleep(1)
                continue
                
            return res
        except Exception as e:
            print(f"Bybit order error (Attempt {attempt+1}): {e}")
            if attempt < 2: time.sleep(1)
    return {}

async def limit_order_chaser(exchange, symbol, side, total_qty, state, filters, trade_records, shared_fill_state):
    step_size = filters["step_size"]
    tick_size = filters["tick_size"]
    min_notional = float(filters["min_notional"])
    
    audit_log(f"🏁 ULTRA-AGGRESSIVE CHASER START: {exchange.upper()} | {side} {total_qty} | Min: ${min_notional}")
    # 🆕 CRITICAL: Check if INITIAL target meets minimum notional
    if side.upper() in ["BUY", "LONG", "Buy"]:
        initial_price = state[exchange]["bid"]
    else:
        initial_price = state[exchange]["ask"]
    
    if initial_price > 0:
        initial_value = total_qty * initial_price
        if initial_value < min_notional:
            audit_log(f"{exchange.upper()} ⚠️ INITIAL TARGET ${initial_value:.2f} < MIN ${min_notional}")
            
            # Boost to meet minimum
            boosted_qty = math.ceil(min_notional / initial_price)
            boosted_qty = round_step_size(boosted_qty, step_size)
            
            audit_log(f"{exchange.upper()} 🎯 BOOSTING from {total_qty} to {boosted_qty} coins (${boosted_qty * initial_price:.2f})")
            total_qty = boosted_qty
    remaining_qty = total_qty

    active_order_id = None
    locked_price = None 
    iteration = 0
    total_filled = 0.0
    total_value = 0.0
    filled_baseline = 0.0  # ✅ Track fills before current order
    value_baseline = 0.0   # ✅ Track value before current order
    
    float_step = float(step_size)
    
    while remaining_qty >= float_step:
        iteration += 1
        # 🔥 DRAIN QUEUE - Always work with LATEST BBO
        drain_bbo_queue(exchange)
        chase_log(f"[BBO_READ] {exchange.upper()} iter={iteration} bid={state[exchange]['bid']:.8f} ask={state[exchange]['ask']:.8f}")

        # 🆕 UPDATE TRACE STATUS

        ws_price = state[exchange].get("ws_active_order_price")
        if active_order_id and ws_price:
            target = state[exchange]["bid"] if side.upper() in ["BUY", "LONG", "Buy"] else state[exchange]["ask"]
            match = "YES" if ws_price == target else "NO"
            state[exchange]["trace_order_status"] = f"{ws_price:.8f} | {match}"
        else:
            state[exchange]["trace_order_status"] = "None"
            
        qty_to_order = remaining_qty
        
        # 🆕 CHECK IF OTHER SIDE FINISHED FIRST
        # 🆕 CHECK IF OTHER SIDE FINISHED FIRST
        other_exchange = "bybit" if exchange == "binance" else "binance"
        if shared_fill_state[exchange]["done"]:
            audit_log(f"{exchange.upper()} ✅ Already done, exiting chaser")
            break

        if shared_fill_state[other_exchange]["done"] and not shared_fill_state[exchange]["done"]:
    
            # 🎯 LOCK the target USDT value once (first time we see it)
            if "locked_target_usdt" not in shared_fill_state[exchange]:
                shared_fill_state[exchange]["locked_target_usdt"] = shared_fill_state[other_exchange]["filled_usdt"]
                audit_log(f"{exchange.upper()} 🔒 LOCKING TARGET: ${shared_fill_state[exchange]['locked_target_usdt']:.4f}")
    
            target_usdt = shared_fill_state[exchange]["locked_target_usdt"]
            current_price = state[exchange]["bid"] if side.upper() in ["BUY", "LONG", "Buy"] else state[exchange]["ask"]
    
            if current_price > 0:
                needed_usdt = target_usdt - total_value
                if needed_usdt <= 0:
                    audit_log(f"{exchange.upper()} 🎯 MATCHED {other_exchange.upper()}'S ${target_usdt:.2f}! Stopping.")
                    break
        
                # Calculate needed quantity
                needed_qty = needed_usdt / current_price
                needed_qty_down = round_step_size(needed_qty, step_size)
                needed_qty_up = needed_qty_down + float(step_size)
        
                # Calculate USDT values
                value_down = needed_qty_down * current_price
                value_up = needed_qty_up * current_price
                
                # Choose the closer one
                delta_down = abs(target_usdt - (total_value + value_down))
                delta_up = abs(target_usdt - (total_value + value_up))
        
                if delta_down <= delta_up:
                    needed_qty_rounded = needed_qty_down
                else:
                    needed_qty_rounded = needed_qty_up  
        
                # 🔥 CRITICAL FIX: Only cancel if target ACTUALLY changed
                new_total_qty = total_filled + needed_qty_rounded
        
                if abs(new_total_qty - total_qty) > float(step_size) * 0.5 and active_order_id:
                    audit_log(f"{exchange.upper()} 🔄 Target changed from {total_qty} to {new_total_qty}! Canceling...")
            
                    # Cancel and refetch fills
                    if exchange == "binance":
                        binance_cancel_order(symbol, active_order_id)
                        await asyncio.sleep(0.1)
                        state[exchange]["trace_order_status"] = "None"  # 🆕 ADD THIS
                        res = binance_get_order(symbol, active_order_id)
                        if res and "orderId" in res:
                            final_filled = float(res.get("executedQty", 0))
                            final_value = float(res.get("cumQuote", 0))
                            total_filled = filled_baseline + final_filled
                            total_value = value_baseline + final_value
                    else:
                        bybit_cancel_order(symbol, active_order_id)
                        await asyncio.sleep(0.1)
                        state[exchange]["trace_order_status"] = "None"  # 🆕 ADD THIS
                        res = bybit_get_order(symbol, active_order_id)
                        result_list = res.get("result", {}).get("list", [])
                        if result_list:
                            final_filled = float(result_list[0].get("cumExecQty", 0))
                            final_value = float(result_list[0].get("cumExecValue", 0))
                            total_filled = filled_baseline + final_filled
                            total_value = value_baseline + final_value
                    audit_log(f"{exchange.upper()} 🎯 Target: {total_qty:.1f} coins (${target_usdt:.4f}) | Filled: {total_filled:.1f} | Remaining: {remaining_qty:.1f}")
                    
                    total_qty = new_total_qty
                    remaining_qty = total_qty - total_filled
                    active_order_id = None
        
                # Update targets
                qty_to_order = remaining_qty
                actual_value_at_price = remaining_qty * current_price
                delta = abs(actual_value_at_price - target_usdt)
                audit_log(f"{exchange.upper()} 🎯 Target: {total_qty:.1f} coins (~${actual_value_at_price:.4f} at current price) | Matching {other_exchange.upper()}: ${target_usdt:.4f} | Delta: ${delta:.4f} | Filled: {total_filled:.1f} | Remaining: {remaining_qty:.1f}")
                
# ✅ Get current BBO (bid/ask, NOT mark price)        
        # ✅ Get current BBO (bid/ask, NOT mark price)

        if side.upper() in ["BUY", "LONG", "Buy"]:
            target_price = state[exchange]["bid"]  # Join the best bid
        else:
            target_price = state[exchange]["ask"]  # Join the best ask
            
        # ✅ CRITICAL FIX: If bid/ask is 0 (WS data not ready), wait!
        if target_price <= 0:
            if iteration % 10 == 0:
                audit_log(f"⏳ {exchange.upper()} waiting for valid BBO (current: {target_price})")
            await asyncio.sleep(0.1)
            continue
        
        if active_order_id:
            # ✅ Fetch order status
            status = None
            if exchange == "binance":
                res = binance_get_order(symbol, active_order_id)
                if res and "orderId" in res:
                    status = {
                        "filled": float(res.get("executedQty", 0)),
                        "value": float(res.get("cumQuote", 0)),
                        "price": float(res.get("price", 0)),
                        "status": res.get("status", "UNKNOWN")
                    }
            else:  # bybit
                res = bybit_get_order(symbol, active_order_id)
                result_list = res.get("result", {}).get("list", [])
                if result_list:
                    o = result_list[0]
                    status = {
                        "filled": float(o.get("cumExecQty", 0)),
                        "value": float(o.get("cumExecValue", 0)),
                        "price": float(o.get("price", 0)),
                        "status": o.get("status", "Unknown")
                    }
            
            if not status:
                audit_log(f"⚠️ Failed to fetch order status, retrying...")
                await asyncio.sleep(0.05)
                continue
            
            # Update fills
            current_delta = float(status["filled"])
            current_value_delta = float(status.get("value", 0))
            total_filled = filled_baseline + current_delta
            total_value = value_baseline + current_value_delta
            remaining_qty = total_qty - total_filled
            qty_to_order = remaining_qty
            audit_log(f"{exchange.upper()} 🔍 Iter#{iteration}: Filled {total_filled}/{total_qty} | "
         f"Order@{status['price']:.8f} | Target@{target_price:.8f}")

        # Check maker/taker ratio every 5 iterations
        if iteration % 5 == 0 and active_order_id:
            maker_count = 0
            taker_count = 0
            
            if exchange == "binance":
                # Fetch trades for this order
                params = binance_sign({
                    "symbol": symbol,
                    "orderId": active_order_id,
                    "timestamp": int(get_accurate_time() * 1000)
                })
                try:
                    r = requests.get(BINANCE_URL + "/fapi/v1/userTrades", 
                                   headers={"X-MBX-APIKEY": BINANCE_KEY}, 
                                   params=params, timeout=2)
                    trades = r.json()
                    if isinstance(trades, list):
                        maker_count = sum(1 for t in trades if t.get("maker"))
                        taker_count = len(trades) - maker_count
                except: pass
            else:  # bybit
                timestamp = str(int(get_accurate_time() * 1000))
                params = {"category": "linear", "symbol": symbol, "orderId": active_order_id}
                signature = bybit_sign(params, timestamp, "")
                queryString = urlencode(sorted(params.items()))
                url = f"{BYBIT_URL}/v5/execution/list?{queryString}"
                headers = {"X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-TIMESTAMP": timestamp, 
                  "X-BAPI-SIGN": signature, "X-BAPI-RECV-WINDOW": "5000"}
                try:
                    r = requests.get(url, headers=headers, timeout=2)
                    data = r.json()
                    if data.get("retCode") == 0:
                        execs = data.get("result", {}).get("list", [])
                        maker_count = sum(1 for e in execs if e.get("isMaker") == "true")
                        taker_count = len(execs) - maker_count
                except: pass
    
            if maker_count + taker_count > 0:
                maker_pct = (maker_count / (maker_count + taker_count)) * 100
                audit_log(f"{exchange.upper()} 📊 Fill Quality: {maker_count}M/{taker_count}T ({maker_pct:.1f}% Maker)")
            # ✅ Check if fully filled
            if remaining_qty <= float(step_size):
                audit_log(f"🎯 {exchange.upper()} FULLY FILLED! (by quantity)")
                break
            elif status["status"] in ["FILLED", "Filled"]:
                # Order says filled but we might have remaining_qty > 0
                # This happens when order partially filled and was canceled
                audit_log(f"🎯 {exchange.upper()} Order marked as {status['status']}, remaining: {remaining_qty}")
                # Don't break! Let it continue with remaining quantity
                if remaining_qty <= float(step_size):
                    break
                active_order_id = None
                continue
            
            # ✅ Handle terminal states
            terminal_states = {
                "binance": ["REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"],
                "bybit": ["Rejected", "Cancelled", "PartiallyFilledCanceled", "Deactivated"]
            }
            
            if status["status"] in terminal_states.get(exchange, []):
                audit_log(f"{exchange.upper()} ⚠️ Terminal state: {status['status']}. Placing new order...")
                active_order_id = None
                last_price = None
                continue
            
            # ✅ ULTRA-AGGRESSIVE: Cancel if price moves by more than 1 tick
            # 🆕 FIX: Only cancel if other exchange is still active (not done)
            if not shared_fill_state[other_exchange]["done"] and status["price"] != target_price:
                chase_log(f"[MISMATCH] {exchange.upper()} order={status['price']:.8f} bbo={target_price:.8f} diff={abs(status['price']-target_price):.8f}")
                _cancel_start = time.time()
                audit_log(f"{exchange.upper()} 🔄 PRICE MOVED >= TICK! {status['price']:.8f} → {target_price:.8f} | CANCELING!")
                
                chase_log(f"[CANCEL_SEND] {exchange.upper()} order_id={active_order_id} price={status['price']:.8f}")

                # Cancel order
                if exchange == "binance":
                    binance_cancel_order(symbol, active_order_id)
                else:
                    bybit_cancel_order(symbol, active_order_id)
                
                chase_log(f"[CANCEL_RECV] {exchange.upper()} order_id={active_order_id} duration={int((time.time()-_cancel_start)*1000)}ms")
                _fetch_start = time.time()
                
                await asyncio.sleep(0.1)  # Brief wait for cancel confirmation

                # 🆕 UPDATE TRACE STATUS
                state[exchange]["trace_order_status"] = "None"
                
                # Re-fetch final fills
                final_filled = 0.0
                if exchange == "binance":
                    res = binance_get_order(symbol, active_order_id)
                    if res and "orderId" in res:
                        final_filled = float(res.get("executedQty", 0))
                        final_value = float(res.get("cumQuote", 0))
                
                else:
                    res = bybit_get_order(symbol, active_order_id)
                    result_list = res.get("result", {}).get("list", [])
                    if result_list:
                        final_filled = float(result_list[0].get("cumExecQty", 0))
                        final_value = float(result_list[0].get("cumExecValue", 0))
                
                chase_log(f"[FETCH_RECV] {exchange.upper()} duration={int((time.time()-_fetch_start)*1000)}ms filled={final_filled}")
                
                total_filled = filled_baseline + float(final_filled)
                total_value = value_baseline + float(final_value)
                remaining_qty = total_qty - total_filled
                qty_to_order = remaining_qty
                audit_log(f"{exchange.upper()} 📉 Post-cancel: Filled={total_filled}, Remaining={remaining_qty}")
                
                if remaining_qty <= 0.001:
                    audit_log(f"{exchange.upper()} 🎯 Filled during cancel!")
                    break
                
                active_order_id = None
                last_price = None
                continue
            
            # 🆕 DRAIN QUEUE - Always use LATEST BBO
            drained = drain_bbo_queue(exchange)
            chase_log(f"[BBO_READ] {exchange.upper()} iter={iteration} drained={drained} bid={state[exchange]['bid']:.8f} ask={state[exchange]['ask']:.8f}")
            if drained > 0:
                # BBO changed! Loop will recheck immediately
                if drained > 10:
                    audit_log(f"{exchange.upper()} ⚡ Backlog: skipped {drained} old BBO updates")
            else:
                # No new BBO, wait briefly
                await asyncio.sleep(0.02)

            continue

        # 🔥 DRAIN QUEUE - Get fresh BBO before placing
        drain_bbo_queue(exchange)
        chase_log(f"[BBO_READ_PREORDER] {exchange.upper()} iter={iteration} bid={state[exchange]['bid']:.8f} ask={state[exchange]['ask']:.8f}")
        
        # ✅ Place new order if none active
        if not active_order_id and remaining_qty >= float_step:
            # 🆕 Check minimum notional (exchange-specific)
            if qty_to_order * target_price < min_notional:
                min_qty_needed = math.ceil(min_notional / target_price)
                min_qty_needed = round_step_size(min_qty_needed, step_size)

                if min_qty_needed <= remaining_qty:
                    qty_to_order = min_qty_needed
                    audit_log(f"{exchange.upper()} 🎯 Boosting order to ${qty_to_order * target_price:.2f} (min ${min_notional})")
                else:
                    # Can't meet min, accept dust
                    audit_log(f"{exchange.upper()} 🧹 Accepting ${remaining_qty * target_price:.2f} dust (below ${min_notional} min)")
                    break
            
            if qty_to_order <= 0:
                audit_log(f"{exchange.upper()} 🧹 Dust remaining ({remaining_qty}), finishing.")
                break
            
            audit_log(f"{exchange.upper()} 🆕 Placing {side} @ {target_price:.8f} for {qty_to_order}")
            _order_start = time.time()
            chase_log(f"[ORDER_SEND] {exchange.upper()} price={target_price:.8f} qty={qty_to_order} bbo_bid={state[exchange]['bid']:.8f} bbo_ask={state[exchange]['ask']:.8f}")
            
            res = {}
            if exchange == "binance":
                res = binance_limit_order(symbol, side, qty_to_order, target_price, step_size, tick_size)
                active_order_id = res.get("orderId")
            else:
                order_side = "Buy" if side.upper() in ["BUY", "LONG"] else "Sell"
                res = bybit_limit_order(symbol, order_side, qty_to_order, target_price, step_size, tick_size)
                active_order_id = res.get("result", {}).get("orderId")
            
            if active_order_id:
                trade_records[exchange]["entry_order_ids"].append(active_order_id)
                _already_stale = "YES" if target_price != (state[exchange]["bid"] if side.upper() in ["BUY","LONG","Buy"] else state[exchange]["ask"]) else "NO"
                chase_log(f"[ORDER_RECV] {exchange.upper()} order_id={active_order_id} duration={int((time.time()-_order_start)*1000)}ms already_stale={_already_stale}")
                if '_cancel_start' in locals():
                    chase_log(f"[CYCLE] {exchange.upper()} cancel_ms={int((time.time()-_cancel_start)*1000)}ms order_ms={int((time.time()-_order_start)*1000)}ms total_ms={int((time.time()-_cancel_start)*1000)}ms")
                else:
                    chase_log(f"[CYCLE] {exchange.upper()} first_order order_ms={int((time.time()-_order_start)*1000)}ms no_cancel_yet")
                locked_price = target_price  # 🆕 ADD THIS LINE
                state[exchange]["trace_order_status"] = f"{target_price:.8f} | YES"  # 🆕 ADD THIS LINE


            if not active_order_id:
                # ✅ Check if this was a dust order that we should accept
                if exchange == "binance" and res.get("code") == -4164 and res.get("is_min_notional"):
                    audit_log(f"{exchange.upper()} 💎 DUST DETECTED: qty={qty_to_order}, value=${qty_to_order*target_price:.6f}")
                    audit_log(f"{exchange.upper()} 💎 Dust order accepted as filled: ${qty_to_order * target_price:.2f}")
                    total_filled += qty_to_order
                    total_value += qty_to_order * target_price
                    remaining_qty = total_qty - total_filled
                    audit_log(f"{exchange.upper()} 🧹 Dust accumulation: {total_filled}/{total_qty}")
                    
                    if remaining_qty <= float_step:
                        audit_log(f"{exchange.upper()} 🏁 Chaser finished via dust acceptance")
                        break
                    else:
                        continue  # Try next chunk
                else:
                    audit_log(f"{exchange.upper()} ⚠️ Order rejected: {json.dumps(res)}")
                    await asyncio.sleep(0.2)
                    continue
            
            filled_baseline = total_filled # ✅ Store fills BEFORE this order
            value_baseline = total_value   # ✅ Store value BEFORE this order
            audit_log(f"{exchange.upper()} ✅ Order placed: {active_order_id}")
            maker_count = 0
            taker_count = 0
        
        await asyncio.sleep(0.02)  # ✅ ULTRA-FAST loop (50 checks/sec)
    
    # Final maker/taker check
    final_maker = 0
    final_taker = 0
    if exchange == "binance":
        params = binance_sign({
            "symbol": symbol,
            "limit": 100,
            "timestamp": int(get_accurate_time() * 1000)
        })
        try:
            r = requests.get(BINANCE_URL + "/fapi/v1/userTrades", 
                       headers={"X-MBX-APIKEY": BINANCE_KEY}, 
                       params=params, timeout=3)
            trades = r.json()
            if isinstance(trades, list):
                # Filter trades from our order IDs
                our_trades = [t for t in trades if str(t.get("orderId")) in [str(o) for o in trade_records[exchange]["entry_order_ids"]]]
                final_maker = sum(1 for t in our_trades if t.get("maker"))
                final_taker = len(our_trades) - final_maker
        except: pass
    else:  # bybit
        timestamp = str(int(get_accurate_time() * 1000))
        params = {"category": "linear", "symbol": symbol, "limit": "100"}
        signature = bybit_sign(params, timestamp, "")
        queryString = urlencode(sorted(params.items()))
        url = f"{BYBIT_URL}/v5/execution/list?{queryString}"
        headers = {"X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-TIMESTAMP": timestamp, 
              "X-BAPI-SIGN": signature, "X-BAPI-RECV-WINDOW": "5000"}
        try:
            r = requests.get(url, headers=headers, timeout=3)
            data = r.json()
            if data.get("retCode") == 0:
                execs = data.get("result", {}).get("list", [])
                our_execs = [e for e in execs if str(e.get("orderId")) in [str(o) for o in trade_records[exchange]["entry_order_ids"]]]
                final_maker = sum(1 for e in our_execs if e.get("isMaker") == "true")
                final_taker = len(our_execs) - final_maker
        except: pass

    maker_pct = (final_maker / (final_maker + final_taker) * 100) if (final_maker + final_taker) > 0 else 0

    # 🆕 Mark this exchange as done
    shared_fill_state[exchange]["done"] = True
    shared_fill_state[exchange]["filled_usdt"] = total_value
    shared_fill_state[exchange]["filled_qty"] = total_filled

    audit_log(f"{exchange.upper()} 🏁 CHASER FINISHED: {total_filled}/{total_qty} filled | Total Value: ${total_value:.8f}")
    audit_log(f"{exchange.upper()} 📊 ENTRY QUALITY: {final_maker} Maker / {final_taker} Taker ({maker_pct:.1f}% Maker)")

    return total_filled, total_value

async def limit_order_exit_chaser_v3(exchange, symbol, side, state, filters, trade_records, position_info, shared_exit_state):
    """
    Exit chaser V3: When ANY exchange's BBO changes, BOTH cancel and recalculate together
    """
    step_size = filters["step_size"]
    tick_size = filters["tick_size"]
    min_notional = float(filters["min_notional"])
    
    entry_price = position_info[exchange]["entry_price"]
    total_qty = position_info[exchange]["size"]
    position_side = position_info[exchange]["side"]
    
    audit_log(f"{'='*80}")
    audit_log(f"🚪 EXIT CHASER V3 INITIALIZED: {exchange.upper()}")
    audit_log(f"   Position: {position_side} {total_qty} @ ${entry_price:.8f}")
    audit_log(f"{'='*80}")
    
    remaining_qty = total_qty
    realized_pnl = 0.0
    realized_filled = 0.0
    realized_value = 0.0
    active_order_id = None
    iteration = 0
    float_step = float(step_size)
    
    # 🆕 CUMULATIVE TRACKING ACROSS ALL ORDERS
    cumulative_filled_qty = 0.0
    cumulative_filled_value = 0.0
    cumulative_realized_pnl = 0.0
    
    # Track locked price for this exchange
    locked_price = None
    
    # OUTER LOOP: Keep trying until position is empty
    while remaining_qty >= float_step:
        iteration += 1
        
        # ====================================
        # STEP 1: Wait for BOTH exchanges to be ready
        # ====================================
        # Set my status as "ready to calculate"
        shared_exit_state[exchange]["status"] = "calculating"
        
        # Wait for other exchange to also be ready
        other_exchange = "bybit" if exchange == "binance" else "binance"
        while shared_exit_state[other_exchange]["status"] not in ["calculating", "done"]:
            await asyncio.sleep(0.01)
        
        # If other exchange is done, I can proceed freely
        if shared_exit_state[other_exchange]["status"] == "done":
            pass  # No coordination needed
        
        # ====================================
        # STEP 2: Get current Best BID/ASK
        # ====================================
        if side.upper() in ["SELL", "Sell"]:
            current_best_price = state[exchange]["ask"]
        else:
            current_best_price = state[exchange]["bid"]
        
        if current_best_price <= 0:
            await asyncio.sleep(0.05)
            continue
        
        # ====================================
        # STEP 3: Calculate THIS exchange's PNL (Realized + Unrealized)
        # ====================================
        # 🆕 Use cumulative values for accurate tracking
        remaining_qty = total_qty - cumulative_filled_qty
        
        if position_side in ["LONG", "Buy"]:
            unrealized_pnl = (current_best_price - entry_price) * remaining_qty
        else:
            unrealized_pnl = (entry_price - current_best_price) * remaining_qty
        
        my_total_pnl = cumulative_realized_pnl + unrealized_pnl
        
        # 🆕 Enhanced logging every 50 iterations
        if iteration % iteration != 0:
            side_str = "BID" if side.upper() in ["BUY", "Buy"] else "ASK"
            audit_log(f"{exchange.upper()} 📊 EXIT TRACKING:")
            audit_log(f"   Filled: {cumulative_filled_qty:.1f}/{total_qty} coins (${cumulative_filled_value:.2f})")
            audit_log(f"   Realized PNL: ${cumulative_realized_pnl:+.6f}")
            audit_log(f"   Remaining: {remaining_qty:.1f} coins @ Entry ${entry_price:.8f}")
            audit_log(f"   Current {side_str}: ${current_best_price:.8f}")
            audit_log(f"   Unrealized PNL: ${unrealized_pnl:+.6f}")
            audit_log(f"   TOTAL PNL: ${my_total_pnl:+.6f}")
        
        # Update shared state with cumulative values
        shared_exit_state[exchange]["realized_pnl"] = cumulative_realized_pnl
        shared_exit_state[exchange]["unrealized_pnl"] = unrealized_pnl
        shared_exit_state[exchange]["total_pnl"] = my_total_pnl
        shared_exit_state[exchange]["remaining_qty"] = remaining_qty
        shared_exit_state[exchange]["best_price"] = current_best_price
        shared_exit_state[exchange]["filled_qty"] = cumulative_filled_qty
        
        # ====================================
        # STEP 4: Wait for OTHER exchange to finish calculating
        # ====================================
        # Brief wait for other exchange to update their shared state
        await asyncio.sleep(0.02)
        
        # ====================================
        # STEP 5: Check COMBINED PNL from BOTH exchanges
        # ====================================
        other_pnl = shared_exit_state[other_exchange]["total_pnl"]
        other_realized = shared_exit_state[other_exchange]["realized_pnl"]
        other_unrealized = shared_exit_state[other_exchange]["unrealized_pnl"]
        combined_pnl = my_total_pnl + other_pnl
        
        if iteration % iteration != 0:
            # 🆕 DETAILED BREAKDOWN LOGGING
            audit_log(f"{'='*90}")
            audit_log(f"📊 COMBINED PNL CHECK - Iteration #{iteration}")
            audit_log(f"")
            audit_log(f"   {exchange.upper():7} | Realized: ${cumulative_realized_pnl:+.6f} + Unrealized: ${unrealized_pnl:+.6f} = ${my_total_pnl:+.6f}")
            audit_log(f"   {other_exchange.upper():7} | Realized: ${other_realized:+.6f} + Unrealized: ${other_unrealized:+.6f} = ${other_pnl:+.6f}")
            audit_log(f"")
            
            # Color coding for combined PNL
            if combined_pnl >= 0:
                audit_log(f"   ✅ COMBINED: ${cumulative_realized_pnl:+.6f} + ${unrealized_pnl:+.6f} + ${other_realized:+.6f} + ${other_unrealized:+.6f} = ${combined_pnl:+.6f}")
            else:
                audit_log(f"   ❌ COMBINED: ${cumulative_realized_pnl:+.6f} + ${unrealized_pnl:+.6f} + ${other_realized:+.6f} + ${other_unrealized:+.6f} = ${combined_pnl:+.6f}")
            
            audit_log(f"{'='*90}")
        
        # ====================================
        # STEP 6: If COMBINED PNL < 0, WAIT (cancel any active order)
        # ====================================
        # 🖨️ ALWAYS PRINT PNL CALCULATION BEFORE DECIDING
        audit_log(f"", also_print=False)
        audit_log(f"{'━'*70}", also_print=False)
        audit_log(f"📊 PNL CHECK — {exchange.upper()} | Iteration #{iteration}", also_print=False)
        audit_log(f"   {exchange.upper()} → Realized: ${cumulative_realized_pnl:+.6f} + Unrealized: ${unrealized_pnl:+.6f} = ${my_total_pnl:+.6f}", also_print=False)
        audit_log(f"   {other_exchange.upper():7} → Total PNL (from shared): ${other_pnl:+.6f}", also_print=False)
        audit_log(f"   ──────────────────────────────────────────────────────", also_print=False)
        if combined_pnl >= 0:
            audit_log(f"   ✅ async def api_pnl_tracker(state, coin, exchanges, pos_info):COMBINED = ${my_total_pnl:+.6f} + ${other_pnl:+.6f} = ${combined_pnl:+.6f} → POSITIVE → {'PLACING ORDER' if not active_order_id else 'KEEPING ORDER'}", also_print=False)
        else:
            audit_log(f"   ❌ COMBINED = ${my_total_pnl:+.6f} + ${other_pnl:+.6f} = ${combined_pnl:+.6f} → NEGATIVE → {'CANCELING ORDER' if active_order_id else 'WAITING'}", also_print=False)
        audit_log(f"{'━'*70}", also_print=False)
        audit_log(f"", also_print=False)
        
        if combined_pnl < 0:
            if active_order_id:
                audit_log(f"{exchange.upper()} ❌ COMBINED PNL NEGATIVE (${combined_pnl:.6f}) - Canceling order")
                audit_log(f"   Order ID to cancel: {active_order_id}")
                
                if exchange == "binance":
                    binance_cancel_order(symbol, active_order_id)
                else:
                    bybit_cancel_order(symbol, active_order_id)
                
                audit_log(f"{exchange.upper()} ⏳ Waiting for WebSocket position update after cancel...")
                await asyncio.sleep(0.2)  # Wait for cancel + WebSocket update
                
                # ✅ GET POSITION FROM WEBSOCKET (NO HTTP REFETCH!)
                current_position_size = state[exchange].get("position_size", total_qty)
                filled_qty = total_qty - current_position_size
                
                audit_log(f"{exchange.upper()} 📡 Position status from WebSocket:")
                audit_log(f"   Total target: {total_qty} coins")
                audit_log(f"   Current position: {current_position_size} coins")
                audit_log(f"   Total filled so far: {filled_qty} coins")
                audit_log(f"   Previously tracked fills: {cumulative_filled_qty} coins")
                
                # Update tracking ONLY if position actually changed
                if filled_qty > cumulative_filled_qty:
                    new_fills = filled_qty - cumulative_filled_qty
                    
                    # Use current BBO price for PNL calculation
                    if side.upper() in ["BUY", "Buy"]:
                        fill_price = state[exchange]["bid"]
                    else:
                        fill_price = state[exchange]["ask"]
                    
                    audit_log(f"{exchange.upper()} 💰 New fills detected: {new_fills} coins @ ${fill_price:.8f}")
                    
                    # Calculate PNL for new fills
                    if position_side in ["LONG", "Buy"]:
                        new_pnl = (fill_price - entry_price) * new_fills
                    else:
                        new_pnl = (entry_price - fill_price) * new_fills
                    
                    cumulative_realized_pnl += new_pnl
                    cumulative_filled_qty = filled_qty
                    
                    audit_log(f"{exchange.upper()} 📊 PNL Update: ${new_pnl:+.6f} added to cumulative")
                    audit_log(f"{exchange.upper()} 📊 Cumulative PNL now: ${cumulative_realized_pnl:+.6f}")
                else:
                    audit_log(f"{exchange.upper()} ℹ️  No new fills detected (position unchanged)")
                
                # Update tracking variables
                realized_filled = cumulative_filled_qty
                realized_pnl = cumulative_realized_pnl
                remaining_qty = total_qty - cumulative_filled_qty
                active_order_id = None
                locked_price = None
                
                audit_log(f"{exchange.upper()} 📊 After cancel summary:")
                audit_log(f"   Realized PNL: ${realized_pnl:+.6f}")
                audit_log(f"   Remaining to close: {remaining_qty} coins")
            
            shared_exit_state[exchange]["status"] = "waiting_pnl"
            await asyncio.sleep(0.05)
            continue
            
            shared_exit_state[exchange]["status"] = "waiting_pnl"
            await asyncio.sleep(0.05)
            continue
        
        # ====================================
        # STEP 7: COMBINED PNL >= 0, Lock price and place order
        # ====================================
        snapshot_price = current_best_price
        
        # Check if we need to place a new order
        if not active_order_id:
            # Check minimum notional
            qty_to_order = remaining_qty
            order_value = qty_to_order * snapshot_price
            
            if order_value < min_notional:
                min_qty_needed = math.ceil(min_notional / snapshot_price)
                min_qty_needed = round_step_size(min_qty_needed, step_size)
                
                if min_qty_needed <= remaining_qty:
                    qty_to_order = min_qty_needed
                else:
                    audit_log(f"{exchange.upper()} 💎 Accepting dust (${remaining_qty * snapshot_price:.2f})")
                    shared_exit_state[exchange]["dust_remaining"] = remaining_qty  # 🆕 ADD THIS LINE
                    shared_exit_state[exchange]["dust_side"] = position_side      # 🆕 ADD THIS LINE
                    break
            
            if qty_to_order <= 0:
                break
            
            audit_log(f"{exchange.upper()} 📤 PLACING: {side} {qty_to_order} @ ${snapshot_price:.8f}")
            
            res = {}
            if exchange == "binance":
                res = binance_limit_order(symbol, side, qty_to_order, snapshot_price, step_size, tick_size)
                active_order_id = res.get("orderId")
            else:
                res = bybit_limit_order(symbol, side, qty_to_order, snapshot_price, step_size, tick_size)
                active_order_id = res.get("result", {}).get("orderId")
            
            if active_order_id:
                trade_records[exchange]["exit_order_ids"].append(active_order_id)
                locked_price = snapshot_price
                shared_exit_state[exchange]["locked_price"] = locked_price
                shared_exit_state[exchange]["status"] = "order_active"
                audit_log(f"{exchange.upper()} ✅ Order placed: ID={active_order_id} @ ${locked_price:.8f}")
            else:
                if exchange == "binance" and res.get("code") == -4164 and res.get("is_min_notional"):
                    audit_log(f"{exchange.upper()} 💎 Dust accepted")
                    break
                await asyncio.sleep(0.2)
                continue
        
        # ====================================
        # STEP 8: Monitor position via WebSocket (NO POLLING!)
        # ====================================
        shared_exit_state[exchange]["status"] = "monitoring"
        
        audit_log(f"{exchange.upper()} 🎧 Monitoring position via WebSocket...")
        
        last_logged_position = None
        
        while active_order_id:
            await asyncio.sleep(0.02)  # Check every 20ms
            
            # ✅ GET LIVE POSITION FROM WEBSOCKET STATE
            current_position_size = state[exchange].get("position_size", total_qty)
            
            # Calculate fills based on position decrease
            filled_qty = total_qty - current_position_size
            remaining_qty = current_position_size
            
            # ✅ LOG ONLY WHEN POSITION CHANGES
            if current_position_size != last_logged_position:
                audit_log(f"{exchange.upper()} 📡 POSITION CHANGED (WebSocket)")
                audit_log(f"   Target: {total_qty} | Current Position: {current_position_size} | Filled: {filled_qty}")
                audit_log(f"   Remaining to close: {remaining_qty}")
                last_logged_position = current_position_size
                
                # Update cumulative tracking
                if filled_qty > cumulative_filled_qty:
                    new_fills = filled_qty - cumulative_filled_qty
                    
                    # Get current price from WebSocket BBO
                    if side.upper() in ["SELL", "Sell"]:
                        fill_price = state[exchange]["ask"]
                    else:
                        fill_price = state[exchange]["bid"]
                    
                    # Calculate PNL for new fills
                    if position_side in ["LONG", "Buy"]:
                        new_pnl = (fill_price - entry_price) * new_fills
                    else:
                        new_pnl = (entry_price - fill_price) * new_fills
                    
                    cumulative_realized_pnl += new_pnl
                    cumulative_filled_qty = filled_qty
                    
                    audit_log(f"{exchange.upper()} 💰 NEW FILLS: +{new_fills} @ ${fill_price:.8f} | PNL: ${new_pnl:+.6f}")
                    audit_log(f"{exchange.upper()} 📊 CUMULATIVE: {cumulative_filled_qty}/{total_qty} filled | Realized: ${cumulative_realized_pnl:+.6f}")
                    
                    # Update shared state
                    shared_exit_state[exchange]["realized_pnl"] = cumulative_realized_pnl
                    shared_exit_state[exchange]["remaining_qty"] = remaining_qty
            
            # ✅ CHECK IF FULLY CLOSED
            if remaining_qty <= float_step:
                audit_log(f"{exchange.upper()} 🎯 POSITION FULLY CLOSED! (WebSocket confirmed)")
                audit_log(f"   Final position: {current_position_size} coins")
                audit_log(f"   Total filled: {filled_qty} coins")
                active_order_id = None
                
                # Update shared state with FINAL values
                shared_exit_state[exchange]["realized_pnl"] = cumulative_realized_pnl
                shared_exit_state[exchange]["unrealized_pnl"] = 0.0
                shared_exit_state[exchange]["total_pnl"] = cumulative_realized_pnl
                shared_exit_state[exchange]["remaining_qty"] = 0
                shared_exit_state[exchange]["status"] = "done"
                break
            
            # Check if fully filled
            if remaining_qty <= float_step:
                audit_log(f"{exchange.upper()} 🎯 FULLY CLOSED!")
                active_order_id = None
    
                # 🆕 CRITICAL FIX: Update shared state with FINAL values before marking done
                shared_exit_state[exchange]["realized_pnl"] = realized_pnl
                shared_exit_state[exchange]["unrealized_pnl"] = 0.0  # Position is closed, no unrealized left
                shared_exit_state[exchange]["total_pnl"] = realized_pnl  # Final PNL
                shared_exit_state[exchange]["remaining_qty"] = 0  # Position fully closed
                shared_exit_state[exchange]["best_price"] = current_best_price  # Last known price
                
                shared_exit_state[exchange]["status"] = "done"
                break
            
            # ====================================
            # 🔥 CRITICAL: Check BBO changes based on fill status
            # ====================================
            
            # Step 1: Determine fill status of BOTH exchanges
            my_is_filled = (remaining_qty <= float_step)
            other_is_filled = (shared_exit_state[other_exchange]["remaining_qty"] <= float_step)
            
            # Step 2: Get fresh BBO prices
            if side.upper() in ["SELL", "Sell"]:
                my_fresh_price = state[exchange]["ask"]
            else:
                my_fresh_price = state[exchange]["bid"]
            
            if other_exchange in ["binance", "bybit"]:
                other_side = "SELL" if shared_exit_state[other_exchange].get("position_side") in ["LONG", "Buy"] else "BUY"
                if other_side == "SELL":
                    other_fresh_price = state[other_exchange]["ask"]
                else:
                    other_fresh_price = state[other_exchange]["bid"]
            else:
                other_fresh_price = None
            
            # Step 3: Check if BBOs changed
            my_bbo_changed = (locked_price and my_fresh_price != locked_price)
            other_locked_price = shared_exit_state[other_exchange].get("locked_price")
            other_bbo_changed = (other_locked_price and other_fresh_price and other_fresh_price != other_locked_price)
            
            # ====================================
            # SCENARIO 1: THIS EXCHANGE IS FILLED
            # ====================================
            if my_is_filled:
                
                if other_bbo_changed:
                    # Other BBO changed, I'm filled → Signal OTHER to cancel
                    audit_log(f"{exchange.upper()} ✅ I'm FILLED, {other_exchange.upper()} BBO changed - signaling them to cancel")
                    audit_log(f"   {other_exchange.upper()} BBO: ${other_locked_price:.8f} → ${other_fresh_price:.8f}")
                    
                    shared_exit_state["cancel_trigger"] = True
                    await asyncio.sleep(0.05)
                    continue
            
            # ====================================
            # SCENARIO 2: OTHER EXCHANGE IS FILLED, I'M PENDING
            # ====================================
            elif other_is_filled:
                
                if my_bbo_changed:
                    # MY BBO changed, I'm pending → Cancel MY order ONLY
                    audit_log(f"{exchange.upper()} ⏳ I'm PENDING, my BBO changed (${locked_price:.8f} → ${my_fresh_price:.8f})")
                    audit_log(f"{exchange.upper()} ❌ Canceling MY order ONLY (other exchange is FILLED)")
                    
                    # Cancel MY order
                    if exchange == "binance":
                        binance_cancel_order(symbol, active_order_id)
                    else:
                        bybit_cancel_order(symbol, active_order_id)
                    
                    await asyncio.sleep(0.1)
                    
                    audit_log(f"{exchange.upper()} ⏳ Waiting for WebSocket position update...")
                    await asyncio.sleep(0.2)
                    
                    # ✅ GET POSITION FROM WEBSOCKET (NO HTTP REFETCH!)
                    current_position_size = state[exchange].get("position_size", total_qty)
                    filled_qty = total_qty - current_position_size
                    
                    audit_log(f"{exchange.upper()} 📡 Position from WebSocket after cancel:")
                    audit_log(f"   Target: {total_qty} | Position: {current_position_size} | Filled: {filled_qty}")
                    audit_log(f"   Previous cumulative: {cumulative_filled_qty}")
                    
                    # Update ONLY if position changed
                    if filled_qty > cumulative_filled_qty:
                        new_fills = filled_qty - cumulative_filled_qty
                        fill_price = my_fresh_price  # Use the fresh BBO price
                        
                        if position_side in ["LONG", "Buy"]:
                            new_pnl = (fill_price - entry_price) * new_fills
                        else:
                            new_pnl = (entry_price - fill_price) * new_fills
                        
                        cumulative_realized_pnl += new_pnl
                        cumulative_filled_qty = filled_qty
                        
                        audit_log(f"{exchange.upper()} 💰 New fills during cancel: +{new_fills} @ ${fill_price:.8f} | PNL: ${new_pnl:+.6f}")
                    else:
                        audit_log(f"{exchange.upper()} ℹ️  No new fills (position same as before)")
                    
                    # Update tracking
                    realized_filled = cumulative_filled_qty
                    realized_pnl = cumulative_realized_pnl
                    remaining_qty = total_qty - cumulative_filled_qty
                    active_order_id = None
                    locked_price = None
                    
                    # Update shared state
                    shared_exit_state[exchange]["realized_pnl"] = realized_pnl
                    shared_exit_state[exchange]["remaining_qty"] = remaining_qty
                    shared_exit_state[exchange]["locked_price"] = None
                    
                    audit_log(f"{exchange.upper()} 📊 After cancel: Realized=${realized_pnl:+.6f}, Remaining={remaining_qty}")
                    
                    # NOW RECALCULATE: Filled side (stable) + My side (new BBO)
                    # Calculate MY new unrealized PNL with new BBO
                    if position_side in ["LONG", "Buy"]:
                        my_unrealized = (my_fresh_price - entry_price) * remaining_qty
                    else:
                        my_unrealized = (entry_price - my_fresh_price) * remaining_qty
                    
                    my_total = realized_pnl + my_unrealized
                    other_total = shared_exit_state[other_exchange]["total_pnl"]  # Stable (filled)
                    new_combined = my_total + other_total
                    
                    audit_log(f"📊 RECALCULATED PNL:")
                    audit_log(f"   {exchange.upper()} (PENDING): Realized=${realized_pnl:+.6f} + Unrealized=${my_unrealized:+.6f} = ${my_total:+.6f}")
                    audit_log(f"   {other_exchange.upper()} (FILLED): ${other_total:+.6f} (stable)")
                    audit_log(f"   NEW COMBINED: ${new_combined:+.6f}")
                    
                    # Update shared state
                    shared_exit_state[exchange]["unrealized_pnl"] = my_unrealized
                    shared_exit_state[exchange]["total_pnl"] = my_total
                    shared_exit_state[exchange]["best_price"] = my_fresh_price
                    
                    # Check if new combined PNL is positive
                    if new_combined >= 0:
                        audit_log(f"✅ NEW COMBINED PNL POSITIVE - placing new order")
                        # GO BACK TO OUTER LOOP to place new order
                        break
                    else:
                        audit_log(f"❌ NEW COMBINED PNL NEGATIVE - waiting")
                        shared_exit_state[exchange]["status"] = "waiting_pnl"
                        await asyncio.sleep(0.05)
                        continue
            
            # ====================================
            # SCENARIO 3: BOTH EXCHANGES ARE PENDING
            # ====================================
            else:
                if my_bbo_changed or other_bbo_changed:
                    # ANY BBO changed → CANCEL BOTH, recalculate sum
                    if my_bbo_changed:
                        audit_log(f"🔄 {exchange.upper()} BBO CHANGED: ${locked_price:.8f} → ${my_fresh_price:.8f}")
                    if other_bbo_changed:
                        audit_log(f"🔄 {other_exchange.upper()} BBO CHANGED: ${other_locked_price:.8f} → ${other_fresh_price:.8f}")
                    
                    audit_log(f"❌❌ BOTH PENDING - CANCELING BOTH EXCHANGES! ❌❌")
                    
                    # Signal to other exchange to cancel too
                    shared_exit_state["cancel_trigger"] = True
                    shared_exit_state[exchange]["status"] = "canceling"
                    
                    # Cancel MY order
                    if exchange == "binance":
                        binance_cancel_order(symbol, active_order_id)
                    else:
                        bybit_cancel_order(symbol, active_order_id)
                    
                    await asyncio.sleep(0.1)

                     # 🆕 UPDATE TRACE STATUS
                    state[exchange]["trace_order_status"] = "None"
                    
                    audit_log(f"{exchange.upper()} ⏳ Waiting for WebSocket position update...")
                    await asyncio.sleep(0.2)
                    
                    # ✅ GET POSITION FROM WEBSOCKET (NO HTTP REFETCH!)
                    current_position_size = state[exchange].get("position_size", total_qty)
                    filled_qty = total_qty - current_position_size
                    
                    audit_log(f"{exchange.upper()} 📡 Position from WebSocket (BOTH CANCEL scenario):")
                    audit_log(f"   Target: {total_qty} coins")
                    audit_log(f"   Current position: {current_position_size} coins")
                    audit_log(f"   Total filled: {filled_qty} coins")
                    audit_log(f"   Previous cumulative: {cumulative_filled_qty} coins")
                    
                    # Update ONLY if fills increased
                    if filled_qty > cumulative_filled_qty:
                        new_fills = filled_qty - cumulative_filled_qty
                        
                        # Use current BBO for price
                        if side.upper() in ["SELL", "Sell"]:
                            fill_price = state[exchange]["ask"]
                        else:
                            fill_price = state[exchange]["bid"]
                        
                        audit_log(f"{exchange.upper()} 💰 Detected {new_fills} new fills @ ${fill_price:.8f}")
                        
                        # Calculate PNL
                        if position_side in ["LONG", "Buy"]:
                            new_pnl = (fill_price - entry_price) * new_fills
                        else:
                            new_pnl = (entry_price - fill_price) * new_fills
                        
                        cumulative_realized_pnl += new_pnl
                        cumulative_filled_qty = filled_qty
                        
                        audit_log(f"{exchange.upper()} 📊 Added ${new_pnl:+.6f} to cumulative PNL")
                    else:
                        audit_log(f"{exchange.upper()} ℹ️  No new fills detected")
                    
                    # Update tracking
                    realized_filled = cumulative_filled_qty
                    realized_pnl = cumulative_realized_pnl
                    remaining_qty = total_qty - cumulative_filled_qty
                    active_order_id = None
                    locked_price = None
                    
                    # Update shared state
                    shared_exit_state[exchange]["realized_pnl"] = realized_pnl
                    shared_exit_state[exchange]["remaining_qty"] = remaining_qty
                    shared_exit_state[exchange]["locked_price"] = None
                    
                    audit_log(f"{exchange.upper()} 📊 After cancel summary:")
                    audit_log(f"   Realized PNL: ${realized_pnl:+.6f}")
                    audit_log(f"   Remaining: {remaining_qty} coins")
                    
                    # Wait for other exchange to also cancel
                    wait_count = 0
                    while shared_exit_state[other_exchange]["status"] not in ["calculating", "waiting_pnl", "done"] and wait_count < 50:
                        await asyncio.sleep(0.05)
                        wait_count += 1
                    
                    # Reset cancel trigger
                    shared_exit_state["cancel_trigger"] = False
                    
                    # GO BACK TO OUTER LOOP (STEP 1) - BOTH EXCHANGES RESTART!
                    audit_log(f"{exchange.upper()} 🔄 RESTARTING FROM STEP 1...")
                    break
            
            # ====================================
            # Check if OTHER exchange triggered cancel (for SCENARIO 1 & 3)
            # ====================================
            if shared_exit_state.get("cancel_trigger") and shared_exit_state[exchange]["status"] != "canceling":
                # Only respond if I'm PENDING (not if I'm already filled)
                if not my_is_filled:
                    audit_log(f"{exchange.upper()} ⚠️ OTHER EXCHANGE TRIGGERED CANCEL - Canceling my order too!")
                    
                    shared_exit_state[exchange]["status"] = "canceling"
                    
                    # Cancel MY order
                    if exchange == "binance":
                        binance_cancel_order(symbol, active_order_id)
                    else:
                        bybit_cancel_order(symbol, active_order_id)
                    
                    await asyncio.sleep(0.1)
                    
                    # Refetch fills with cumulative tracking
                    if exchange == "binance":
                        res = binance_get_order(symbol, active_order_id)
                        if res and "orderId" in res:
                            this_order_filled = float(res.get("executedQty", 0))
                            this_order_value = float(res.get("cumQuote", 0))
                    else:
                        res = bybit_get_order(symbol, active_order_id)
                        result_list = res.get("result", {}).get("list", [])
                        if result_list:
                            this_order_filled = float(result_list[0].get("cumExecQty", 0))
                            this_order_value = float(result_list[0].get("cumExecValue", 0))
                    
                    # Only count NEW fills
                    new_fills = this_order_filled - (cumulative_filled_qty - realized_filled)
                    new_value = this_order_value - (cumulative_filled_value - realized_value)
                    
                    if new_fills > 0:
                        avg_price = new_value / new_fills
                        if position_side in ["LONG", "Buy"]:
                            new_pnl = (avg_price - entry_price) * new_fills
                        else:
                            new_pnl = (entry_price - avg_price) * new_fills
                        
                        cumulative_realized_pnl += new_pnl
                        cumulative_filled_qty += new_fills
                        cumulative_filled_value += new_value
                    
                    # Update tracking
                    realized_filled = cumulative_filled_qty
                    realized_value = cumulative_filled_value
                    realized_pnl = cumulative_realized_pnl
                    remaining_qty = total_qty - cumulative_filled_qty
                    active_order_id = None
                    locked_price = None
                    
                    # Update shared state
                    shared_exit_state[exchange]["realized_pnl"] = realized_pnl
                    shared_exit_state[exchange]["remaining_qty"] = remaining_qty
                    shared_exit_state[exchange]["locked_price"] = None
                    
                    # Wait for sync, then restart
                    wait_count = 0
                    while shared_exit_state[other_exchange]["status"] not in ["calculating", "waiting_pnl", "done"] and wait_count < 50:
                        await asyncio.sleep(0.05)
                        wait_count += 1
                    
                    shared_exit_state["cancel_trigger"] = False
                    audit_log(f"{exchange.upper()} 🔄 RESTARTING FROM STEP 1...")
                    break
    
    audit_log(f"{exchange.upper()} 🏁 EXIT COMPLETE: Realized PNL = ${realized_pnl:.6f}")

    # 🆕 CRITICAL FIX: Update shared state with FINAL values before exiting function
    shared_exit_state[exchange]["realized_pnl"] = cumulative_realized_pnl
    shared_exit_state[exchange]["unrealized_pnl"] = 0.0
    shared_exit_state[exchange]["total_pnl"] = cumulative_realized_pnl
    shared_exit_state[exchange]["remaining_qty"] = 0
    shared_exit_state[exchange]["status"] = "done"
    shared_exit_state[exchange]["done"] = True

    return realized_filled, realized_value



async def dust_cleanup_chaser(exchange, symbol, dust_qty, original_side, leverage, state, filters, trade_records):
    """
    Clean up dust positions by:
    1. Adding minimum lot size coins (using FOK LIMIT)
    2. Closing the combined position (using FOK LIMIT)
    Both orders chase aggressively like entry/exit chasers
    """
    step_size = filters["step_size"]
    tick_size = filters["tick_size"]
    
    audit_log(f"{'='*80}")
    audit_log(f"🧹 DUST CLEANUP START: {exchange.upper()}")
    audit_log(f"   Dust Remaining: {dust_qty} coins")
    audit_log(f"   Original Side: {original_side}")
    audit_log(f"{'='*80}")
    
    # Step 1: Fetch minimum lot size from filters
    min_lot_size = float(filters["min_qty"])
    audit_log(f"📏 Minimum Lot Size: {min_lot_size} coins")
    
    # Step 2: Coins to ADD = full min lot size (NOT difference!)
    coins_to_add = min_lot_size
    audit_log(f"🎯 Will ADD: {coins_to_add} coins (full minimum)")
    
    # Step 3: Determine ADD side (same as original entry)
    if original_side in ["LONG", "Buy"]:
        add_side = "BUY" if exchange == "binance" else "Buy"
    else:
        add_side = "SELL" if exchange == "binance" else "Sell"
    
    audit_log(f"📊 ADD Side: {add_side}")
    
    # Step 4: Place FOK LIMIT order to ADD coins (aggressive chasing)
    added_qty = 0
    add_order_id = None
    iteration = 0
    
    while added_qty < coins_to_add:
        iteration += 1
        
        # Get current best price
        if add_side in ["BUY", "Buy"]:
            target_price = state[exchange]["bid"]
        else:
            target_price = state[exchange]["ask"]
        
        if target_price <= 0:
            await asyncio.sleep(0.05)
            continue
        
        # Check if we have an active order
        if add_order_id:
            # Fetch order status
            status = None
            if exchange == "binance":
                res = binance_get_order(symbol, add_order_id)
                if res and "orderId" in res:
                    status = {
                        "filled": float(res.get("executedQty", 0)),
                        "price": float(res.get("price", 0)),
                        "status": res.get("status", "UNKNOWN")
                    }
            else:  # bybit
                res = bybit_get_order(symbol, add_order_id)
                result_list = res.get("result", {}).get("list", [])
                if result_list:
                    o = result_list[0]
                    status = {
                        "filled": float(o.get("cumExecQty", 0)),
                        "price": float(o.get("price", 0)),
                        "status": o.get("orderStatus", "Unknown")
                    }
            
            if not status:
                await asyncio.sleep(0.02)
                continue
            
            added_qty = status["filled"]
            
            # Check if filled
            if status["status"] in ["FILLED", "Filled"]:
                audit_log(f"{exchange.upper()} ✅ ADD order FILLED: {added_qty} coins @ ${status['price']:.8f}")
                break
            
            # Check if rejected/cancelled
            terminal_states = {
                "binance": ["REJECTED", "EXPIRED", "CANCELED", "EXPIRED_IN_MATCH"],
                "bybit": ["Rejected", "Cancelled", "Deactivated"]
            }
            
            if status["status"] in terminal_states.get(exchange, []):
                audit_log(f"{exchange.upper()} ⚠️ ADD order {status['status']}. Retrying...")
                add_order_id = None
                await asyncio.sleep(0.1)
                continue
            
            # Check if price moved (cancel and replace)
            if abs(status["price"] - target_price) >= float(tick_size):
                audit_log(f"{exchange.upper()} 🔄 Price moved! Canceling ADD order...")
                
                if exchange == "binance":
                    binance_cancel_order(symbol, add_order_id)
                else:
                    bybit_cancel_order(symbol, add_order_id)
                
                await asyncio.sleep(0.1)
                add_order_id = None
                continue
            
             # 🆕 DRAIN QUEUE - Always use LATEST BBO
            drained = drain_bbo_queue(exchange)
            if drained > 0:
                # BBO changed! Loop will recheck immediately
                if drained > 10:
                    audit_log(f"{exchange.upper()} ⚡ Backlog: skipped {drained} old BBO updates")
            else:
                # No new BBO, wait briefly
                await asyncio.sleep(0.02)

            continue
        
        # Place new ADD order
        qty_to_order = coins_to_add - added_qty
        
        audit_log(f"{exchange.upper()} 🆕 Placing ADD: {add_side} {qty_to_order} @ ${target_price:.8f} (FOK)")
        
        res = {}
        if exchange == "binance":
            # Use FOK inline
            qty_to_order = round_step_size(qty_to_order, step_size)
            target_price = round_price(target_price, tick_size)
            params = binance_sign({
                "symbol": symbol,
                "side": add_side,
                "type": "LIMIT",
                "quantity": qty_to_order,
                "price": target_price,
                "timeInForce": "FOK",
                "timestamp": int(get_accurate_time()*1000)
            })
            r = requests.post(BINANCE_URL + "/fapi/v1/order", headers={"X-MBX-APIKEY": BINANCE_KEY}, params=params, timeout=5)
            res = r.json()
            add_order_id = res.get("orderId")
        else:
            # Use FOK inline
            qty_to_order = round_step_size(qty_to_order, step_size)
            target_price = round_price(target_price, tick_size)
            timestamp = str(int(get_accurate_time() * 1000))
            params = {
                "category": "linear",
                "symbol": symbol,
                "side": add_side,
                "orderType": "Limit",
                "qty": str(qty_to_order),
                "price": str(target_price),
                "timeInForce": "FOK"
            }
            signature = bybit_sign({}, timestamp, json.dumps(params))
            headers = {"X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-SIGN": signature, "X-BAPI-RECV-WINDOW": "5000", "Content-Type": "application/json"}
            r = requests.post(BYBIT_URL + "/v5/order/create", headers=headers, json=params, timeout=5)
            res = r.json()
            add_order_id = res.get("result", {}).get("orderId")
        
        if add_order_id:
            trade_records[exchange]["dust_order_ids"].append(add_order_id)
            audit_log(f"{exchange.upper()} ✅ ADD order placed: {add_order_id}")
        else:
            audit_log(f"{exchange.upper()} ⚠️ ADD order failed: {json.dumps(res)}")
            await asyncio.sleep(0.2)
            continue
        
        await asyncio.sleep(0.02)
    
    audit_log(f"{exchange.upper()} 🎯 ADD COMPLETE: {added_qty} coins added")
    
    # Step 5: Calculate total to CLOSE
    total_to_close = dust_qty + added_qty
    audit_log(f"📊 Total to CLOSE: {dust_qty} (dust) + {added_qty} (added) = {total_to_close} coins")
    
    # Step 6: Determine CLOSE side (opposite of entry)
    if original_side in ["LONG", "Buy"]:
        close_side = "SELL" if exchange == "binance" else "Sell"
    else:
        close_side = "BUY" if exchange == "binance" else "Buy"
    
    audit_log(f"📊 CLOSE Side: {close_side}")
    
    # Step 7: Place FOK LIMIT order to CLOSE (aggressive chasing)
    closed_qty = 0
    close_order_id = None
    iteration = 0
    
    while closed_qty < total_to_close:
        iteration += 1
        
        # Get current best price
        if close_side in ["BUY", "Buy"]:
            target_price = state[exchange]["bid"]
        else:
            target_price = state[exchange]["ask"]
        
        if target_price <= 0:
            await asyncio.sleep(0.05)
            continue
        
        # Check if we have an active order
        if close_order_id:
            # Fetch order status
            status = None
            if exchange == "binance":
                res = binance_get_order(symbol, close_order_id)
                if res and "orderId" in res:
                    status = {
                        "filled": float(res.get("executedQty", 0)),
                        "price": float(res.get("price", 0)),
                        "status": res.get("status", "UNKNOWN")
                    }
            else:  # bybit
                res = bybit_get_order(symbol, close_order_id)
                result_list = res.get("result", {}).get("list", [])
                if result_list:
                    o = result_list[0]
                    status = {
                        "filled": float(o.get("cumExecQty", 0)),
                        "price": float(o.get("price", 0)),
                        "status": o.get("orderStatus", "Unknown")
                    }
            
            if not status:
                await asyncio.sleep(0.02)
                continue
            
            closed_qty = status["filled"]
            
            # Check if filled
            if status["status"] in ["FILLED", "Filled"]:
                audit_log(f"{exchange.upper()} ✅ CLOSE order FILLED: {closed_qty} coins @ ${status['price']:.8f}")
                break
            
            # Check if rejected/cancelled
            terminal_states = {
                "binance": ["REJECTED", "EXPIRED", "CANCELED", "EXPIRED_IN_MATCH"],
                "bybit": ["Rejected", "Cancelled", "Deactivated"]
            }
            
            if status["status"] in terminal_states.get(exchange, []):
                audit_log(f"{exchange.upper()} ⚠️ CLOSE order {status['status']}. Retrying...")
                close_order_id = None
                await asyncio.sleep(0.1)
                continue
            
            # Check if price moved (cancel and replace)
            if abs(status["price"] - target_price) >= float(tick_size):
                audit_log(f"{exchange.upper()} 🔄 Price moved! Canceling CLOSE order...")
                
                if exchange == "binance":
                    binance_cancel_order(symbol, close_order_id)
                else:
                    bybit_cancel_order(symbol, close_order_id)
                
                await asyncio.sleep(0.1)
                close_order_id = None
                continue
            
            await asyncio.sleep(0.02)
            continue
        
        # Place new CLOSE order
        qty_to_order = total_to_close - closed_qty
        
        audit_log(f"{exchange.upper()} 🆕 Placing CLOSE: {close_side} {qty_to_order} @ ${target_price:.8f} (FOK)")
        
        res = {}
        if exchange == "binance":
            # Use FOK inline
            qty_to_order = round_step_size(qty_to_order, step_size)
            target_price = round_price(target_price, tick_size)
            params = binance_sign({
                "symbol": symbol,
                "side": close_side,
                "type": "LIMIT",
                "quantity": qty_to_order,
                "price": target_price,
                "timeInForce": "FOK",
                "timestamp": int(get_accurate_time()*1000)
            })
            r = requests.post(BINANCE_URL + "/fapi/v1/order", headers={"X-MBX-APIKEY": BINANCE_KEY}, params=params, timeout=5)
            res = r.json()
            close_order_id = res.get("orderId")

        else:
            # Use FOK inline
            qty_to_order = round_step_size(qty_to_order, step_size)
            target_price = round_price(target_price, tick_size)
            timestamp = str(int(get_accurate_time() * 1000))
            params = {
                "category": "linear",
                "symbol": symbol,
                "side": close_side,
                "orderType": "Limit",
                "qty": str(qty_to_order),
                "price": str(target_price),
                "timeInForce": "FOK"
            }
            signature = bybit_sign({}, timestamp, json.dumps(params))
            headers = {"X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-SIGN": signature, "X-BAPI-RECV-WINDOW": "5000", "Content-Type": "application/json"}
            r = requests.post(BYBIT_URL + "/v5/order/create", headers=headers, json=params, timeout=5)
            res = r.json()
            close_order_id = res.get("result", {}).get("orderId")
        
        if close_order_id:
            trade_records[exchange]["dust_order_ids"].append(close_order_id)
            audit_log(f"{exchange.upper()} ✅ CLOSE order placed: {close_order_id}")
        else:
            audit_log(f"{exchange.upper()} ⚠️ CLOSE order failed: {json.dumps(res)}")
            await asyncio.sleep(0.2)
            continue
        
        await asyncio.sleep(0.02)
    
    audit_log(f"{exchange.upper()} 🎯 DUST CLEANUP COMPLETE!")
    audit_log(f"{'='*80}")
    audit_log(f"✅ {exchange.upper()} CLEARED")
    audit_log(f"{'='*80}")

    # =========================
    # BYBIT PNL FETCHER (NEW)
    # =========================
async def api_pnl_tracker(state, coin, exchanges, pos_info):
    await asyncio.sleep(1)
    audit_log("💰 Starting WebSocket-based PNL tracker...")
    
    while state.get("running", True):
        try:
            net_pnl = 0.0

            for exchange in exchanges:
                if exchange not in ["binance", "bybit"]:
                    continue

                pos_size = state[exchange].get("position_size", 0)
                entry_price = state[exchange].get("position_entry", 0)
                pos_side = state[exchange].get("position_side", "FLAT")

                # Fallback to REST position info if WebSocket hasn't updated yet
                if (pos_size <= 0 or entry_price <= 0 or pos_side == "FLAT") and pos_info.get(exchange):
                    pos_size = pos_info[exchange].get("size", 0)
                    entry_price = pos_info[exchange].get("entry_price", 0)
                    pos_side = pos_info[exchange].get("side", "FLAT")

                if pos_size <= 0 or entry_price <= 0 or pos_side == "FLAT":
                    state[exchange]["pnl"] = None
                    continue

                if pos_side == "LONG":
                    exit_price = state[exchange]["ask"]
                    pnl = (exit_price - entry_price) * pos_size
                else:  # SHORT
                    exit_price = state[exchange]["bid"]
                    pnl = (entry_price - exit_price) * pos_size

                state[exchange]["pnl"] = pnl
                net_pnl += pnl

            state["net_pnl"] = net_pnl
            await asyncio.sleep(0.1)

        except Exception as e:
            await asyncio.sleep(1)

def calculate_exit_pnl_projection(position_info, state):
    """Calculate projected PNL if we close ALL remaining positions at current BBO"""
    net_projection = 0.0
    
    # Binance
    if position_info.get("binance") and position_info["binance"].get("entry_price"):
        entry = position_info["binance"]["entry_price"]
        size = position_info["binance"]["size"]
        side = position_info["binance"]["side"]
        
        # Use ASK for LONG exit (sell at ask), BID for SHORT exit (buy at bid)
        exit_price = state["binance"]["ask"] if side == "LONG" else state["binance"]["bid"]
        
        if exit_price <= 0: return None  # Data not ready
        
        if side == "LONG":
            projected_pnl = (exit_price - entry) * size
        else:
            projected_pnl = (entry - exit_price) * size
        
        net_projection += projected_pnl
    
    # Bybit
    if position_info.get("bybit") and position_info["bybit"].get("entry_price"):
        entry = position_info["bybit"]["entry_price"]
        size = position_info["bybit"]["size"]
        side = position_info["bybit"]["side"]
        
        # Use ASK for Buy/LONG exit (sell at ask), BID for Sell/SHORT exit (buy at bid)
        exit_price = state["bybit"]["ask"] if side == "Buy" else state["bybit"]["bid"]
        
        if exit_price <= 0: return None  # Data not ready
        
        if side == "Buy":
            projected_pnl = (exit_price - entry) * size
        else:
            projected_pnl = (entry - exit_price) * size
        
        net_projection += projected_pnl
    
    return net_projection

def time_left_precise(ts):
    """Return seconds until funding (for debugging)"""
    if not ts:
        return "N/A"
    diff = int(ts / 1000 - get_accurate_time())  # Use NTP-synced time
    return f"{diff}s"
    
# =========================
# PRINTER (UPDATED)
# =========================
async def printer(state, exchanges, shared_exit_state=None):
    while state.get("running", True):
        # Check if terminal is paused
        if terminal_paused:
            await asyncio.sleep(0.5)
            continue

        print("\n" + "=" * 90)

        # 1. Exchange Data
        for ex in exchanges:
            price_str = f"{state[ex]['price']:.8f}" if state[ex]['price'] else "N/A"
            funding_str = f"{state[ex]['funding']:.6f}%" if state[ex]['funding'] is not None else "N/A"
            time_str = time_left(state[ex]['next_ts'])
            
            pnl = state[ex].get("pnl", None)
            if pnl is not None:
                pnl_str = f"${pnl:+.3f}"
                color = "\033[92m" if pnl >= 0 else "\033[91m"
                reset = "\033[0m"
                pnl_display = f"{color}{pnl_str}{reset}"
            else:
                pnl_display = "Waiting..."
            
            print(f"{ex.upper():7} | Price: {price_str} | B/A: {state[ex]['bid']:.8f}/{state[ex]['ask']:.8f} | "
                  f"Funding: {funding_str} | Time: {time_str} | PNL: {pnl_display}")
            
        # 2. Strategy & Signal Status
        print("-" * 90)
        if not state.get("trade_fired"):
            sig = state.get("signal")
            if sig:
                print(f"📡 SIGNAL ACTIVE: LONG {sig['long'].upper()} / SHORT {sig['short'].upper()}")
                print(f"   Spread: {sig['spread']:.6f}% | Target Req: {state.get('min_spread')}%")
            else:
                print(f"📡 SEARCHING: Scanning for arbitrage spread > {state.get('min_spread')}%...")
            
            print(f"🕒 TRIGGERS AT: {state.get('execution_time')} before funding")
        else:
            print(f"🚀 POSITION ACTIVE | Strategy: {state.get('active_strategy', 'Hedged')}")
            exit_info = state.get('exit_time') if state.get('exit_mode') == "2" else "Funding Reset (00:00:00)"
            print(f"🕒 EXIT TARGET: {exit_info}")

        # 3. Footer & Net PNL
        adjust_info = f" (Adj: {MANUAL_ADJUST:+d}s)" if MANUAL_ADJUST != 0 else ""
        if adjust_info:
             print(f"Note: Manual Time Adjustment Active{adjust_info}")
        
        net_pnl = state.get("net_pnl", None)
        if net_pnl is not None:
            net_color = "\033[92m\033[1m" if net_pnl >= 0 else "\033[91m\033[1m"
            reset = "\033[0m"
            print("=" * 90)
            print(f"💰 NET PNL: {net_color}${net_pnl:+.3f}{reset}")
            
            # 🆕 INTEGRATED PROJECTION (Cleaner terminal)
            proj_pnl = state.get("exit_pnl_projection")
            if proj_pnl is not None:
                proj_color = "\033[92m" if proj_pnl >= 0 else "\033[91m"
                print(f"⏳ Waiting for positive PNL... Current: {proj_color}${proj_pnl:+.3f}{reset}")

            # 🆕 SHOW EXIT MODE PNL (access from state)
            shared_exit_state = state.get("shared_exit_state")
            if state.get("exit_triggered") and shared_exit_state:
                b_pnl = shared_exit_state.get("binance", {}).get("total_pnl", 0)
                y_pnl = shared_exit_state.get("bybit", {}).get("total_pnl", 0)
                combined = b_pnl + y_pnl
                color = "\033[92m" if combined >= 0 else "\033[91m"
                print(f"🚪 EXIT MODE: Binance ${b_pnl:+.6f} + Bybit ${y_pnl:+.6f} = {color}${combined:+.6f}\033[0m")
        
        await asyncio.sleep(0.2)

# =========================
# TIME CHECKER
# =========================
def should_execute(ts, target_time_str):
    """Check if current time matches target execution time"""
    if not ts:
        return False
    
    # Calculate seconds until funding
    ts_seconds = ts / 1000
    current_time = get_accurate_time()  # Use NTP-synced time
    seconds_until_funding = int(ts_seconds - current_time)
    
    if seconds_until_funding <= 0:
        print(f"⚠️ Funding time already passed! (diff: {seconds_until_funding}s)")
        return False  # Funding already passed
    
    # Parse target time string (HH:MM:SS)
    try:
        target_h, target_m, target_s = map(int, target_time_str.split(':'))
        target_seconds = target_h * 3600 + target_m * 60 + target_s
        
        # ✅ FIX: Trigger EXACTLY at target time (±1 second tolerance)
        return abs(target_seconds - seconds_until_funding) <= 1
        
    except Exception as e:
        print(f"❌ Error parsing target time '{target_time_str}': {e}")
        return False
async def fetch_binance_trades_by_orders(session, symbol, order_ids):
    """Fetch all fills for given Binance order IDs"""
    try:
        if not order_ids: return []
        params = binance_sign({
            "symbol": symbol,
            "limit": 100,
            "timestamp": int(get_accurate_time() * 1000)
        })
        async with session.get(BINANCE_URL + "/fapi/v1/userTrades", params=params, timeout=5) as r:
            trades = await r.json()
            if isinstance(trades, list):
                return [t for t in trades if str(t.get("orderId")) in [str(o) for o in order_ids]]
        return []
    except Exception as e:
        audit_log(f"Binance trade fetch error: {e}")
        return []

async def fetch_bybit_trades_by_orders(session, symbol, order_ids):
    """Fetch all fills for given Bybit order IDs"""
    try:
        if not order_ids: return []
        timestamp = str(int(get_accurate_time() * 1000))
        params = {"category": "linear", "symbol": symbol, "limit": "100"}
        signature = bybit_sign(params, timestamp, "")
        queryString = urlencode(sorted(params.items()))
        url = f"{BYBIT_URL}/v5/execution/list?{queryString}"
        headers = {"X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-SIGN": signature, "X-BAPI-RECV-WINDOW": "5000"}
        async with session.get(url, headers=headers, timeout=5) as r:
            data = await r.json()
            if data.get("retCode") == 0:
                trades = data.get("result", {}).get("list", [])
                return [t for t in trades if str(t.get("orderId")) in [str(o) for o in order_ids]]
        return []
    except Exception as e:
        audit_log(f"Bybit trade fetch error: {e}")
        return []

async def generate_detailed_order_breakdown(trade_records, coin):
    """Generate detailed order-by-order breakdown for entry, exit, and dust phases"""
    audit_log("\n" + "="*80)
    audit_log(" " * 20 + "DETAILED ORDER BREAKDOWN")
    audit_log("="*80)
    
    # Wait for exchange sync
    await asyncio.sleep(3)
    
    async with aiohttp.ClientSession(headers={"X-MBX-APIKEY": BINANCE_KEY}) as session:
        for ex in ["binance", "bybit"]:
            symbol = coin + "USDT"
            
            # Retry mechanism for fetching trades
            for attempt in range(100):
                if ex == "binance":
                    entry_trades = await fetch_binance_trades_by_orders(session, symbol, trade_records[ex]["entry_order_ids"])
                    exit_trades = await fetch_binance_trades_by_orders(session, symbol, trade_records[ex]["exit_order_ids"])
                    dust_trades = await fetch_binance_trades_by_orders(session, symbol, trade_records[ex]["dust_order_ids"])
                else:
                    entry_trades = await fetch_bybit_trades_by_orders(session, symbol, trade_records[ex]["entry_order_ids"])
                    exit_trades = await fetch_bybit_trades_by_orders(session, symbol, trade_records[ex]["exit_order_ids"])
                    dust_trades = await fetch_bybit_trades_by_orders(session, symbol, trade_records[ex]["dust_order_ids"])
                
                # Retry if missing expected data
                if (trade_records[ex]["entry_order_ids"] and not entry_trades) or \
                   (trade_records[ex]["exit_order_ids"] and not exit_trades) or \
                   (trade_records[ex]["dust_order_ids"] and not dust_trades):
                    await asyncio.sleep(2)
                    continue
                break
            
            side = trade_records[ex]['side']
            
            # Print exchange header
            audit_log(f"\n[{ex.upper()}] Side: {side}")
            audit_log(f"")
            
            # ========================================
            # ENTRY PHASE - ORDER BY ORDER
            # ========================================
            audit_log(f"  {'━' * 76}")
            audit_log(f"  📊 ENTRY PHASE - LIMIT ORDER BREAKDOWN")
            audit_log(f"  {'━' * 76}")
            audit_log(f"")
            
            if entry_trades:
                # Group trades by order ID
                from collections import defaultdict
                entry_by_order = defaultdict(list)
                for t in entry_trades:
                    order_id = str(t.get("orderId"))
                    entry_by_order[order_id].append(t)
                
                running_qty = 0
                running_value = 0
                total_entry_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in entry_trades)
                entry_maker = 0
                entry_taker = 0
                
                order_num = 1
                for order_id in trade_records[ex]["entry_order_ids"]:
                    order_trades = entry_by_order[str(order_id)]
                    if not order_trades:
                        continue
                    
                    # Sum up fills for this order
                    order_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in order_trades)
                    order_value = sum(float(t.get("quoteQty") or (float(t.get("execQty", 0)) * float(t.get("execPrice", 0)))) for t in order_trades)
                    order_avg_price = order_value / order_qty if order_qty > 0 else 0
                    
                    # Get timestamp of first fill
                    if ex == "binance":
                        timestamp_ms = int(order_trades[0].get("time", 0))
                        is_maker = order_trades[0].get("maker", False)
                    else:
                        timestamp_ms = int(order_trades[0].get("execTime", 0))
                        is_maker = order_trades[0].get("isMaker") == "true"
                    
                    timestamp_str = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S.%f')[:-3]
                    
                    # Update running totals
                    running_qty += order_qty
                    running_value += order_value
                    
                    # Count maker/taker
                    if ex == "binance":
                        order_maker = sum(1 for t in order_trades if t.get("maker"))
                    else:
                        order_maker = sum(1 for t in order_trades if t.get("isMaker") == "true")
                    order_taker = len(order_trades) - order_maker
                    
                    entry_maker += order_maker
                    entry_taker += order_taker
                    
                    # Determine side string
                    if ex == "binance":
                        side_str = order_trades[0].get("side", "")
                    else:
                        side_str = order_trades[0].get("side", "")
                    
                    # Print order details
                    complete_marker = "✅ COMPLETE" if running_qty >= total_entry_qty else ""
                    audit_log(f"  Order #{order_num}: {side_str} {order_qty} coins @ ${order_avg_price:.8f} - FILLED at {timestamp_str}")
                    audit_log(f"            Balance: {running_qty:.0f}/{total_entry_qty:.0f} coins | ${running_value:.2f} USDT {complete_marker}")
                    audit_log(f"")
                    
                    order_num += 1
                
                # Entry summary
                entry_fees = sum(float(t.get("commission") or t.get("execFee", 0)) for t in entry_trades)
                entry_avg = running_value / running_qty if running_qty > 0 else 0
                entry_maker_pct = (entry_maker / (entry_maker + entry_taker) * 100) if (entry_maker + entry_taker) > 0 else 0
                
                audit_log(f"  Total Entry: {running_qty} coins @ ${entry_avg:.8f} avg | Total: ${running_value:.8f}")
                audit_log(f"  Entry Fees: ${entry_fees:.8f} USDT | {entry_maker}M/{entry_taker}T ({entry_maker_pct:.1f}% Maker)")
            else:
                audit_log(f"  No entry trades found")
            
            # ========================================
            # EXIT PHASE - ORDER BY ORDER
            # ========================================
            audit_log(f"")
            audit_log(f"  {'━' * 76}")
            audit_log(f"  📊 EXIT PHASE - LIMIT ORDER BREAKDOWN")
            audit_log(f"  {'━' * 76}")
            audit_log(f"")
            
            if exit_trades:
                # Get entry price for PNL calculation
                entry_price = 0
                if entry_trades:
                    total_entry_val = sum(float(t.get("quoteQty") or (float(t.get("execQty", 0)) * float(t.get("execPrice", 0)))) for t in entry_trades)
                    total_entry_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in entry_trades)
                    entry_price = total_entry_val / total_entry_qty if total_entry_qty > 0 else 0
                
                # Group exit trades by order ID
                exit_by_order = defaultdict(list)
                for t in exit_trades:
                    order_id = str(t.get("orderId"))
                    exit_by_order[order_id].append(t)
                
                running_exit_qty = 0
                running_exit_value = 0
                cumulative_pnl = 0
                total_exit_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in exit_trades)
                exit_maker = 0
                exit_taker = 0
                
                order_num = 1
                for order_id in trade_records[ex]["exit_order_ids"]:
                    order_trades = exit_by_order[str(order_id)]
                    if not order_trades:
                        continue
                    
                    # Sum up fills for this order
                    order_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in order_trades)
                    order_value = sum(float(t.get("quoteQty") or (float(t.get("execQty", 0)) * float(t.get("execPrice", 0)))) for t in order_trades)
                    order_avg_price = order_value / order_qty if order_qty > 0 else 0
                    
                    # Get timestamp
                    if ex == "binance":
                        timestamp_ms = int(order_trades[0].get("time", 0))
                    else:
                        timestamp_ms = int(order_trades[0].get("execTime", 0))
                    
                    timestamp_str = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S.%f')[:-3]
                    
                    # Calculate PNL for this order
                    if side in ["LONG", "Buy"]:
                        order_pnl = (order_avg_price - entry_price) * order_qty
                    else:
                        order_pnl = (entry_price - order_avg_price) * order_qty
                    
                    cumulative_pnl += order_pnl
                    
                    # Update running totals
                    running_exit_qty += order_qty
                    running_exit_value += order_value
                    remaining_qty = total_entry_qty - running_exit_qty
                    
                    # Count maker/taker
                    if ex == "binance":
                        order_maker = sum(1 for t in order_trades if t.get("maker"))
                    else:
                        order_maker = sum(1 for t in order_trades if t.get("isMaker") == "true")
                    order_taker = len(order_trades) - order_maker
                    
                    exit_maker += order_maker
                    exit_taker += order_taker
                    
                    # Determine side string
                    if ex == "binance":
                        side_str = order_trades[0].get("side", "")
                    else:
                        side_str = order_trades[0].get("side", "")
                    
                    # Print order details
                    complete_marker = "✅ ALMOST COMPLETE" if remaining_qty <= 10 and remaining_qty > 0 else ""
                    audit_log(f"  Order #{order_num}: {side_str} {order_qty} coins @ ${order_avg_price:.8f} - FILLED at {timestamp_str}")
                    audit_log(f"            Remaining: {remaining_qty:.0f}/{total_entry_qty:.0f} coins | PNL: ${cumulative_pnl:+.2f} {complete_marker}")
                    audit_log(f"")
                    
                    order_num += 1
                
                # Exit summary
                exit_fees = sum(float(t.get("commission") or t.get("execFee", 0)) for t in exit_trades)
                exit_avg = running_exit_value / running_exit_qty if running_exit_qty > 0 else 0
                exit_maker_pct = (exit_maker / (exit_maker + exit_taker) * 100) if (exit_maker + exit_taker) > 0 else 0
                
                audit_log(f"  Total Exit: {running_exit_qty} coins @ ${exit_avg:.8f} avg | Total: ${running_exit_value:.8f}")
                audit_log(f"  Exit Fees: ${exit_fees:.8f} USDT | {exit_maker}M/{exit_taker}T ({exit_maker_pct:.1f}% Maker)")
                
                # Show dust warning if any
                dust_remaining = total_entry_qty - running_exit_qty
                if dust_remaining > 0:
                    audit_log(f"")
                    audit_log(f"  ⚠️  Dust Remaining: {dust_remaining} coins")
            else:
                audit_log(f"  No exit trades found")
            
            # ========================================
            # DUST CLEANUP - ORDER BY ORDER
            # ========================================
            if dust_trades:
                audit_log(f"")
                audit_log(f"  {'━' * 76}")
                audit_log(f"  🧹 DUST CLEANUP PHASE")
                audit_log(f"  {'━' * 76}")
                audit_log(f"")
                
                # Separate ADD and CLOSE trades
                dust_add_trades = []
                dust_close_trades = []
                
                for t in dust_trades:
                    trade_side = t.get("side") if ex == "binance" else t.get("side")
                    if ex == "binance":
                        if (side == "LONG" and trade_side == "BUY") or (side == "SHORT" and trade_side == "SELL"):
                            dust_add_trades.append(t)
                        else:
                            dust_close_trades.append(t)
                    else:
                        if (side == "Buy" and trade_side == "Buy") or (side == "Sell" and trade_side == "Sell"):
                            dust_add_trades.append(t)
                        else:
                            dust_close_trades.append(t)
                
                # Process ADD trades
                if dust_add_trades:
                    add_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in dust_add_trades)
                    add_value = sum(float(t.get("quoteQty") or (float(t.get("execQty", 0)) * float(t.get("execPrice", 0)))) for t in dust_add_trades)
                    add_avg = add_value / add_qty if add_qty > 0 else 0
                    
                    audit_log(f"  Minimum Lot Size Required: {add_qty} coins")
                    audit_log(f"  Dust Position: {dust_remaining} coins")
                    audit_log(f"  Coins Added: {add_qty} coins")
                    audit_log(f"")
                    audit_log(f"  Step 1 - ADD Position (FOK LIMIT):")
                    
                    # Group by order
                    add_by_order = defaultdict(list)
                    for t in dust_add_trades:
                        add_by_order[str(t.get("orderId"))].append(t)
                    
                    order_num = 1
                    for order_id, order_trades in add_by_order.items():
                        order_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in order_trades)
                        order_value = sum(float(t.get("quoteQty") or (float(t.get("execQty", 0)) * float(t.get("execPrice", 0)))) for t in order_trades)
                        order_avg = order_value / order_qty if order_qty > 0 else 0
                        
                        if ex == "binance":
                            timestamp_ms = int(order_trades[0].get("time", 0))
                            side_str = order_trades[0].get("side", "")
                        else:
                            timestamp_ms = int(order_trades[0].get("execTime", 0))
                            side_str = order_trades[0].get("side", "")
                        
                        timestamp_str = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S.%f')[:-3]
                        
                        audit_log(f"    Order #{order_num}: {side_str} {order_qty} coins @ ${order_avg:.8f} - FILLED at {timestamp_str}")
                        audit_log(f"              New Balance: {dust_remaining + order_qty} coins")
                        audit_log(f"")
                        order_num += 1
                
                # Process CLOSE trades
                if dust_close_trades:
                    close_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in dust_close_trades)
                    close_value = sum(float(t.get("quoteQty") or (float(t.get("execQty", 0)) * float(t.get("execPrice", 0)))) for t in dust_close_trades)
                    close_avg = close_value / close_qty if close_qty > 0 else 0
                    
                    audit_log(f"  Step 2 - CLOSE Combined Position (FOK LIMIT):")
                    
                    # Group by order
                    close_by_order = defaultdict(list)
                    for t in dust_close_trades:
                        close_by_order[str(t.get("orderId"))].append(t)
                    
                    order_num = 1
                    for order_id, order_trades in close_by_order.items():
                        order_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in order_trades)
                        order_value = sum(float(t.get("quoteQty") or (float(t.get("execQty", 0)) * float(t.get("execPrice", 0)))) for t in order_trades)
                        order_avg = order_value / order_qty if order_qty > 0 else 0
                        
                        if ex == "binance":
                            timestamp_ms = int(order_trades[0].get("time", 0))
                            side_str = order_trades[0].get("side", "")
                        else:
                            timestamp_ms = int(order_trades[0].get("execTime", 0))
                            side_str = order_trades[0].get("side", "")
                        
                        timestamp_str = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S.%f')[:-3]
                        
                        audit_log(f"    Order #{order_num}: {side_str} {order_qty} coins @ ${order_avg:.8f} - FILLED at {timestamp_str}")
                        audit_log(f"              Final Balance: 0 coins ✅ CLEARED")
                        audit_log(f"")
                        order_num += 1
                
                # Dust summary
                dust_fees = sum(float(t.get("commission") or t.get("execFee", 0)) for t in dust_trades)
                audit_log(f"  Dust Cleanup Fees: ${dust_fees:.8f} USDT")


async def generate_final_report(trade_records, coin):
    """Generate and display final trade report with retries for late data"""

    await generate_detailed_order_breakdown(trade_records, coin)

    audit_log("\n" + "="*40 + " FINAL TRADE REPORT " + "="*40)
    total_fee = 0.0
    total_gross_pnl = 0.0
    total_net_pnl = 0.0
    
    # Wait 3 seconds instead of 1 for exchange sync
    await asyncio.sleep(3)
    
    async with aiohttp.ClientSession(headers={"X-MBX-APIKEY": BINANCE_KEY}) as session:
        for ex in ["binance", "bybit"]:
            symbol = coin + "USDT"
            
            # Retry mechanism for Bybit/Binance lag
            for attempt in range(100):
                if ex == "binance":
                    entry_trades = await fetch_binance_trades_by_orders(session, symbol, trade_records[ex]["entry_order_ids"])
                    exit_trades = await fetch_binance_trades_by_orders(session, symbol, trade_records[ex]["exit_order_ids"])
                else:
                    entry_trades = await fetch_bybit_trades_by_orders(session, symbol, trade_records[ex]["entry_order_ids"])
                    exit_trades = await fetch_bybit_trades_by_orders(session, symbol, trade_records[ex]["exit_order_ids"])
                
                # If we expect data but got none, wait and retry
                if (trade_records[ex]["entry_order_ids"] and not entry_trades) or \
                   (trade_records[ex]["exit_order_ids"] and not exit_trades):
                    await asyncio.sleep(2)
                    continue
                break

            def process_trades(trades, exchange):
                total_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in trades)
                total_val = sum(float(t.get("quoteQty") or (float(t.get("execQty", 0)) * float(t.get("execPrice", 0)))) for t in trades)
                fees = sum(float(t.get("commission") or t.get("execFee", 0)) for t in trades)
                asset = trades[0].get("commissionAsset") if (trades and exchange == "binance") else ("USDT" if trades else "")
                avg_price = total_val / total_qty if total_qty > 0 else 0

                # Count maker/taker
                if exchange == "binance":
                    maker = sum(1 for t in trades if t.get("maker"))
                    taker = len(trades) - maker  # ✅ FIX: Define taker for binance too!
                else:  # bybit
                    maker = sum(1 for t in trades if t.get("isMaker") == "true")
                    taker = len(trades) - maker

                return total_qty, avg_price, fees, asset, maker, taker

            ent_qty, ent_p, ent_f, ent_a, ent_m, ent_t = process_trades(entry_trades, ex)
            ext_qty, ext_p, ext_f, ext_a, ext_m, ext_t = process_trades(exit_trades, ex)
            
            # Calculate PNL for this side
            side = trade_records[ex]['side']
            if side in ["LONG", "Buy"]:
                gross_pnl = (ext_p - ent_p) * min(ent_qty, ext_qty)
            else: # SHORT or Sell
                gross_pnl = (ent_p - ext_p) * min(ent_qty, ext_qty)
            
            fees = ent_f + ext_f
            net_pnl = gross_pnl - fees
            
            total_fee += fees
            total_gross_pnl += gross_pnl
            total_net_pnl += net_pnl
            
            audit_log(f"[{ex.upper()}] Side: {side}")
            ent_maker_pct = (ent_m / (ent_m + ent_t) * 100) if (ent_m + ent_t) > 0 else 0
            ext_maker_pct = (ext_m / (ext_m + ext_t) * 100) if (ext_m + ext_t) > 0 else 0

            audit_log(f"  Entry: {ent_qty} @ ${ent_p:.8f} | Total: ${ent_qty*ent_p:.4f} | {ent_m}M/{ent_t}T ({ent_maker_pct:.1f}% Maker)")
            audit_log(f"  Exit:  {ext_qty} @ ${ext_p:.8f} | Total: ${ext_qty*ext_p:.4f} | {ext_m}M/{ext_t}T ({ext_maker_pct:.1f}% Maker)")
            audit_log(f"  Fees Paid: ${fees:.8f} {ent_a}")
            audit_log(f"  Gross PNL (No Fees): ${gross_pnl:+.4f} USDT")
            audit_log(f"  Net PNL (With Fees): ${net_pnl:+.4f} USDT")
        
    audit_log(f"\n💎 --- FINAL SUMMARY ---")
    audit_log(f"💰 TOTAL GROSS PNL: ${total_gross_pnl:+.4f} USDT")
    audit_log(f"💰 TOTAL FEES PAID: ${total_fee:.6f} USDT")
    audit_log(f"💰 TOTAL NET PNL:   ${total_net_pnl:+.4f} USDT")
    audit_log("="*100 + "\n")

async def generate_complete_final_report(trade_records, coin):
    """Generate complete report including dust cleanup details"""
    audit_log("\n" + "="*80)
    audit_log(" " * 20 + "COMPLETE FINAL TRADE REPORT")
    audit_log("="*80)
    
    total_fee = 0.0
    total_gross_pnl = 0.0
    total_net_pnl = 0.0
    
    # Wait for exchange sync
    await asyncio.sleep(3)
    
    async with aiohttp.ClientSession(headers={"X-MBX-APIKEY": BINANCE_KEY}) as session:
        for ex in ["binance", "bybit"]:
            symbol = coin + "USDT"
            
            # Retry mechanism for fetching trades
            for attempt in range(100):
                if ex == "binance":
                    entry_trades = await fetch_binance_trades_by_orders(session, symbol, trade_records[ex]["entry_order_ids"])
                    exit_trades = await fetch_binance_trades_by_orders(session, symbol, trade_records[ex]["exit_order_ids"])
                    dust_trades = await fetch_binance_trades_by_orders(session, symbol, trade_records[ex]["dust_order_ids"])
                else:
                    entry_trades = await fetch_bybit_trades_by_orders(session, symbol, trade_records[ex]["entry_order_ids"])
                    exit_trades = await fetch_bybit_trades_by_orders(session, symbol, trade_records[ex]["exit_order_ids"])
                    dust_trades = await fetch_bybit_trades_by_orders(session, symbol, trade_records[ex]["dust_order_ids"])
                
                # Retry if missing expected data
                if (trade_records[ex]["entry_order_ids"] and not entry_trades) or \
                   (trade_records[ex]["exit_order_ids"] and not exit_trades) or \
                   (trade_records[ex]["dust_order_ids"] and not dust_trades):
                    await asyncio.sleep(2)
                    continue
                break
            
            # Process trades helper function
            def process_trades(trades, exchange):
                total_qty = sum(float(t.get("qty") or t.get("execQty", 0)) for t in trades)
                total_val = sum(float(t.get("quoteQty") or (float(t.get("execQty", 0)) * float(t.get("execPrice", 0)))) for t in trades)
                fees = sum(float(t.get("commission") or t.get("execFee", 0)) for t in trades)
                asset = trades[0].get("commissionAsset") if (trades and exchange == "binance") else ("USDT" if trades else "")
                avg_price = total_val / total_qty if total_qty > 0 else 0
                
                if exchange == "binance":
                    maker = sum(1 for t in trades if t.get("maker"))
                    taker = len(trades) - maker
                else:
                    maker = sum(1 for t in trades if t.get("isMaker") == "true")
                    taker = len(trades) - maker
                
                return total_qty, avg_price, fees, asset, maker, taker
            
            # Process initial exit trades
            ent_qty, ent_p, ent_f, ent_a, ent_m, ent_t = process_trades(entry_trades, ex)
            ext_qty, ext_p, ext_f, ext_a, ext_m, ext_t = process_trades(exit_trades, ex)
            
            # Calculate initial PNL
            side = trade_records[ex]['side']
            if side in ["LONG", "Buy"]:
                initial_gross_pnl = (ext_p - ent_p) * min(ent_qty, ext_qty)
            else:
                initial_gross_pnl = (ent_p - ext_p) * min(ent_qty, ext_qty)
            
            initial_fees = ent_f + ext_f
            initial_net_pnl = initial_gross_pnl - initial_fees
            
            # Print exchange header
            audit_log(f"\n[{ex.upper()}] Side: {side}")
            audit_log(f"")
            audit_log(f"  {'━' * 76}")
            audit_log(f"  📊 INITIAL EXIT")
            audit_log(f"  {'━' * 76}")
            audit_log(f"")
            
            ent_maker_pct = (ent_m / (ent_m + ent_t) * 100) if (ent_m + ent_t) > 0 else 0
            ext_maker_pct = (ext_m / (ext_m + ext_t) * 100) if (ext_m + ext_t) > 0 else 0
            
            audit_log(f"  Entry:  {ent_qty} coins @ ${ent_p:.8f} | Total: ${ent_qty*ent_p:.8f} | {ent_m}M/{ent_t}T ({ent_maker_pct:.1f}% Maker)")
            audit_log(f"  Exit:   {ext_qty} coins @ ${ext_p:.8f} | Total: ${ext_qty*ext_p:.8f} | {ext_m}M/{ext_t}T ({ext_maker_pct:.1f}% Maker)")
            audit_log(f"")
            audit_log(f"  Fees Paid: ${initial_fees:.8f} {ent_a}")
            audit_log(f"  Gross PNL (No Fees): ${initial_gross_pnl:+.8f} USDT")
            audit_log(f"  Net PNL (With Fees): ${initial_net_pnl:+.8f} USDT")
            
            # Check if dust cleanup happened
            if dust_trades:
                audit_log(f"")
                audit_log(f"  ⚠️  Dust Remaining: {ent_qty - ext_qty} coins")
                audit_log(f"")
                audit_log(f"  {'━' * 76}")
                audit_log(f"  🧹 DUST CLEANUP")
                audit_log(f"  {'━' * 76}")
                audit_log(f"")
                
                # Separate dust trades into ADD and CLOSE
                dust_add_trades = []
                dust_close_trades = []
                
                for t in dust_trades:
                    trade_side = t.get("side") if ex == "binance" else t.get("side")
                    # ADD trades match entry side, CLOSE trades are opposite
                    if ex == "binance":
                        if (side == "LONG" and trade_side == "BUY") or (side == "SHORT" and trade_side == "SELL"):
                            dust_add_trades.append(t)
                        else:
                            dust_close_trades.append(t)
                    else:  # bybit
                        if (side == "Buy" and trade_side == "Buy") or (side == "Sell" and trade_side == "Sell"):
                            dust_add_trades.append(t)
                        else:
                            dust_close_trades.append(t)
                
                # Process dust ADD trades
                add_qty, add_p, add_f, add_a, add_m, add_t = process_trades(dust_add_trades, ex)
                add_maker_pct = (add_m / (add_m + add_t) * 100) if (add_m + add_t) > 0 else 0
                
                # Process dust CLOSE trades
                close_qty, close_p, close_f, close_a, close_m, close_t = process_trades(dust_close_trades, ex)
                close_maker_pct = (close_m / (close_m + close_t) * 100) if (close_m + close_t) > 0 else 0
                
                # 🆕 CALCULATE DUST PNL (Gross & Net)
                if side in ["LONG", "Buy"]:
                    dust_gross_pnl = (close_p - add_p) * min(add_qty, close_qty)
                else:
                    dust_gross_pnl = (add_p - close_p) * min(add_qty, close_qty)
                
                dust_fees = add_f + close_f
                dust_net_pnl = dust_gross_pnl - dust_fees
                
                audit_log(f"  Minimum Lot Size Required: {add_qty} coins")
                audit_log(f"  Dust Position: {ent_qty - ext_qty} coins")
                audit_log(f"  Coins Added: {add_qty} coins")
                audit_log(f"")
                
                add_side_str = "BUY" if (side in ["LONG", "Buy"]) else "SELL"
                if ex == "bybit":
                    add_side_str = "Buy" if (side == "Buy") else "Sell"
                
                audit_log(f"  Step 1 - Added Position (FOK LIMIT):")
                audit_log(f"    {add_side_str} {add_qty} coins @ ${add_p:.8f} | Total: ${add_qty*add_p:.8f} | {add_m}M/{add_t}T ({add_maker_pct:.1f}% Maker)")
                audit_log(f"    Fees: ${add_f:.8f} USDT")
                audit_log(f"")
                
                close_side_str = "SELL" if (side in ["LONG", "Buy"]) else "BUY"
                if ex == "bybit":
                    close_side_str = "Sell" if (side == "Buy") else "Buy"
                
                audit_log(f"  Step 2 - Closed Combined Position (FOK LIMIT):")
                audit_log(f"    {close_side_str} {close_qty} coins @ ${close_p:.8f} | Total: ${close_qty*close_p:.8f} | {close_m}M/{close_t}T ({close_maker_pct:.1f}% Maker)")
                audit_log(f"    Fees: ${close_f:.8f} USDT")
                audit_log(f"")
                
                # 🆕 NEW: DUST PNL BREAKDOWN
                audit_log(f"  {'─' * 76}")
                audit_log(f"  💰 DUST CLEANUP PNL BREAKDOWN:")
                audit_log(f"  {'─' * 76}")
                audit_log(f"")
                audit_log(f"  📊 Dust Entry Price:  ${add_p:.8f}")
                audit_log(f"  📊 Dust Exit Price:   ${close_p:.8f}")
                audit_log(f"  📊 Price Difference:  ${abs(close_p - add_p):.8f} ({'↑' if close_p > add_p else '↓'})")
                audit_log(f"")
                audit_log(f"  💵 Dust Gross PNL (Before Fees): ${dust_gross_pnl:+.8f} USDT")
                audit_log(f"  💸 Dust Fees Paid:               ${dust_fees:.8f} USDT")
                audit_log(f"  💰 Dust Net PNL (After Fees):    ${dust_net_pnl:+.8f} USDT")
                audit_log(f"")
                
                # Calculate exchange totals (initial + dust)
                exchange_total_fees = initial_fees + dust_fees
                exchange_total_gross = initial_gross_pnl + dust_gross_pnl
                exchange_total_net = initial_net_pnl + dust_net_pnl
                exchange_total_coins = ent_qty + ext_qty + add_qty + close_qty
                exchange_total_maker = ent_m + ext_m + add_m + close_m
                exchange_total_taker = ent_t + ext_t + add_t + close_t
                
                # Separate dust trades into ADD and CLOSE
                # First half = ADD, second half = CLOSE (based on side)
                dust_add_trades = []
                dust_close_trades = []
                
                for t in dust_trades:
                    trade_side = t.get("side") if ex == "binance" else t.get("side")
                    # ADD trades match entry side, CLOSE trades are opposite
                    if ex == "binance":
                        if (side == "LONG" and trade_side == "BUY") or (side == "SHORT" and trade_side == "SELL"):
                            dust_add_trades.append(t)
                        else:
                            dust_close_trades.append(t)
                    else:  # bybit
                        if (side == "Buy" and trade_side == "Buy") or (side == "Sell" and trade_side == "Sell"):
                            dust_add_trades.append(t)
                        else:
                            dust_close_trades.append(t)
                
                # Process dust ADD trades
                add_qty, add_p, add_f, add_a, add_m, add_t = process_trades(dust_add_trades, ex)
                add_maker_pct = (add_m / (add_m + add_t) * 100) if (add_m + add_t) > 0 else 0
                
                # Process dust CLOSE trades
                close_qty, close_p, close_f, close_a, close_m, close_t = process_trades(dust_close_trades, ex)
                close_maker_pct = (close_m / (close_m + close_t) * 100) if (close_m + close_t) > 0 else 0
                
                # Calculate dust PNL
                if side in ["LONG", "Buy"]:
                    dust_gross_pnl = (close_p - add_p) * min(add_qty, close_qty)
                else:
                    dust_gross_pnl = (add_p - close_p) * min(add_qty, close_qty)
                
                dust_fees = add_f + close_f
                dust_net_pnl = dust_gross_pnl - dust_fees
                
                audit_log(f"  Minimum Lot Size Required: {add_qty} coins")
                audit_log(f"  Dust Position: {ent_qty - ext_qty} coins")
                audit_log(f"  Coins Added: {add_qty} coins")
                audit_log(f"")
                
                add_side_str = "BUY" if (side in ["LONG", "Buy"]) else "SELL"
                if ex == "bybit":
                    add_side_str = "Buy" if (side == "Buy") else "Sell"
                
                audit_log(f"  Step 1 - Added Position (FOK LIMIT):")
                audit_log(f"    {add_side_str} {add_qty} coins @ ${add_p:.8f} | Total: ${add_qty*add_p:.8f} | {add_m}M/{add_t}T ({add_maker_pct:.1f}% Maker)")
                audit_log(f"    Fees: ${add_f:.8f} USDT")
                audit_log(f"")
                
                close_side_str = "SELL" if (side in ["LONG", "Buy"]) else "BUY"
                if ex == "bybit":
                    close_side_str = "Sell" if (side == "Buy") else "Buy"
                
                audit_log(f"  Step 2 - Closed Combined Position (FOK LIMIT):")
                audit_log(f"    {close_side_str} {close_qty} coins @ ${close_p:.8f} | Total: ${close_qty*close_p:.8f} | {close_m}M/{close_t}T ({close_maker_pct:.1f}% Maker)")
                audit_log(f"    Fees: ${close_f:.8f} USDT")
                audit_log(f"")
                
                audit_log(f"  Dust Cleanup Fees: ${dust_fees:.8f} USDT")
                audit_log(f"  Dust Cleanup Gross PNL: ${dust_gross_pnl:+.8f} USDT")
                audit_log(f"  Dust Cleanup Net PNL: ${dust_net_pnl:+.8f} USDT")
                
                # Calculate exchange totals (initial + dust)
                exchange_total_fees = initial_fees + dust_fees
                exchange_total_gross = initial_gross_pnl + dust_gross_pnl
                exchange_total_net = initial_net_pnl + dust_net_pnl
                exchange_total_coins = ent_qty + ext_qty + add_qty + close_qty
                exchange_total_maker = ent_m + ext_m + add_m + close_m
                exchange_total_taker = ent_t + ext_t + add_t + close_t
                
            else:
                audit_log(f"")
                audit_log(f"  ✅ No Dust (Fully closed in initial exit)")
                
                # No dust, so totals = initial
                exchange_total_fees = initial_fees
                exchange_total_gross = initial_gross_pnl
                exchange_total_net = initial_net_pnl
                exchange_total_coins = ent_qty + ext_qty
                exchange_total_maker = ent_m + ext_m
                exchange_total_taker = ent_t + ext_t
            
            # Print exchange totals
            audit_log(f"")
            audit_log(f"  {'━' * 76}")
            audit_log(f"  💰 {ex.upper()} TOTAL")
            audit_log(f"  {'━' * 76}")
            audit_log(f"")
            
            total_maker_pct = (exchange_total_maker / (exchange_total_maker + exchange_total_taker) * 100) if (exchange_total_maker + exchange_total_taker) > 0 else 0
            
            audit_log(f"  Total Coins Traded: {exchange_total_coins} coins")
            audit_log(f"  Total Fees Paid: ${exchange_total_fees:.8f} USDT")
            audit_log(f"  Total Gross PNL: ${exchange_total_gross:+.8f} USDT")
            audit_log(f"  Total Net PNL: ${exchange_total_net:+.8f} USDT")
            audit_log(f"  Maker/Taker: {exchange_total_maker}M/{exchange_total_taker}T ({total_maker_pct:.1f}% Maker)")
            
            # Add to grand totals
            total_fee += exchange_total_fees
            total_gross_pnl += exchange_total_gross
            total_net_pnl += exchange_total_net
    
    # Print overall summary
    audit_log(f"\n")
    audit_log(f"💎 {'═' * 76}")
    audit_log(f"                          COMPLETE SUMMARY")
    audit_log(f"💎 {'═' * 76}")
    audit_log(f"")
    audit_log(f"💰 TOTAL GROSS PNL: ${total_gross_pnl:+.8f} USDT")
    audit_log(f"💰 TOTAL FEES PAID: ${total_fee:.8f} USDT")
    audit_log(f"💰 TOTAL NET PNL:   ${total_net_pnl:+.8f} USDT")
    audit_log(f"")
    audit_log(f"✅ ALL POSITIONS FULLY CLOSED (Including Dust Cleanup)")
    audit_log(f"{'═' * 80}\n")

# =========================
# MAIN
# =========================
async def main():
    # Sync time with NTP servers first
    print("🕐 Syncing time with NTP servers...")
    sync_time_with_ntp()
    print()
    
    coin = input("Coin (BTC/ETH/SOL): ").upper()
    exchanges = input("Exchanges (binance,bybit,mexc): ").lower().split(",")
    usdt = float(input("USDT per side: "))
    leverage = int(input("Leverage: "))
    EXECUTION_TIME = input("Execute at funding time left (HH:MM:SS): ")
    print("\nExit Options:")
    print("1. Auto-exit when funding resets (00:00:00)")
    print("2. Exit at custom time before funding")
    exit_mode = input("Choose exit mode (1/2): ").strip()

    if exit_mode == "1":
        EXIT_TIME = None
        print("✅ Will auto-exit when funding resets!")
    elif exit_mode == "2":
        EXIT_TIME = input("Exit at funding time left (HH:MM:SS): ")
        print(f"✅ Will exit at {EXIT_TIME} before funding!")
    else:
        print("Invalid choice, defaulting to auto-exit")
        EXIT_TIME = None
        exit_mode = "1"
    
    MIN_SPREAD = float(input("Minimum funding spread (%): "))
    
    # Sync with specific exchange for better accuracy
    print("\n🕐 Re-syncing with exchange time for precision...")
    # Initial sync
    if "binance" in exchanges:
        get_binance_server_time()
    elif "bybit" in exchanges:
        get_bybit_server_time()
    
    state = {
        "running": True,
        "net_pnl": None,
        "signal": None,
        "trade_fired": False,
        "min_spread": MIN_SPREAD,
        "execution_time": EXECUTION_TIME,
        "exit_mode": exit_mode,
        "exit_time": EXIT_TIME if exit_mode == "2" else "Funding Reset (+5s)",
        "exit_triggered": False
    }

    sync_thread = threading.Thread(target=continuous_time_sync, args=(state, exchanges), daemon=True)
    sync_thread.start()
    audit_log("✅ Continuous time sync started (every 2 minutes)")

    # Start background thread for PNL threshold override
    listener_thread = threading.Thread(target=pnl_threshold_listener, args=(state,), daemon=True)
    listener_thread.start()
    audit_log("✅ PNL threshold listener started")
    print("💡 TIP: Type 'p' and press ENTER anytime to set custom PNL exit threshold!\n")

    position_info = {
        "binance": {"entry_price": None, "size": 0, "side": None},
        "bybit": {"entry_price": None, "size": 0, "side": None}
    }
    trade_records = {
    "binance": {"entry_order_ids": [], "exit_order_ids": [], "dust_order_ids": [], "side": None},
    "bybit": {"entry_order_ids": [], "exit_order_ids": [], "dust_order_ids": [], "side": None}
    }
    tasks = []

    if "binance" in exchanges:
        state["binance"] = {"price": 0, "bid": 0, "ask": 0, "funding": None, "next_ts": None, "pnl": None, "position_size": 0, "position_entry": 0, "position_pnl": 0, "position_side": "FLAT", "ws_active_order_price": None}
        tasks.append(binance_ws(coin + "USDT", state))
        tasks.append(binance_position_ws(coin + "USDT", state))
    if "bybit" in exchanges:
        state["bybit"] = {"price": 0, "bid": 0, "ask": 0, "funding": None, "next_ts": None, "pnl": None, "position_size": 0, "position_entry": 0, "position_pnl": 0, "position_side": "FLAT", "ws_active_order_price": None}
        tasks.append(bybit_ws(coin + "USDT", state))
        tasks.append(bybit_position_ws(coin + "USDT", state))
    if "mexc" in exchanges:
        state["mexc"] = {"price": 0, "bid": 0, "ask": 0, "funding": None, "next_ts": None, "pnl": None}
        tasks.append(mexc_rest(coin + "_USDT", state))

    trade_fired = False
    exit_fired = False

    async def execution_watcher(position_info):
        nonlocal trade_fired
        nonlocal trade_records  # Add this line
        print(f"\n⏰ Waiting for execution time: {EXECUTION_TIME} before funding")
        print("📊 Monitoring funding rates...")
        
        binance_symbol = coin + "USDT"
        bybit_symbol = coin + "USDT"
        binance_filters = get_binance_symbol_filters(binance_symbol)
        bybit_filters = get_bybit_symbol_filters(bybit_symbol)

        # ✅ Validate filters
        if not binance_filters or not bybit_filters:
            print("❌ CRITICAL ERROR: Failed to fetch symbol filters!")
            return

        
        while state.get("running", True):
            if "binance" in exchanges and "bybit" in exchanges:
                if (state["binance"]["next_ts"] and state["bybit"]["next_ts"] and
                    not trade_fired):
                    
                    sig = funding_arbitrage_signal(state, exchanges, MIN_SPREAD)
                    state["signal"] = sig
                    
                    exchange_to_check = "binance" if "binance" in exchanges else "bybit"
                    
                    if should_execute(state[exchange_to_check]["next_ts"], EXECUTION_TIME):
                        if sig and sig['spread'] >= MIN_SPREAD:
                            audit_log(f"🚀 EXECUTING HEDGE at {time_left(state[exchange_to_check]['next_ts'])}")
                            # 🆕 Force sync before entry
                            get_binance_server_time() if "binance" in exchanges else get_bybit_server_time()
                            await asyncio.sleep(0.1)
                            audit_log(f"Strategy: Long {sig['long']}, Short {sig['short']}")

                            binance_set_leverage(binance_symbol, leverage)
                            bybit_set_leverage(bybit_symbol, leverage)
                            
                            # ✅ TRUE USDT-NEUTRAL HEDGE (Using Entry Prices, NOT Mark Price)
                            target_usdt = usdt * leverage
                            
                            # Determine entry prices based on strategy (Bidding at the side we want to join)
                            entry_p_b = state["binance"]["bid"] if sig["long"] == "binance" else state["binance"]["ask"]
                            entry_p_y = state["bybit"]["bid"] if sig["long"] == "bybit" else state["bybit"]["ask"]
                            
                            if entry_p_b <= 0 or entry_p_y <= 0:
                                audit_log("⚠️ CRITICAL: Entry BBO is 0! WebSocket data lagging. Aborting execution.")
                                return

                            # 1. Calculate Binance quantity
                            qty_b_ideal = target_usdt / entry_p_b
                            pure_qty_b = round_step_size_nearest(qty_b_ideal, binance_filters["step_size"])
                            actual_usdt_b = pure_qty_b * entry_p_b

                            # 2. Calculate Bybit quantity
                            qty_y_ideal = target_usdt / entry_p_y
                            pure_qty_y = round_step_size(qty_y_ideal, bybit_filters["step_size"])
                            actual_usdt_y = pure_qty_y * entry_p_y
                            
                            mismatch = actual_usdt_b - actual_usdt_y
                            
                            audit_log(f"📐 USDT-Neutral Plan: Target=${target_usdt:.4f}")
                            audit_log(f"🎯 Binance: {pure_qty_b} coins @ ${entry_p_b:.8f} (~${actual_usdt_b:.4f})")
                            audit_log(f"🎯 Bybit:   {pure_qty_y} coins @ ${entry_p_y:.8f} (~${actual_usdt_y:.4f})")
                            audit_log(f"⚖️ Parity Delta: ${abs(mismatch):.8f} (Using BBO, NOT Mark Price)")
                            
                            if pure_qty_b <= 0 or pure_qty_y <= 0:
                                print("❌ Error: Calculated quantity is 0. Increase USDT or Leverage.")
                                return

                            # Use Limit Order Chasing
                            # ... (task execution remains same) ...

                            # 🆕 SHARED STATE FOR PERFECT PARITY
                            shared_fill_state = {
                                "binance": {"done": False, "filled_usdt": 0, "filled_qty": 0},
                                "bybit": {"done": False, "filled_usdt": 0, "filled_qty": 0}
                            }

                             # Use Limit Order Chasing WITH SHARED STATE
                            tasks_chaser = []
                            if sig["long"] == "binance":
                                tasks_chaser.append(limit_order_chaser("binance", binance_symbol, "BUY", pure_qty_b, state, binance_filters, trade_records, shared_fill_state))
                            else:
                                tasks_chaser.append(limit_order_chaser("binance", binance_symbol, "SELL", pure_qty_b, state, binance_filters, trade_records, shared_fill_state))

                            if sig["long"] == "bybit":
                                tasks_chaser.append(limit_order_chaser("bybit", bybit_symbol, "Buy", pure_qty_y, state, bybit_filters, trade_records, shared_fill_state))
                            else:
                                tasks_chaser.append(limit_order_chaser("bybit", bybit_symbol, "Sell", pure_qty_y, state, bybit_filters, trade_records, shared_fill_state))                            
                            # Wait for both and log parity
                            results = await asyncio.gather(*tasks_chaser)
                            
                            # results[0] is Binance, results[1] is Bybit
                            actual_b_val = results[0][1]
                            actual_y_val = results[1][1]
                            actual_mismatch = abs(actual_b_val - actual_y_val)
                            
                            audit_log(f"SYSTEM ⚖️ HEDGE PARITY AUDIT: Actual Delta=${actual_mismatch:.8f}")
                            if actual_mismatch > 2.0: # Alert if delta is significant (> $2)
                                audit_log(f"⚠️ HIGH PARITY DELTA DETECTED: ${actual_mismatch:.2f}")
                            
                            # Store position sides
                            trade_records["binance"]["side"] = "LONG" if sig["long"] == "binance" else "SHORT"
                            trade_records["bybit"]["side"] = "Buy" if sig["long"] == "bybit" else "Sell"
                            
                            trade_fired = True
                            state["trade_fired"] = True
                            state["active_strategy"] = f"Long {sig['long']} / Short {sig['short']}"
                            # 🆕 CAPTURE funding time when trade executes
                            nonlocal funding_time_snapshot
                            funding_time_snapshot = state[exchange_to_check]["next_ts"]
                            print("✅ Orders placed! Fetching position info...")
                            
                            # Wait 500ms for orders to settle
                            await asyncio.sleep(0.5)

                            # Fetch entry prices and sizes with retry
                            if "binance" in exchanges:
                                async with aiohttp.ClientSession(headers={"X-MBX-APIKEY": BINANCE_KEY}) as session:
                                    for attempt in range(10):
                                        binance_info = await get_binance_entry_info(session, binance_symbol)
                                        if binance_info:
                                            position_info["binance"] = binance_info
                                            print(f"✅ Binance: Entry=${binance_info['entry_price']:.2f}, Size={binance_info['size']}, Side={binance_info['side']}")
                                            break
                                        print(f"⏳ Waiting for Binance position... (attempt {attempt + 1}/10)")
                                        await asyncio.sleep(0.2)
                            
                            if "bybit" in exchanges:
                                async with aiohttp.ClientSession() as session:
                                    for attempt in range(10):
                                        bybit_info = await get_bybit_entry_info(session, bybit_symbol)
                                        if bybit_info:
                                            position_info["bybit"] = bybit_info
                                            print(f"✅ Bybit: Entry=${bybit_info['entry_price']:.2f}, Size={bybit_info['size']}, Side={bybit_info['side']}")
                                            break
                                        print(f"⏳ Waiting for Bybit position... (attempt {attempt + 1}/10)")
                                        await asyncio.sleep(0.2)

                            # Debug: Check if position info was fetched
                            if not position_info["binance"]["entry_price"] and "binance" in exchanges:
                                print("⚠️ WARNING: Binance position info not found!")
                            if not position_info["bybit"]["entry_price"] and "bybit" in exchanges:
                                print("⚠️ WARNING: Bybit position info not found!")

                            print("💰 Starting WebSocket-based PNL tracking...")
                            asyncio.create_task(api_pnl_tracker(state, coin, exchanges, position_info))
                            
                        else:
                            print(f"❌ No valid signal (spread: {sig['spread'] if sig else 'N/A'}%, "
                                  f"min required: {MIN_SPREAD}%)")
            
            await asyncio.sleep(0.05)

    funding_time_snapshot = None

    async def exit_watcher():
        nonlocal exit_fired, exit_mode, funding_time_snapshot
        nonlocal trade_records  # Add this line
        
        if exit_mode == "1":
            print(f"\n⏰ Exit will trigger automatically when funding resets (00:00:00)")
        else:
            print(f"\n⏰ Exit will trigger at: {EXIT_TIME} before funding")   
                
        binance_symbol = coin + "USDT"
        bybit_symbol = coin + "USDT"
        binance_filters = get_binance_symbol_filters(binance_symbol)
        bybit_filters = get_bybit_symbol_filters(bybit_symbol)
        
        while not trade_fired:
            await asyncio.sleep(1)  # Wait until trade is executed
        
        print("✅ Trade executed, monitoring for exit time...")
        
        while not exit_fired:
            exchange_to_check = "binance" if "binance" in exchanges else "bybit"
            
            # Check exit condition based on mode
            should_exit = False
    
            if exit_mode == "1":
                # Auto-exit when funding resets (using CAPTURED time)
                if funding_time_snapshot:
                    ts_seconds = funding_time_snapshot / 1000
                    current_time = get_accurate_time()
                    seconds_until_funding = int(ts_seconds - current_time)

                    if seconds_until_funding <= -5:   # Exit 5 seconds AFTER funding reset
                        should_exit = True
            else:
                # Custom time exit - Use CAPTURED time to avoid issues when live next_ts updates
                if funding_time_snapshot:
                    should_exit = should_execute(funding_time_snapshot, EXIT_TIME)
    
            if should_exit:
                audit_log(f"{'='*90}")
                # 🆕 Force sync before exit
                get_binance_server_time() if "binance" in exchanges else get_bybit_server_time()
                await asyncio.sleep(0.1)
                audit_log(f"🚪 EXIT TIME REACHED!")
                audit_log(f"   Exit Mode: {exit_mode}")
                audit_log(f"   Funding Snapshot: {funding_time_snapshot}")
                audit_log(f"   Current Positions:")
                if "binance" in exchanges and position_info["binance"]["entry_price"]:
                    audit_log(f"      Binance: {position_info['binance']['side']} {position_info['binance']['size']} @ ${position_info['binance']['entry_price']:.8f}")
                if "bybit" in exchanges and position_info["bybit"]["entry_price"]:
                    audit_log(f"      Bybit: {position_info['bybit']['side']} {position_info['bybit']['size']} @ ${position_info['bybit']['entry_price']:.8f}")
                audit_log(f"{'='*90}")
    
                audit_log(f"🚪 EXIT TIME REACHED! Waiting for PNL >= ${user_pnl_threshold:+.2f}")
                print(f"\n🚪 EXIT TIME REACHED! Waiting for PNL >= ${user_pnl_threshold:+.2f}...")
                state["exit_triggered"] = True
    
                # 🆕 Wait for PNL to reach user threshold (REAL-TIME)
                while True:
                    # ✅ Calculate PNL in REAL-TIME using live BBO prices
                    projected_pnl = calculate_exit_pnl_projection(position_info, state)
                    state["exit_pnl_projection"] = projected_pnl # Update for printer
                    
                    if projected_pnl is None:
                        await asyncio.sleep(0.01)
                        continue

                    # Dynamic log message
                    if not terminal_paused:
                        print(f"\r⏳ Waiting for PNL >= ${user_pnl_threshold:+.2f}... Current: ${projected_pnl:+.2f}    ", end='', flush=True)
                    
                    # Check against user threshold
                    if projected_pnl >= user_pnl_threshold:
                        print()  # New line after \r printing
                        audit_log(f"{'='*90}")
                        audit_log(f"✅ PNL THRESHOLD ${user_pnl_threshold:+.2f} REACHED!")
                        audit_log(f"   Current PNL: ${projected_pnl:+.2f}")
                        audit_log(f"   Projected PNL: ${projected_pnl:+.6f}")
                        if "binance" in exchanges:
                            audit_log(f"   Binance Best ASK: ${state['binance']['ask']:.8f}")
                        if "bybit" in exchanges:
                            audit_log(f"   Bybit Best BID: ${state['bybit']['bid']:.8f}")
                        audit_log(f"   Proceeding to close positions...")
                        audit_log(f"{'='*90}")
                        break
                    
                    await asyncio.sleep(0.001)  # Check every 1ms with LIVE data (BG ONLY)
    
                print(f"\n🚪 CLOSING ALL POSITIONS...")
                
                audit_log(f"🚪 INITIATING EXIT CHASERS ON BOTH EXCHANGES...")

                # 🆕 SHARED STATE FOR EXIT COORDINATION
                shared_exit_state = {
                    "binance": {
                        "realized_pnl": 0, 
                        "unrealized_pnl": 0, 
                        "total_pnl": 0, 
                        "remaining_qty": 0,
                        "best_price": 0,
                        "locked_price": None,
                        "status": "ready",
                        "done": False,
                        "position_side": position_info["binance"]["side"] if "binance" in exchanges else None,
                        "dust_remaining": 0, 
                        "dust_side": None
                    },
                    "bybit": {
                        "realized_pnl": 0, 
                        "unrealized_pnl": 0, 
                        "total_pnl": 0,
                        "remaining_qty": 0,
                        "best_price": 0,
                        "locked_price": None,
                        "status": "ready",
                        "done": False,
                        "position_side": position_info["bybit"]["side"] if "bybit" in exchanges else None,
                        "dust_remaining": 0, 
                        "dust_side": None
                    },
                    "cancel_trigger": False
                }
                
                # Update printer to show exit PNL
                state["shared_exit_state"] = shared_exit_state
                
                # 🆕 USE EXIT CHASER V3 WITH SHARED STATE
                exit_tasks = []

                if "binance" in exchanges and position_info["binance"]["entry_price"]:
                    side_to_close = "SELL" if position_info["binance"]["side"] == "LONG" else "BUY"
                    print(f"🚪 Closing Binance {position_info['binance']['side']} with EXIT CHASER V3...")
                    exit_tasks.append(limit_order_exit_chaser_v3(
                        "binance", binance_symbol, side_to_close, 
                        state, binance_filters, trade_records, position_info, shared_exit_state
                    ))

                if "bybit" in exchanges and position_info["bybit"]["entry_price"]:
                    side_to_close = "Sell" if position_info["bybit"]["side"] == "Buy" else "Buy"
                    print(f"🚪 Closing Bybit {position_info['bybit']['side']} with EXIT CHASER V3...")
                    exit_tasks.append(limit_order_exit_chaser_v3(
                        "bybit", bybit_symbol, side_to_close, 
                        state, bybit_filters, trade_records, position_info, shared_exit_state
                    ))

                # Wait for both exit chasers to complete
                results = await asyncio.gather(*exit_tasks)
                
                exit_fired = True
                audit_log("✅ All positions closed!")

                dust_tasks = []
                if "binance" in exchanges:
                    dust_qty = shared_exit_state["binance"].get("dust_remaining", 0)
                    if dust_qty > 0:
                        audit_log(f"⚠️ Binance has {dust_qty} coins dust")
                        dust_tasks.append(dust_cleanup_chaser("binance", binance_symbol, dust_qty, trade_records["binance"]["side"], leverage, state, binance_filters, trade_records))
                    else:
                        audit_log("✅ Binance CLEARED (no dust)")
                
                if "bybit" in exchanges:
                    dust_qty = shared_exit_state["bybit"].get("dust_remaining", 0)
                    if dust_qty > 0:
                        audit_log(f"⚠️ Bybit has {dust_qty} coins dust")
                        dust_tasks.append(dust_cleanup_chaser("bybit", bybit_symbol, dust_qty, trade_records["bybit"]["side"], leverage, state, bybit_filters, trade_records))
                    else:
                        audit_log("✅ Bybit CLEARED (no dust)")
                
                if dust_tasks:
                    await asyncio.gather(*dust_tasks)
                
                # SHUTDOWN Background Tasks
                state["running"] = False
                audit_log("🛑 Stopping background processes...")
                await asyncio.sleep(1) # Wait for loops to see flag

                # Generate final report
                print("\n" + "="*90)
                print("📊 Fetching exact trade data from exchanges...")
                await generate_final_report(trade_records, coin)
                await generate_complete_final_report(trade_records, coin) 
                break
            
            await asyncio.sleep(0.05)
    # Create a holder for shared_exit_state (will be set later)
    shared_exit_state_holder = {}
    
    tasks.append(printer(state, exchanges, lambda: state.get("shared_exit_state")))
    tasks.append(execution_watcher(position_info))
    tasks.append(exit_watcher())
    await asyncio.gather(*tasks)

# =========================
# RUN
# =========================
if __name__ == "__main__":
    asyncio.run(main())    