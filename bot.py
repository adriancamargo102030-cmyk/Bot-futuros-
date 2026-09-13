import os
import subprocess
import time
import asyncio
import json
from datetime import datetime, timezone, timedelta
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
import aiohttp

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TIMEFRAME = '1h'
LIMIT_CANDLES = 120
COOLDOWN_HOURS = 6  # 6 horas de descanso por cada moneda para evitar repeticiones
HISTORIAL_FILE = "historial_senales.json"

def cargar_historial():
    if os.path.exists(HISTORIAL_FILE):
        try:
            with open(HISTORIAL_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def guardar_historial_y_sincronizar(historial):
    try:
        with open(HISTORIAL_FILE, "w") as f:
            json.dump(historial, f, indent=4)
        
        # Sincronización automática con GitHub para que el Cooldown persista entre ejecuciones horarias
        subprocess.run(["git", "config", "--global", "user.name", "Bot Signal"], check=False)
        subprocess.run(["git", "config", "--global", "user.email", "bot@actions.local"], check=False)
        subprocess.run(["git", "add", HISTORIAL_FILE], check=False)
        subprocess.run(["git", "commit", "-m", "Actualizar historial de cooldown [skip ci]"], check=False)
        subprocess.run(["git", "push"], check=False)
    except Exception as e:
        print(f"Error al sincronizar historial con Git: {e}")

async def send_telegram_message(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        async with session.post(url, json=payload) as response:
            await response.text()
    except Exception as e:
        print(f"Error al enviar mensaje a Telegram: {e}")

async def analizar_par(exchange, symbol, semaphore):
    async with semaphore:
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=LIMIT_CANDLES)
            if not ohlcv or len(ohlcv) < 100:
                return None

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # --- INDICADORES DE ALTA EFECTIVIDAD (MA99 + EMA20 + EMA50 + RSI + ATR) ---
            df['ma99'] = df['close'].rolling(window=99).mean()
            df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
            df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
            
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))

            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            df['atr'] = tr.rolling(window=14).mean()

            current_price = df['close'].iloc[-1]
            ma99 = df['ma99'].iloc[-1]
            ema20 = df['ema20'].iloc[-1]
            ema50 = df['ema50'].iloc[-1]
            rsi = df['rsi'].iloc[-1]
            atr = df['atr'].iloc[-1]
            volume = df['volume'].iloc[-1]

            if pd.isna(ma99) or pd.isna(ema20) or pd.isna(ema50) or pd.isna(rsi) or pd.isna(atr):
                return None

            direction = None
            # Filtros estrictos para asegurar alta probabilidad en movimientos rápidos (1h - a pocas horas)
            if current_price > ma99 and ema20 > ema50 and rsi <= 43.0:
                direction = 'LONG'
            elif current_price < ma99 and ema20 < ema50 and rsi >= 57.0:
                direction = 'SHORT'

            if direction:
                return {
                    'symbol': symbol,
                    'direction': direction,
                    'current_price': current_price,
                    'ma99': ma99,
                    'rsi': rsi,
                    'atr': atr,
                    'volume': volume
                }
        except Exception:
            return None
        return None

async def main():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Faltan las credenciales de Telegram en los Secrets.")
        return

    exchange = ccxt.binance({
        'enableRateLimit': False,
        'timeout': 5000,
        'options': {
            'defaultType': 'spot',
            'adjustForTimeDifference': True
        },
        'urls': {
            'api': {
                'public': 'https://data-api.binance.vision/api/v3',
                'private': 'https://data-api.binance.vision/api/v3',
                'v3': 'https://data-api.binance.vision/api/v3',
            }
        }
    })
    
    exchange.has['fetchOHLCV'] = True
    exchange.options['fetchMarkets'] = ['spot']

    try:
        print("Cargando mercados Spot de Binance...")
        markets = await exchange.fetch_markets()
        lista_pares = [m['symbol'] for m in markets if m['quote'] == 'USDT' and m['active']]
        
        if not lista_pares:
            await exchange.load_markets()
            lista_pares = [s for s in exchange.symbols if s.endswith('/USDT') and not ':' in s and not '-' in s]

    except Exception as e:
        print(f"Error al conectar con Binance Spot: {e}")
        await exchange.close()
        return

    print(f"¡Mercados cargados! Total pares USDT: {len(lista_pares)}")
    print(f"Escaneando mercados con filtros de alta efectividad en {TIMEFRAME}...")
    
    semaphore = asyncio.Semaphore(15)
    tasks = [analizar_par(exchange, symbol, semaphore) for symbol in lista_pares[:150]]
    resultados = await asyncio.gather(*tasks)
    
    await exchange.close()

    potential_signals = [res for res in resultados if res is not None]

    if not potential_signals:
        print("No se encontraron señales de alta calidad en este ciclo.")
        return

    # --- APLICACIÓN DEL COOLDOWN PERSISTENTE ---
    historial = cargar_historial()
    ahora = datetime.now(timezone.utc)
    
    pares_filtrados = []
    for sig in potential_signals:
        sym = sig['symbol']
        if sym in historial:
            tiempo_ultima = datetime.fromisoformat(historial[sym])
            if ahora - tiempo_ultima < timedelta(hours=COOLDOWN_HOURS):
                print(f"Moneda {sym} en Cooldown (ignorada).")
                continue
        pares_filtrados.append(sig)

    if not pares_filtrados:
        print("Hay señales pero todas están en período de Cooldown activo.")
        return

    # Ordenar por volumen de negociación para elegir siempre la opción más líquida y fuerte
    pares_filtrados = sorted(pares_filtrados, key=lambda x: x['volume'], reverse=True)
    best_signal = pares_filtrados[0]

    # Guardar en historial y sincronizar con git
    historial[best_signal['symbol']] = ahora.isoformat()
    guardar_historial_y_sincronizar(historial)

    print(f"Se seleccionó la mejor opción fuera de cooldown: {best_signal['symbol']}. Enviando alerta...")

    async with aiohttp.ClientSession() as session:
        sig = best_signal
        symbol = sig['symbol']
        direction = sig['direction']
        current_price = sig['current_price']
        ma99 = sig['ma99']
        rsi = sig['rsi']
        atr = sig['atr']
        
        coin_name = symbol.split('/')[0]
        
        if direction == 'LONG':
            action_label = "LONG - COMPRA 🟢"
        else:
            action_label = "SHORT - VENTA 🔴"

        min_atr = current_price * 0.005
        if atr < min_atr:
            atr = min_atr

        if current_price < 0.0001:
            decimals = 8
        elif current_price < 0.01:
            decimals = 6
        elif current_price < 1.0:
            decimals = 4
        else:
            decimals = 2

        fmt = f"{{:.{decimals}f}}"
        leverage = "x3 - x5 (Margen Aislado)" if (atr / current_price) > 0.02 else "x5 - x8 (Margen Aislado)"

        if direction == 'LONG':
            entry_min = current_price - (atr * 0.2)
            entry_max = current_price
            sl = current_price - (atr * 1.5)
            tp1 = current_price + (atr * 1.0)
            tp2 = current_price + (atr * 1.8)
            tp3 = current_price + (atr * 3.0)
        else:
            entry_min = current_price
            entry_max = current_price + (atr * 0.2)
            sl = current_price + (atr * 1.5)
            tp1 = current_price - (atr * 1.0)
            tp2 = current_price - (atr * 1.8)
            tp3 = current_price - (atr * 3.0)

        s_entry_min = fmt.format(entry_min)
        s_entry_max = fmt.format(entry_max)
        s_sl = fmt.format(sl)
        s_tp1 = fmt.format(tp1)
        s_tp2 = fmt.format(tp2)
        s_tp3 = fmt.format(tp3)
        s_ma99 = fmt.format(ma99)
        s_atr = fmt.format(atr)

        message = f"""SEÑAL VIP
${coin_name} - {action_label}

Plan de Comercio:
• Entrada: {s_entry_min} – {s_entry_max}
• Stop Loss: {s_sl}

Take Profits:
• TP1: {s_tp1}
• TP2: {s_tp2}
• TP3: {s_tp3}

Apalancamiento sugerido: {leverage}

Justificación:
La estructura de 1h mantiene un sesgo de alta probabilidad {'alcista' if direction == 'LONG' else 'bajista'}, respaldado por la alineación institucional de la MA99 ({s_ma99}) y las EMAs rápidas, asegurando un impulso dinámico a corto plazo.
El RSI en 1h (~{rsi:.1f}) marca un punto de entrada óptimo tras un retroceso sano, propicio para resolverse en las próximas horas.
La zona de entrada entre {s_entry_min} y {s_entry_max} optimiza el riesgo/beneficio con un Stop Loss técnico en {s_sl}.
La volatilidad del ATR (~{s_atr}) confirma actividad ideal para capturar TP1 rápidamente.

⚠️ Mantener estricta disciplina en {s_sl}.

Sesgo: {'Continuidad alcista de corto plazo.' if direction == 'LONG' else 'Continuidad bajista de corto plazo.'}

This message was sent automatically with GitHub Actions"""

        await send_telegram_message(session, message)
        print(f"¡Alerta de alta efectividad enviada para {coin_name}!")

if __name__ == "__main__":
    asyncio.run(main())
    
