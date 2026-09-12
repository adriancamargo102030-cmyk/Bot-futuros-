import os
import time
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
import aiohttp

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TIMEFRAME = '1h'
LIMIT_CANDLES = 120

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
            
            # Indicadores Técnicos
            df['ma99'] = df['close'].rolling(window=99).mean()
            
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
            rsi = df['rsi'].iloc[-1]
            atr = df['atr'].iloc[-1]
            volume = df['volume'].iloc[-1]

            if pd.isna(ma99) or pd.isna(rsi) or pd.isna(atr):
                return None

            direction = None
            # LONG: Precio > MA99 y RSI <= 45.0
            if current_price > ma99 and rsi <= 45.0:
                direction = 'LONG'
            # SHORT: Precio < MA99 y RSI >= 55.0
            elif current_price < ma99 and rsi >= 55.0:
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
        print("Cargando mercados Spot de Binance a través de binance.vision (async)...")
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
    print(f"Escaneando mercados de forma concurrente en temporalidad de {TIMEFRAME}...")
    
    semaphore = asyncio.Semaphore(15)
    tasks = [analizar_par(exchange, symbol, semaphore) for symbol in lista_pares[:150]]
    resultados = await asyncio.gather(*tasks)
    
    await exchange.close()

    potential_signals = [res for res in resultados if res is not None]

    if not potential_signals:
        print("No se encontraron señales en este ciclo.")
        return

    # REGLA NUEVA: Ordenar por volumen y seleccionar estrictamente LA MEJOR (1 sola opción por hora)
    potential_signals = sorted(potential_signals, key=lambda x: x['volume'], reverse=True)
    best_signal = potential_signals[0]

    print(f"Se seleccionó la mejor opción: {best_signal['symbol']}. Enviando alerta única...")

    async with aiohttp.ClientSession() as session:
        sig = best_signal
        symbol = sig['symbol']
        direction = sig['direction']
        current_price = sig['current_price']
        ma99 = sig['ma99']
        rsi = sig['rsi']
        atr = sig['atr']
        
        coin_name = symbol.split('/')[0]
        
        # Formato visual de Compra/Venta con color
        if direction == 'LONG':
            action_label = "LONG - COMPRA 🟢"
        else:
            action_label = "SHORT - VENTA 🔴"

        # Blindaje ATR
        min_atr = current_price * 0.005
        if atr < min_atr:
            atr = min_atr

        # Decimales dinámicos
        if current_price < 0.01:
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
La estructura de 1h mantiene un sesgo claramente {'alcista' if direction == 'LONG' else 'bajista'}, con el precio operando por {'encima' if direction == 'LONG' else 'debajo'} de la MA99 ({s_ma99}), lo que valida la tendencia de fondo y respalda la continuidad del movimiento.
El RSI en 1h (~{rsi:.1f}) se encuentra en zona operativa óptima para retrocesos, lo que deja margen para una nueva extensión antes de encontrar resistencia fuerte.
La zona de entrada entre {s_entry_min} y {s_entry_max} ofrece una relación riesgo/beneficio favorable con Stop Loss bien definido en {s_sl}.
La volatilidad medida por el ATR (~{s_atr}) muestra un mercado activo en 1h, aumentando las probabilidades de éxito.
TP1 busca capturar el primer movimiento hacia {s_tp1}, mientras que TP2 y TP3 apuntan a una extensión hacia {s_tp2} y {s_tp3}.

⚠️ Mientras el precio permanezca por {'encima' if direction == 'LONG' else 'debajo'} de {s_sl}, el escenario {direction} continúa siendo válido.

Sesgo: {'Alcista con potencial de continuidad si se mantiene el soporte.' if direction == 'LONG' else 'Bajista con potencial de continuidad si se mantiene la resistencia.'}

This message was sent automatically with GitHub Actions"""

        await send_telegram_message(session, message)
        print(f"¡Alerta única enviada para {coin_name}!")

if __name__ == "__main__":
    asyncio.run(main())
            
