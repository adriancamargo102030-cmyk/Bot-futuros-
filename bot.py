import os
import time
import ccxt
import pandas as pd
import numpy as np
import requests

# Credenciales privadas desde los Secrets de GitHub
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TIMEFRAME = '1h'
LIMIT_CANDLES = 120

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"Error al enviar mensaje a Telegram: {e}")

def main():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Faltan las credenciales de Telegram en los Secrets.")
        return

    # Inicializar Binance Futures mediante CCXT (evita bloqueos de IP de GitHub)
    exchange = ccxt.binance({
        'options': {
            'defaultType': 'future',
        },
        'enableRateLimit': True
    })

    try:
        print("Cargando mercados de Binance Futures...")
        exchange.load_markets()
        # Filtrar solo pares perpetuos de USDT
        symbols = [symbol for symbol in exchange.symbols if '/USDT:USDT' in symbol or (symbol.endswith('/USDT') and exchange.markets[symbol]['linear'])]
    except Exception as e:
        print(f"Error al conectar con Binance a través de CCXT: {e}")
        return

    print(f"Escaneando {len(symbols)} pares en temporalidad de {TIMEFRAME}...")
    potential_signals = []

    for symbol in symbols:
        try:
            # Obtener velas (OHLCV)
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=LIMIT_CANDLES)
            if not ohlcv or len(ohlcv) < 100:
                continue

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # Calcular Indicadores Técnicos
            df['ma99'] = df['close'].rolling(window=99).mean()
            
            # Cálculo de RSI (14)
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))

            # Cálculo de ATR (14)
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
                continue

            direction = None
            # Tus filtros ajustados:
            # LONG: Precio > MA99 y RSI <= 45.0
            if current_price > ma99 and rsi <= 45.0:
                direction = 'LONG'
            # SHORT: Precio < MA99 y RSI >= 55.0
            elif current_price < ma99 and rsi >= 55.0:
                direction = 'SHORT'

            if direction:
                potential_signals.append({
                    'symbol': symbol,
                    'direction': direction,
                    'current_price': current_price,
                    'ma99': ma99,
                    'rsi': rsi,
                    'atr': atr,
                    'volume': volume
                })
        except Exception as err:
            # Continuar si falla un par individual para que el bot no se detenga
            continue

    if not potential_signals:
        print("No se encontraron señales en este ciclo.")
        return

    # Ordenar por volumen para seleccionar estrictamente los 5 mejores mercados
    potential_signals = sorted(potential_signals, key=lambda x: x['volume'], reverse=True)
    top_signals = potential_signals[:5]

    print(f"Se seleccionaron los {len(top_signals)} mejores pares. Enviando alertas...")

    for sig in top_signals:
        symbol = sig['symbol']
        direction = sig['direction']
        current_price = sig['current_price']
        ma99 = sig['ma99']
        rsi = sig['rsi']
        atr = sig['atr']
        
        # Limpiar nombre del par para el mensaje (ej. BTC/USDT:USDT -> BTC)
        coin_name = symbol.split('/')[0]
        
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

        # Plantilla VIP exacta que pediste
        message = f"""SEÑAL VIP
${coin_name} - {direction} {'📈' if direction == 'LONG' else '📉'}

Plan de Comercio:
• Entrada: {entry_min:.4f} – {entry_max:.4f}
• Stop Loss: {sl:.4f}

Take Profits:
• TP1: {tp1:.4f}
• TP2: {tp2:.4f}
• TP3: {tp3:.4f}

Apalancamiento sugerido: {leverage}

Justificación:
La estructura de 1h mantiene un sesgo claramente {'alcista' if direction == 'LONG' else 'bajista'}, con el precio operando por {'encima' if direction == 'LONG' else 'debajo'} de la MA99 ({ma99:.4f}), lo que valida la tendencia de fondo y respalda la continuidad del movimiento.
El RSI en 1h (~{rsi:.1f}) se encuentra en zona operativa óptima para retrocesos, lo que deja margen para una nueva extensión antes de encontrar resistencia fuerte.
La zona de entrada entre {entry_min:.4f} y {entry_max:.4f} ofrece una relación riesgo/beneficio favorable con Stop Loss bien definido en {sl:.4f}.
La volatilidad medida por el ATR (~{atr:.4f}) muestra un mercado activo en 1h, aumentando las probabilidades de éxito.
TP1 busca capturar el primer movimiento hacia {tp1:.4f}, mientras que TP2 y TP3 apuntan a una extensión hacia {tp2:.4f} y {tp3:.4f}.

⚠️ Mientras el precio permanezca por {'encima' if direction == 'LONG' else 'debajo'} de {sl:.4f}, el escenario {direction} continúa siendo válido.

Sesgo: {'Alcista con potencial de continuidad si se mantiene el soporte.' if direction == 'LONG' else 'Bajista con potencial de continuidad si se mantiene la resistencia.'}

This message was sent automatically with GitHub Actions"""

        send_telegram_message(message)
        print(f"Alerta enviada para {coin_name}. Esperando 10 segundos...")
        time.sleep(10) # Pausa estricta de 10 segundos anti-spam para Telegram

if __name__ == "__main__":
    main()
          
