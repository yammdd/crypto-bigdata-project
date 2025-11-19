import asyncio
import websockets
import json
import time
from kafka import KafkaProducer

BINANCE_WS = "wss://stream.binance.com:9443/ws"
SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt", "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"]

producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

async def produce():
    streams = '/'.join([f"{symbol}@ticker" for symbol in SYMBOLS])
    url = f"{BINANCE_WS}/{streams}"

    async with websockets.connect(url) as ws:
        print("Crypto producer running on Binance WebSocket...")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                output = {
                    "source": "binance",
                    "category": "crypto",
                    "symbol": data['s'].lower(),
                    "price": float(data['c']),
                    "price_change": float(data['p']),
                    "price_change_pct": float(data['P']),
                    "high_price": float(data['h']),
                    "low_price": float(data['l']),
                    "open_price": float(data['o']),
                    "volume_token": float(data['v']),
                    "volume_quote": float(data['q']),
                    "best_bid_price": float(data['b']),
                    "best_ask_price": float(data['a']),
                    "timestamp": int(data['E'] // 1000)  
                }

                producer.send('crypto_prices', output)
                print(f"Sent: {output}")

            except Exception as e:
                print("Error:", e)
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(produce())