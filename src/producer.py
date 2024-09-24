import os
import yfinance as yf
import time
from kafka import KafkaProducer
import json
from dotenv import load_dotenv

load_dotenv()

SYMBOL = 'AAPL'

def get_data():
    stock_data = yf.download(SYMBOL, period="1d", interval="1m")
    if stock_data.empty:
        print("No data retrieved")
        return None

    latest_data = stock_data.iloc[-1]  
    return {
        'timestamp': latest_data.name.strftime('%Y-%m-%d %H:%M:%S'),
        'stock_symbol': SYMBOL,
        'open': latest_data['Open'],
        'high': latest_data['High'],
        'low': latest_data['Low'],
        'close': latest_data['Close'],
        'volume': latest_data['Volume']
    }

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

if __name__ == "__main__":
    while True:
        data = get_data()
        if data:
            producer.send('test', value=data)
            print(f"Sent: {data}")
        time.sleep(60)
