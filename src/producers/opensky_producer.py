import os
import time
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'raw.traffic.opensky'
OPENSKY_URL = 'https://opensky-network.org/api/states/all'

OPENSKY_USER = os.getenv('OPENSKY_USER')
OPENSKY_SECRET = os.getenv('OPENSKY_SECRET')

# Инициализация Kafka Producer
producer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': 'opensky_producer',
    'linger.ms': 100,  # Небольшая задержка для буферизации пакетов (минимизирует нагрузку на сеть)
    'compression.type': 'gzip',
    'acks': 1
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Ошибка доставки сообщения: {err}")

def fetch_and_send():
    print(f" [{datetime.now().strftime('%H:%M:%S')}] Запрос данных из OpenSky API...")
    
    try:
        response = requests.get(OPENSKY_URL, auth=(OPENSKY_USER, OPENSKY_SECRET), timeout=10)
        
        if response.status_code != 200:
            print(f"⚠️ Ошибка API OpenSky: Код {response.status_code}")
            return

        data = response.json()
        states = data.get('states', [])
        
        if not states:
            print("🛬 В небе сейчас пусто (или API вернуло пустой массив).")
            return

        print(f"✈️ Найдено {len(states)} самолетов. Отправка в Kafka...")

        # Документация OpenSky: https://openskynetwork.github.io/opensky-api/rest.html
        for state in states:
            icao24 = state[0]
            
            # Формируем плоский JSON
            aircraft_msg = {
                "source": "opensky",
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "payload": {
                    "icao24": icao24,
                    "callsign": state[1],
                    "origin_country": state[2],
                    "time_position": state[3],
                    "longitude": state[5],
                    "latitude": state[6],
                    "baro_altitude": state[7],
                    "on_ground": state[8],
                    "velocity": state[9],
                    "true_track": state[10],
                    "vertical_rate": state[11]
                }
            }

            # Сериализуем в строку и отправляем в Kafka
            payload_bytes = json.dumps(aircraft_msg).encode('utf-8')
            
            # Ключ (icao24) гарантирует, что трек одного самолета пойдет в одну партицию
            producer.produce(
                topic=TOPIC_NAME,
                key=icao24.encode('utf-8'),
                value=payload_bytes,
                callback=delivery_report
            )

        # Сбрасываем буфер, дожидаясь отправки всех сообщений партии
        producer.flush()
        print(f"✅ Успешно отправлено {len(states)} сообщений.")

    except Exception as e:
        print(f"❌ Критическая ошибка во время итерации: {e}")

if __name__ == '__main__':
    print("🚀 Продюсер OpenSky запущен.")
    
    INTERVAL = 45 
    
    while True:
        fetch_and_send()
        time.sleep(INTERVAL)