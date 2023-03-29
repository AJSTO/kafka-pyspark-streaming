import json

import requests
import time
from datetime import datetime
from confluent_kafka import Producer


api_key = 'ea5652d1-d201-4150-834c-6c1ec5b18880'
resource_id = 'f2e5503e-927d-4ad3-9500-4ab9e55deb59'

tram_url = f"https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id={resource_id}&apikey={api_key}&type=2"
bus_url = f"https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id={resource_id}&apikey={api_key}&type=1"

producer = Producer(
    {
        'bootstrap.servers': 'localhost:9092'
    }
)

if __name__ == '__main__':
    while True:
        # Generate informations
        bus_info = requests.get(bus_url).json()['result']
        tram_info = requests.get(tram_url).json()['result']
        # Send it to topic
        print(
            f'Produce data @ {datetime.now()}.'
        )
        producer.produce('buses_v2', json.dumps(bus_info))
        producer.produce('trams', json.dumps(tram_info))

        # Sleep
        time.sleep(60)

