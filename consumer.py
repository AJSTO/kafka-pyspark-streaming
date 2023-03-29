import pandas_gbq
import pandas as pd
import json

from confluent_kafka import Consumer
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime



PROJECT_ID = 'kafka-training-381817'
DATASET_ID = 'public_transport_warsaw'
BUSES_TABLE = 'buses_data'
TRAMS_TABLE = 'trams_data'
JSON_KEY_PATH = './kafka-training-381817-46823968c033.json'
CREDENTIALS = service_account.Credentials.from_service_account_file(
    JSON_KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=CREDENTIALS.project_id, )

consumer = Consumer(
    {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }
)
consumer.subscribe(['buses_v2'])


if __name__ == '__main__':
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        row = json.loads(msg.value().decode("utf-8"))
        print(
            f'Captured data. {datetime.now()}'
        )
        df = pd.DataFrame(row)
        df['Time'] = df['Time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
        #print(df.dtypes)
        #print(df['Time'][1].to_pydatetime())

        pandas_gbq.to_gbq(df, f'{DATASET_ID}.{BUSES_TABLE}', project_id=f'{PROJECT_ID}',
                          if_exists='append', credentials=CREDENTIALS)



