from datetime import datetime
import requests
import json
import time

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'airscholar',
    'start_date' : datetime(2024, 5, 5, 10, 00 )
}


def get_sp500_data():
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/auto-complete"
    querystring = {"q": "GSPC", "region": "EUR"}
    headers = {
        "X-RapidAPI-Key": "da5702d33amsh515a2d6daabbd72p10862fjsn6e8df468787a",
        "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print("Error fetching data:", e)
        return None

def manipulate_data(data):
    if data is not None:
        if 'quotes' in data and data['quotes']:
            symbol = data['quotes'][0]['shortname']
            score = data['quotes'][0]['score']
            news_items = data.get('news', [])
            news_titles = [item.get('title', '') for item in news_items]
            return symbol, score, news_titles
        else:
            print("No quotes found in data.")
            return None
    else:
        return None


def stream_data():
    
    from kafka import KafkaProducer
    import time
    
    sp500_data = get_sp500_data()
    manipulated_data = manipulate_data(sp500_data)
    
    # Get the current datetime and format it
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create a dictionary with the data and datetime
    data_dict = {
        "symbol": manipulated_data[0],
        "score": manipulated_data[1],
        "datetime": current_datetime,
        "news_titles": manipulated_data[2]
    }
    
    #res = json.dumps(data_dict)
    #print(json.dumps(data_dict, indent=3))
    #return res
    #print(json.dumps(data_dict, indent=3))

    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], max_block_ms = 5000)
    
    producer.send('users_created', json.dumps(data_dict).encode('utf-8'))
    
    
    



# with DAG('user_automation',
#          default_args = default_args,
#          schedule_interval = '@daily',
#          catchup = False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id = 'stream_data_from_api',
#         python_callable = stream_data
#     )
    
stream_data()