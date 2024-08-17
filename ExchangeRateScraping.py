import requests
from bs4 import BeautifulSoup
import pika
import json

url = "https://xn--dviz-5qa.com/"
rabbitmq_url = "amqps://yjmbpqsn:omJw2L1EuhX1D2JVvfneg_ZC8W1mqkBz@fish.rmq.cloudamqp.com/yjmbpqsn"
queue_name = 'instant_currency_rate_queue'

def fetchCurrencyData(url, currency_name=None):
    try:
        response = requests.get(url)
        response.raise_for_status()
        html_content = response.content
    except requests.exceptions.RequestException as e:
        print(f"HTTP error occurred: {e}")
        return None

    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {'class': 'table table-striped table-sm'})

    if table is None:
        print("Table is not found.")
        return None

    rows = table.select('tbody > tr')

    for row in rows:
        cols = row.find_all('td')
        currency = cols[1].text.strip()

        if currency ==currency_name:
            buy_price = cols[2].text.strip()
            sell_price = cols[3].text.strip()

            currentlyDict = {
            'Currency': currency,
            'BuyPrice': buy_price,
            'SellPrice': sell_price
                }
        
            sendToQueue(currentlyDict, rabbitmq_url, queue_name)    
            return currentlyDict

    print("Currency is not found.") 
    return None


# Send to RabbitMQ queue
def sendToQueue(data, rabbitmq_url, queue_name):
    try:
        connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        channel = connection.channel()

        channel.queue_declare(queue=queue_name, durable=True)

        message = json.dumps(data)
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=1,  
            )
        )
        print(f"Sent message to {queue_name}")
    except Exception as e:
        print(f"Failed to send message to RabbitMQ: {e}")
    finally:
        if connection:
            connection.close()
