import json
import pika
from ExchangeRateScraping import fetchCurrencyData   
from ExchangeRateScraping import sendToQueue 


def callback(ch, method, properties, body):
     message = json.loads(body)
     datas=fetchCurrencyData("https://xn--dviz-5qa.com/",message['ExchangeType'])    
     print(datas)   

def consume_from_queue(queue_name, amqp_url):
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()

    # Kuyruğa direkt bağlan ve mesajları tüket
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    print(f"Waiting for messages from queue '{queue_name}'. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    queue_name = 'CreateExchangeRateQueue'  
    amqp_url = 'amqps://yjmbpqsn:omJw2L1EuhX1D2JVvfneg_ZC8W1mqkBz@fish.rmq.cloudamqp.com/yjmbpqsn'
    consume_from_queue(queue_name, amqp_url)
