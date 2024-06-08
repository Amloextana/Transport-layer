from django.http import HttpResponse
from rest_framework.decorators import api_view
from kafka import KafkaConsumer, KafkaProducer
import requests
import threading
import json
import base64 
import time

LINK_LAYER_IP= 'http://192.168.244.162:8000/code/'
APP_LAYER_IP = 'http://192.168.244.96:8080/receive/'
APP_LAYER_IP_reserved = 'http://127.0.0.1:8008/app_layer/'
LINK_LAYER_IP_reserved  = 'http://127.0.0.1:8008/link_layer/'
byte_size = 120
topic = "segments"

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    batch_size=1
)

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='transport_layer',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)



@api_view(['POST'])
def post_segment(request):
    """
        Получение сегмента с канального уровня и помещение в очередь kafka
    """

    request_data = json.loads(request.body)

    print(f"Received from code: {request_data}")
    producer.send(topic, request.data)
    return HttpResponse(status=200) 


@api_view(['POST'])
def transfer_msg(request):
    """
        Получение сообщения с уровня приложений, разбиение его на сегменты и передача сегментов на канальный уровень
    """
    try:
        request_data = json.loads(request.body)
    except json.JSONDecodeError:
        request_data = None
    print(f"Received from app: {request_data}")
    # encode = str => bytes, decode = bytes => str  segment.decode('utf-8')
    message_bytes = request.data['message'].encode('utf-8')
    segments = [message_bytes[i:i+byte_size] for i in range(0, len(message_bytes), byte_size)]

    for index, segment in enumerate(segments):
        segment_data = {
            'segment_number': len(segments) - 1 - index,
            'amount_segments': len(segments),
            'segment_data': base64.b64encode(segment).decode('utf-8'),
            'datetime': request.data['datetime'],
            'user': request.data['user']
        }
        response = requests.post(LINK_LAYER_IP, json=segment_data)
        print('send to link:', segment_data)

    return HttpResponse(status=200)

def send_mesg_to_app_layer(datetime, user, message, isError):

    json_data = {
        "datetime": datetime,
        "user": user,
        "message": message,
        "isError": isError
    }

    print("json to app: ", json_data)
    requests.post(APP_LAYER_IP, json=json_data)


def consumer_thread(consumer):
    segments_dict = dict()
    while True:
         # добавляем полученные сегменты в словарь ключ - время отправки сообщения, значение - массив сегментов 
        for record in consumer:
            segment = record.value
            if segment['datetime'] in segments_dict:
                segments_dict['datetime'].append(segment)
            else:
                segments_dict['datetime'] = segment
        # проверяем собрались ли все сегменты для сообщения
        for message in segments_dict:
            current_message = message['datetime']
            # если да, то собираем сегменты в сообщение и отправляем
            if len(current_message) == current_message[0]['amount_segments']
                sorted_message = sorted(current_message, key=lambda x: x['segment_number'], reverse=True)
                message_to_send = ""
                for i in range(len(sorted_message)):
                    text = sorted_message[i]['segment_data']
                    message_to_send += base64.b64decode(text).decode('utf-8')
                send_mesg_to_app_layer(current_message['datetime'], current_message['user'], message_to_send, 0)
                del current_message
            # если сообщение не собирается более 10 секунд, то возвращаем ошибку
            elif int(time.time()) - current_message['datetime'] > 10:
                send_mesg_to_app_layer(current_message['datetime'], current_message['user'], "Error", 1)
                del current_message       


consumer_thread = threading.Thread(target=consumer_thread, args=(consumer))
consumer_thread.start()
