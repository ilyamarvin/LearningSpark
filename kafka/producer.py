from confluent_kafka import Producer
import clickhouse_connect
import json

f = open('secrets.json')
data = json.load(f)

config = {
    'bootstrap.servers': '192.168.47.146:29092',
    'client.id': 'simple-producer',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'PLAINTEXT'
}

producer = Producer(**config)


def get_data(data):
    client = clickhouse_connect.get_client(host=data['host'],
                                           port=data['port'],
                                           username=data['username'],
                                           password=data['password'])

    # Берем 100 строк из таблицы assembly_task_issued
    query_result = client.query('''
        select *
        from assembly_task_issued
        where issued_dt > now() - interval 1 minute
        order by issued_dt
        limit 100
    ''')

    client.close()

    return query_result


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def send_message(data):
    try:
        # Асинхронная отправка сообщения
        producer.produce('data_from_clickhouse', data.encode('utf-8'), callback=delivery_report)
        producer.poll(0)  # Поллинг для обработки обратных вызовов
    except BufferError:
        print(f"Local producer queue is full ({len(producer)} messages awaiting delivery): try again")


if __name__ == '__main__':
    # получаем результат
    query_result = get_data(data)
    # Делаем объединение, чтобы понимать какие это колонки и построчно отправляем в кафку
    for row in query_result.result_set:
        message = json.dumps(dict(zip(query_result.column_names, row)), default=str)
        send_message(message)
    producer.flush()