from confluent_kafka import Consumer, Producer, KafkaError
import uuid
import json
import threading
import time
from confluent_kafka.admin import AdminClient, NewTopic


class KafkaChatClient:
    def __init__(self, bootstrap_servers='127.0.0.1:29092', initial_channel='general', username='anon',
                 income_message_callback=None):
        """
        Инициализация чат-клиента Kafka

        :param bootstrap_servers: Адрес сервера Kafka
        :param initial_channel: Начальный канал для подключения
        :param username: Имя пользователя
        :param income_message_callback: Функция обратного вызова для входящих сообщений
        """
        self.bootstrap_servers = bootstrap_servers
        self.username = username
        self.current_channel = initial_channel
        self.consumer = None
        self.producer = None
        self.admin_client = None
        self.running = False
        self.consumer_thread = None
        self.income_message_callback = income_message_callback

        self.consumer_group_id = f"chat-client-{uuid.uuid4()}"

        self._init_admin_client()
        self._init_producer()
        self._init_consumer()

        self._create_topic_if_not_exists(initial_channel)
        self.switch_channel(initial_channel)
        self.consumer.subscribe([self.current_channel])
        self._start_consuming()

    def _init_admin_client(self):
        self.admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})

    def _init_producer(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f'chat-producer-{self.username}'
        }
        self.producer = Producer(conf)

    def _init_consumer(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.consumer_group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(conf)

    def _create_topic_if_not_exists(self, topic_name):
        topic_metadata = self.admin_client.list_topics(timeout=5)
        if topic_name not in topic_metadata.topics:
            new_topic = NewTopic(
                topic_name,
                num_partitions=1,
                replication_factor=1
            )
            fs = self.admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Создан новый топик: {topic}")
                except Exception as e:
                    print(f"Ошибка при создании топика {topic}: {e}")

    def _start_consuming(self):
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_messages)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()

    def _consume_messages(self):
        try:
            while self.running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Ошибка потребителя: {msg.error()}")
                        break

                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    if self.income_message_callback:
                        self.income_message_callback(message_data)
                except Exception as e:
                    print(f"Ошибка обработки сообщения: {e}")

        except Exception as e:
            print(f"Ошибка в потоке потребления: {e}")
        finally:
            self.consumer.close()

    def send_message(self, message):
        message_data = {
            'username': self.username,
            'channel': self.current_channel,
            'message': message,
            'timestamp': int(time.time() * 1000)
        }

        try:
            self.producer.produce(
                topic=self.current_channel,
                value=json.dumps(message_data).encode('utf-8'),
                callback=lambda err, msg: print(f"Ошибка доставки: {err}") if err else None
            )
            self.producer.flush()
        except Exception as e:
            print(f"Ошибка отправки сообщения: {e}")

    def switch_channel(self, new_channel):
        if new_channel == self.current_channel:
            return

        self._create_topic_if_not_exists(new_channel)

        self.consumer.unsubscribe()

        self.current_channel = new_channel

        self.consumer.subscribe([new_channel])

        if not self.running:
            self._start_consuming()

    def close(self):
        self.running = False
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=1.0)

        if self.producer:
            self.producer.flush()
            self.producer = None

        if self.consumer:
            self.consumer.close()
            self.consumer = None

        if self.admin_client:
            self.admin_client = None