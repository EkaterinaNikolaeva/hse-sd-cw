import unittest
from unittest.mock import patch, MagicMock
from lib.kafka_chat_client import KafkaChatClient
import json
import time

class TestKafkaChatClient(unittest.TestCase):
    def setUp(self):
        patcher_admin = patch('lib.kafka_chat_client.AdminClient')
        patcher_producer = patch('lib.kafka_chat_client.Producer')
        patcher_consumer = patch('lib.kafka_chat_client.Consumer')

        self.mock_admin = patcher_admin.start()
        self.mock_producer = patcher_producer.start()
        self.mock_consumer = patcher_consumer.start()

        self.addCleanup(patcher_admin.stop)
        self.addCleanup(patcher_producer.stop)
        self.addCleanup(patcher_consumer.stop)

        self.mock_admin_instance = MagicMock()
        self.mock_producer_instance = MagicMock()
        self.mock_consumer_instance = MagicMock()

        self.mock_admin.return_value = self.mock_admin_instance
        self.mock_producer.return_value = self.mock_producer_instance
        self.mock_consumer.return_value = self.mock_consumer_instance

        self.mock_admin_instance.list_topics.return_value.topics = {}

        self.received_messages = []

        fake_msg = MagicMock()
        fake_msg.value.return_value = json.dumps({
            'username': 'someone',
            'channel': 'test-channel',
            'message': 'hello',
            'timestamp': int(time.time() * 1000)
        }).encode('utf-8')

        fake_msg.error.return_value = None

        self.mock_consumer_instance.poll.side_effect = [fake_msg, None, None]

        self.client = KafkaChatClient(
            bootstrap_servers='localhost:9092',
            initial_channel='test-channel',
            username='test-user',
            income_message_callback=self.received_messages.append
        )

    def test_send_message(self):
        self.client.send_message("hello world")
        self.mock_producer_instance.produce.assert_called()
        self.mock_producer_instance.flush.assert_called()

        _, kwargs = self.mock_producer_instance.produce.call_args
        sent_value = json.loads(kwargs['value'].decode('utf-8'))
        self.assertEqual(sent_value['username'], 'test-user')
        self.assertEqual(sent_value['channel'], 'test-channel')
        self.assertEqual(sent_value['message'], 'hello world')
        self.assertIn('timestamp', sent_value)

    def test_switch_channel(self):
        self.mock_admin_instance.list_topics.return_value.topics = {}
        self.client.switch_channel("new-channel")

        self.mock_consumer_instance.unsubscribe.assert_called()
        self.mock_consumer_instance.subscribe.assert_called_with(["new-channel"])
        self.assertEqual(self.client.current_channel, "new-channel")

    def tearDown(self):
        self.client.close()


if __name__ == '__main__':
    unittest.main()
