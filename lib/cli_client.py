import argparse
import signal
from kafka_chat_client import KafkaChatClient
from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout
import uuid

class Chat:
    def __init__(self, username, channel, bootstrap_servers):
        self.name = username
        self.topic = channel
        self.running = True
        self.session = PromptSession()

        self.client = KafkaChatClient(
            bootstrap_servers=bootstrap_servers,
            initial_channel=channel,
            username=username,
            income_message_callback=self.parse_message,
        )

    def parse_message(self, msg):
        if msg["username"] == self.name:
            return
        print(f"[{msg['channel']}:{msg['timestamp']}] {msg['username']} :: {msg['message']}")

    def produce_loop(self):
        print(f"Type your messages below (Ctrl+C to exit):")
        while self.running:
            try:
                with patch_stdout():
                    text = self.session.prompt(f"[{self.topic}] > ")
                if not text.strip():
                    continue
                text = text.strip()

                if text.startswith("!switch"):
                    parts = text.split(maxsplit=1)
                    if len(parts) < 2 or not parts[1].strip():
                        print("Usage: !switch <channel>")
                        continue
                    self.topic = parts[1].strip()
                    self.client.switch_channel(self.topic)
                    continue

                if text == "!exit":
                    self.stop()
                    break

                self.client.send_message(message=text)
            except (EOFError, KeyboardInterrupt):
                break


    def stop(self):
        if self.running:
            print(f"\n[{self.name}] Shutting down...")
            self.running = False
            self.client.close()

def main():
    parser = argparse.ArgumentParser(description="Kafka CLI Chat Client")
    parser.add_argument('--username', default="anon", help='Username for chat')
    parser.add_argument('--host', default='127.0.0.1', help='Kafka broker host')
    parser.add_argument('--port', type=int, default=29092, help='Kafka broker port')
    parser.add_argument('--channel', default='general', help='Initial channel name')

    args = parser.parse_args()

    bootstrap_servers = f"{args.host}:{args.port}"

    chat = Chat(
        username=f"{args.username}_{uuid.uuid4().hex[:4]}",
        channel=args.channel,
        bootstrap_servers=bootstrap_servers
    )

    signal.signal(signal.SIGINT, lambda *_: chat.stop())

    chat.produce_loop()
    chat.stop()

if __name__ == "__main__":
    main()
