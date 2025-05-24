import tkinter as tk
from tkinter import simpledialog
import uuid
import argparse
from kafka_chat_client import KafkaChatClient

class GUIChat:
    def __init__(self, root, initial_channel, host, port):
        self.root = root
        base = simpledialog.askstring("Username", "Введите имя пользователя:", parent=root) or "User"
        self.username = f"{base}_{uuid.uuid4().hex[:4]}"
        self.current_channel = initial_channel
        self.bootstrap_servers = f"{host}:{port}"

        self.client = KafkaChatClient(
            bootstrap_servers=self.bootstrap_servers,
            initial_channel=self.current_channel,
            username=self.username,
            income_message_callback=self.display_msg)

        root.title(f"Kafka Chat - {self.current_channel} ({self.username}) [{self.bootstrap_servers}]")
        self.text = tk.Text(root, state='disabled', height=20)
        self.text.pack(fill='both', expand=True, padx=5, pady=5)
        self.entry = tk.Entry(root)
        self.entry.pack(fill='x', padx=5, pady=(0,5))
        self.entry.bind('<Return>', self.send_msg)

        root.protocol("WM_DELETE_WINDOW", self.on_close)

    def display_msg(self, message_data):
        print("ffdf")
        if message_data.get('username') == self.username:
            return
        formatted = f"{message_data.get('username')}: {message_data.get('message')}"
        self.text.config(state='normal')
        self.text.insert('end', formatted + '\n')
        self.text.see('end')
        self.text.config(state='disabled')

    def display_info(self, info_text):
        self.text.config(state='normal')
        self.text.insert('end', f"[INFO] {info_text}\n")
        self.text.see('end')
        self.text.config(state='disabled')

    def send_msg(self, event=None):
        text = self.entry.get().strip()
        if not text:
            return
        if text.startswith('!switch '):
            new_channel = text[len('!switch '):].strip()
            if new_channel:
                self.client.switch_channel(new_channel)
                self.current_channel = new_channel
                self.root.title(f"Kafka Chat - {self.current_channel} ({self.username}) [{self.bootstrap_servers}]")
                self.display_info(f"Switched to channel: {new_channel}")
        else:
            self.client.send_message(text)
            self.text.config(state='normal')
            self.text.insert('end', f"{self.username}: {text}\n")
            self.text.see('end')
            self.text.config(state='disabled')
        self.entry.delete(0, 'end')

    def on_close(self):
        try:
            self.client.close()
        except Exception:
            pass
        self.root.destroy()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1', help='Kafka broker host')
    parser.add_argument('--port', type=int, default=29092, help='Kafka broker port')
    parser.add_argument('--channel', default='general', help='Initial channel name')
    args = parser.parse_args()

    root = tk.Tk()
    GUIChat(root, initial_channel=args.channel, host=args.host, port=args.port)
    root.mainloop()
