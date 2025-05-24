# hse-sd-cw

## Описание репозитория

Консольный и GUI-клиент для чата, использующего Apache Kafka.

### Возможности

* Переключение каналов командой `!switch <channel>`

* Получение только новых сообщений (без истории)

* Автоматическое создание каналов (Kafka topics)

Подробнее с требованиями можно ознакомиться `docs/requirements.md`

## Требования

- Python 3.8+
- Apache Kafka (запущен локально или удаленно)
- Установленные зависимости:

```bash
pip install -r requirements.txt
```

### Kafka

Для запуска Kafka-сервера рекомендуем воспользоваться предложенным docker-compose.yml

```bash
docker compose up --build
```

Тогда адрес кафка-сервера = 127.0.0.1:29092

## Запуск

### CLI клиент:

```bash
python lib/cli_client.py [--host HOST] [--port PORT] [--channel CHANNEL]
```

### GUI клиент:

```bash
python lib/gui_client.py [--host HOST] [--port PORT] [--channel CHANNEL]
```

### Defaults:
- `--host 127.0.0.1`
- `--port 29092`
- `--channel general`

## Запуск тестов

Из директории проекта выполните:

```bash
python -m unittest discover tests/
```
