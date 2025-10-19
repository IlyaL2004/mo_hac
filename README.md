1. pip install -r requirements.txt
2. Открываем wsl, если windows
2. docker-compose up -d
3. Бакет создать
переходим http://localhost:9001
жмем на плюс в верхнем левом углу
вводим название moscow-industry-data
4. curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @chan-updated-connector.json

5. в другом терминале python debezium_consumer.py
6. в другом терминале uvicorn kafka_connect_producer:app --reload --host 0.0.0.0 --port 8001

# Проверить топики
ilya@DESKTOP-2T73313:/mnt/c/Users/ilyal/PycharmProjects/hacc/hhh$ docker exec -it kafka-broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --list
__consumer_offsets
_connect-configs
_connect-offsets
_connect-status
connect-configs
connect-offsets
connect-status
file-chunks-topic
pgserver.public.file_metadata

7. test_data/taxes_data.csv
8. url = "https://jsonplaceholder.typicode.com/posts/1"

