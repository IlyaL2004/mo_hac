from kafka import KafkaConsumer
import json
from typing import Callable, Optional
from ..storage.minio_client import MinioClient
from ..processing.ml_processor import MLProcessor
from ..database.clickhouse_client import ClickHouseClient


class KafkaDebeziumConsumer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.minio_client = MinioClient()
        self.ml_processor = MLProcessor()
        self.clickhouse_client = ClickHouseClient()

    def create_consumer(self, topic: str, group_id: str = 'debezium-file-processor-ml'):
        """Создает Kafka consumer"""
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id=group_id
        )

    def process_message(self, message: dict) -> bool:
        """
        Обрабатывает сообщение из Kafka

        Args:
            message: Сообщение из Kafka

        Returns:
            bool: Успешность обработки
        """
        try:
            value = message.value
            after_data = value.get('payload', {}).get('after')

            if not after_data:
                return False

            file_id = after_data.get('id')
            file_name = after_data.get('file_name')
            s3_path = after_data.get('path_file_s3')

            print(f"\nОБРАБОТКА НОВОГО ФАЙЛА:")
            print(f"  ID: {file_id}")
            print(f"  Файл: {file_name}")
            print(f"  S3 путь: {s3_path}")

            ts = after_data.get('upload_timestamp')
            if ts:
                from datetime import datetime
                dt = datetime.fromtimestamp(ts / 1000000)
                print(f"  Время загрузки: {dt}")

            print(f"  Статус: {after_data.get('status')}")

            if s3_path:
                print(f"Ищу JSON файлы в MinIO...")
                success = self.minio_client.download_and_process_json_files(
                    s3_path, self.clickhouse_client
                )
                return success
            else:
                print("Путь к файлу в S3 отсутствует")
                return False

        except Exception as e:
            print(f"Ошибка при обработке сообщения: {e}")
            return False

    def start_consuming(self, topic: str = 'pgserver.public.file_metadata'):
        """Запускает потребитель"""
        print("\n" + "=" * 70)
        print("ЗАПУСК УМНОГО ПОТРЕБИТЕЛЯ С ML МОДЕЛЬЮ И CLICKHOUSE")
        print("=" * 70)

        # Подключаемся к ClickHouse
        if not self.clickhouse_client.connect():
            print("Не удалось подключиться к ClickHouse. Прекращаем работу.")
            return

        if not self.clickhouse_client.test_connection():
            print("Тестирование подключения не удалось. Прекращаем работу.")
            return

        # Создаем consumer
        self.create_consumer(topic)
        print("Ожидание сообщений...\n")

        processed_files = set()

        for message in self.consumer:
            try:
                if self.process_message(message):
                    file_id = message.value.get('payload', {}).get('after', {}).get('id')
                    if file_id:
                        processed_files.add(file_id)
            except Exception as e:
                print(f"Ошибка при обработке сообщения: {e}")

    def close(self):
        """Закрывает consumer и соединения"""
        if self.consumer:
            self.consumer.close()
        self.clickhouse_client.close()