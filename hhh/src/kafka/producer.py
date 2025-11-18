from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import json
import hashlib
import time
from datetime import datetime
from typing import Optional, Dict, Any
from ..database.postgres_client import PostgresClient


class KafkaFileProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            batch_size=32768,
            linger_ms=100,
            compression_type=None,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda v: str(v).encode('utf-8') if v else None,
            acks='all',
            request_timeout_ms=60000,
            retries=10,
            retry_backoff_ms=1000,
            max_block_ms=120000,
            buffer_memory=67108864,
        )
        self.global_send_counter = 0
        self.postgres_client = PostgresClient()

    def get_next_send_id(self) -> int:
        """Возвращает следующий глобальный номер отправки"""
        self.global_send_counter += 1
        return self.global_send_counter

    def create_message_with_schema(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Создает сообщение с JSON-схемой для Kafka Connect"""
        schema = {
            "type": "struct",
            "fields": [
                {"type": "string", "optional": False, "field": "inn"},
                {"type": "string", "optional": False, "field": "file_name"},
                {"type": "string", "optional": False, "field": "file_extension"},
                {"type": "string", "optional": False, "field": "file_data"},
                {"type": "string", "optional": False, "field": "file_hash"},
                {"type": "int32", "optional": False, "field": "file_size"},
                {"type": "string", "optional": False, "field": "processing_date"},
                {"type": "string", "optional": False, "field": "timestamp"},
                {"type": "string", "optional": False, "field": "path_s3"},
                {"type": "string", "optional": True, "field": "parsed_content"},
                {"type": "string", "optional": True, "field": "file_type"}
            ],
            "optional": False,
            "name": "file_chunk"
        }

        return {
            "schema": schema,
            "payload": payload
        }

    def send_message_with_confirmation(self, topic: str, key: str, value: dict, timeout: int = 30) -> bool:
        """Отправляет сообщение с подтверждением доставки"""
        max_retries = 3
        attempt = 0

        while attempt < max_retries:
            try:
                future = self.producer.send(topic, key=key, value=value)
                record_metadata = future.get(timeout=timeout)
                print(
                    f"Сообщение доставлено в {record_metadata.topic} [{record_metadata.partition}] по смещению {record_metadata.offset}")
                return True
            except KafkaTimeoutError as e:
                attempt += 1
                print(f"Попытка {attempt} не удалась из-за таймаута: {e}")
                if attempt < max_retries:
                    print(f"Повторная попытка через {attempt} секунд...")
                    time.sleep(attempt)
                else:
                    print(f"Не удалось отправить сообщение после {max_retries} попыток: {e}")
                    return False
            except KafkaError as e:
                print(f"Ошибка Kafka: {e}")
                return False
            except Exception as e:
                print(f"Непредвиденная ошибка: {e}")
                return False
        return False

    def process_file(self, file_path: str, inn: str) -> bool:
        """
        Обрабатывает файл и отправляет его в Kafka

        Args:
            file_path: Путь к файлу
            inn: ИНН организации

        Returns:
            bool: Успешность операции
        """
        import os
        from ..processing.file_processor import FileProcessor

        file_processor = FileProcessor()
        file_content = file_processor.extract_file_content(file_path)

        if not file_content:
            return False

        file_name = os.path.basename(file_path)
        file_extension = file_name.split('.')[-1] if '.' in file_name else 'bin'

        with open(file_path, 'rb') as f:
            file_data = f.read()

        file_hash = hashlib.md5(file_data).hexdigest()
        file_size = len(file_data)

        send_id = self.get_next_send_id()
        current_datetime = datetime.now()
        current_date = current_datetime.strftime("%Y-%m-%d")

        print(f"Обработка {file_name} для ИНН {inn}, размер файла: {file_size} байт")
        print(f"Глобальный ID отправки: {send_id}")

        path_s3 = f"/data/{current_date}/inn_{inn}_send_{send_id}/{file_name}"

        payload = {
            "inn": inn,
            "file_name": file_name,
            "file_extension": file_extension,
            "file_data": file_data.hex(),
            "file_hash": file_hash,
            "file_size": file_size,
            "processing_date": current_date,
            "timestamp": current_datetime.isoformat(),
            "path_s3": path_s3,
            "parsed_content": json.dumps(file_content, ensure_ascii=False),
            "file_type": file_content['file_type']
        }

        message = self.create_message_with_schema(payload)
        key = f"{inn}_{current_date}_{send_id}"

        if self.send_message_with_confirmation('file-chunks-topic', key, message):
            print(f"Файл {file_name} успешно отправлен. Сохраняем метаданные в PostgreSQL.")

            # Подключаемся к PostgreSQL если нужно
            if not self.postgres_client.connection:
                self.postgres_client.connect()

            if self.postgres_client.save_file_metadata(file_name, path_s3, current_datetime):
                print(f"Файл {file_name} успешно обработан (Глобальный ID отправки: {send_id})")
                return True
            else:
                print(f"Не удалось сохранить метаданные для {file_name}")
                return False
        else:
            print(f"Не удалось отправить файл {file_name}")
            return False

    def close(self):
        """Закрывает продюсер и соединения"""
        if self.producer:
            self.producer.flush(timeout=30)
            self.producer.close()
        self.postgres_client.close()