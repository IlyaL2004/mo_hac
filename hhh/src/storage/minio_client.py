import boto3
from botocore.client import Config
import json
from typing import List, Optional, Tuple
from ..processing.ml_processor import MLProcessor
from ..processing.file_processor import FileProcessor
import re


class MinioClient:
    """Клиент для работы с MinIO/S3"""

    def __init__(self):
        self.client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            config=Config(signature_version='s3v4'),
            verify=False
        )

    def parse_s3_path(self, s3_path: str) -> Tuple[str, str]:
        """Парсит S3 путь и извлекает бакет и ключ - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        print(f"Парсинг S3 пути: {s3_path}")

        # Всегда используем дефолтный bucket
        bucket = "moscow-industry-data"

        # В оригинальном коде файлы сохраняются по пути:
        # topics/file-chunks-topic/path_s3=/data/2025-11-19/inn_111111111111_send_3/taxes_data.csv
        # Поэтому нам нужно найти все файлы в этой структуре

        # Извлекаем дату и inn из пути
        path_parts = s3_path.strip('/').split('/')
        if len(path_parts) >= 4:
            date_part = path_parts[1]  # 2025-11-19
            inn_send_part = path_parts[2]  # inn_111111111111_send_3
            file_name = path_parts[3]  # taxes_data.csv

            # Формируем префикс для поиска в MinIO
            search_prefix = f"topics/file-chunks-topic/path_s3=/data/{date_part}/{inn_send_part}/"
            print(f"Сформирован префикс для поиска: {search_prefix}")
            return bucket, search_prefix
        else:
            # Если не можем разобрать путь, используем общий поиск
            search_prefix = "topics/file-chunks-topic/path_s3=/data/"
            print(f"Используем общий префикс для поиска: {search_prefix}")
            return bucket, search_prefix

    def find_json_files_in_folder(self, bucket: str, search_prefix: str) -> List[str]:
        """Находит все JSON файлы в указанной папке - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            print(f"Поиск JSON файлов в bucket: '{bucket}', префикс: '{search_prefix}'")

            # Проверим, существует ли bucket
            try:
                self.client.head_bucket(Bucket=bucket)
            except Exception as e:
                print(f"Bucket '{bucket}' не существует или недоступен: {e}")
                return []

            # Ищем все файлы с указанным префиксом
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=search_prefix)

            json_files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # В MinIO файлы сохраняются без расширения .json, но содержат JSON данные
                    # Берем все файлы, независимо от расширения
                    json_files.append(obj['Key'])
                    print(f"Найден файл: {obj['Key']}")

            print(f"Найдено файлов: {len(json_files)}")
            return json_files
        except Exception as e:
            print(f"Ошибка при поиске файлов: {e}")
            return []

    def extract_file_data_from_json(self, json_content: str) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        """Извлекает данные файла из JSON структуры"""
        try:
            data = json.loads(json_content)
            payload = data.get('payload', data)

            file_data = None
            file_name = None
            file_extension = None

            if 'file_data' in payload and payload['file_data']:
                try:
                    hex_data = payload['file_data']
                    file_data = bytes.fromhex(hex_data)
                    print("Данные файла извлечены из file_data (hex)")
                except Exception as e:
                    print(f"Ошибка декодирования hex: {e}")

            if 'file_name' in payload:
                file_name = payload['file_name']
                if '.' in file_name:
                    file_extension = file_name.split('.')[-1].lower()
                else:
                    file_extension = 'bin'

            return file_data, file_name, file_extension

        except Exception as e:
            print(f"Ошибка при извлечении данных из JSON: {e}")
            return None, None, None

    def download_and_process_json_files(self, s3_path: str, clickhouse_client) -> bool:
        """Скачивает и обрабатывает все JSON файлы - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            print(f"Обработка S3 пути: {s3_path}")

            # Парсим путь для получения bucket и префикса поиска
            bucket, search_prefix = self.parse_s3_path(s3_path)

            # Ищем файлы
            json_files = self.find_json_files_in_folder(bucket, search_prefix)

            if not json_files:
                print(f"В bucket {bucket} по префиксу {search_prefix} не найдено файлов")
                return False

            processed_count = 0
            ml_processed_count = 0

            ml_processor = MLProcessor()
            file_processor = FileProcessor()

            for json_file in json_files:
                print(f"\nОбрабатываю файл: {json_file}")

                try:
                    response = self.client.get_object(Bucket=bucket, Key=json_file)
                    json_content = response['Body'].read().decode('utf-8')

                    file_data, file_name, file_extension = self.extract_file_data_from_json(json_content)

                    if file_data and file_name:
                        print(f"Найден файл: {file_name} (тип: {file_extension})")

                        # Обрабатываем файл с ML моделью
                        inn = file_processor.extract_inn_from_filename(file_name)
                        insert_data = ml_processor.process_file_data_to_sql(
                            file_data, file_name, file_extension, inn
                        )

                        if insert_data:
                            if clickhouse_client.insert_data(insert_data):
                                processed_count += 1
                                ml_processed_count += 1
                                print(f"Файл {file_name} успешно обработан и загружен в ClickHouse")
                            else:
                                print(f"Не удалось вставить данные в ClickHouse для файла {file_name}")
                        else:
                            print(f"ML модель не сгенерировала данные для файла {file_name}")
                    else:
                        print(f"Не удалось извлечь данные файла из {json_file}")

                except Exception as e:
                    print(f"Ошибка при обработке файла {json_file}: {e}")

            print(f"\nИТОГ:")
            print(f"  Обработано: {processed_count} из {len(json_files)} файлов")
            print(f"  Успешно обработано ML моделью: {ml_processed_count} файлов")
            return processed_count > 0

        except Exception as e:
            print(f"Ошибка при обработке файлов: {e}")
            return False