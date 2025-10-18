from kafka import KafkaConsumer
import json
from datetime import datetime
import boto3
from botocore.client import Config
import io
import csv
import pandas as pd
from openpyxl import load_workbook
import docx
import PyPDF2
import chardet


def get_minio_client():
    """Создает клиент для работы с MinIO"""
    return boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4'),
        verify=False
    )


def parse_s3_path(s3_path):
    """Парсит S3 путь и извлекает бакет и ключ"""
    if s3_path.startswith('s3a://'):
        s3_path = s3_path[6:]
    elif s3_path.startswith('s3://'):
        s3_path = s3_path[5:]

    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    return bucket, key


def find_json_files_in_folder(minio_client, bucket, folder_path):
    """Находит все JSON файлы в указанной папке"""
    try:
        if not folder_path.endswith('/'):
            folder_path += '/'

        print(f" Поиск JSON файлов в папке: {folder_path}")
        response = minio_client.list_objects_v2(Bucket=bucket, Prefix=folder_path)

        json_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'):
                    json_files.append(obj['Key'])
                    print(f" Найден JSON файл: {obj['Key']}")

        return json_files
    except Exception as e:
        print(f" Ошибка при поиске JSON файлов: {e}")
        return []


def detect_encoding(file_bytes):
    """Определяет кодировку файла"""
    try:
        result = chardet.detect(file_bytes)
        encoding = result['encoding'] if result['encoding'] else 'utf-8'
        confidence = result['confidence']
        print(f" Определена кодировка: {encoding} (уверенность: {confidence:.2%})")
        return encoding
    except:
        return 'utf-8'


def extract_file_data_from_json(json_content):
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
                print(" Данные файла извлечены из file_data (hex)")
            except Exception as e:
                print(f" Ошибка декодирования hex: {e}")

        if 'file_name' in payload:
            file_name = payload['file_name']
            if '.' in file_name:
                file_extension = file_name.split('.')[-1].lower()
            else:
                file_extension = 'bin'

        return file_data, file_name, file_extension

    except Exception as e:
        print(f" Ошибка при извлечении данных из JSON: {e}")
        return None, None, None


def display_csv_content(file_data, file_name):
    """Выводит содержимое CSV файла"""
    try:
        encoding = detect_encoding(file_data)
        text_content = file_data.decode(encoding)

        print(f"\n СОДЕРЖИМОЕ CSV ФАЙЛА {file_name}:")
        print("=" * 80)

        csv_reader = csv.reader(io.StringIO(text_content))
        for i, row in enumerate(csv_reader):
            if i == 0:
                print("️  ЗАГОЛОВОК:", " | ".join(row))
                print("-" * 80)
            else:
                print(f" СТРОКА {i}:", " | ".join(row))

        print("=" * 80)
        return True
    except Exception as e:
        print(f" Ошибка при обработке CSV: {e}")
        return False


def display_excel_content(file_data, file_name):
    """Выводит содержимое Excel файла"""
    try:
        excel_file = io.BytesIO(file_data)

        print(f"\n СОДЕРЖИМОЕ EXCEL ФАЙЛА {file_name}:")
        print("=" * 80)

        excel_data = pd.ExcelFile(excel_file)

        print(f" Листы в файле: {', '.join(excel_data.sheet_names)}")

        for sheet_name in excel_data.sheet_names:
            print(f"\n ЛИСТ: {sheet_name}")
            print("-" * 40)

            df = pd.read_excel(excel_file, sheet_name=sheet_name)

            print(df.head(10).to_string(index=False))

            if len(df) > 10:
                print(f"... и еще {len(df) - 10} строк")

        print("=" * 80)
        return True
    except Exception as e:
        print(f" Ошибка при обработке Excel: {e}")
        return False


def display_text_content(file_data, file_name):
    """Выводит содержимое текстового файла"""
    try:
        encoding = detect_encoding(file_data)
        text_content = file_data.decode(encoding)

        print(f"\n СОДЕРЖИМОЕ ТЕКСТОВОГО ФАЙЛА {file_name}:")
        print("=" * 80)

        lines = text_content.split('\n')
        for i, line in enumerate(lines[:50]):
            print(f"{i + 1:3d}: {line}")

        if len(lines) > 50:
            print(f"... и еще {len(lines) - 50} строк")

        print("=" * 80)
        return True
    except Exception as e:
        print(f" Ошибка при обработке текстового файла: {e}")
        return False


def display_json_content(file_data, file_name):
    """Выводит содержимое JSON файла"""
    try:
        encoding = detect_encoding(file_data)
        text_content = file_data.decode(encoding)

        print(f"\n СОДЕРЖИМОЕ JSON ФАЙЛА {file_name}:")
        print("=" * 80)

        json_data = json.loads(text_content)
        print(json.dumps(json_data, indent=2, ensure_ascii=False))

        print("=" * 80)
        return True
    except Exception as e:
        print(f" Ошибка при обработке JSON: {e}")
        return False


def display_pdf_content(file_data, file_name):
    """Выводит содержимое PDF файла (первые страницы)"""
    try:
        print(f"\n СОДЕРЖИМОЕ PDF ФАЙЛА {file_name}:")
        print("=" * 80)

        pdf_file = io.BytesIO(file_data)
        pdf_reader = PyPDF2.PdfReader(pdf_file)

        print(f" Количество страниц: {len(pdf_reader.pages)}")

        for page_num in range(min(3, len(pdf_reader.pages))):
            print(f"\n СТРАНИЦА {page_num + 1}:")
            print("-" * 40)

            page = pdf_reader.pages[page_num]
            text = page.extract_text()

            if text.strip():
                lines = text.split('\n')
                for line in lines[:20]:
                    print(line)

                if len(lines) > 20:
                    print("...")
            else:
                print("Текст не обнаружен (возможно, сканированный документ)")

        print("=" * 80)
        return True
    except Exception as e:
        print(f" Ошибка при обработке PDF: {e}")
        return False


def display_word_content(file_data, file_name):
    """Выводит содержимое Word документа"""
    try:
        print(f"\n СОДЕРЖИМОЕ WORD ДОКУМЕНТА {file_name}:")
        print("=" * 80)

        doc_file = io.BytesIO(file_data)
        doc = docx.Document(doc_file)

        print(f" Количество параграфов: {len(doc.paragraphs)}")

        for i, paragraph in enumerate(doc.paragraphs[:20]):
            if paragraph.text.strip():
                print(f"{i + 1:3d}: {paragraph.text}")

        if len(doc.paragraphs) > 20:
            print(f"... и еще {len(doc.paragraphs) - 20} параграфов")

        print("=" * 80)
        return True
    except Exception as e:
        print(f" Ошибка при обработке Word документа: {e}")
        return False


def display_binary_content(file_data, file_name, file_extension):
    """Выводит информацию о бинарном файле"""
    print(f"\n🔧 ИНФОРМАЦИЯ О БИНАРНОМ ФАЙЛЕ {file_name}:")
    print("=" * 80)
    print(f"📏 Размер: {len(file_data)} байт")
    print(f"📁 Тип: {file_extension.upper()}")
    print(f" HEX превью (первые 64 байта):")

    hex_dump = file_data[:64].hex()
    for i in range(0, len(hex_dump), 32):
        print(f"    {hex_dump[i:i + 32]}")

    print("=" * 80)
    return True


def process_file_data(file_data, file_name, file_extension):
    """Обрабатывает файл в зависимости от его типа"""
    if not file_data:
        print(" Нет данных для обработки")
        return False

    file_processors = {
        'csv': display_csv_content,
        'xlsx': display_excel_content,
        'xls': display_excel_content,
        'txt': display_text_content,
        'log': display_text_content,
        'json': display_json_content,
        'pdf': display_pdf_content,
        'docx': display_word_content,
        'doc': display_word_content,
    }

    text_extensions = ['xml', 'html', 'htm', 'yml', 'yaml', 'ini', 'conf', 'cfg', 'sql', 'py', 'js', 'java', 'cpp', 'c',
                       'h']

    if file_extension in file_processors:
        return file_processors[file_extension](file_data, file_name)
    elif file_extension in text_extensions:
        return display_text_content(file_data, file_name)
    else:
        return display_binary_content(file_data, file_name, file_extension)


def download_and_process_json_files(minio_client, bucket, folder_path):
    """Скачивает и обрабатывает все JSON файлы в папке"""
    try:
        json_files = find_json_files_in_folder(minio_client, bucket, folder_path)

        if not json_files:
            print(f" В папке {folder_path} не найдено JSON файлов")
            return False

        processed_count = 0

        for json_file in json_files:
            print(f"\n Обрабатываю JSON файл: {json_file}")

            try:
                response = minio_client.get_object(Bucket=bucket, Key=json_file)
                json_content = response['Body'].read().decode('utf-8')

                file_data, file_name, file_extension = extract_file_data_from_json(json_content)

                if file_data and file_name:
                    print(f" Найден файл: {file_name} (тип: {file_extension})")

                    success = process_file_data(file_data, file_name, file_extension)

                    if success:
                        processed_count += 1
                    else:
                        print(f" Не удалось обработать файл {file_name}")
                else:
                    print(f" Не удалось извлечь данные файла из {json_file}")
                    print(f" Содержимое JSON: {json_content[:500]}...")

            except Exception as e:
                print(f" Ошибка при обработке файла {json_file}: {e}")

        print(f"\n Обработано {processed_count} из {len(json_files)} JSON файлов")
        return processed_count > 0

    except Exception as e:
        print(f" Ошибка при обработке JSON файлов: {e}")
        return False


def enhanced_debezium_consumer():
    """Улучшенный потребитель с загрузкой файлов из MinIO"""
    minio_client = get_minio_client()

    consumer = KafkaConsumer(
        'pgserver.public.file_metadata',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='debezium-file-processor'
    )

    print(" Запуск универсального потребителя Debezium...")
    print(" Поддерживаемые форматы: CSV, Excel, JSON, TXT, PDF, Word, XML, HTML и другие")
    print(" Ожидание сообщений...\n")

    processed_files = set()

    for message in consumer:
        try:
            value = message.value
            after_data = value.get('payload', {}).get('after')

            if after_data:
                file_id = after_data.get('id')
                file_name = after_data.get('file_name')
                s3_path = after_data.get('path_file_s3')

                if file_id in processed_files:
                    continue

                processed_files.add(file_id)

                print(f"\nОБРАБОТКА НОВОГО ФАЙЛА:")
                print(f"   ID: {file_id}")
                print(f"    Файл: {file_name}")
                print(f"    S3 путь: {s3_path}")

                ts = after_data.get('upload_timestamp')
                if ts:
                    dt = datetime.fromtimestamp(ts / 1000000)
                    print(f"    Время загрузки: {dt}")

                print(f"    Статус: {after_data.get('status')}")

                if s3_path:
                    print(f"\n Ищу JSON файлы в MinIO...")
                    bucket, folder_path = parse_s3_path(s3_path)

                    success = download_and_process_json_files(minio_client, bucket, folder_path)

                    if success:
                        print(f" Файлы успешно обработаны!")
                    else:
                        print(f" Не удалось обработать файлы")
                else:
                    print(" Путь к файлу в S3 отсутствует")

        except Exception as e:
            print(f" Ошибка при обработке сообщения: {e}")


if __name__ == "__main__":
    enhanced_debezium_consumer()