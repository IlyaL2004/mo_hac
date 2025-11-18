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
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import re
import warnings
import clickhouse_connect

warnings.filterwarnings('ignore')


class SQLDataGenerator:
    """
    Генерирует SQL-вставки из файлов с промышленными данными
    с автоматическим определением типов данных
    """

    def __init__(self):
        self.model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')

        self.base_indicators = [
            'Наименование организации', 'Полное наименование организации',
            'Статус СПАРК', 'Статус внутренний', 'Статус ИТОГ', 'Дата добавления в реестр',
            'Юридический адрес', 'Адрес производства', 'Адрес дополнительной площадки',
            'Основная отрасль', 'Подотрасль (Основная)', 'Дополнительная отрасль',
            'Подотрасль (Дополнительная)', 'Отраслевые презентации', 'Основной ОКВЭД (СПАРК)',
            'Вид деятельности по основному ОКВЭД (СПАРК)', 'Производственный ОКВЭД',
            'Вид деятельности по производственному ОКВЭД', 'Общие сведения об организации',
            'Размер предприятия (итог)', 'Размер предприятия (по численности)',
            'Размер предприятия (по выручке)', 'Дата регистрации', 'Системообразующее предприятие',
            'Статус МСП', 'То самое', 'Финансово-экономические показатели',
            'Руководитель', 'Головная организация', 'ИНН головной организации',
            'Вид отношения головной организации', 'Контактные данные руководства',
            'Почта руководства', 'Контакт сотрудника организации', 'Номер телефона',
            'Контактные данные ответственного по ЧС', 'Сайт', 'Электронная почта',
            'Данные о мерах поддержки', 'Наличие особого статуса', 'Площадка итог',
            'Получена поддержка от г. Москвы', 'Выручка предприятия, тыс. руб.',
            'Чистая прибыль (убыток),тыс. руб.',
            'Среднесписочная численность персонала (всего по компании), чел',
            'Среднесписочная численность персонала, работающего в Москве, чел',
            'Фонд оплаты труда всех сотрудников организации, тыс. руб',
            'Фонд оплаты труда сотрудников, работающих в Москве, тыс. руб',
            'Средняя з.п. всех сотрудников организации, тыс.руб.',
            'Средняя з.п. сотрудников, работающих в Москве, тыс.руб.',
            'Налоги, уплаченные в бюджет Москвы (без акцизов), тыс.руб.',
            'Налог на прибыль, тыс.руб.', 'Налог на имущество, тыс.руb.',
            'Налог на землю, тыс.руб.', 'НДФЛ, тыс.руб.', 'Транспортный налог, тыс.руб.',
            'Прочие налоги', 'Акцизы, тыс.руб.', 'Инвестиции в Мск тыс. руб.',
            'Объем экспорта, тыс. руб.', 'Имущественно-земельный комплекс',
            'Кадастровый номер ЗУ', 'Площадь ЗУ', 'Вид разрешенного использования ЗУ',
            'Вид собственности ЗУ', 'Собственник ЗУ', 'Кадастровый номер ОКСа',
            'Площадь ОКСов', 'Вид разрешенного использования ОКСов',
            'Тип строения и цель использования', 'СобственникОКСов',
            'Перечень производимой продукции по кодам ОКПД 2',
            'Перечень производимой продукции по типам и сегментам', 'Каталог продукции',
            'Наличие поставок продукции на экспорт', 'Наличие госзаказа',
            'Уровень загрузки производственных мощностей',
            'Перечень государств куда экспортируется продукция',
            'Объем экспорта (млн.руб.) за предыдущий календарный год',
            'Код ТН ВЭД ЕАЭС', 'Развитие Реестра',
            'Отрасль промышленности по Спарк и Справочнику',
            'Площадь производственных помещений, кв.м.', 'Производимая продукция',
            'Стандартизированная продукция', 'Название (виды производимой продукции)',
            'Координаты юридического адреса', 'Координаты адреса производства',
            'Координаты адреса дополнительной площадки', 'Координаты (широта)',
            'Координаты (долгота)', 'Округ', 'Район'
        ]

        # эмбеддинги для базовых показателей
        print("Инициализация модели...")
        self.base_embeddings = self.model.encode(
            [self._normalize_text(col) for col in self.base_indicators],
            convert_to_tensor=True
        )
        print(f"Базовая схема: {len(self.base_indicators)} показателей")

    def _normalize_text(self, text):
        """Нормализация текста для сравнения"""
        text = str(text).lower().strip()
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _extract_year(self, column_name):
        """Извлекает год из названия колонки"""
        year_match = re.search(r'(?:19|20)\d{2}', str(column_name))
        return int(year_match.group()) if year_match else None

    def _determine_value_type(self, value):
        """
        Определяет тип значения и возвращает соответствующие значения для колонок
        Возвращает кортеж: (string_value, numeric_value, bool_value, date_value)
        """
        if pd.isna(value) or value is None:
            return (None, None, None, None)

        str_value = str(value).strip()

        # Проверка на булево значение
        if str_value.lower() in ['true', 'false', '1', '0', 'да', 'нет', 'yes', 'no']:
            bool_val = True if str_value.lower() in ['true', '1', 'да', 'yes'] else False
            return (None, None, bool_val, None)

        # Проверка на число (целое или дробное)
        try:
            # Убираем пробелы в числах (например: "150 000" -> "150000")
            cleaned_num = re.sub(r'[^\d.,-]', '', str_value)
            cleaned_num = cleaned_num.replace(',', '.')

            if cleaned_num and cleaned_num != '-':
                # Пробуем преобразовать в float
                num_val = float(cleaned_num)
                return (None, num_val, None, None)
        except (ValueError, TypeError):
            pass

        # Проверка на дату
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}\.\d{2}\.\d{4}',
            r'\d{2}/\d{2}/\d{4}'
        ]

        for pattern in date_patterns:
            if re.match(pattern, str_value):
                try:
                    # Приводим к формату YYYY-MM-DD для ClickHouse
                    if '.' in str_value:
                        parts = str_value.split('.')
                        if len(parts) == 3:
                            date_val = datetime.strptime(str_value, '%d.%m.%Y').date()
                            return (None, None, None, date_val)
                    elif '/' in str_value:
                        parts = str_value.split('/')
                        if len(parts) == 3:
                            date_val = datetime.strptime(str_value, '%d/%m/%Y').date()
                            return (None, None, None, date_val)
                    else:
                        date_val = datetime.strptime(str_value, '%Y-%m-%d').date()
                        return (None, None, None, date_val)
                except ValueError:
                    pass

        # По умолчанию - строковое значение
        return (str_value, None, None, None)

    def _map_to_base_indicator(self, column_name):
        """Сопоставляет колонку с базовым показателем"""
        column_norm = self._normalize_text(column_name)
        column_embedding = self.model.encode([column_norm], convert_to_tensor=True)

        similarities = cosine_similarity(
            column_embedding.cpu().numpy(),
            self.base_embeddings.cpu().numpy()
        )[0]

        best_idx = np.argmax(similarities)
        best_similarity = similarities[best_idx]

        if best_similarity > 0.6:
            return self.base_indicators[best_idx], best_similarity
        else:
            return None, best_similarity

    def _read_file_from_bytes(self, file_data, file_extension):
        """Чтение файла из байтов с поддержкой разных форматов"""
        try:
            if file_extension in ['xlsx', 'xls']:
                return pd.read_excel(io.BytesIO(file_data))
            elif file_extension == 'csv':
                try:
                    return pd.read_csv(io.BytesIO(file_data), encoding='utf-8')
                except UnicodeDecodeError:
                    return pd.read_csv(io.BytesIO(file_data), encoding='cp1251')
            else:
                raise ValueError(f"Неподдерживаемый формат: {file_extension}")
        except Exception as e:
            raise Exception(f"Ошибка чтения файла: {str(e)}")

    def process_file_data_to_sql(self, file_data, file_name, file_extension, inn, default_source='file_upload'):
        """
        Обрабатывает файл из байтов и генерирует данные для вставки в ClickHouse

        Args:
            file_data (bytes): Данные файла в байтах
            file_name (str): Имя файла
            file_extension (str): Расширение файла
            inn (str): ИНН организации
            default_source (str): Источник данных по умолчанию

        Returns:
            list: Список словарей с данными для вставки
        """
        print(f"📁 Обработка из памяти: {file_name}")

        try:
            df = self._read_file_from_bytes(file_data, file_extension)
            print(f"   📊 Загружено: {len(df.columns)} колонок, {len(df)} строк")

            insert_data = []

            # Обрабатываем каждую колонку
            for column in df.columns:
                base_indicator, similarity = self._map_to_base_indicator(column)

                if base_indicator and similarity > 0.6:
                    year = self._extract_year(column)
                    period = year if year else None

                    print(f"   ✅ {column} -> {base_indicator} ({similarity:.3f})")

                    # Обрабатываем каждую строку в колонке
                    for idx, value in df[column].items():
                        if pd.notna(value) and str(value).strip() and str(value).strip().lower() != 'nan':
                            # Определяем тип значения
                            string_val, numeric_val, bool_val, date_val = self._determine_value_type(value)

                            record = {
                                'period': period,
                                'inn': inn,
                                'indicator_name': base_indicator,
                                'string_value': string_val,
                                'numeric_value': numeric_val,
                                'bool_value': bool_val,
                                'date_value': date_val
                            }
                            insert_data.append(record)
                else:
                    print(f"   ❌ {column} -> не найдено ({similarity:.3f})")

            return insert_data

        except Exception as e:
            print(f"❌ Ошибка обработки файла: {e}")
            return []


def get_clickhouse_client():
    """Создает клиент для работы с ClickHouse"""
    try:
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='default',
            password='',
            database='events'
        )
        print("✅ Подключение к ClickHouse установлено")

        # Проверим существование таблицы
        result = client.command("EXISTS events.company_indicators_typed")
        print(f"✅ Таблица существует: {result == 1}")

        return client
    except Exception as e:
        print(f"❌ Ошибка подключения к ClickHouse: {e}")
        print("🔧 Проверьте:")
        print("   - Запущен ли контейнер ClickHouse")
        print("   - Порт 8123 доступен")
        print("   - База данных 'events' существует")
        return None


def format_value_for_sql(value, value_type):
    """Форматирует значение для SQL вставки"""
    if value is None:
        return 'NULL'

    if value_type == 'string':
        # Экранируем кавычки и специальные символы
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"
    elif value_type == 'numeric':
        return str(value)
    elif value_type == 'bool':
        return '1' if value else '0'
    elif value_type == 'date':
        return f"'{value}'"
    elif value_type == 'period':
        return 'NULL' if value is None else str(value)
    else:
        return 'NULL'


def insert_to_clickhouse_sql(client, data):
    """
    Вставляет данные в таблицу ClickHouse с помощью прямого SQL запроса

    Args:
        client: Клиент ClickHouse
        data: Список словарей с данными для вставки
    """
    if not data:
        print("⚠️ Нет данных для вставки")
        return False

    try:
        print(f"💾 Подготовка SQL вставки для {len(data)} записей...")

        # Формируем VALUES для SQL запроса
        values = []
        for record in data:
            period = format_value_for_sql(record.get('period'), 'period')
            inn = format_value_for_sql(record.get('inn'), 'string')
            indicator_name = format_value_for_sql(record.get('indicator_name'), 'string')
            string_value = format_value_for_sql(record.get('string_value'), 'string')
            numeric_value = format_value_for_sql(record.get('numeric_value'), 'numeric')
            bool_value = format_value_for_sql(record.get('bool_value'), 'bool')
            date_value = format_value_for_sql(record.get('date_value'), 'date')

            value_str = f"({period}, {inn}, {indicator_name}, {string_value}, {numeric_value}, {bool_value}, {date_value})"
            values.append(value_str)

        # Формируем полный SQL запрос
        sql = f"""
        INSERT INTO events.company_indicators_typed 
        (period, inn, indicator_name, string_value, numeric_value, bool_value, date_value)
        VALUES {','.join(values)}
        """

        print("🚀 Выполнение SQL запроса...")

        # Выполняем запрос
        result = client.command(sql)
        print(f"✅ Успешно вставлено {len(data)} записей в ClickHouse")
        return True

    except Exception as e:
        print(f"❌ ОШИБКА при вставке в ClickHouse:")
        print(f"   Тип ошибки: {type(e).__name__}")
        print(f"   Сообщение: {str(e)}")

        # Выводим отладочную информацию
        if data:
            print(f"\n🔍 ДЕБАГ ПЕРВОЙ ЗАПИСИ:")
            first_record = data[0]
            for key, value in first_record.items():
                print(f"   {key}: {value} (тип: {type(value).__name__})")

        return False


def debug_data_types(data):
    """Выводит отладочную информацию о типах данных"""
    print("\n🔍 ДЕБАГ ТИПОВ ДАННЫХ:")
    for i, record in enumerate(data[:2]):  # Первые 2 записи
        print(f"  Запись {i + 1}:")
        for key, value in record.items():
            print(f"    {key}: {value} (тип: {type(value).__name__})")
    print()


def insert_to_clickhouse(client, data):
    """
    Вставляет данные в таблицу ClickHouse

    Args:
        client: Клиент ClickHouse
        data: Список словарей с данными для вставки
    """
    if not data:
        print("⚠️ Нет данных для вставки")
        return False

    try:
        # ДЕБАГ: выводим информацию о данных
        debug_data_types(data)

        print(f"💾 Попытка вставить {len(data)} записей в ClickHouse...")

        # Используем SQL вставку вместо client.insert
        success = insert_to_clickhouse_sql(client, data)

        if success:
            print(f"🎉 Всего вставлено {len(data)} записей в ClickHouse")
            return True
        else:
            return False

    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА при вставке в ClickHouse:")
        print(f"   Тип ошибки: {type(e).__name__}")
        print(f"   Сообщение: {str(e)}")

        # Дополнительная диагностика
        print(f"\n🔍 ДИАГНОСТИКА:")
        print(f"   Размер данных: {len(data)} записей")
        if data:
            print(f"   Пример первой записи: {data[0]}")
            print(f"   Типы данных в первой записи:")
            for key, value in data[0].items():
                print(f"     {key}: {type(value).__name__} = {value}")

        return False


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

        print(f"🔍 Поиск JSON файлов в папке: {folder_path}")
        response = minio_client.list_objects_v2(Bucket=bucket, Prefix=folder_path)

        json_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'):
                    json_files.append(obj['Key'])
                    print(f"📄 Найден JSON файл: {obj['Key']}")

        return json_files
    except Exception as e:
        print(f"❌ Ошибка при поиске JSON файлов: {e}")
        return []


def detect_encoding(file_bytes):
    """Определяет кодировку файла"""
    try:
        result = chardet.detect(file_bytes)
        encoding = result['encoding'] if result['encoding'] else 'utf-8'
        confidence = result['confidence']
        print(f"📝 Определена кодировка: {encoding} (уверенность: {confidence:.2%})")
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
                print("✅ Данные файла извлечены из file_data (hex)")
            except Exception as e:
                print(f"❌ Ошибка декодирования hex: {e}")

        if 'file_name' in payload:
            file_name = payload['file_name']
            if '.' in file_name:
                file_extension = file_name.split('.')[-1].lower()
            else:
                file_extension = 'bin'

        return file_data, file_name, file_extension

    except Exception as e:
        print(f"❌ Ошибка при извлечении данных из JSON: {e}")
        return None, None, None


def extract_inn_from_filename(file_name):
    """Извлекает ИНН из имени файла или возвращает дефолтный"""
    if file_name:
        inn_match = re.search(r'\b\d{10,12}\b', file_name)
        if inn_match:
            return inn_match.group(0)

    return "7700000000"


def send_to_ml_model(file_data, file_name, file_extension, clickhouse_client):
    """
    Отправляет содержимое файла в ML модель для обработки
    и вставляет результаты в ClickHouse
    """
    print(f"\n🤖 ПЕРЕДАЧА В ML МОДЕЛЬ: {file_name}")
    print("=" * 80)

    try:
        # Инициализируем ML модель
        ml_generator = SQLDataGenerator()

        # Извлекаем ИНН из имени файла
        inn = extract_inn_from_filename(file_name)
        print(f"📋 ИНН для обработки: {inn}")

        # Обрабатываем файл через ML модель
        insert_data = ml_generator.process_file_data_to_sql(
            file_data, file_name, file_extension, inn
        )

        if insert_data:
            print(f"✅ ML модель успешно обработала файл")
            print(f"📊 Сгенерировано {len(insert_data)} записей для вставки")

            # Выводим примеры данных для отладки
            print("\n📋 ПРИМЕРЫ ДАННЫХ ДЛЯ ВСТАВКИ:")
            for i, record in enumerate(insert_data[:3]):  # Показываем первые 3 записи
                print(f"  {i + 1}. {record}")

            # ВСТАВЛЯЕМ ДАННЫЕ В CLICKHOUSE
            print("\n💾 ВСТАВКА ДАННЫХ В CLICKHOUSE...")
            success = insert_to_clickhouse(clickhouse_client, insert_data)

            if success:
                print("🎉 ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ В CLICKHOUSE!")
                return True
            else:
                print("❌ ОШИБКА ПРИ ВСТАВКЕ В CLICKHOUSE")
                return False
        else:
            print("❌ ML модель не смогла сгенерировать данные для вставки")
            return False

    except Exception as e:
        print(f"❌ Ошибка при работе ML модели: {e}")
        return False


def display_csv_content_with_ml(file_data, file_name, clickhouse_client):
    """Выводит содержимое CSV файла и передает в ML модель"""
    try:
        encoding = detect_encoding(file_data)
        text_content = file_data.decode(encoding)

        print(f"\n📊 СОДЕРЖИМОЕ CSV ФАЙЛА {file_name}:")
        print("=" * 80)

        csv_reader = csv.reader(io.StringIO(text_content))
        for i, row in enumerate(csv_reader):
            if i == 0:
                print("📝 ЗАГОЛОВОК:", " | ".join(row))
                print("-" * 80)
            else:
                print(f"📄 СТРОКА {i}:", " | ".join(row))

        print("=" * 80)

        # Передаем в ML модель
        success = send_to_ml_model(file_data, file_name, 'csv', clickhouse_client)
        return success

    except Exception as e:
        print(f"❌ Ошибка при обработке CSV: {e}")
        return False


def display_excel_content_with_ml(file_data, file_name, clickhouse_client):
    """Выводит содержимое Excel файла и передает в ML модель"""
    try:
        excel_file = io.BytesIO(file_data)

        print(f"\n📊 СОДЕРЖИМОЕ EXCEL ФАЙЛА {file_name}:")
        print("=" * 80)

        excel_data = pd.ExcelFile(excel_file)

        print(f"📑 Листы в файле: {', '.join(excel_data.sheet_names)}")

        for sheet_name in excel_data.sheet_names:
            print(f"\n📋 ЛИСТ: {sheet_name}")
            print("-" * 40)

            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            print(df.head(10).to_string(index=False))

            if len(df) > 10:
                print(f"... и еще {len(df) - 10} строк")

        print("=" * 80)

        # Передаем в ML модель
        success = send_to_ml_model(file_data, file_name, 'xlsx', clickhouse_client)
        return success

    except Exception as e:
        print(f"❌ Ошибка при обработке Excel: {e}")
        return False


def display_text_content_with_ml(file_data, file_name, clickhouse_client):
    """Выводит содержимое текстового файла и передает в ML модель"""
    try:
        encoding = detect_encoding(file_data)
        text_content = file_data.decode(encoding)

        print(f"\n📄 СОДЕРЖИМОЕ ТЕКСТОВОГО ФАЙЛА {file_name}:")
        print("=" * 80)

        lines = text_content.split('\n')
        for i, line in enumerate(lines[:50]):
            print(f"{i + 1:3d}: {line}")

        if len(lines) > 50:
            print(f"... и еще {len(lines) - 50} строк")

        print("=" * 80)

        # Для текстовых файлов создаем CSV-подобную структуру для ML модели
        if len(lines) > 1 and ',' in lines[0]:  # Если похоже на CSV
            success = send_to_ml_model(file_data, file_name, 'csv', clickhouse_client)
        else:
            # Для обычного текста создаем простую таблицу
            csv_data = "value\n" + "\n".join(
                [f'"{line.replace(chr(34), chr(34) + chr(34))}"' for line in lines if line.strip()])
            success = send_to_ml_model(csv_data.encode('utf-8'), file_name, 'csv', clickhouse_client)

        return success

    except Exception as e:
        print(f"❌ Ошибка при обработке текстового файла: {e}")
        return False


def display_json_content_with_ml(file_data, file_name, clickhouse_client):
    """Выводит содержимое JSON файла и передает в ML модель"""
    try:
        encoding = detect_encoding(file_data)
        text_content = file_data.decode(encoding)

        print(f"\n📋 СОДЕРЖИМОЕ JSON ФАЙЛА {file_name}:")
        print("=" * 80)

        json_data = json.loads(text_content)
        print(json.dumps(json_data, indent=2, ensure_ascii=False))

        print("=" * 80)

        # Преобразуем JSON в CSV-подобный формат для ML модели
        if isinstance(json_data, list) and len(json_data) > 0:
            # Если JSON представляет собой массив объектов
            df = pd.json_normalize(json_data)
            csv_data = df.to_csv(index=False)
            success = send_to_ml_model(csv_data.encode('utf-8'), file_name, 'csv', clickhouse_client)
        else:
            # Для простого JSON объекта
            csv_data = "key,value\n" + "\n".join([f'"{k}","{v}"' for k, v in json_data.items()])
            success = send_to_ml_model(csv_data.encode('utf-8'), file_name, 'csv', clickhouse_client)

        return success

    except Exception as e:
        print(f"❌ Ошибка при обработке JSON: {e}")
        return False


def process_file_data_with_ml(file_data, file_name, file_extension, clickhouse_client):
    """Обрабатывает файл и передает его содержимое в ML модель"""
    if not file_data:
        print("❌ Нет данных для обработки")
        return False

    file_processors = {
        'csv': display_csv_content_with_ml,
        'xlsx': display_excel_content_with_ml,
        'xls': display_excel_content_with_ml,
        'txt': display_text_content_with_ml,
        'log': display_text_content_with_ml,
        'json': display_json_content_with_ml,
    }

    text_extensions = ['xml', 'html', 'htm', 'yml', 'yaml', 'ini', 'conf', 'cfg', 'sql', 'py', 'js', 'java', 'cpp', 'c',
                       'h']

    if file_extension in file_processors:
        return file_processors[file_extension](file_data, file_name, clickhouse_client)
    elif file_extension in text_extensions:
        return display_text_content_with_ml(file_data, file_name, clickhouse_client)
    else:
        print(f"❌ Формат {file_extension} не поддерживается ML моделью")
        return False


def download_and_process_json_files(minio_client, bucket, folder_path, clickhouse_client):
    """Скачивает и обрабатывает все JSON файлы в папке с использованием ML модели"""
    try:
        json_files = find_json_files_in_folder(minio_client, bucket, folder_path)

        if not json_files:
            print(f"❌ В папке {folder_path} не найдено JSON файлов")
            return False

        processed_count = 0
        ml_processed_count = 0

        for json_file in json_files:
            print(f"\n🔄 Обрабатываю JSON файл: {json_file}")

            try:
                response = minio_client.get_object(Bucket=bucket, Key=json_file)
                json_content = response['Body'].read().decode('utf-8')

                file_data, file_name, file_extension = extract_file_data_from_json(json_content)

                if file_data and file_name:
                    print(f"✅ Найден файл: {file_name} (тип: {file_extension})")

                    # Обрабатываем файл с ML моделью
                    success = process_file_data_with_ml(file_data, file_name, file_extension, clickhouse_client)

                    if success:
                        processed_count += 1
                        ml_processed_count += 1
                        print(f"✅ Файл {file_name} успешно обработан")
                    else:
                        print(f"❌ Не удалось обработать файл {file_name} с ML моделью")
                else:
                    print(f"❌ Не удалось извлечь данные файла из {json_file}")

            except Exception as e:
                print(f"❌ Ошибка при обработке файла {json_file}: {e}")

        print(f"\n📊 ИТОГ:")
        print(f"   Обработано: {processed_count} из {len(json_files)} JSON файлов")
        print(f"   Успешно обработано ML моделью: {ml_processed_count} файлов")
        return processed_count > 0

    except Exception as e:
        print(f"❌ Ошибка при обработке JSON файлов: {e}")
        return False


def test_clickhouse_connection():
    """Тестирует подключение к ClickHouse и структуру таблицы"""
    try:
        client = get_clickhouse_client()
        if not client:
            return False

        # Проверим структуру таблицы
        result = client.query("DESCRIBE events.company_indicators_typed")
        print("\n📋 СТРУКТУРА ТАБЛИЦЫ:")
        for row in result.result_rows:
            print(f"   {row}")

        # Проверим количество записей
        count = client.command("SELECT count() FROM events.company_indicators_typed")
        print(f"📊 Текущее количество записей в таблице: {count}")

        return True
    except Exception as e:
        print(f"❌ Ошибка при тестировании ClickHouse: {e}")
        return False


def enhanced_debezium_consumer_with_ml():
    """Улучшенный потребитель с загрузкой файлов из MinIO и обработкой ML моделью"""
    minio_client = get_minio_client()

    # Сначала тестируем подключение к ClickHouse
    print("🔧 ТЕСТИРОВАНИЕ ПОДКЛЮЧЕНИЯ К CLICKHOUSE...")
    if not test_clickhouse_connection():
        print("❌ Тестирование подключения не удалось. Прекращаем работу.")
        return

    clickhouse_client = get_clickhouse_client()
    if not clickhouse_client:
        print("❌ Не удалось подключиться к ClickHouse. Проверьте настройки подключения.")
        return

    consumer = KafkaConsumer(
        'pgserver.public.file_metadata',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='debezium-file-processor-ml'
    )

    print("\n" + "=" * 70)
    print("🤖 ЗАПУСК УМНОГО ПОТРЕБИТЕЛЯ С ML МОДЕЛЬЮ И CLICKHOUSE")
    print("=" * 70)
    print("⚙️  Функциональность:")
    print("   • Чтение сообщений из Kafka")
    print("   • Загрузка файлов из MinIO")
    print("   • Автоматический анализ содержимого")
    print("   • Генерация данных с помощью ML модели")
    print("   • АВТОМАТИЧЕСКАЯ ВСТАВКА В CLICKHOUSE")
    print("=" * 70)
    print("⏳ Ожидание сообщений...\n")

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

                print(f"\n🎯 ОБРАБОТКА НОВОГО ФАЙЛА С ML И CLICKHOUSE:")
                print(f"   📁 ID: {file_id}")
                print(f"   📄 Файл: {file_name}")
                print(f"   📍 S3 путь: {s3_path}")

                ts = after_data.get('upload_timestamp')
                if ts:
                    dt = datetime.fromtimestamp(ts / 1000000)
                    print(f"   ⏰ Время загрузки: {dt}")

                print(f"   📊 Статус: {after_data.get('status')}")

                if s3_path:
                    print(f"\n🔍 Ищу JSON файлы в MinIO...")
                    bucket, folder_path = parse_s3_path(s3_path)

                    success = download_and_process_json_files(minio_client, bucket, folder_path, clickhouse_client)

                    if success:
                        print(f"✅ Файлы успешно обработаны ML моделью и загружены в ClickHouse!")
                    else:
                        print(f"❌ Не удалось обработать файлы ML моделью")
                else:
                    print("❌ Путь к файлу в S3 отсутствует")

        except Exception as e:
            print(f"❌ Ошибка при обработке сообщения: {e}")


if __name__ == "__main__":
    enhanced_debezium_consumer_with_ml()