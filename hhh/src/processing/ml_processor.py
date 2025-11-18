import pandas as pd
import numpy as np
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import warnings

warnings.filterwarnings('ignore')

class MLProcessor:
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
            'Средняя з.п. сотрудников, работающих в Москве, тыс.руb.',
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

    def _normalize_text(self, text: str) -> str:
        """Нормализация текста для сравнения"""
        text = str(text).lower().strip()
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _extract_year(self, column_name: str) -> Optional[int]:
        """Извлекает год из названия колонки"""
        year_match = re.search(r'(?:19|20)\d{2}', str(column_name))
        return int(year_match.group()) if year_match else None

    def _determine_value_type(self, value: Any) -> Tuple[Optional[str], Optional[float], Optional[bool], Optional[datetime.date]]:
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

    def _map_to_base_indicator(self, column_name: str) -> Tuple[Optional[str], float]:
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

    def _read_file_from_bytes(self, file_data: bytes, file_extension: str) -> pd.DataFrame:
        """Чтение файла из байтов с поддержкой разных форматов"""
        import io
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

    def process_file_data_to_sql(self, file_data: bytes, file_name: str,
                               file_extension: str, inn: str,
                               default_source: str = 'file_upload') -> List[Dict[str, Any]]:
        """
        Обрабатывает файл из байтов и генерирует данные для вставки в ClickHouse

        Args:
            file_data: Данные файла в байтах
            file_name: Имя файла
            file_extension: Расширение файла
            inn: ИНН организации
            default_source: Источник данных по умолчанию

        Returns:
            list: Список словарей с данными для вставки
        """
        print(f"Обработка из памяти: {file_name}")

        try:
            df = self._read_file_from_bytes(file_data, file_extension)
            print(f"Загружено: {len(df.columns)} колонок, {len(df)} строк")

            insert_data = []

            # Обрабатываем каждую колонку
            for column in df.columns:
                base_indicator, similarity = self._map_to_base_indicator(column)

                if base_indicator and similarity > 0.6:
                    year = self._extract_year(column)
                    period = year if year else None

                    print(f"{column} -> {base_indicator} ({similarity:.3f})")

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
                    print(f"  {column} -> не найдено ({similarity:.3f})")

            return insert_data

        except Exception as e:
            print(f"Ошибка обработки файла: {e}")
            return []