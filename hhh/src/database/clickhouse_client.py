import clickhouse_connect
from typing import Optional, Dict, Any, List


class ClickHouseClient:
    def __init__(self):
        self.client = None

    def connect(self, host='localhost', port=8123, username='default', password='', database='events'):
        """Создает клиент для работы с ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database
            )
            print("Подключение к ClickHouse установлено")
            return True
        except Exception as e:
            print(f"Ошибка подключения к ClickHouse: {e}")
            return False

    def test_connection(self):
        """Тестирует подключение к ClickHouse"""
        try:
            if not self.client:
                return False

            result = self.client.command("EXISTS events.company_indicators_typed")
            print(f"Таблица существует: {result == 1}")
            return True
        except Exception as e:
            print(f"Ошибка при тестировании ClickHouse: {e}")
            return False

    def insert_data(self, data: List[Dict[str, Any]]) -> bool:
        """
        Вставляет данные в таблицу ClickHouse

        Args:
            data: Список словарей с данными для вставки

        Returns:
            bool: Успешность операции
        """
        if not data or not self.client:
            print("Нет данных для вставки или клиент не подключен")
            return False

        try:
            print(f"Попытка вставить {len(data)} записей в ClickHouse...")

            # ДЕБАГ: выводим информацию о данных
            self.debug_data_types(data)

            # Используем вставку через INSERT VALUES с правильным синтаксисом
            columns = ['period', 'inn', 'indicator_name', 'string_value',
                       'numeric_value', 'bool_value', 'date_value']

            insert_data = []
            for record in data:
                row = [
                    record.get('period'),
                    record.get('inn', '7700000000'),  # гарантируем, что inn не None
                    record.get('indicator_name', ''),
                    record.get('string_value'),
                    record.get('numeric_value'),
                    record.get('bool_value'),
                    record.get('date_value')
                ]
                insert_data.append(row)

            # Правильный способ вставки в ClickHouse
            result = self.client.insert(
                table='events.company_indicators_typed',
                data=insert_data,
                column_names=columns  # Используем column_names вместо columns
            )

            print(f"Успешно вставлено {len(data)} записей в ClickHouse")
            return True

        except Exception as e:
            print(f"Ошибка при вставке в ClickHouse: {e}")
            # Выводим отладочную информацию
            if data:
                print(f"ДЕБАГ ПЕРВОЙ ЗАПИСИ:")
                first_record = data[0]
                for key, value in first_record.items():
                    print(f"  {key}: {value} (тип: {type(value).__name__})")
            return False

    def debug_data_types(self, data):
        """Выводит отладочную информацию о типах данных"""
        print("\nДЕБАГ ТИПОВ ДАННЫХ:")
        for i, record in enumerate(data[:2]):  # Первые 2 записи
            print(f"Запись {i + 1}:")
            for key, value in record.items():
                print(f"  {key}: {value} (тип: {type(value).__name__})")
        print()

    def close(self):
        """Закрывает соединение с ClickHouse"""
        if self.client:
            self.client.close()