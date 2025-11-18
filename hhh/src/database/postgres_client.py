import psycopg2
from typing import List, Dict, Any, Optional


class PostgresClient:
    def __init__(self, host="localhost", port="5433", database="mydb",
                 user="user", password="password"):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def connect(self):
        """Устанавливает соединение с PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print("Подключение к PostgreSQL установлено")
            return True
        except Exception as e:
            print(f"Ошибка подключения к PostgreSQL: {e}")
            return False

    def check_table_exists(self, table_name: str = "file_metadata") -> bool:
        """Проверяет существование таблицы"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            exists = cursor.fetchone()[0]
            cursor.close()
            return exists
        except Exception as e:
            print(f"Ошибка проверки таблицы: {e}")
            return False

    def create_table_if_not_exists(self):
        """Создает таблицу если она не существует"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_metadata (
                    id SERIAL PRIMARY KEY,
                    file_name VARCHAR(255),
                    path_file_s3 VARCHAR(500),
                    upload_timestamp TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'uploaded'
                );
            """)
            self.connection.commit()
            cursor.close()
            print("Таблица 'file_metadata' создана или уже существует")
            return True
        except Exception as e:
            print(f"Ошибка создания таблицы: {e}")
            return False

    def save_file_metadata(self, file_name: str, path_file_s3: str,
                           upload_timestamp, status: str = 'uploaded') -> bool:
        """Сохраняет метаданные файла в PostgreSQL"""
        try:
            cursor = self.connection.cursor()
            insert_query = """
            INSERT INTO file_metadata (file_name, path_file_s3, upload_timestamp, status)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_query, (file_name, path_file_s3, upload_timestamp, status))
            self.connection.commit()
            cursor.close()
            print(f"Метаданные сохранены в PostgreSQL: {file_name}")
            return True
        except Exception as e:
            print(f"Ошибка сохранения в PostgreSQL: {e}")
            return False

    def get_file_metadata(self) -> List[Dict[str, Any]]:
        """Возвращает данные из таблицы file_metadata"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM file_metadata ORDER BY upload_timestamp DESC;")
            rows = cursor.fetchall()

            data = []
            for row in rows:
                data.append({
                    "id": row[0],
                    "file_name": row[1],
                    "path_file_s3": row[2],
                    "upload_timestamp": row[3].isoformat() if row[3] else None,
                    "status": row[4]
                })
            cursor.close()
            return data
        except Exception as e:
            print(f"Ошибка чтения данных таблицы: {e}")
            return []

    def close(self):
        """Закрывает соединение с PostgreSQL"""
        if self.connection:
            self.connection.close()