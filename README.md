Хакатон



#  Система мониторинга промышленного сектора Москвы

Единая платформа для сбора, консолидации, анализа и визуализации данных предприятий Москвы

## О проекте

Цифровая платформа для совершенствования промышленной политики города Москвы через комплексный мониторинг и анализ данных промышленных предприятий.

### Решаемые проблемы

Центральная проблема:  
Отсутствие единой оперативной аналитической системы для мониторинга промышленного сектора Москвы

Последствия:
-  Фрагментарность данных
-  Запаздывание аналитики  
-  Невозможность сквозного анализа
-  Снижение качества управленческих решений

Вторичные проблемы:
- Отсутствие автоматической верификации данных
- Высокие трудозатраты на подготовку отчетности
- Невозможность оперативного реагирования на изменения

## Ключевые преимущества

### Технологические инновации

| Преимущество | Описание |
|-------------|-----------|
|  Автоматическое сопоставление | ML-модель самостоятельно определяет соответствие столбцов |
|  Гибкая типизация | Система автоматически определяет типы данных |
|  Потоковая обработка | Данные обрабатываются в реальном времени |
|  Универсальность | Поддержка структурированных и неструктурированных данных |

### Уникальные особенности

-  Гибридная обработка - поддержка файловых загрузок, ручного ввода и API-интеграций
- Open-source стек - отсутствие лицензионных отчислений
-  Модульность - поэтапное внедрение и масштабирование

## Новаторские решения

###  Интеллектуальные технологии

# "Умное" партиционирование в S3
s3://moscow-industry/INN=1234567890/date=2024-01-01/

# Семантический поиск показателей
cosine_similarity(column_name, target_indicator)

Полиглотная архитектура
Компонент	Назначение	Технология
Метаданные	Хранение схемы и настроек	PostgreSQL
Аналитика	Обработка больших данных	ClickHouse
Хранилище	Файлы и документы	S3-совместимое

Self-service аналитика
Бизнес-пользователи могут самостоятельно создавать дашборды без привлечения IT-специалистов

Демонстрация системы
<div align="center">

Управление источниками

![Analytics Dashboard](https://github.com/user-attachments/files/23277495/capture_20251101015809880.bmp)

![Data Monitoring](https://github.com/user-attachments/files/23277497/capture_20251101015822752.bmp)

Визуализация показателей

![Metrics Visualization](https://github.com/user-attachments/files/23277499/capture_20251101015843823.bmp)

![Data Sources](https://github.com/user-attachments/files/23277500/capture_20251101015853338.bmp)

</div>

Предварительные требования
Docker и Docker Compose

Python 3.8+

WSL (для Windows пользователей)



# Запуск

# Установка python3-venv

sudo apt update
sudo apt install python3-venv -y

# Создание виртуальной среды
python3 -m venv venv

# Активация виртуальной среды
source venv/bin/activate

# Установка зависимостей

/mnt/c/Users/ilyal/Desktop/hac/mo_hac/hhh/venv/bin/python -m pip install -r requirements.txt

# Запуск докера

docker-compose up -d

# Создание Kafka коннекторов
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @chan-updated-connector.json
curl -X POST http://localhost:8084/connectors -H "Content-Type: application/json" -d @connector-config.json

# Создаём бакет в S3

создаём бакет в s3
moscow-industry-data

# Запуск Python сервисов
# Терминал 1 - ML обработчик:

cd /mnt/c/Users/ilyal/Desktop/hac/mo_hac/hhh
source venv/bin/activate
python debezium_consumer.py

# Терминал 2 - API сервер:

cd /mnt/c/Users/ilyal/Desktop/hac/mo_hac/hhh
source venv/bin/activate
uvicorn kafka_connect_producer:app --reload --host 0.0.0.0 --port 8000

# Запуск фронтенда
bash
cd /mnt/c/Users/ilyal/Desktop/hac/mo_hac/frontend
npx http-server -p 3000

# Проверка работы
Фронтенд: http://localhost:3000

API документация: http://localhost:8000/docs

MinIO: http://localhost:9001

Kafka UI: http://localhost:8090

Superset: http://localhost:8088


# Eсли не загружается в s3, то перезагрузить коннектор

curl -X DELETE http://localhost:8083/connectors/minio-file-chunks-sink
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @chan-updated-connector.json

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

# В приложении можно загрузить файл

test_data/taxes_data.csv
