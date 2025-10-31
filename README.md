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

Управление источниками

[capture_20251101015809880.bmp](https://github.com/user-attachments/files/23277554/capture_20251101015809880.bmp)

[capture_20251101015822752.bmp](https://github.com/user-attachments/files/23277555/capture_20251101015822752.bmp)

Визуализация показателей

[capture_20251101015843823.bmp](https://github.com/user-attachments/files/23277557/capture_20251101015843823.bmp)

[capture_20251101015853338.bmp](https://github.com/user-attachments/files/23277559/capture_20251101015853338.bmp)

Предварительные требования
Docker и Docker Compose

Python 3.8+

WSL (для Windows пользователей)




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
9. заходим в папку frontend и пишем в терминале start frontend.html
npx create-react-app frontend 
10.  python -m http.server 8001
11. http://localhost:8001/frontend.html

 npx http-server -p 3000
