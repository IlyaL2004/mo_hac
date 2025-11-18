#!/usr/bin/env python3
"""
Основной скрипт для запуска потребителя Kafka с ML обработкой
"""

import sys
import os

# Добавляем src в путь для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.kafka.consumer import KafkaDebeziumConsumer


def main():
    """Основная функция запуска потребителя"""
    print("Запуск умного потребителя с ML обработкой...")

    consumer = KafkaDebeziumConsumer()

    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        print("\nОстановка потребителя...")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()