#!/usr/bin/env python3
"""
Основной скрипт для запуска API сервера
"""

import sys
import os

# Добавляем src в путь для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

import uvicorn
from src.api.endpoints import create_app


def main():
    """Основная функция запуска API"""
    app = create_app()

    print("Запуск API сервера...")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()