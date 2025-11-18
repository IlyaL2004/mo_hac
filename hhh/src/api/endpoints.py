from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import asyncio
import os
import requests
import time
import webbrowser
import threading
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware

from .models import (
    FileProcessRequest, ApiDownloadRequest, ProcessResponse,
    ApiDownloadResponse
)
from ..kafka.producer import KafkaFileProducer

# Глобальная переменная для продюсера
producer = None

def check_superset_availability():
    """Проверяет, доступен ли Superset"""
    try:
        response = requests.get("http://localhost:8088", timeout=5)
        return response.status_code == 200
    except:
        return False

def download_from_api(api_url: str, timeout: int = 30) -> dict:
    """Загружает данные из внешнего API"""
    try:
        start_time = time.time()
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()

        download_time = time.time() - start_time

        try:
            data = response.json()
            content_type = "json"
        except:
            data = response.text
            content_type = "text"

        return {
            "status": "success",
            "data": data,
            "content_type": content_type,
            "file_size": len(response.content),
            "download_time": download_time,
            "status_code": response.status_code
        }

    except requests.exceptions.Timeout:
        return {"status": "error", "error": f"Таймаут запроса к API ({timeout} секунд)"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error": f"HTTP ошибка: {e}"}
    except requests.exceptions.RequestException as e:
        return {"status": "error", "error": f"Ошибка запроса: {e}"}
    except Exception as e:
        return {"status": "error", "error": f"Неожиданная ошибка: {e}"}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = KafkaFileProducer()
    print("Kafka продюсер запущен")
    yield
    if producer:
        producer.close()
    print("Kafka продюсер остановлен")

def create_app() -> FastAPI:
    app = FastAPI(
        title="File Processor API",
        description="API для обработки файлов и отправки в Kafka",
        version="1.0.0",
        lifespan=lifespan
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://192.168.3.17:3000",
            "http://192.168.137.1:3000"
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.post("/process-file/", response_model=ProcessResponse)
    async def process_file_endpoint(request: FileProcessRequest):
        if not os.path.exists(request.file_path):
            raise HTTPException(status_code=404, detail="Файл не найден")

        if not request.inn.isdigit() or len(request.inn) not in [10, 12]:
            raise HTTPException(status_code=400, detail="ИНН должен содержать 10 или 12 цифр")

        try:
            success = await asyncio.to_thread(producer.process_file, request.file_path, request.inn)

            if success:
                return ProcessResponse(
                    status="success",
                    message=f"Файл {os.path.basename(request.file_path)} успешно обработан",
                    inn=request.inn,
                    file_name=os.path.basename(request.file_path)
                )
            else:
                raise HTTPException(status_code=500, detail="Не удалось обработать файл")

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Ошибка обработки: {str(e)}")

    @app.post("/download-from-api/", response_model=ApiDownloadResponse)
    async def download_from_api_endpoint(request: ApiDownloadRequest):
        if not request.inn.isdigit() or len(request.inn) not in [10, 12]:
            raise HTTPException(status_code=400, detail="ИНН должен содержать 10 или 12 цифр")

        if not request.api_url.startswith(('http://', 'https://')):
            raise HTTPException(status_code=400, detail="Некорректный URL API")

        if not request.file_name:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            request.file_name = f"api_data_{request.inn}_{timestamp}.{request.file_extension}"

        try:
            api_result = download_from_api(request.api_url)

            if api_result["status"] == "error":
                raise HTTPException(status_code=500, detail=f"Ошибка загрузки из API: {api_result['error']}")

            # Здесь должна быть логика обработки данных API
            # Временно возвращаем успешный ответ без реальной обработки
            return ApiDownloadResponse(
                status="success",
                message="Данные из API успешно загружены (обработка в разработке)",
                inn=request.inn,
                file_name=request.file_name,
                api_url=request.api_url,
                file_size=api_result.get("file_size"),
                download_time=api_result.get("download_time")
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Ошибка обработки: {str(e)}")

    @app.get("/open-superset")
    async def open_superset(autolaunch: bool = False):
        superset_url = "http://localhost:8088"
        is_superset_available = check_superset_availability()

        if autolaunch:
            try:
                def open_browser():
                    time.sleep(1)
                    webbrowser.open(superset_url)

                threading.Thread(target=open_browser, daemon=True).start()

                return {
                    "status": "success",
                    "url": superset_url,
                    "message": "Superset открывается в браузере...",
                    "superset_available": is_superset_available,
                    "credentials": "Логин: admin, пароль: admin"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "url": superset_url,
                    "message": f"Не удалось автоматически открыть браузер: {str(e)}",
                    "superset_available": is_superset_available,
                    "credentials": "Логин: admin, пароль: admin"
                }
        else:
            return {
                "status": "success",
                "url": superset_url,
                "message": "Перейдите по ссылку для доступа к Superset",
                "superset_available": is_superset_available,
                "credentials": "Логин: admin, пароль: admin"
            }

    return app