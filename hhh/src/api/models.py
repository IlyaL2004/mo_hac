from pydantic import BaseModel
from typing import Optional

class FileProcessRequest(BaseModel):
    file_path: str
    inn: str

class ApiDownloadRequest(BaseModel):
    api_url: str
    inn: str
    file_name: Optional[str] = None
    file_extension: Optional[str] = "json"

class ProcessResponse(BaseModel):
    status: str
    message: str
    inn: Optional[str] = None
    file_name: Optional[str] = None

class FileContentResponse(BaseModel):
    status: str
    file_name: str
    file_type: str
    content: Optional[dict] = None
    text_content: Optional[str] = None
    error: Optional[str] = None

class ApiDownloadResponse(BaseModel):
    status: str
    message: str
    inn: str
    file_name: str
    api_url: str
    file_size: Optional[int] = None
    download_time: Optional[float] = None