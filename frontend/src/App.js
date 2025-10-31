import React, { useState } from 'react';
import './App.css';

function App() {
  // Состояния для обработки файлов
  const [filePath, setFilePath] = useState('');
  const [innFile, setInnFile] = useState('');
  const [fileProcessingStatus, setFileProcessingStatus] = useState('');
  const [fileProcessingLoading, setFileProcessingLoading] = useState(false);

  // Состояния для загрузки из API
  const [apiUrl, setApiUrl] = useState('');
  const [innApi, setInnApi] = useState('');
  const [fileName, setFileName] = useState('');
  const [fileExtension, setFileExtension] = useState('json');
  const [apiDownloadStatus, setApiDownloadStatus] = useState('');
  const [apiDownloadLoading, setApiDownloadLoading] = useState(false);

  // Состояния для Superset
  const [supersetStatus, setSupersetStatus] = useState('');
  const [supersetLoading, setSupersetLoading] = useState(false);

  // Обработка файла
  const handleFileProcess = async (e) => {
    e.preventDefault();

    if (!filePath.trim() || !innFile.trim()) {
      setFileProcessingStatus('Пожалуйста, заполните все поля');
      return;
    }

    setFileProcessingLoading(true);
    setFileProcessingStatus('Обработка...');

    try {
      const response = await fetch('http://localhost:8001/process-file/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          file_path: filePath.trim(),
          inn: innFile.trim()
        }),
      });

      const data = await response.json();

      if (response.ok) {
        setFileProcessingStatus(`✅ ${data.message}`);
        setFilePath('');
        setInnFile('');
      } else {
        setFileProcessingStatus(`❌ Ошибка: ${data.detail}`);
      }
    } catch (error) {
      setFileProcessingStatus(`❌ Ошибка соединения: ${error.message}`);
    } finally {
      setFileProcessingLoading(false);
    }
  };

  // Загрузка из API
  const handleApiDownload = async (e) => {
    e.preventDefault();

    if (!apiUrl.trim() || !innApi.trim()) {
      setApiDownloadStatus('Пожалуйста, заполните URL API и ИНН');
      return;
    }

    setApiDownloadLoading(true);
    setApiDownloadStatus('Загрузка...');

    try {
      const requestBody = {
        api_url: apiUrl.trim(),
        inn: innApi.trim(),
        file_extension: fileExtension
      };

      if (fileName.trim()) {
        requestBody.file_name = fileName.trim();
      }

      const response = await fetch('http://localhost:8001/download-from-api/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
      });

      const data = await response.json();

      if (response.ok) {
        let message = `✅ ${data.message}`;
        if (data.file_size) {
          message += ` (Размер: ${formatFileSize(data.file_size)})`;
        }
        if (data.download_time) {
          message += ` (Время: ${data.download_time.toFixed(2)}с)`;
        }
        setApiDownloadStatus(message);

        // Очищаем форму
        setApiUrl('');
        setInnApi('');
        setFileName('');
        setFileExtension('json');
      } else {
        setApiDownloadStatus(`❌ Ошибка: ${data.detail}`);
      }
    } catch (error) {
      setApiDownloadStatus(`❌ Ошибка соединения: ${error.message}`);
    } finally {
      setApiDownloadLoading(false);
    }
  };

  // Открытие Superset
  const handleOpenSuperset = async (autolaunch = false) => {
    setSupersetLoading(true);
    setSupersetStatus('Проверка доступности Superset...');

    try {
      const response = await fetch(`http://localhost:8001/open-superset?autolaunch=${autolaunch}`);
      const data = await response.json();

      if (response.ok) {
        if (data.superset_available) {
          setSupersetStatus(`✅ ${data.message}`);
          if (autolaunch && data.url) {
            // Если автозапуск включен, открываем в новом окне
            window.open(data.url, '_blank');
          }
        } else {
          setSupersetStatus(`⚠️ Superset недоступен: ${data.message}`);
        }

        // Показываем учетные данные
        if (data.credentials) {
          setTimeout(() => {
            alert(`Учетные данные для Superset:\n${data.credentials}`);
          }, 500);
        }
      } else {
        setSupersetStatus(`❌ Ошибка: ${data.detail || data.message}`);
      }
    } catch (error) {
      setSupersetStatus(`❌ Ошибка соединения: ${error.message}`);
    } finally {
      setSupersetLoading(false);
    }
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>File Processor Dashboard</h1>
        <p>Система обработки файлов и данных через Kafka</p>

        {/* Секция обработки файлов */}
        <div className="section">
          <h2>📁 Обработка файлов</h2>
          <form onSubmit={handleFileProcess} className="form">
            <div className="form-group">
              <label htmlFor="filePath">Путь к файлу:</label>
              <input
                type="text"
                id="filePath"
                value={filePath}
                onChange={(e) => setFilePath(e.target.value)}
                placeholder="Например: /path/to/file.csv"
                className="form-input"
              />
            </div>

            <div className="form-group">
              <label htmlFor="innFile">ИНН организации:</label>
              <input
                type="text"
                id="innFile"
                value={innFile}
                onChange={(e) => setInnFile(e.target.value)}
                placeholder="10 или 12 цифр"
                className="form-input"
              />
            </div>

            <button
              type="submit"
              className="submit-btn"
              disabled={fileProcessingLoading}
            >
              {fileProcessingLoading ? 'Обработка...' : 'Обработать файл'}
            </button>
          </form>

          {fileProcessingStatus && (
            <div className={`status-message ${
              fileProcessingStatus.includes('❌') ? 'error' :
              fileProcessingStatus.includes('✅') ? 'success' : 'info'
            }`}>
              {fileProcessingStatus}
            </div>
          )}
        </div>

        {/* Секция загрузки из API */}
        <div className="section">
          <h2>🌐 Загрузка из API</h2>
          <form onSubmit={handleApiDownload} className="form">
            <div className="form-group">
              <label htmlFor="apiUrl">URL API:</label>
              <input
                type="url"
                id="apiUrl"
                value={apiUrl}
                onChange={(e) => setApiUrl(e.target.value)}
                placeholder="https://api.example.com/data"
                className="form-input"
              />
            </div>

            <div className="form-group">
              <label htmlFor="innApi">ИНН организации:</label>
              <input
                type="text"
                id="innApi"
                value={innApi}
                onChange={(e) => setInnApi(e.target.value)}
                placeholder="10 или 12 цифр"
                className="form-input"
              />
            </div>

            <div className="form-row">
              <div className="form-group">
                <label htmlFor="fileName">Имя файла (опционально):</label>
                <input
                  type="text"
                  id="fileName"
                  value={fileName}
                  onChange={(e) => setFileName(e.target.value)}
                  placeholder="auto_generated_if_empty"
                  className="form-input"
                />
              </div>

              <div className="form-group">
                <label htmlFor="fileExtension">Расширение:</label>
                <select
                  id="fileExtension"
                  value={fileExtension}
                  onChange={(e) => setFileExtension(e.target.value)}
                  className="form-select"
                >
                  <option value="json">JSON</option>
                  <option value="txt">TXT</option>
                  <option value="csv">CSV</option>
                  <option value="xml">XML</option>
                </select>
              </div>
            </div>

            <button
              type="submit"
              className="submit-btn"
              disabled={apiDownloadLoading}
            >
              {apiDownloadLoading ? 'Загрузка...' : 'Загрузить из API'}
            </button>
          </form>

          {apiDownloadStatus && (
            <div className={`status-message ${
              apiDownloadStatus.includes('❌') ? 'error' :
              apiDownloadStatus.includes('✅') ? 'success' : 'info'
            }`}>
              {apiDownloadStatus}
            </div>
          )}
        </div>

        {/* Секция Superset */}
        <div className="section">
          <h2>📊 Apache Superset</h2>
          <div className="superset-actions">
            <button
              onClick={() => handleOpenSuperset(false)}
              className="action-btn"
              disabled={supersetLoading}
            >
              {supersetLoading ? 'Проверка...' : 'Проверить доступность'}
            </button>

            <button
              onClick={() => handleOpenSuperset(true)}
              className="action-btn primary"
              disabled={supersetLoading}
            >
              {supersetLoading ? 'Открытие...' : 'Открыть Superset'}
            </button>
          </div>

          {supersetStatus && (
            <div className={`status-message ${
              supersetStatus.includes('❌') ? 'error' :
              supersetStatus.includes('✅') ? 'success' : 'info'
            }`}>
              {supersetStatus}
            </div>
          )}

          <div className="info-box">
            <h4>Информация о Superset:</h4>
            <ul>
              <li>URL: http://localhost:8088</li>
              <li>Логин: admin</li>
              <li>Пароль: admin</li>
              <li>Для аналитики и визуализации данных</li>
            </ul>
          </div>
        </div>

        {/* Информационная панель */}
        <div className="info-panel">
          <h3>ℹ️ Информация о системе</h3>
          <div className="info-grid">
            <div className="info-item">
              <strong>Kafka:</strong> localhost:9092
            </div>
            <div className="info-item">
              <strong>PostgreSQL:</strong> localhost:5433
            </div>
            <div className="info-item">
              <strong>Backend API:</strong> localhost:8001
            </div>
            <div className="info-item">
              <strong>Топик:</strong> file-chunks-topic
            </div>
          </div>
        </div>
      </header>
    </div>
  );
}

export default App;