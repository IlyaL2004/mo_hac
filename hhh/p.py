

import os
import sys

output_file = './output.txt'
script_name = os.path.basename(sys.argv[0])
excluded_files = [output_file, script_name]
excluded_dirs = ['minio-data', 'clickhouse-data', 'logs']  # Добавляем папку для исключения
root_dir = os.getcwd()

with open(output_file, 'w', encoding='utf-8') as f:
    # Записываем директорию запуска
    f.write(f"Скрипт запущен в папке: {root_dir}\n\n")

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Пропускаем исключенные директории
        if any(excluded_dir in dirpath for excluded_dir in excluded_dirs):
            continue

        # Формируем относительный путь
        relative_path = os.path.relpath(dirpath, root_dir)
        folder_name = os.path.basename(dirpath) if relative_path == '.' else relative_path

        # Записываем информацию о папке
        f.write(f"【 Папка: {folder_name} 】\n")
        f.write("Содержимое:\n")

        # Перечисляем подпапки
        for d in dirnames:
            f.write(f"    {d}/\n")

        # Перечисляем файлы (включая исключенные)
        for file in filenames:
            f.write(f"    {file}\n")

        f.write("\n")

        # Обрабатываем содержимое файлов (кроме исключенных)
        for filename in [f for f in filenames if f not in excluded_files]:
            file_path = os.path.join(dirpath, filename)
            relative_file_path = os.path.relpath(file_path, root_dir)

            f.write(f"Файл: {relative_file_path} \n")

            try:
                with open(file_path, 'r', encoding='utf-8') as fc:
                    content = fc.read()
                    f.write(f"Содержимое:\n{content}\n\n")
            except UnicodeDecodeError:
                f.write("Содержимое: [Бинарные данные]\n\n")
            except PermissionError:
                f.write("Содержимое: [Нет доступа]\n\n")
            except Exception as e:
                f.write(f"Ошибка чтения: {str(e)}\n\n")

    f.write("\n" + "=" * 40 + "\n")
    f.write("Сканирование завершено успешно!")
