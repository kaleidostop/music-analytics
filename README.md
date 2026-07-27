# Music Analytics Pipeline

Учебный ETL/BI-проект для анализа акустических характеристик записей из AcousticBrainz.

## Архитектура

```text
AcousticBrainz → Airflow → Spark → ClickHouse → Power BI
```

- Airflow оркестрирует загрузку и обработку.
- Spark очищает и преобразует JSON-данные.
- ClickHouse хранит факты и аналитические витрины.
- Power BI визуализирует жанры, настроение, BPM и качество данных.

## Статус

Добавлены загрузчик официальных sample-архивов и инструмент исследования
JSON-схемы.

## Загрузка данных

Оба sample-архива (около 2,1 ГБ):

```powershell
python src/download_data.py
```

Только меньший high-level архив:

```powershell
python src/download_data.py --dataset highlevel
```

Файлы сохраняются в `data/raw/archives`. Загрузчик проверяет официальные
SHA256-суммы, пишет сначала во временный `.part` и не скачивает повторно уже
проверенный архив.

## Проверка схемы JSON

```powershell
python src/schema_inspector.py path/to/json/files --limit 100
```

Обязательные поля можно проверить параметром `--required`:

```powershell
python src/schema_inspector.py data/raw/lowlevel `
  --required metadata `
  --required rhythm.bpm `
  --required tonal.key_key
```

Подробности доступны через `python src/schema_inspector.py --help`.

## High-level PySpark-трансформация

Установить зависимости:

```powershell
python -m pip install -r requirements.txt
```

Запустить трансформацию на извлечённой выборке:

```powershell
python src/transform_highlevel.py
```

Скрипт использует явную схему, приводит вложенный JSON к плоской таблице,
проверяет MBID, нормализует вероятности и удаляет дубликаты. Пока результат
выводится в консоль; запись в ClickHouse будет следующим этапом.

Инструкция полного запуска появится после подготовки Docker Compose и первого сквозного ETL-процесса.
