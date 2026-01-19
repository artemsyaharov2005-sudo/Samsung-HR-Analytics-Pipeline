# Vacancy Analytics Pipeline (PySpark + Hive + Dashboard)

Данный проект представляет собой систему для автоматизированного анализа вакансий: ETL → аналитика → ML → отчёты → веб-дашборд.
Решение ориентировано на работу с большими объёмами данных и включает тестирование масштабируемости.
“В репозитории присутствует пример датасета (vacancies_stage.csv) для быстрого запуска и проверки работы проекта.”

## Технологии
- Python
- PySpark
- Docker / Docker Compose
- HDFS / Hive
- Streamlit (Dashboard)
- Matplotlib

## Структура проекта
- `tesh_hive` - тест хайва
- `run_pipeline.py` — запуск всего конвейера 
- `analytics_and_ml.py` — аналитика и ML-модуль
- `scalability_benchmark.py` — тест масштабируемости и замеры времени обработки
- `dashboaed_streamlit.py`  — дашборд Streamlit
- `data/stage/` — подготовленные данные после ETL
- `data/reports/` — отчёты, графики и результаты модели

## Запуск проекта

### 
1) (Опционально) Запуск инфраструктуры в Docker

docker compose up -d

2) Запуск пайплайна
python run_pipeline.py

После выполнения будут сформированы результаты в папках:

data/stage/

data/reports/

3) Запуск дашборда
streamlit run app.py

4) Команда запуска дашборда
streamlit run dashboard_streamlit.py



Дашборд откроется в браузере:
http://localhost:8501

Результаты работы

После запуска формируются отчёты и визуализации:

vacancies_stage.csv — подготовленный датасет после ETL

top_cities.csv/png — топ городов по количеству вакансий

salary_by_city.csv/png — анализ зарплат по городам

ml_results.txt — результаты ML-модуля

scalability_results.csv — таблица замеров производительности

scalability_plot.png — график масштабируемости

Авторы

Сахаров Артём


Селиванов Иван
