# Валидация модели обнаружения мошенничества

### Обязательные задания

1. **Запустить систему Apache Airflow** в сервисе Yandex Cloud Managed Service for Apache Airflow.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/airflow_cluster.png width=1080 />

2. **Запустить систему MLflow** на отдельной виртуальной машине, а также базу данных метаданных для MLflow в сервисе Yandex Cloud Managed Service for PostgreSQL/MySQL либо на отдельной ВМ.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_07/screenshots/mlflow.png width=1080 />

3. **Выбрать стратегию для валидации модели**. Создать код для проведения А/В теста и оценить метрики модели на выбранной стратегии.
Для валидации новой версии модели применяется champion–challenger подход на общей отложенной выборке. Статистическая значимость различий оценивается с помощью парного bootstrap-теста: обе модели делают прогнозы на одних и тех же объектах, после чего bootstrap-ресэмплинг используется для оценки доверительного интервала и p-value разницы F1-score. Challenger принимается, если его F1 выше, 95%-й доверительный интервал разницы F1 не включает ноль и p-value < 0.05.
[Код для проведения А/В теста](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_07/src/ab_test.py)

4. **Добавить в AirFlow шаг по валидации модели** и фиксации метрик в MLFlow.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_07/screenshots/dag_test_pipeline.png width=1080 />

5. **Обеспечить сохранение метрик модели и артефактов** (обученной модели) в S3 хранилище (Object storage).
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/mlflow_models_metrics.png width=1080 />

<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/mlflow_artifacts.png width=1080 />

<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_07/screenshots/ab_test_results.png width=1080 />

6. **Разрешить периодическое исполнение** разработанного DAG в Apache AirFlow и протестировать его работоспособность.
[schedule_interval=timedelta(minutes=180)](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/80907e6ef05f709e2923b6af4920a46052b577ec/hw_07/dags/ab_test_pipeline.py#L141).
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_07/screenshots/dag_test_pipeline.png width=1080 />

### Дополнительные задания

7. **Изменить статус задач** на Kanban-доске в GitHub Projects в соответствии с достигнутыми результатами. Возможно, некоторые задачи нужно будет скорректировать, разделить на подзадачи или объединить друг с другом.
