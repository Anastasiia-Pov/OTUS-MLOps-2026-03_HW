# Регулярное переобучение модели обнаружения мошенничества

### Обязательные задания

1. **Запустить систему Apache Airflow** в сервисе Yandex Cloud Managed Service for Apache Airflow.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/airflow_cluster.png width=1080 />

2. **Запустить систему MLflow** на отдельной виртуальной машине, а также базу данных метаданных для MLflow в сервисе Yandex Cloud Managed Service for PostgreSQL/MySQL либо на отдельной ВМ.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/mlflow_server.png width=1080 />

3. **Создать python скрипт** с использованием PySpark для обучения модели на облачном Spark-кластере и фиксацией результатов в MLFlow сервере.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/mlflow_experiments.png width=1080 />

<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/mlflow_training_20260824_1304.png width=1080 />

4. **Обеспечить сохранение метрик модели и артефактов** (обученной модели) в S3 хранилище (Object storage).
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/mlflow_models_metrics.png width=1080 />

<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/mlflow_artifacts.png width=1080 />

5. **Разрешить периодическое исполнение** разработанного DAG в Apache AirFlow и протестировать его работоспособность.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/screenshots/airflow_dag_training_pipeline.png width=1080 />

### Дополнительные задания

6. **Изменить статус задач** на Kanban-доске в GitHub Projects в соответствии с достигнутыми результатами. Возможно, некоторые задачи нужно будет скорректировать, разделить на подзадачи или объединить друг с другом.

7. **Полностью удалить созданный кластер**, чтобы избежать оплаты ресурсов в период его простаивания.


### Особенности реализации
а. **В input_data были загружены подготовленные .parquet-файлы в формате директорий**
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_06/data/input_data/airflow_bucket_input_data.png width=1080 />

б. **В связи с ограниченностью вычеслительных ресурсов был реализован следующий алгоритм:**
    - [рандомно выбирается директория с .parquet-файлами](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/b016c6cbf95a7919b2776af4dbe81af2d7e26c0b/hw_06/src/train.py#L140-L148)
    - [рекурсивно читаются все .snappy.parquet-файлы](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/b016c6cbf95a7919b2776af4dbe81af2d7e26c0b/hw_06/src/train.py#L154)
    - [выбираем кол-во строк `--sample-size`, которые будут задействованы в обучении и тесте](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/b016c6cbf95a7919b2776af4dbe81af2d7e26c0b/hw_06/dags/training_pipeline.py#L177)
    Если в каталоге достаточно строк, выборка содержит ровно `--sample-size` строк. В противном случае используются все доступные строки.