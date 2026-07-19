### Обязательные задания

1. **Запустить систему Apache Airflow** в сервисе Yandex Cloud Managed Service for Apache Airflow.  

2. **Создать DAG для ежедневного автоматизированного создания и удаления** Spark-кластера и запуска скрипта очистки датасета и разместить его в директории для DAG'ов, доступной Apache Airflow. Для этого можно удобно использовать S3 bucket. В графе следует прописать этапы копирования скрипта и необходимых ему файлов на Spark-кластер, а также его запуска на кластере посредством spark-submit.  
[DAG's pipeline](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/tree/main/hw_05/dags)

3. **Убедиться, что граф загрузился** в систему и отображается в графическом интерфейсе. Файл(-ы) с DAG необходимо разместить в Вашем GitHub-репозитории и предоставить для проверки.  
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_05/screenshot/apache_airflow_dag.png width=1080 />

4. **Разрешить периодическое исполнение** разработанного DAG в Apache AirFlow и протестировать его работоспособность. Требуется дождаться не менее трёх успешных запусков процедуры очистки датасета по расписанию. Снимок экрана, подтверждающий успешную работу системы, необходимо привести в README-файле Вашего GitHub-репозитория.  
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_05/screenshot/periodical_apache_airflow_perfomance.png width=1080 />


### Дополнительные задания

5. **Изменить статус задач** на Kanban-доске в GitHub Projects в соответствии с достигнутыми результатами. Возможно, некоторые задачи нужно будет скорректировать, разделить на подзадачи или объединить друг с другом.  
[Kanban-доска](https://github.com/users/Anastasiia-Pov/projects/2/views/2?pane=issue&itemId=215375986&issue=Anastasiia-Pov%7COTUS-MLOps-2026-03_HW%7C6)

6. **Добавить CI/CD пайплайн в GitHub Actions** для автоматизации изменения кода DAG'ов и python-скриптов.  
[Github-workflow](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_05/.github/workflows/main.yml)

7. **Полностью удалить созданный кластер**, чтобы избежать оплаты ресурсов в период его простаивания.  
```terraform destroy```
