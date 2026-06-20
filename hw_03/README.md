### Обязательные задания

4. **Проанализировать датасет** мошеннических транзакций на наличие в нем ошибочных данных. Данное действие было выполнено с помощью среды Jupyter Notebook, запущенной на мастер-узле кластера = [скрипт для анализа](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_03/notebooks/feature_extraction.ipynb)

5. **Создать скрипт очистки данных** на основе проведенного анализа качества с использованием Apache Spark. Скрипт должен иметь возможность автоматического запуска внешней системой. = [скрипт для батчего запуска](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_03/notebooks/batch_process.py)

**Исправленные ошибки:**
Всего: 1_879_794_138 записей
- опечатка в заголовке таблиц: tranaction_id => transaction_id
- tx_datetime: было обнаружено некорректное время 24ч (2019-09-13 24:00:00) - замена на 00:00:00
- terminal_id - встречались значения Err, которые были обозначены, как -1
- terminal_id - встречались значения isNull, которые на данный момент были оставлены без изменений, как Null (т.к. присваивать рандомный id неизвестному терминалу кажется необоснованным на данный момент кол-во пустых значений 33,091)
- была осуществлена проверка tx_fraud и tx_fraud_scenario колонок на соответствие (проверялось условие: если tx_fraud=0 tx_fraud_scenario не может иметь значение отличное от 0)
Предварительная оценка: кол-во ошибок незначильно относительно общего масштаба данных

**Возможные ошибки**
- если tx_datetime=NULL => можно заменить отсутсвующие значения на минимальную дату в файле


Результаты работы сохранены в бакет
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_03/screenshots/bucket_parquets.png width=1080 />

В каждой директории хранятся партиции одного файла
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_03/screenshots/bucket_parquets_inside.png width=1080 />

[Ссылка на Object Storage](https://console.yandex.cloud/folders/b1g4ki09n8igs1si54v2/storage/buckets/otus-bucket-b1g4ki09n8igs1si54v2?versionsDisplay=false)


### Дополнительные задания

7. **Изменить статус задач** на Kanban-доске в GitHub Projects в соответствии с достигнутыми результатами. Возможно, некоторые задачи нужно будет скорректировать, разделить на подзадачи или объединить друг с другом. [ссылка на Kanban-доску](https://github.com/users/Anastasiia-Pov/projects/2/views/2?pane=issue&itemId=178500587&issue=Anastasiia-Pov%7COTUS-MLOps-2026-03_HW%7C2)
