Обязательные задания
1. **Создать новый bucket в Yandex Cloud Object Storage с использованием terraform скрипта.**
Оснойвной скрипт запуска Terraform [main.tf](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/infra/main.tf).
Для инициализации использовались команды
```
terraform init
terraform plan
terraform apply
```

2. **Скопировать содержимое предоставленного хранилища с использованием инструмента s3cmd.**
Точка доступа: https://console.yandex.cloud/folders/b1g4ki09n8igs1si54v2/storage/buckets/otus-bucket-b1g4ki09n8igs1si54v2?versionsDisplay=false.
Команда для копирования всего хранилища в бакет находится в файле [user_data.sh](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/26131cfe287e7d5c5f05c202c477d237e1f3f732/hw_02/infra/scripts/user_data.sh#L113).
```
s3cmd cp \
    --config=/home/ubuntu/.s3cfg \
    --acl-public \
    --recursive \
    s3://otus-mlops-source-data/ \
    s3://otus-bucket-{yc_folder_id}/
```
Общедостпуность бакета обеспечивается [аргументами](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/2e7da7e03fda4764e179895d310fb5e97df10a8b/hw_02/infra/main.tf#L120)
```
anonymous_access_flags {
    read = true
    list = true
  }
```

3. **Создать Spark-кластер в Yandex Data Processing с двумя подкластерами согласно указанным характеристикам.**
Cкрипт main.tf resource "yandex_dataproc_cluster" [line 127](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/26131cfe287e7d5c5f05c202c477d237e1f3f732/hw_02/infra/main.tf#L127).

4.**Соединиться по SSH с мастер-узлом и выполнить на нём команду копирования содержимого хранилища в файловую систему HDFS с использованием инструмента hadoop distcp.**
Содержимое HDFS-директории в консоли
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/screenshots/HW_02_%D1%81%D0%BE%D0%B4%D0%B5%D1%80%D0%B6%D0%B8%D0%BC%D0%BE%D0%B5_HDFS-%D0%B4%D0%B8%D1%80%D0%B5%D0%BA%D1%82%D0%BE%D1%80%D0%B8%D0%B8_%D0%B2_%D0%BA%D0%BE%D0%BD%D1%81%D0%BE%D0%BB%D0%B8.png width=1080 />

<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/screenshots/HW_02_%D1%81%D0%BE%D0%B4%D0%B5%D1%80%D0%B6%D0%B8%D0%BC%D0%BE%D0%B5_HDFS-%D0%B4%D0%B8%D1%80%D0%B5%D0%BA%D1%82%D0%BE%D1%80%D0%B8%D0%B8_%D0%B2_%D0%BA%D0%BE%D0%BD%D1%81%D0%BE%D0%BB%D0%B8_2.png width=1080 />

5.**Оценить месячные затраты используя тарифный калькулятор Yandex Cloud для поддержания работоспособности созданного кластера.** Оценить, насколько использование HDFS-хранилища дороже, чем объектного.
Если кластер работает 24/7, то получается приблизительно: 48.15 руб/час × 24 × 30 = 34_668 руб/месяц
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/screenshots/5_%D0%9E%D1%86%D0%B5%D0%BD%D0%B8%D1%82%D1%8C%20%D0%BC%D0%B5%D1%81%D1%8F%D1%87%D0%BD%D1%8B%D0%B5%20%D0%B7%D0%B0%D1%82%D1%80%D0%B0%D1%82%D1%8B.png width=1080 />

Использование HDFS-хранилища обходится дороже объектного хранилища, поскольку данные размещаются на локальных SSD-дисках узлов кластера, которые требуют постоянного резервирования вычислительных ресурсов и оплаты виртуальных машин.
Object Storage является более экономичным вариантом хранения данных, так как не требует постоянно работающих вычислительных узлов и оплачивается отдельно от вычислительных ресурсов.
При этом HDFS обеспечивает более высокую производительность при обработке данных в Spark за счёт локального размещения данных на узлах кластера и уменьшения сетевых задержек.

**Дополнительные задания**
6.**Предложить способы для оптимизации затрат на содержание Spark-кластера в облаке и попробовать их реализовать.**

7.**Изменить статус задач на Kanban-доске в GitHub Projects в соответствии с достигнутыми результатами. Возможно, некоторые задачи нужно будет скорректировать, разделить на подзадачи или объединить друг с другом.**  
Статус задачи переведен в "На ревью" - https://github.com/users/Anastasiia-Pov/projects/2/views/2

8.**Полностью удалить созданный кластер с помощью команды terraform destroy, чтобы избежать оплаты ресурсов в период его простаивания.**
Кластер удален с помощью команды ```terraform destroy```