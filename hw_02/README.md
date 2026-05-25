Обязательные задания
1. **Создать новый bucket в Yandex Cloud Object Storage с использованием terraform скрипта.** Примеры вы сможете найти в материалах занятия или на странице документации Yandex Cloud. Выложить созданный вами скрипт в GitHub репозиторий c заданием.
Оснойвной скрипт запуска Terraform [main.tf](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/infra/main.tf).
Для инициализации использовались команды
```
terraform init
terraform plan
terraform apply
```

2. **Скопировать содержимое предоставленного хранилища с использованием инструмента s3cmd.** Для проверки преподавателем данный bucket необходимо сделать общедоступным, а точку доступа к нему привести в README-файле Вашего GitHub-репозитория.
Точка доступа: https://console.yandex.cloud/folders/b1g4ki09n8igs1si54v2/storage/buckets/otus-bucket-b1g4ki09n8igs1si54v2?versionsDisplay=false
Команда для копирования всего хранилища в бакет находится в файле [user_data.sh](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/26131cfe287e7d5c5f05c202c477d237e1f3f732/hw_02/infra/scripts/user_data.sh#L113)
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

3. **Создать Spark-кластер в Yandex Data Processing с двумя подкластерами согласно указанным характеристикам.** Для экономии ресурсов необходимо использовать terraform скрипт для создания и удаления кластера. Примеры вы сможете найти в материалах занятия или на странице документации Yandex Cloud.
Cкрипт main.tf resource "yandex_dataproc_cluster"[line 127](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/26131cfe287e7d5c5f05c202c477d237e1f3f732/hw_02/infra/main.tf#L127)  

4.**Соединиться по SSH с мастер-узлом и выполнить на нём команду копирования содержимого хранилища в файловую систему HDFS с использованием инструмента hadoop distcp.** Для проверки преподавателем необходимо вывести содержимое HDFS-директории в консоль, а снимок экрана с этой информацией привести в README-файле Вашего GitHub-репозитория.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/screenshots/HW_02_%D1%81%D0%BE%D0%B4%D0%B5%D1%80%D0%B6%D0%B8%D0%BC%D0%BE%D0%B5_HDFS-%D0%B4%D0%B8%D1%80%D0%B5%D0%BA%D1%82%D0%BE%D1%80%D0%B8%D0%B8_%D0%B2_%D0%BA%D0%BE%D0%BD%D1%81%D0%BE%D0%BB%D0%B8.png width=1080 />

<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/screenshots/HW_02_%D1%81%D0%BE%D0%B4%D0%B5%D1%80%D0%B6%D0%B8%D0%BC%D0%BE%D0%B5_HDFS-%D0%B4%D0%B8%D1%80%D0%B5%D0%BA%D1%82%D0%BE%D1%80%D0%B8%D0%B8_%D0%B2_%D0%BA%D0%BE%D0%BD%D1%81%D0%BE%D0%BB%D0%B8_2.png width=1080 />

5.**Оценить месячные затраты используя тарифный калькулятор Yandex Cloud для поддержания работоспособности созданного кластера.** Оценить, насколько использование HDFS-хранилища дороже, чем объектного.


**Дополнительные задания**
6.**Предложить способы для оптимизации затрат на содержание Spark-кластера в облаке и попробовать их реализовать.**

7.**Изменить статус задач на Kanban-доске в GitHub Projects в соответствии с достигнутыми результатами. Возможно, некоторые задачи нужно будет скорректировать, разделить на подзадачи или объединить друг с другом.**

8.**Полностью удалить созданный кластер с помощью команды terraform destroy, чтобы избежать оплаты ресурсов в период его простаивания.**
s3cmd ls s3://otus-mlops-source-data/ | awk '{print $4}' | while read file; do echo "Copying: $file" s3cmd cp --config=/home/ubuntu/.s3cfg --acl-public "$file" s3://otus-bucket-b1g4ki09n8igs1si54v2/ sleep 1 done