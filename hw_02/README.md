Обязательные задания
1. **Создать новый bucket в Yandex Cloud Object Storage с использованием terraform скрипта.** Примеры вы сможете найти в материалах занятия или на странице документации Yandex Cloud. Выложить созданный вами скрипт в GitHub репозиторий c заданием.
Оснойвной скрипт запуска Terraform.
[main.tf](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/infra/main.tf).
Для инициализации использовались команды
```
terraform init
terraform plan
terraform apply
```

2. **Скопировать содержимое предоставленного хранилища с использованием инструмента s3cmd.** Для проверки преподавателем данный bucket необходимо сделать общедоступным, а точку доступа к нему привести в README-файле Вашего GitHub-репозитория.
Команда для копирования всего хранилища в бакет находится в файле [user_data.sh](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/26131cfe287e7d5c5f05c202c477d237e1f3f732/hw_02/infra/scripts/user_data.sh#L113)
```
s3cmd cp \
    --config=/home/ubuntu/.s3cfg \
    --acl-public \
    s3://otus-mlops-source-data/ \
    s3://$TARGET_BUCKET/
```


3. **Создать Spark-кластер в Yandex Data Processing с двумя подкластерами согласно указанным характеристикам.** Для экономии ресурсов необходимо использовать terraform скрипт для создания и удаления кластера. Примеры вы сможете найти в материалах занятия или на странице документации Yandex Cloud.
Cкрипт main.tf resource "yandex_dataproc_cluster"[line 127](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/26131cfe287e7d5c5f05c202c477d237e1f3f732/hw_02/infra/main.tf#L127)


4.**Соединиться по SSH с мастер-узлом и выполнить на нём команду копирования содержимого хранилища в файловую систему HDFS с использованием инструмента hadoop distcp.** Для проверки преподавателем необходимо вывести содержимое HDFS-директории в консоль, а снимок экрана с этой информацией привести в README-файле Вашего GitHub-репозитория.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/screenshots/HW_02_%D1%81%D0%BE%D0%B4%D0%B5%D1%80%D0%B6%D0%B8%D0%BC%D0%BE%D0%B5_HDFS-%D0%B4%D0%B8%D1%80%D0%B5%D0%BA%D1%82%D0%BE%D1%80%D0%B8%D0%B8_%D0%B2_%D0%BA%D0%BE%D0%BD%D1%81%D0%BE%D0%BB%D0%B8.png width=750 />

<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_02/screenshots/HW_02_%D1%81%D0%BE%D0%B4%D0%B5%D1%80%D0%B6%D0%B8%D0%BC%D0%BE%D0%B5_HDFS-%D0%B4%D0%B8%D1%80%D0%B5%D0%BA%D1%82%D0%BE%D1%80%D0%B8%D0%B8_%D0%B2_%D0%BA%D0%BE%D0%BD%D1%81%D0%BE%D0%BB%D0%B8_2.png width=750 />

5.**Оценить месячные затраты используя тарифный калькулятор Yandex Cloud для поддержания работоспособности созданного кластера.** Оценить, насколько использование HDFS-хранилища дороже, чем объектного.
