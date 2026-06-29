### Обязательные задания

1. **Создайте 2 новые Feature View** на основе данных примера из лекции. Каждая Feature View должна содержать логически связанные признаки и иметь осмысленное назначение.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_04/screenshots/project_feature_store.png width=1080 />

<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_04/screenshots/feature_views.png width=1080 />


2. **Создайте 1 on-demand Feature View** для вычисления признаков в реальном времени. Данная Feature View должна использовать функции трансформации для создания новых признаков на основе существующих данных.
<img src=https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_04/screenshots/feature_on_demand.png width=1080 />


3. **Приложите ноутбук**, в котором делаете запрос к этим Feature View. Ноутбук должен содержать примеры получения признаков как для исторических данных (для обучения), так и для online-инференса.
Ссылка на [ноутбук](https://github.com/Anastasiia-Pov/OTUS-MLOps-2026-03_HW/blob/main/hw_04/example.ipynb)



```
python3.11 -m venv .venv3.11
source .venv3.11/bin/activate
pip install pandas pyarrow feast
pip install ipykernel

pip install --upgrade pip
pip install poetry
poetry show | grep feast
poetry env use python3.11
cd feature_store/feature_repo
poetry run feast apply
poetry run feast ui
```