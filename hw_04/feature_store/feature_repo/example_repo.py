import os

from datetime import timedelta

import numpy as np
import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64

REPO_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(REPO_PATH, "data")

# Определяем сущность для водителя. Сущность можно рассматривать как первичный ключ,
# который используется для получения признаков
driver = Entity(name="driver", join_keys=["driver_id"])

# Читаем данные из parquet файлов.
driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path=os.path.join(DATA_PATH, "driver_stats.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)


# Наши parquet файлы содержат примеры данных, включающие столбец driver_id,
# временные метки и три столбца с признаками. Здесь мы определяем Feature View,
# который позволит нам передавать эти данные в нашу модель онлайн
driver_performance_fv = FeatureView(
    # Уникальное имя этого представления признаков. Два представления признаков
    # в одном проекте не могут иметь одинаковое имя
    name="DriverPerformanceFV",
    entities=[driver],
    ttl=timedelta(days=1),
    # Список признаков, определенных ниже, действует как схема для материализации
    # признаков в хранилище, а также используется как ссылки при извлечении
    # для создания обучающего набора данных или предоставления признаков
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
    ],
    online=True,
    source=driver_stats_source,
    # Теги - это определенные пользователем пары ключ/значение,
    # которые прикрепляются к каждому представлению признаков
    tags={"team": "driver_performance"},
)

driver_activity_fv = FeatureView(
    # Уникальное имя этого представления признаков. Два представления признаков
    # в одном проекте не могут иметь одинаковое имя
    name="DriverActivityFV",
    entities=[driver],
    ttl=timedelta(days=1),
    # Список признаков, определенных ниже, действует как схема для материализации
    # признаков в хранилище, а также используется как ссылки при извлечении
    # для создания обучающего набора данных или предоставления признаков
    schema=[
        Field(name="avg_daily_trips", dtype=Int64, description="Среднее количество поездок в день"),
    ],
    online=True,
    source=driver_stats_source,
    # Теги - это определенные пользователем пары ключ/значение,
    # которые прикрепляются к каждому представлению признаков
    tags={"team": "driver_activity"},
)

# Определяем представление признаков по требованию, которое может генерировать
# новые признаки на основе существующих представлений и признаков из RequestSource
@on_demand_feature_view(
    sources=[driver_performance_fv],
    schema=[
        Field(name="efficiency_score", dtype=Float32),
    ],
)

def compute_driver_scores(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["efficiency_score"] = (
        inputs["conv_rate"] * inputs["acc_rate"]
    ).astype("float32")
    return df


# FeatureService группирует признаки в версию модели
driver_efficiency = FeatureService(
    name="driver_features_v1",
    features=[
        driver_performance_fv,
        driver_activity_fv,
        compute_driver_scores,
    ],
    logging_config=LoggingConfig(
        destination=FileLoggingDestination(path=DATA_PATH)
    ),
)
