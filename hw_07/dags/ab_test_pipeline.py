"""Airflow DAG for comparing registered fraud models on Yandex Data Proc."""

import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Connection, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule


YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_INPUT_DATA_BUCKET = f"s3a://{S3_BUCKET_NAME}/input_data"
S3_SRC_BUCKET = f"s3a://{S3_BUCKET_NAME}/src"
S3_VENV_ARCHIVE = f"s3a://{S3_BUCKET_NAME}/venvs/venv.tar.gz"

DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
AB_TEST_EXPERIMENT_NAME = Variable.get(
    "AB_TEST_EXPERIMENT_NAME", default_var="fraud_detection_ab_test"
)
AB_TEST_MODEL_NAME = Variable.get(
    "AB_TEST_MODEL_NAME", default_var="fraud_detection_model"
)
AB_TEST_SAMPLE_SIZE = Variable.get("AB_TEST_SAMPLE_SIZE", default_var="100000")
AB_TEST_BOOTSTRAP_ITERATIONS = Variable.get(
    "AB_TEST_BOOTSTRAP_ITERATIONS", default_var="200"
)
AB_TEST_ALPHA = Variable.get("AB_TEST_ALPHA", default_var="0.05")
AB_TEST_AUTO_PROMOTE = Variable.get(
    "AB_TEST_AUTO_PROMOTE", default_var="false"
).lower() in {"1", "true", "yes"}

YC_S3_CONNECTION = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host=S3_ENDPOINT_URL,
    extra={
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
        "host": S3_ENDPOINT_URL,
    },
)
YC_SA_CONNECTION = Connection(
    conn_id="yc-sa",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY,
        "extra__yandexcloud__service_account_json": DP_SA_JSON,
    },
)


def setup_airflow_connections(*connections):
    """Create the shared Yandex Cloud connections when they do not exist."""
    session = Session()
    try:
        for connection in connections:
            exists = (
                session.query(Connection)
                .filter(Connection.conn_id == connection.conn_id)
                .first()
            )
            if not exists:
                session.add(connection)
                print(f"Added Airflow connection: {connection.conn_id}")
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def run_setup_connections(**_context):
    """PythonOperator entry point for connection initialization."""
    setup_airflow_connections(YC_S3_CONNECTION, YC_SA_CONNECTION)


default_args = {
    "owner": "AnPov",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

job_args = [
    "--input",
    S3_INPUT_DATA_BUCKET,
    "--tracking-uri",
    MLFLOW_TRACKING_URI,
    "--experiment-name",
    AB_TEST_EXPERIMENT_NAME,
    "--model-name",
    AB_TEST_MODEL_NAME,
    "--production-alias",
    "champion",
    "--candidate-alias",
    "challenger",
    "--sample-size",
    AB_TEST_SAMPLE_SIZE,
    "--bootstrap-iterations",
    AB_TEST_BOOTSTRAP_ITERATIONS,
    "--alpha",
    AB_TEST_ALPHA,
    "--s3-endpoint-url",
    S3_ENDPOINT_URL,
    "--s3-access-key",
    S3_ACCESS_KEY,
    "--s3-secret-key",
    S3_SECRET_KEY,
]
if AB_TEST_AUTO_PROMOTE:
    job_args.append("--auto-promote")


with DAG(
    dag_id="ab_test_pipeline",
    default_args=default_args,
    description="A/B test champion and challenger Spark models",
    schedule_interval=None,
    max_active_runs=1,
    start_date=datetime(2025, 3, 27),
    catchup=False,
    tags=["mlops", "ab-test", "yandex-cloud"],
) as dag:
    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=run_setup_connections,
    )

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="spark-cluster-create-task",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-dp-ab-test-{uuid.uuid4()}",
        cluster_description="Temporary Data Proc cluster for model A/B testing",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=S3_BUCKET_NAME,
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        security_group_ids=[DP_SECURITY_GROUP_ID],
        zone=YC_ZONE,
        cluster_image_version="2.0",
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=50,
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=50,
        datanode_count=1,
        computenode_count=0,
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        connection_id=YC_SA_CONNECTION.conn_id,
    )

    run_ab_test = DataprocCreatePysparkJobOperator(
        task_id="run-ab-test",
        main_python_file_uri=f"{S3_SRC_BUCKET}/ab_test.py",
        connection_id=YC_SA_CONNECTION.conn_id,
        args=job_args,
        properties={
            "spark.submit.deployMode": "cluster",
            "spark.yarn.dist.archives": f"{S3_VENV_ARCHIVE}#.venv",
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./.venv/bin/python3",
            "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON": "./.venv/bin/python3",
        },
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="spark-cluster-delete-task",
        connection_id=YC_SA_CONNECTION.conn_id,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    setup_connections >> create_spark_cluster >> run_ab_test >> delete_spark_cluster
