"""Compare two registered Spark models on Yandex Data Proc.

The job loads a single evaluation sample from Yandex Object Storage, scores the
MLflow ``champion`` and ``challenger`` model versions on exactly the same rows,
and estimates the F1 difference with a paired bootstrap. Comparison metrics and
the complete JSON result are stored in MLflow.
"""

import argparse
import json
import os
import secrets
import sys
import traceback
from datetime import datetime, timezone

import mlflow
import mlflow.spark
import numpy as np
from mlflow.tracking import MlflowClient
from pyspark import StorageLevel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


LABEL_COL = "tx_fraud"
ROW_ID_COL = "_ab_test_row_id"
METRIC_NAMES = ("accuracy", "precision", "recall", "f1")


def create_spark_session(s3_config=None):
    """Build a Spark session configured for Yandex Object Storage."""
    builder = SparkSession.builder.appName("FraudDetectionABTest")

    if s3_config:
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config("spark.hadoop.fs.s3a.endpoint", s3_config["endpoint_url"])
            .config("spark.hadoop.fs.s3a.access.key", s3_config["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", s3_config["secret_key"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        )

    return builder.getOrCreate()


def list_parquet_directories(spark, input_path):
    """Return immediate ``*.parquet`` directories below an S3 path."""
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(input_path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return sorted(
        status.getPath().toString()
        for status in filesystem.listStatus(hadoop_path)
        if status.isDirectory()
        and status.getPath().getName().endswith(".parquet")
    )


def load_evaluation_data(
    spark,
    input_path,
    sample_size,
    sample_fraction,
    sample_seed,
    use_all_files,
):
    """Load and randomly sample Parquet data without collecting it on the driver."""
    if sample_size <= 0:
        raise ValueError("--sample-size must be greater than zero")
    if not 0 < sample_fraction <= 1:
        raise ValueError("--sample-fraction must be in the interval (0, 1]")

    selected_input_directory = None
    if use_all_files:
        source = (
            spark.read.option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.snappy.parquet")
            .parquet(input_path)
        )
        source = source.sample(
            withReplacement=False,
            fraction=sample_fraction,
            seed=sample_seed,
        )
    else:
        directories = list_parquet_directories(spark, input_path)
        if not directories:
            raise FileNotFoundError(
                f"No *.parquet directories were found below {input_path}"
            )
        selected_input_directory = directories[sample_seed % len(directories)]
        source = (
            spark.read.option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.snappy.parquet")
            .parquet(selected_input_directory)
        )

    if LABEL_COL not in source.columns:
        raise ValueError(f"Required label column '{LABEL_COL}' is missing")
    if ROW_ID_COL in source.columns:
        raise ValueError(f"Reserved column '{ROW_ID_COL}' already exists in input data")

    sampled = source.orderBy(F.rand(sample_seed)).limit(sample_size)
    return sampled, selected_input_directory


def resolve_model_version(client, model_name, alias, version=None):
    """Resolve an alias or explicit version and return an immutable model URI."""
    if version is None:
        model_version = client.get_model_version_by_alias(model_name, alias)
    else:
        model_version = client.get_model_version(model_name, str(version))

    return model_version, f"models:/{model_name}/{model_version.version}"


def required_features(model):
    """Extract input columns from VectorAssembler-like pipeline stages."""
    features = []
    for stage in getattr(model, "stages", []):
        if hasattr(stage, "getInputCols"):
            features.extend(stage.getInputCols())
    return features


def prepare_shared_sample(dataframe, production_model, candidate_model):
    """Remove rows invalid for either model and assign a shared row identifier."""
    feature_cols = sorted(
        set(required_features(production_model) + required_features(candidate_model))
    )
    missing_cols = [
        column
        for column in feature_cols + [LABEL_COL]
        if column not in dataframe.columns
    ]
    if missing_cols:
        raise ValueError(f"Input data is missing model columns: {missing_cols}")

    required_cols = feature_cols + [LABEL_COL]
    shared_sample = (
        dataframe.dropna(subset=required_cols)
        .withColumn(ROW_ID_COL, F.monotonically_increasing_id())
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    row_count = shared_sample.count()
    if row_count == 0:
        raise ValueError("The evaluation sample is empty after removing invalid rows")
    distinct_labels = {
        row[LABEL_COL]
        for row in shared_sample.select(LABEL_COL).distinct().limit(3).collect()
    }
    if distinct_labels != {0, 1}:
        raise ValueError(
            "The evaluation sample must contain only and both binary labels 0 and 1; "
            f"found {sorted(distinct_labels)}"
        )

    return shared_sample, row_count


def score_models(production_model, candidate_model, shared_sample):
    """Score both models and return paired predictions plus their ROC AUC values."""
    production_predictions = production_model.transform(shared_sample).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    candidate_predictions = candidate_model.transform(shared_sample).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    evaluator = BinaryClassificationEvaluator(
        labelCol=LABEL_COL,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    production_auc = float(evaluator.evaluate(production_predictions))
    candidate_auc = float(evaluator.evaluate(candidate_predictions))

    paired = (
        production_predictions.select(
            ROW_ID_COL,
            F.col(LABEL_COL).cast("int").alias("label"),
            F.col("prediction").cast("int").alias("production_prediction"),
        )
        .join(
            candidate_predictions.select(
                ROW_ID_COL,
                F.col("prediction").cast("int").alias("candidate_prediction"),
            ),
            on=ROW_ID_COL,
            how="inner",
        )
        .select("label", "production_prediction", "candidate_prediction")
    )

    rows = paired.collect()
    production_predictions.unpersist()
    candidate_predictions.unpersist()

    if not rows:
        raise ValueError("Neither model produced predictions for the shared sample")

    labels = np.fromiter((row.label for row in rows), dtype=np.int8)
    production = np.fromiter(
        (row.production_prediction for row in rows), dtype=np.int8
    )
    candidate = np.fromiter(
        (row.candidate_prediction for row in rows), dtype=np.int8
    )
    return labels, production, candidate, production_auc, candidate_auc


def classification_metrics(labels, predictions):
    """Calculate binary classification metrics from NumPy arrays."""
    true_positive = int(np.sum((labels == 1) & (predictions == 1)))
    true_negative = int(np.sum((labels == 0) & (predictions == 0)))
    false_positive = int(np.sum((labels == 0) & (predictions == 1)))
    false_negative = int(np.sum((labels == 1) & (predictions == 0)))

    total = true_positive + true_negative + false_positive + false_negative
    precision_denominator = true_positive + false_positive
    recall_denominator = true_positive + false_negative
    precision = (
        true_positive / precision_denominator if precision_denominator else 0.0
    )
    recall = true_positive / recall_denominator if recall_denominator else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    return {
        "accuracy": (true_positive + true_negative) / total if total else 0.0,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def paired_bootstrap(
    labels,
    production_predictions,
    candidate_predictions,
    iterations,
    alpha,
    random_state,
):
    """Estimate candidate-minus-production metric differences on paired samples."""
    if iterations <= 0:
        raise ValueError("--bootstrap-iterations must be greater than zero")
    if not 0 < alpha < 1:
        raise ValueError("--alpha must be in the interval (0, 1)")

    generator = np.random.default_rng(random_state)
    sample_count = labels.size
    differences = {
        metric_name: np.empty(iterations, dtype=np.float64)
        for metric_name in METRIC_NAMES
    }

    for iteration in range(iterations):
        indices = generator.integers(0, sample_count, size=sample_count)
        production_metrics = classification_metrics(
            labels[indices], production_predictions[indices]
        )
        candidate_metrics = classification_metrics(
            labels[indices], candidate_predictions[indices]
        )
        for metric_name in METRIC_NAMES:
            differences[metric_name][iteration] = (
                candidate_metrics[metric_name] - production_metrics[metric_name]
            )

    result = {}
    for metric_name, values in differences.items():
        lower, upper = np.quantile(values, [alpha / 2, 1 - alpha / 2])
        probability_non_positive = (np.count_nonzero(values <= 0) + 1) / (
            iterations + 1
        )
        probability_non_negative = (np.count_nonzero(values >= 0) + 1) / (
            iterations + 1
        )
        result[metric_name] = {
            "mean_difference": float(np.mean(values)),
            "confidence_interval_lower": float(lower),
            "confidence_interval_upper": float(upper),
            "p_value_two_sided": float(
                min(1.0, 2 * min(probability_non_positive, probability_non_negative))
            ),
        }

    return result


def promote_candidate(client, model_name, production_version, candidate_version):
    """Promote the candidate and retain the previous champion as challenger."""
    client.set_registered_model_alias(model_name, "champion", str(candidate_version))
    client.set_registered_model_alias(model_name, "challenger", str(production_version))


def log_results(experiment_name, run_name, result):
    """Log flattened metrics and the complete comparison document to MLflow."""
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.set_tags(
            {
                "job_type": "ab_test",
                "model_name": result["model_name"],
                "production_version": result["production_version"],
                "candidate_version": result["candidate_version"],
                "decision": result["decision"],
            }
        )
        mlflow.log_params(
            {
                "production_alias": result["production_alias"],
                "candidate_alias": result["candidate_alias"],
                "sample_size": result["sample_size"],
                "bootstrap_iterations": result["bootstrap_iterations"],
                "alpha": result["alpha"],
                "sample_seed": result["sample_seed"],
            }
        )
        for side in ("production", "candidate"):
            for metric_name, metric_value in result[f"{side}_metrics"].items():
                mlflow.log_metric(f"{side}_{metric_name}", metric_value)
        for metric_name, comparison in result["bootstrap_comparison"].items():
            mlflow.log_metric(
                f"{metric_name}_mean_difference", comparison["mean_difference"]
            )
            mlflow.log_metric(
                f"{metric_name}_p_value", comparison["p_value_two_sided"]
            )
        mlflow.log_dict(result, "ab_test_results.json")
        return run.info.run_id


def parse_args():
    """Parse Data Proc job arguments."""
    parser = argparse.ArgumentParser(description="A/B test registered Spark models")
    parser.add_argument("--input", required=True, help="S3A input dataset root")
    parser.add_argument("--tracking-uri", required=True, help="MLflow tracking URI")
    parser.add_argument("--experiment-name", default="fraud_detection_ab_test")
    parser.add_argument("--model-name", default="fraud_detection_model")
    parser.add_argument("--production-alias", default="champion")
    parser.add_argument("--candidate-alias", default="challenger")
    parser.add_argument("--production-version", default=None)
    parser.add_argument("--candidate-version", default=None)
    parser.add_argument("--sample-size", type=int, default=100_000)
    parser.add_argument("--sample-fraction", type=float, default=0.001)
    parser.add_argument("--sample-seed", type=int, default=None)
    parser.add_argument("--use-all-files", action="store_true")
    parser.add_argument("--bootstrap-iterations", type=int, default=200)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--bootstrap-seed", type=int, default=42)
    parser.add_argument("--auto-promote", action="store_true")
    parser.add_argument("--run-name", default=None)
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--s3-access-key", required=True)
    parser.add_argument("--s3-secret-key", required=True)
    return parser.parse_args()


def main():
    """Run the complete model comparison."""
    args = parse_args()
    os.environ["GIT_PYTHON_REFRESH"] = "quiet"
    os.environ["AWS_ACCESS_KEY_ID"] = args.s3_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = args.s3_secret_key
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = args.s3_endpoint_url
    mlflow.set_tracking_uri(args.tracking_uri)

    s3_config = {
        "endpoint_url": args.s3_endpoint_url,
        "access_key": args.s3_access_key,
        "secret_key": args.s3_secret_key,
    }
    sample_seed = (
        args.sample_seed
        if args.sample_seed is not None
        else secrets.randbelow(2_147_483_647)
    )
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_name = args.run_name or f"ab_test_{timestamp}"
    spark = create_spark_session(s3_config)
    shared_sample = None

    try:
        client = MlflowClient()
        production_version, production_uri = resolve_model_version(
            client,
            args.model_name,
            args.production_alias,
            args.production_version,
        )
        candidate_version, candidate_uri = resolve_model_version(
            client,
            args.model_name,
            args.candidate_alias,
            args.candidate_version,
        )
        if production_version.version == candidate_version.version:
            raise ValueError(
                "Production and candidate resolve to the same model version "
                f"({production_version.version})"
            )

        print(f"Loading production model: {production_uri}")
        production_model = mlflow.spark.load_model(production_uri)
        print(f"Loading candidate model: {candidate_uri}")
        candidate_model = mlflow.spark.load_model(candidate_uri)

        raw_sample, selected_directory = load_evaluation_data(
            spark,
            args.input,
            args.sample_size,
            args.sample_fraction,
            sample_seed,
            args.use_all_files,
        )
        shared_sample, input_row_count = prepare_shared_sample(
            raw_sample, production_model, candidate_model
        )
        (
            labels,
            production_predictions,
            candidate_predictions,
            production_auc,
            candidate_auc,
        ) = score_models(production_model, candidate_model, shared_sample)

        production_metrics = classification_metrics(labels, production_predictions)
        candidate_metrics = classification_metrics(labels, candidate_predictions)
        production_metrics["auc"] = production_auc
        candidate_metrics["auc"] = candidate_auc
        comparison = paired_bootstrap(
            labels,
            production_predictions,
            candidate_predictions,
            args.bootstrap_iterations,
            args.alpha,
            args.bootstrap_seed,
        )
        f1_comparison = comparison["f1"]
        should_promote = (
            candidate_metrics["f1"] > production_metrics["f1"]
            and f1_comparison["confidence_interval_lower"] > 0
            and f1_comparison["p_value_two_sided"] < args.alpha
        )
        promoted = bool(should_promote and args.auto_promote)
        decision = "promoted" if promoted else (
            "promote" if should_promote else "keep_champion"
        )

        result = {
            "model_name": args.model_name,
            "production_alias": args.production_alias,
            "candidate_alias": args.candidate_alias,
            "production_version": str(production_version.version),
            "candidate_version": str(candidate_version.version),
            "sample_size": int(labels.size),
            "input_rows_before_scoring": input_row_count,
            "selected_input_directory": selected_directory,
            "sample_seed": sample_seed,
            "bootstrap_iterations": args.bootstrap_iterations,
            "alpha": args.alpha,
            "production_metrics": production_metrics,
            "candidate_metrics": candidate_metrics,
            "bootstrap_comparison": comparison,
            "statistically_better": should_promote,
            "auto_promote": args.auto_promote,
            "decision": decision,
        }

        if promoted:
            promote_candidate(
                client,
                args.model_name,
                production_version.version,
                candidate_version.version,
            )

        result["mlflow_run_id"] = log_results(args.experiment_name, run_name, result)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print(f"A/B test completed: {decision}")
    except Exception as error:
        print(f"A/B test failed: {error}")
        print(traceback.format_exc())
        raise
    finally:
        if shared_sample is not None:
            shared_sample.unpersist()
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
