import argparse
import logging
import sys
import warnings

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings('ignore')

APP_NAME = "OTUS"
DEFAULT_BUCKET = "otus-bucket-b1g4ki09n8igs1si54v2"
DEFAULT_ENDPOINT = "storage.yandexcloud.net"
DEFAULT_RAW_PREFIX = "unprocessed/"            # исходные .parquet лежат прямо в бакете в папке unprocessed
DEFAULT_PROCESSED_PREFIX = "processed/"


def parse_args():
    """
    Все параметры принимаются через CLI-аргументы, а не через os.environ —
    Data Proc job (DataprocCreatePysparkJobOperator) передаёт значения именно
    через args, окружение процесса на драйвере/воркерах не пробрасывается.
    """
    parser = argparse.ArgumentParser(
        description="OTUS ETL: process one file or all files in an S3 bucket"
    )
    parser.add_argument(
        "--bucket", dest="bucket", default=DEFAULT_BUCKET,
        help="S3 bucket с исходными и обработанными данными",
    )
    parser.add_argument(
        "--access-key", dest="access_key", required=True,
        help="Access key для Yandex Object Storage (s3a)",
    )
    parser.add_argument(
        "--secret-key", dest="secret_key", required=True,
        help="Secret key для Yandex Object Storage (s3a)",
    )
    parser.add_argument(
        "--endpoint", dest="endpoint", default=DEFAULT_ENDPOINT,
        help="S3 endpoint (по умолчанию storage.yandexcloud.net)",
    )
    parser.add_argument(
        "--raw-prefix", dest="raw_prefix", default=DEFAULT_RAW_PREFIX,
        help="Префикс в бакете, где лежат исходные .parquet файлы",
    )
    parser.add_argument(
        "--processed-prefix", dest="processed_prefix", default=DEFAULT_PROCESSED_PREFIX,
        help="Префикс в бакете, куда пишется результат обработки",
    )
    parser.add_argument(
        "--file-date", dest="file_date", default=None,
        help="Имя (дата) одного файла для обработки, без .parquet. "
             "Если не указано — обрабатываются все файлы в бакете (batch mode).",
    )
    return parser.parse_args()


def create_spark_session(endpoint, access_key, secret_key):
    """Create and configure Spark session with s3a support for Yandex Object Storage."""
    spark = (
        SparkSession
        .builder
        .appName(APP_NAME)
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .getOrCreate()
    )
    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
    return spark


def get_files_from_s3(spark, bucket_name, raw_prefix, processed_prefix):
    """
    Get list of top-level *.parquet entries directly under raw_prefix.
    Each entry is itself a Spark multi-part parquet dataset (a "directory"
    containing part-NNNNN-....parquet + _SUCCESS), NOT a single file.
    We must NOT recurse into it here — spark.read.parquet() will read the
    whole dataset directory in one call later.

    Uses globStatus with a non-recursive pattern (raw_prefix + "*.parquet"),
    which by definition only matches direct children — unlike listStatus,
    which on S3A can end up listing nested objects too.

    Returns:
        List of sorted dataset names (without .parquet extension)
    """
    logger.info(f"Listing top-level .parquet datasets from s3a://{bucket_name}/{raw_prefix}")

    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    glob_path = jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket_name}/{raw_prefix}*.parquet")
    fs = glob_path.getFileSystem(hadoop_conf)

    statuses = fs.globStatus(glob_path)

    files = []
    if statuses:
        for status in statuses:
            key = status.getPath().getName()  # e.g. "2019-08-22.parquet"
            if key.startswith(processed_prefix.rstrip('/')):
                continue
            if key.endswith(".parquet"):
                file_name = key[:-len(".parquet")]
                files.append(file_name)

    files = sorted(set(files))
    logger.info(f"Found {len(files)} files to process")

    return files


def process_single_file(spark, file_date, bucket_name, raw_prefix, processed_prefix):
    """
    Process a single file through the ETL pipeline, reading and writing to S3.

    Returns:
        Dictionary with processing results
    """
    logger.info("=" * 70)
    logger.info(f"PROCESSING FILE: {file_date}")
    logger.info("=" * 70)

    result = {}

    try:
        # 1. READ SOURCE PARQUET FILE FROM S3 (исходники уже в формате parquet)
        source_path = f"s3a://{bucket_name}/{raw_prefix}{file_date}.parquet"
        logger.info(f"Reading source file: {source_path}")
        df = spark.read.parquet(source_path)

        # Если в исходном файле колонка называется 'tranaction_id' (опечатка в источнике) —
        # переименуем сразу, дальше работаем с 'transaction_id'
        if 'tranaction_id' in df.columns and 'transaction_id' not in df.columns:
            df = df.withColumnRenamed('tranaction_id', 'transaction_id')

        # Кэшируем сразу после чтения — дальше идут 3 действия (count/filter/count),
        # без кэша каждое из них заново читает файл из S3 и пересчитывает план.
        df = df.cache()
        raw_count = df.count()
        logger.info(f"  Raw records read: {raw_count:,}")
        logger.info(f"  Schema: {df.dtypes}")

        # 2. DATA PROCESSING
        logger.info("Processing data")
        schema_types = dict(df.dtypes)

        # Fix 24:00:00 time и приведение к timestamp — только если колонка ещё строковая
        if schema_types.get('tx_datetime') == 'string':
            df = df.withColumn('tx_datetime', F.regexp_replace('tx_datetime', '24:00:00', '00:00:00'))
            df = df.withColumn('tx_datetime', F.to_timestamp('tx_datetime', 'yyyy-MM-dd HH:mm:ss'))

        # Empty terminal_id -> NULL (актуально только для строкового представления)
        if schema_types.get('terminal_id') == 'string':
            df = df.withColumn(
                'terminal_id',
                F.when(F.col('terminal_id') == '', F.lit(None))
                 .otherwise(F.col('terminal_id'))
            )

        # Convert integer columns (безопасно даже если уже int — cast no-op)
        int_columns = [
            'transaction_id', 'customer_id', 'terminal_id',
            'tx_time_seconds', 'tx_time_days', 'tx_fraud', 'tx_fraud_scenario'
        ]
        for col_name in int_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))

        # Convert amount to double
        if 'tx_amount' in df.columns:
            df = df.withColumn('tx_amount', F.col('tx_amount').cast(DoubleType()))

        # Check fraud condition
        violations = df.filter(
            (F.col('tx_fraud') == 0) & (F.col('tx_fraud_scenario') != 0)
        ).count()

        if violations > 0:
            logger.warning(f"  Found {violations} violations (fraud=0 but scenario!=0)")
        else:
            logger.info("  Fraud check: OK")

        final_count = df.count()
        logger.info(f"  After processing: {final_count:,} records")

        # 3. FINAL SAVE TO S3
        logger.info("Saving to S3")
        output_path = f"s3a://{bucket_name}/{processed_prefix}{file_date}.parquet"

        (
            df.write
            .mode("overwrite")
            .parquet(output_path)
        )

        logger.info(f"  Saved to: {output_path}")
        logger.info(f"  Records: {final_count:,}")

        result = {
            'raw_count': raw_count,
            'final_count': final_count,
            'violations': violations
        }

        df.unpersist()

    except Exception as e:
        logger.error(f"Error processing {file_date}: {e}", exc_info=True)
        result = {'error': str(e)}
        try:
            df.unpersist()
        except NameError:
            pass

    return result


def _print_summary(results):
    """Print a final aggregated report over a dict of {file_date: result}."""
    logger.info("=" * 70)
    logger.info("FINAL REPORT")
    logger.info("=" * 70)

    total_raw = 0
    total_final = 0
    total_violations = 0
    had_errors = False

    for file_date, data in results.items():
        if 'error' in data:
            had_errors = True
            logger.error(f"  ERROR {file_date}: {data['error']}")
        else:
            raw = data.get('raw_count', 0)
            final = data.get('final_count', 0)
            viol = data.get('violations', 0)
            total_raw += raw
            total_final += final
            total_violations += viol
            logger.info(
                f"  OK {file_date}: raw={raw:,} -> processed={final:,} | violations={viol}"
            )

    logger.info("=" * 70)
    logger.info("TOTALS:")
    logger.info(f"  Files processed: {len(results)}")
    logger.info(f"  Raw records: {total_raw:,}")
    logger.info(f"  After processing: {total_final:,}")
    logger.info(f"  Fraud logic violations: {total_violations}")
    logger.info("=" * 70)

    return had_errors


def process_one_file(spark, file_date, bucket_name, raw_prefix, processed_prefix):
    """Process a single named file and print a summary for it."""
    result = process_single_file(spark, file_date, bucket_name, raw_prefix, processed_prefix)
    _print_summary({file_date: result})
    return result


def process_all_files(spark, bucket_name, raw_prefix, processed_prefix):
    """Process every raw file found in the bucket (loop over all files)."""
    files = get_files_from_s3(spark, bucket_name, raw_prefix, processed_prefix)

    if not files:
        logger.warning("No files found to process")
        return {}

    results = {}
    for file_date in files:
        results[file_date] = process_single_file(
            spark, file_date, bucket_name, raw_prefix, processed_prefix
        )

    _print_summary(results)
    return results


def main():
    args = parse_args()

    spark = create_spark_session(args.endpoint, args.access_key, args.secret_key)
    logger.info(f"Spark session created: {spark}")

    had_errors = False

    if args.file_date:
        logger.info(f"Single-file mode: processing '{args.file_date}'")
        result = process_one_file(
            spark, args.file_date, args.bucket, args.raw_prefix, args.processed_prefix
        )
        had_errors = 'error' in result
    else:
        logger.info("Batch mode: processing all files found in the bucket")
        results = process_all_files(
            spark, args.bucket, args.raw_prefix, args.processed_prefix
        )
        had_errors = any('error' in r for r in results.values())

    spark.stop()
    logger.info("Spark session stopped")

    if had_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
