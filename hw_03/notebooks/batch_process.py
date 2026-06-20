import logging
import subprocess
import warnings
from itertools import groupby

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

# Spark configuration
SPARK_UI_PORT = 4040
APP_NAME = "OTUS"


def create_spark_session():
    """Create and configure Spark session."""
    spark = (
        SparkSession
        .builder
        .appName(APP_NAME)
        .getOrCreate()
    )
    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
    return spark


def get_files_from_hdfs(hdfs_path="/user/ubuntu/data/"):
    """
    Get list of file names from HDFS.

    Args:
        hdfs_path: Path to HDFS directory

    Returns:
        List of sorted file names (without .txt extension)
    """
    logger.info(f"Listing files from HDFS: {hdfs_path}")

    result = subprocess.run(
        ["hdfs", "dfs", "-ls", hdfs_path],
        capture_output=True,
        text=True
    )

    files = []
    for line in result.stdout.strip().split('\n'):
        if '.txt' in line:
            file_path = line.split()[-1]
            file_name = file_path.split('/')[-1].replace('.txt', '')
            files.append(file_name)

    files = sorted(files)
    logger.info(f"Found {len(files)} files to process")

    return files


def process_single_file(spark, file_date, bucket_name):
    """
    Process a single file through the ETL pipeline.

    Args:
        spark: SparkSession instance
        file_date: Date identifier for the file
        bucket_name: S3 bucket name

    Returns:
        Dictionary with processing results
    """
    logger.info("=" * 70)
    logger.info(f"PROCESSING FILE: {file_date}")
    logger.info("=" * 70)

    result = {}

    try:
        # 1. READ SOURCE FILE
        logger.info(f"Reading source file: data/{file_date}.txt")
        rdd = spark.sparkContext.textFile(f"data/{file_date}.txt")
        first_line = rdd.first()

        columns = [
            'tranaction_id', 'tx_datetime', 'customer_id', 'terminal_id',
            'tx_amount', 'tx_time_seconds', 'tx_time_days',
            'tx_fraud', 'tx_fraud_scenario'
        ]

        if first_line.startswith("#"):
            data = rdd.filter(lambda x: not x.startswith("#")).map(lambda x: x.split(","))
            logger.info("  Format: with # header")
        else:
            data = rdd.map(lambda x: x.split(","))
            logger.info("  Format: without header")

        df_raw = spark.createDataFrame(data, schema=columns)
        raw_count = df_raw.count()
        logger.info(f"  Raw records read: {raw_count:,}")

        # 2. PARTITIONING
        logger.info("Saving with partitioning by tx_time_days")
        (
            df_raw
            .write
            .mode("overwrite")
            .partitionBy("tx_time_days")
            .parquet(f"data/{file_date}_partitioned.parquet")
        )
        logger.info(f"  Saved partitioned to: data/{file_date}_partitioned.parquet")

        # 3. READ PARTITIONED DATA
        logger.info("Reading partitioned data")
        df = spark.read.parquet(f"data/{file_date}_partitioned.parquet")
        logger.info(f"  Partitions count: {df.rdd.getNumPartitions()}")

        # 4. DATA PROCESSING
        logger.info("Processing data")

        # Fix 24:00:00 time
        df = df.withColumn('tx_datetime', F.regexp_replace('tx_datetime', '24:00:00', '00:00:00'))

        # Convert to timestamp
        df = df.withColumn('tx_datetime', F.to_timestamp('tx_datetime', 'yyyy-MM-dd HH:mm:ss'))

        # Empty terminal_id -> NULL
        df = df.withColumn(
            'terminal_id',
            F.when(F.col('terminal_id') == '', F.lit(None))
             .otherwise(F.col('terminal_id'))
        )

        # Convert integer columns
        int_columns = [
            'tranaction_id', 'customer_id', 'terminal_id',
            'tx_time_seconds', 'tx_time_days', 'tx_fraud', 'tx_fraud_scenario'
        ]
        for col_name in int_columns:
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))

        # Convert amount to double
        df = df.withColumn('tx_amount', F.col('tx_amount').cast(DoubleType()))

        # Rename column
        df = df.withColumnRenamed('tranaction_id', 'transaction_id')

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

        # 5. FINAL SAVE TO S3
        logger.info("Saving to S3")
        output_path = f"s3a://{bucket_name}/{file_date}.parquet"

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

    except Exception as e:
        logger.error(f"Error processing {file_date}: {e}", exc_info=True)
        result = {'error': str(e)}

    return result


def process_all_files():
    """Main function to process all files."""
    spark = create_spark_session()
    logger.info(f"Spark session created: {spark}")

    files = get_files_from_hdfs()

    if not files:
        logger.warning("No files found to process")
        return {}

    bucket_name = "otus-bucket-b1g4ki09n8igs1si54v2"
    results = {}

    for file_date in files:
        results[file_date] = process_single_file(spark, file_date, bucket_name)

    # FINAL REPORT
    logger.info("=" * 70)
    logger.info("FINAL REPORT - ALL FILES PROCESSED")
    logger.info("=" * 70)

    total_raw = 0
    total_final = 0
    total_violations = 0

    for file_date, data in results.items():
        if 'error' in data:
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
    logger.info(f"  Raw records: {total_raw:,}")
    logger.info(f"  After processing: {total_final:,}")
    logger.info(f"  Fraud logic violations: {total_violations}")
    logger.info("=" * 70)

    spark.stop()
    logger.info("Spark session stopped")

    return results


if __name__ == "__main__":
    all_results = process_all_files()