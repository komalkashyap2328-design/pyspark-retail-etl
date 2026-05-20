import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, round, sum, to_timestamp


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_FILE = PROJECT_ROOT / "data" / "data.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "etl_pipeline.log"


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w"),
            logging.StreamHandler()
        ]
    )


def create_spark_session():
    spark = SparkSession.builder \
        .appName("End-to-End PySpark Retail ETL Pipeline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract_data(spark):
    logging.info("Extracting data from %s", INPUT_FILE)

    df = spark.read.csv(
        str(INPUT_FILE),
        header=True,
        inferSchema=True
    )

    total_rows = df.count()
    logging.info("Raw row count: %s", total_rows)

    if total_rows == 0:
        raise ValueError("Input file is empty.")

    return df


def transform_data(df):
    logging.info("Starting transformation layer")

    transformed_df = df.withColumn(
        "Revenue",
        round(col("Quantity") * col("UnitPrice"), 2)
    )

    transformed_df = transformed_df.dropna()
    transformed_df = transformed_df.dropDuplicates()

    transformed_df = transformed_df.filter(col("Quantity") > 0)
    transformed_df = transformed_df.filter(col("UnitPrice") > 0)
    transformed_df = transformed_df.filter(col("Revenue") > 0)

    transformed_df = transformed_df.withColumn(
        "InvoiceTimestamp",
        to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
    )

    transformed_df = transformed_df.filter(col("InvoiceTimestamp").isNotNull())

    transformed_df = transformed_df.withColumn(
        "InvoiceMonth",
        date_format(col("InvoiceTimestamp"), "yyyy-MM")
    )

    clean_rows = transformed_df.count()
    logging.info("Clean row count: %s", clean_rows)

    if clean_rows == 0:
        raise ValueError("No records left after cleaning.")

    return transformed_df


def create_top_products(df):
    return df.groupBy("Description") \
        .agg(round(sum("Revenue"), 2).alias("Total_Revenue")) \
        .orderBy(col("Total_Revenue").desc())


def create_country_revenue(df):
    return df.groupBy("Country") \
        .agg(round(sum("Revenue"), 2).alias("Total_Revenue")) \
        .orderBy(col("Total_Revenue").desc())


def create_monthly_revenue(df):
    return df.groupBy("InvoiceMonth") \
        .agg(round(sum("Revenue"), 2).alias("Total_Revenue")) \
        .orderBy("InvoiceMonth")


def create_data_quality_report(raw_df, clean_df):
    raw_count = raw_df.count()
    clean_count = clean_df.count()
    removed_count = raw_count - clean_count

    return pd.DataFrame({
        "Metric": ["Raw Records", "Clean Records", "Removed Records"],
        "Value": [raw_count, clean_count, removed_count]
    })


def save_spark_df_as_csv(df, file_name):
    OUTPUT_DIR.mkdir(exist_ok=True)

    output_path = OUTPUT_DIR / file_name
    logging.info("Saving output file: %s", output_path)

    df.toPandas().to_csv(output_path, index=False)


def save_pandas_df_as_csv(df, file_name):
    OUTPUT_DIR.mkdir(exist_ok=True)

    output_path = OUTPUT_DIR / file_name
    logging.info("Saving output file: %s", output_path)

    df.to_csv(output_path, index=False)


def main():
    setup_logging()
    logging.info("ETL pipeline started")

    spark = create_spark_session()

    try:
        raw_df = extract_data(spark)
        clean_df = transform_data(raw_df)

        top_products = create_top_products(clean_df)
        country_revenue = create_country_revenue(clean_df)
        monthly_revenue = create_monthly_revenue(clean_df)
        data_quality_report = create_data_quality_report(raw_df, clean_df)

        print("===== TOP PRODUCTS =====")
        top_products.show(10, truncate=False)

        print("===== COUNTRY REVENUE =====")
        country_revenue.show(10, truncate=False)

        print("===== MONTHLY REVENUE =====")
        monthly_revenue.show(20, truncate=False)

        print("===== DATA QUALITY REPORT =====")
        print(data_quality_report)

        save_spark_df_as_csv(top_products, "top_products.csv")
        save_spark_df_as_csv(country_revenue, "country_revenue.csv")
        save_spark_df_as_csv(monthly_revenue, "monthly_revenue.csv")
        save_pandas_df_as_csv(data_quality_report, "data_quality_report.csv")

        logging.info("ETL pipeline completed successfully")
        print("===== ETL PIPELINE COMPLETED SUCCESSFULLY =====")

    except Exception as error:
        logging.exception("ETL pipeline failed")
        raise error

    finally:
        spark.stop()
        logging.info("Spark session stopped")


if __name__ == "__main__":
    main()