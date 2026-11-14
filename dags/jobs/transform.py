import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv")
    parser.add_argument("output_dir")
    parser.add_argument("--min-year", type=int, default=2015)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Q9_NetflixGenreCounts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        titles = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .csv(args.input_csv)
        )

        genres = (
            titles.where(F.col("release_year").cast("int") >= args.min_year)
            .select(
                "type",
                F.explode(F.split(F.col("listed_in"), ",")).alias("genre"),
            )
            .withColumn("genre", F.trim(F.col("genre")))
            .where(F.col("genre") != "")
        )

        summary = genres.groupBy("type", "genre").agg(
            F.count("*").alias("title_count")
        )

        summary.coalesce(1).write.mode("overwrite").option(
            "compression", "snappy"
        ).parquet(args.output_dir)

        summary.orderBy("type", "genre").show(20, truncate=False)
        print(f"Aggregated rows: {summary.count()}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()