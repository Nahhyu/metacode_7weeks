from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder.appName("WordCount_배나현").getOrCreate()

    try:
        lines = spark.read.text("/opt/spark/data/wordcount.txt")
        words = lines.select(
            F.explode(F.split(F.col("value"), r"(?U)\s+")).alias("word")
        ).where(F.col("word") != "")

        counts = words.groupBy("word").count()
        counts.orderBy(F.desc("count"), F.asc("word")).show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()