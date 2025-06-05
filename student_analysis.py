# student_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit

def build_dataframe(spark):
    data = [("Alice", 95), ("Bob", 67), ("Charlie", 88), ("Daisy", 73), ("Evan", 55)]
    columns = ["name", "score"]
    df = spark.createDataFrame(data, columns)

    graded_df = df.withColumn(
        "grade",
        when(df["score"] >= 90, lit("A"))
        .when(df["score"] >= 75, lit("B"))
        .when(df["score"] >= 60, lit("C"))
        .otherwise(lit("D"))
    )
    return graded_df
