# %python
# %pip install tabulate
from pyspark.sql.functions import col, when, round, avg
from pyspark.sql import SparkSession


def generate_report(content, title, filename="./result_report.md", write_to_file=False, mode='a', query=None):
    """Generate a markdown report."""
    if not write_to_file:
            return
    with open(filename, mode, encoding="utf-8") as file:
        file.write(f"\n## {title}\n")
        if query:
            file.write(f"### Query:\n```\n{query}\n```\n\n")
        file.write(f"{content}\n")


def start_spark(appName):
    """Initialize and return a Spark session."""
    return SparkSession.builder.appName(appName).getOrCreate()

def extract_data(spark, file_path="dbfs:/FileStore/mini_project11/jf361_iris.csv", generate_flag=False):
    df_spark = spark.read.csv(file_path, header=True, inferSchema=True)

    output = df_spark.limit(10).toPandas().to_markdown()
    print("Data extracted successfully.")
    
    if generate_flag:
        generate_report(content=output, title="Iris Data Extracted", write_to_file=True, mode='w')
    return df_spark

def transform_data(df, generate_flag=False):
    """Apply transformations to the input Spark DataFrame."""
    # Step 1: Rename columns for better readability
    df = df.withColumnRenamed("sepal_length", "SepalLength") \
           .withColumnRenamed("sepal_width", "SepalWidth") \
           .withColumnRenamed("petal_length", "PetalLength") \
           .withColumnRenamed("petal_width", "PetalWidth")

    # Step 2: Add a new column: Petal-to-Sepal Ratio
    df = df.withColumn("PetalToSepalRatio", round(col("PetalLength") / col("SepalLength"), 2))

    # Step 3: Filter rows to exclude invalid or missing values
    df = df.filter((col("SepalLength").isNotNull()) &
                   (col("SepalWidth").isNotNull()) &
                   (col("PetalLength").isNotNull()) &
                   (col("PetalWidth").isNotNull()))

    # Step 4: Create a new categorical column based on Petal Length
    df = df.withColumn(
        "PetalCategory",
        when(col("PetalLength") < 2.0, "Small") \
        .when((col("PetalLength") >= 2.0) & (col("PetalLength") < 5.0), "Medium") \
        .otherwise("Large")
    )

    # Step 5: Group by species and calculate summary statistics
    summary_df = df.groupBy("species").agg(
        round(avg(col("PetalLength")), 2).alias("AvgPetalLength"),
        round(avg(col("SepalWidth")), 2).alias("AvgSepalWidth"),
        round(avg(col("PetalToSepalRatio")), 2).alias("AvgPetalToSepalRatio")
    )

    print("Transformations applied successfully.")
    summary_markdown = summary_df.toPandas().to_markdown()
    if generate_flag:
        generate_report(summary_markdown, "Summary of Iris Data Transformations", write_to_file=True, mode='a')
    return df


def load(df, table_name, mode="overwrite"):
    """Load a Spark DataFrame into a Databricks table."""
    try:
        df.write.format("delta").mode(mode).saveAsTable(table_name)
        print(f"Data successfully loaded into table: {table_name}")
    except Exception as e:
        print(f"An error occurred while loading data into table {table_name}: {str(e)}")


if __name__ == "__main__":
    process_spark = start_spark("Process Data")
    process_df = extract_data(process_spark)
    process_df = transform_data(process_df)
    load(process_df, "jf361_iris_table")
