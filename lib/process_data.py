from pyspark.sql.functions import col, when, round, avg
from lib.extract import generate_report

def transform_data(df):
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
    generate_report(summary_markdown, "Summary of Iris Data Transformations", write_to_file=True, mode='a')
    return df


def load(df, table_name, mode="overwrite"):
    """Load a Spark DataFrame into a Databricks table."""
    try:
        df.write.format("delta").mode(mode).saveAsTable(table_name)
        print(f"Data successfully loaded into table: {table_name}")
    except Exception as e:
        print(f"An error occurred while loading data into table {table_name}: {str(e)}")


def query(spark, table_name, sql_query):
    """Query a Databricks table and generate a markdown report."""
    query_result = spark.sql(sql_query)

    query_result_md = query_result.limit(10).toPandas().to_markdown()
    generate_report(content=query_result_md,
                    title=f"Query Result for Table: {table_name}",
                    write_to_file=True,
                    mode='a',
                    query=sql_query)
    print(f"Query executed and report generated")
    return query_result