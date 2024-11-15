import requests
from pyspark.sql import SparkSession

def start_spark(appName):
    """Initialize and return a Spark session."""
    return SparkSession.builder.appName(appName).getOrCreate()


def generate_report(content, title, filename="./result_report.md", write_to_file=False, mode='a', query=None):
    """Generate a markdown report."""
    if not write_to_file:
            return
    with open(filename, mode, encoding="utf-8") as file:
        file.write(f"\n## {title}\n")
        if query:
            file.write(f"### Query:\n```\n{query}\n```\n\n")
        file.write(f"{content}\n")


def extract(spark, url='https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv'):
    """Extract data from a URL and load it into a Spark DataFrame."""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            local_file = "/tmp/jf361_iris.csv"
            with open(local_file, "wb") as f:
                f.write(response.content)

            df_spark = spark.read.csv(local_file, header=True, inferSchema=True)

            output = df_spark.limit(10).toPandas().to_markdown()
            print("Data extracted successfully.")

            generate_report(content=output, title="Iris Data Extracted", write_to_file=True, mode='w')
            return df_spark
        else:
            error_message = f"Failed to fetch data. Status code: {response.status_code}"
            print(error_message)
            return None
    except Exception as e:
        error_message = f"An error occurred during data extraction: {str(e)}"
        print(error_message)
        return None