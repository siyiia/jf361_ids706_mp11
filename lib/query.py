# %python
# %pip install tabulate
from lib.process_data import start_spark, generate_report


def query(spark, table_name, sql_query, generate_flag=False):
    """Query a Databricks table and generate a markdown report."""
    query_result = spark.sql(sql_query)

    query_result_md = query_result.limit(10).toPandas().to_markdown()
    if generate_flag:
        generate_report(content=query_result_md,
                        title=f"Query Result for Table: {table_name}",
                        write_to_file=True,
                        mode='a',
                        query=sql_query)
    print(f"Query executed and report generated")
    return query_result

if __name__ == "__main__":
    query_spark = start_spark("Query")
    query(query_spark, "jf361_iris_table", "SELECT * FROM jf361_iris_table LIMIT 10")