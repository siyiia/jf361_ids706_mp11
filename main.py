from lib.extract import extract
from lib.process_data import start_spark, extract_data, transform_data, load
from lib.query import query

if __name__ == "__main__":
    extract()
    iris_spark = start_spark('Iris Dataset Pipeline')
    iris_df = extract_data(iris_spark, generate_flag=True)
    iris_df = transform_data(iris_df, generate_flag=True)
    load(iris_df, "jf361_iris_table")
    query(iris_spark, "jf361_iris_table", "SELECT * FROM jf361_iris_table LIMIT 10", generate_flag=True)