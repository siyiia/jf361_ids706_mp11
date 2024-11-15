from lib.extract import start_spark, extract
from lib.process_data import transform_data, load, query

if __name__ == "__main__":
    iris_spark = start_spark('Iris Dataset Pipeline')
    iris_df = extract(iris_spark)
    iris_df = transform_data(iris_df)
    load(iris_df, "jf361_iris_table")
    query(iris_spark, "jf361_iris_table", "SELECT * FROM jf361_iris_table LIMIT 10")