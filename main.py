from lib.extract import extract
from lib.process_data import start_spark, transform_data, load
from lib.query import query

if __name__ == "__main__":
    iris_df = extract()
    iris_spark = start_spark('Iris Dataset Pipeline')
    iris_df = transform_data(iris_df)
    load(iris_df, "jf361_iris_table")
    query(iris_spark, "jf361_iris_table", "SELECT * FROM jf361_iris_table LIMIT 10")