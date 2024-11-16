import requests

def extract(url='https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv?raw=true'):
    """Extract data from a URL and load it into a Spark DataFrame."""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            local_file = "/tmp/jf361_iris.csv"
            dbfs_file = "dbfs:/FileStore/mini_project11/jf361_iris.csv"

            with open(local_file, "wb") as f:
                f.write(response.content)

            # Use dbutils to copy the file to DBFS
            dbutils.fs.cp(f"file://{local_file}", dbfs_file)
    except Exception as e:
        error_message = f"An error occurred during data extraction: {str(e)}"
        print(error_message)


if __name__ == "__main__":
    extract()