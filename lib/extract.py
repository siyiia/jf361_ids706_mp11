
import requests
# from pyspark import dbutils
import os
import json
import base64
from dotenv import load_dotenv


def generate_report(content, title, filename="./result_report.md", write_to_file=False, mode='a', query=None):
    """Generate a markdown report."""
    if not write_to_file:
            return
    with open(filename, mode, encoding="utf-8") as file:
        file.write(f"\n## {title}\n")
        if query:
            file.write(f"### Query:\n```\n{query}\n```\n\n")
        file.write(f"{content}\n")


# def extract(url='https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv?raw=true'):
#     """Extract data from a URL and load it into a Spark DataFrame."""
#     try:
#         response = requests.get(url)
#         if response.status_code == 200:
#             local_file = "dbfs:/FileStore/mini_project11/jf361_iris.csv"
#
#             print("extract")
#             with open(local_file, "wb") as f:
#                 f.write(response.content)
#             print("write file")
#             # df_spark = spark.read.csv(local_file, header=True, inferSchema=True)
#             #
#             # output = df_spark.limit(10).toPandas().to_markdown()
#             # print("Data extracted successfully.")
#             #
#             # generate_report(content=output, title="Iris Data Extracted", write_to_file=True, mode='w')
#             # return df_spark
#         else:
#             error_message = f"Failed to fetch data. Status code: {response.status_code}"
#             print(error_message)
#             return None
#     except Exception as e:
#         error_message = f"An error occurred during data extraction: {str(e)}"
#         print(error_message)
#         return None
#
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

            # Load the file into a Spark DataFrame
        #     df_spark = spark.read.csv(dbfs_file, header=True, inferSchema=True)
        #
        #     output = df_spark.limit(10).toPandas().to_markdown()
        #     print("Data extracted successfully.")
        #
        #     generate_report(content=output, title="Iris Data Extracted", write_to_file=True, mode='w')
        #     return df_spark
        # else:
        #     error_message = f"Failed to fetch data. Status code: {response.status_code}"
        #     print(error_message)
        #     return None
    except Exception as e:
        error_message = f"An error occurred during data extraction: {str(e)}"
        print(error_message)
        # return None
#
#
# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")  # Databricks server hostname
access_token = os.getenv("ACCESS_TOKEN")  # Databricks personal access token
FILESTORE_PATH = "dbfs:/FileStore/mini_project11"  # Base path in DBFS
headers = {'Authorization': f'Bearer {access_token}'}
url = f"https://{server_h}/api/2.0"


def perform_query(path, headers, data={}):
    """Perform a Databricks API POST request."""
    session = requests.Session()
    resp = session.post(
        url + path,
        data=json.dumps(data),
        verify=True,
        headers=headers
    )
    resp.raise_for_status()
    return resp.json()


def create(path, overwrite, headers):
    """Create a new file in DBFS."""
    _data = {'path': path, 'overwrite': overwrite}
    return perform_query('/dbfs/create', headers=headers, data=_data)


def add_block(handle, data, headers):
    """Add a block of data to the file."""
    _data = {'handle': handle, 'data': data}
    return perform_query('/dbfs/add-block', headers=headers, data=_data)


def close(handle, headers):
    """Close the file handle."""
    _data = {'handle': handle}
    return perform_query('/dbfs/close', headers=headers, data=_data)


def put_file_from_url(url, dbfs_path, overwrite, headers):
    """
    Fetch a file from a URL and upload it to DBFS.
    """
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print(f"Uploading file to {dbfs_path}")
        for i in range(0, len(content), 2 ** 20):  # Upload in 1 MB chunks
            add_block(
                handle,
                base64.standard_b64encode(content[i:i + 2 ** 20]).decode(),
                headers=headers
            )
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Failed to fetch file from {url}. Status code: {response.status_code}")


def extract(
        url="https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv",
        file_path=FILESTORE_PATH + "/jf361_iris.csv",
        overwrite=True
):
    """
    Extract data from a URL and load it into a Spark DataFrame.
    """
    try:
        # Upload the file from URL to DBFS
        put_file_from_url(url, file_path, overwrite, headers=headers)

        # Load the file into a Spark DataFrame
        # df_spark = spark.read.csv(file_path, header=True, inferSchema=True)
        # print("Data extracted and loaded successfully into a Spark DataFrame.")
        #
        # output = df_spark.limit(10).toPandas().to_markdown()
        # print("Data extracted successfully.")
        #
        # generate_report(content=output, title="Iris Data Extracted", write_to_file=True, mode='w')
        # return df_spark
    except Exception as e:
        print(f"An error occurred during data extraction: {e}")
        # return None


# Example usage
if __name__ == "__main__":
    extract()