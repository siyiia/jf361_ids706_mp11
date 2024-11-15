import requests
from dotenv import load_dotenv
import os

load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
url = f"https://{server_h}/api/2.0/clusters/list"

def check_credentials(headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print("Credentials are valid, and API call succeeded.")
        return True
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP Error: {http_err}")
        return False
    except Exception as err:
        print(f"Error: {err}")
        return False

def test_databricks_credentials():
    headers = {'Authorization': f'Bearer {access_token}'}
    assert check_credentials(headers) is True, "The credentials are invalid or the API call failed."


if __name__ == "__main__":
    try:
        test_databricks_credentials()
        print("Databricks credentials test passed.")
    except AssertionError as e:
        print(f"Test failed: {e}")
    except Exception as e:
        print(f"Unexpected error during test: {e}")
