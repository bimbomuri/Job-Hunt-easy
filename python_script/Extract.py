import os
import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import httpx
import datetime
import io


current_date = datetime.datetime.now()
month = current_date.month
year = current_date.year
# Step 1: Get the HTML content of the webpage
url = 'https://assets.publishing.service.gov.uk/media/664db5abf34f9b5a56adcc06/2024-05-22_-_Worker_and_Temporary_Worker.csv'
response = requests.get(url)
html_content = response.content

# Check if the request was successful
if response.status_code == 200:
    # Save the content of the response as a CSV file
    with open(f'Data/{year}_{month}_worker_and_temporary_worker.csv', 'wb') as file:
        file.write(response.content)
    print(f"CSV file downloaded and saved as '{year}_{month}_worker_and_temporary_worker.csv'")
else:
    print(f"Failed to download the CSV file. Status code: {response.status_code}")

