import pandas as pd
import datetime
import openai
import time
from langchain.llms import OpenAI
from openai.error import RateLimitError, OpenAIError,APIError
import concurrent.futures
import json
# Function to read the API key from the config file
def read_api_key(file_path):
    with open(file_path,'r') as config_file:
         data = json.load(config_file)
         return data.get('OPENAI_API_KEY')
    return None

# Read the API key from the config file
config_file_path = 'config/config.json'
api_key = read_api_key(config_file_path)
if api_key is None:
    raise ValueError("API key not found in the config file")

# Load your API key from an environment variable
llm = OpenAI(openai_api_key=api_key)

# Get current date
current_date = datetime.datetime.now()
month = current_date.month
year = current_date.year

# Read the CSV file into a DataFrame
input_file_path = f'Data/{year}_{month}_worker_and_temporary_worker.csv'
output_file_path = f'Data/{year}_{month}_updated_organization_file.csv'
data = pd.read_csv(input_file_path)

# Function to get the industry type using GPT
def get_industry_type(company_info):
    prompts = [f"Classify the industry for the company: {name} in the town/city{town}" for name,town in company_info]
    responses = []
    for prompt in prompts:
        try:
            response = llm.invoke(prompt)
            responses.append(response)
        except (RateLimitError, APIError) as e:
            print(f"Error {e}. Retrying in 60 seconds.")
            time.sleep(60)
            return get_industry_type(company_info)
        except Exception as e:
            print(f"Error: {e}")
            responses.append("Unknown")
    return responses

def process_batch(batch):
    company_info = list(zip(batch['Organisation Name'], batch['Town/City']))
    industries = get_industry_type(company_info)
    return industries

# Batch size for processing
batch_size = 100

# Split data into batches
batches = [data.iloc[i:i + batch_size] for i in range(0, data.shape[0], batch_size)]

# Process batches in parallel
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = list(executor.map(process_batch, batches))

# Flatten the results list
flat_results = [item for sublist in results for item in sublist]

# Add the results to the DataFrame
data['Industry'] = flat_results

# Save the updated DataFrame to a CSV file
data.to_csv(output_file_path, index=False)

print("Processing complete. Results saved to:", output_file_path)