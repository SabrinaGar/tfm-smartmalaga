import requests

url = 'https://datosabiertos.malaga.eu/api/3/action/datastore_search?resource_id=0dcf7abd-26b4-42c8-af19-4992f1ee60c6&limit=5'

response = requests.get(url)

if response.status_code == 200:  # Check if request was successful
    print(response.json())  # Convert response to JSON and print
else:
    print(f"Error: {response.status_code}")