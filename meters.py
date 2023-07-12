import requests
import csv
from concurrent.futures import ThreadPoolExecutor
from retrying import retry
import os
import json


def fetch_data(url):
    headers = {"accept": "application/geo+json"}

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def _fetch_data():
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    try:
        return _fetch_data()
    except requests.RequestException as e:
        print(f"Error: Failed to retrieve data from the API. {str(e)}")
        return []


def retrieve_meters_and_save_csv(id):

    url_template = "https://api.euskadi.eus/traffic/v1.0/meters/bySource/{id}?_elements=100&_page={page}"
    csv_file = f"meters_id{id}.csv"
    # File to store the checkpoint
    checkpoint_file = f"checkpoint_meters_{id}.txt"

    # Make the first call to the API to get the total number of pages
    first_url = url_template.format(id=id, page=1)
    first_response = fetch_data(first_url)

    if "features" not in first_response or not first_response["features"]:
        print(f"No features found for ID {id}. Skipping...")
        return

    max_pages = first_response["totalPages"]
    print(first_response)

    print(f"The total pages for {id} are {max_pages}")

    # Read the last processed page from the checkpoint file
    try:
        with open(checkpoint_file, "r") as f:
            last_page = int(f.read().strip()) + 1
            print(f"last page was: {last_page}")
    except FileNotFoundError:
        last_page = 1

    with ThreadPoolExecutor() as executor:
        futures = []
        for page in range(last_page, max_pages + 1):
            url = url_template.format(id=id, page=page)
            futures.append(executor.submit(fetch_data, url))

        # Use 'a' mode to append to the existing CSV file
        with open(csv_file, "a", newline="") as file:
            fieldnames = set()
            for feature in first_response["features"]:
                properties = feature["properties"]
                fieldnames.update(properties.keys())
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if last_page == 1:
                # Write the header row only for the first page
                writer.writeheader()

            total_pages = len(futures)
            completed_pages = 0

            for future in futures:

                data = future.result().get("features", [])
                if data:
                    for item in data:
                        properties = item["properties"]
                        # Write the properties rows
                        writer.writerow(properties)

                completed_pages += 1
                percentage = (completed_pages / total_pages) * 100
                print(
                    f"Progress: {completed_pages}/{total_pages} pages ({percentage:.2f}%) out of the total pages of {max_pages}")

                # Save the current page as the last processed page in the checkpoint file
                with open(checkpoint_file, "w") as f:
                    f.write(str(completed_pages+last_page))

    print(f"Data retrieved successfully and saved to {csv_file}.")
    # Remove the checkpoint file as all pages have been processed
    try:
        os.remove(checkpoint_file)
    except FileNotFoundError:
        pass
