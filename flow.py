import requests
import csv
from concurrent.futures import ThreadPoolExecutor
from retrying import retry
import os


def fetch_data(url):
    headers = {"accept": "application/json"}

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


def retrieve_flows_and_save_csv(id, year, month):
    url_template = "https://api.euskadi.eus/traffic/v1.0/flows/byMonth/{year}/{month}/bySource/{id}?_page={page}"
    csv_file = f"flow_{year}_{month}_id{id}.csv"
    # File to store the checkpoint
    checkpoint_file = f"checkpoint_{id}_{year}.txt"

    # Make the first call to the API to get the total number of pages
    first_url = url_template.format(id=id, year=year, month=month, page=1)
    first_response = fetch_data(first_url)
    max_pages = first_response["totalPages"]
    print(first_response)

    print(f"The total pages for {year} {month} and {id} are {max_pages}")

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
            url = url_template.format(id=id, page=page, year=year, month=month)
            futures.append(executor.submit(fetch_data, url))

        # Use 'a' mode to append to the existing CSV file
        with open(csv_file, "a", newline="") as file:
            writer = csv.writer(file)

            total_pages = len(futures)
            completed_pages = 0

            for future in futures:
                data = future.result().get("flows", [])
                if data:
                    if futures.index(future) == 0 and page == 1:
                        # Write the header row only for the first page
                        writer.writerow(data[0].keys())
                    for item in data:
                        writer.writerow(item.values())  # Write the data rows

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


# Usage example
# id = 6
# max_pages = 182019
# retrieve_flows_and_save_csv(id, max_pages)
