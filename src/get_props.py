import requests
import time
import os

def run_query_with_retries(query_template, offset, limit, output_file_path, timeout_duration=5, max_retries=5, initial_wait_time=10):
    url = 'https://query.wikidata.org/sparql'
    headers = {'Accept': 'application/sparql-results+json'}
    wait_time = initial_wait_time

    paginated_query = f"""
    {query_template}
    LIMIT {limit} OFFSET {offset}
    """

    for attempt in range(1, max_retries + 1):
        try:
            start_time = time.time()
            response = requests.get(url, params={'query': paginated_query}, headers=headers, timeout=timeout_duration)
            elapsed_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                print(f"Query executed in {elapsed_time:.2f} seconds.")

                with open(output_file_path, 'a', encoding='utf-8') as f:
                    for item in data['results']['bindings']:
                        property_uri = item['property']['value'].rstrip('/')
                        label = item['label']['value'].replace('"', '\\"')
                        f.write(f"<{property_uri}> <http://www.w3.org/2000/01/rdf-schema#label> \"{label}\" .\n")

                print(f"Results written to {output_file_path}")
                return data
            else:
                print(f"Error: {response.status_code} {response.text}")
                if attempt < max_retries:
                    print(f"Attempt {attempt} failed. Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                    wait_time *= 2 # Exponential backoff
                else:
                    print("Maximum retries reached. Exiting.")
                    return None
            
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt} failed with error: {e}")
            if attempt < max_retries:
                print(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                wait_time *= 2
            else:
                print("Maximum retries reached. Exiting.")
                return None

def main():
    output_directory = '/scratch/hpc-prf-lola/albert/get_props/data/data_endpoint'
    output_file_path = os.path.join(output_directory, 'wikidata_properties.nt')
    progress_file_path = os.path.join(output_directory, 'progress.txt')

    os.makedirs(output_directory, exist_ok=True)

    query_template = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wikibase: <http://wikiba.se/ontology#>

    SELECT DISTINCT ?property ?label
    WHERE {
        ?property a wikibase:Property .
        ?property rdfs:label ?label .
        FILTER (lang(?label) = "en")
    }
    """

    timeout_duration = 30
    max_retries = 5
    limit = 1000

    if os.path.exists(progress_file_path):
        with open(progress_file_path, 'r') as pf:
            offset = int(pf.read())
    else:
        offset = 0
        if os.path.exists(output_file_path):
            os.remove(output_file_path)

    while True:
        print(f"Fetching records with OFFSET {offset}")
        data = run_query_with_retries(query_template, offset, limit, output_file_path, timeout_duration, max_retries)
        if data is None:
            print("Failed to fetch data. Exiting.")
            break

        num_results = len(data['results']['bindings'])
        print(f"Number of results fetched: {num_results}")

        if num_results > 0:
            offset += limit
            with open(progress_file_path, 'w') as pf:
                pf.write(str(offset))
        if num_results < limit:
            print("All data fetched.")
            if os.path.exists(progress_file_path):
                os.remove(progress_file_path)
            break

if __name__ == "__main__":
    main()
