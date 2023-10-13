import requests
import concurrent.futures


marquez_url = "http://localhost:5000/api/v1"
marquez_webui_url = "http://localhost:3000"
infinite_counter = [i for i in range(10000000)]


def hit_marquez():
    # response = requests.get(url=f'{marquez_url}/namespaces')
    response = requests.get(url=f'{marquez_webui_url}')
    return response.status_code


def print_hit(infinite_counter):
    print(f'request {infinite_counter} | status response: {hit_marquez()}')


def send_concurrent_requests(infinite_counter):
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(print_hit, infinite_counter)
    

send_concurrent_requests(infinite_counter)