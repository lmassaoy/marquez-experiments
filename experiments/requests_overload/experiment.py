import requests
import concurrent.futures


marquez_url = "http://localhost:5000/api/v1"
infinite_counter = [i for i in range(10000000)]


def hit_marquez():
    response = requests.get(url=f'{marquez_url}/namespaces')
    return response.status_code


def print_hit(infinite_counter):
    print(f'request {infinite_counter} | status response: {hit_marquez()}')


def send_concurrent_requests(infinite_counter):
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(print_hit, infinite_counter)
    

send_concurrent_requests(infinite_counter)