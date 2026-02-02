import requests


def call_api(method, url, headers=None, params=None, body=None):
    # Disable compression to avoid zstd decompression issues
    request_headers = headers or {}
    request_headers['Accept-Encoding'] = 'gzip, deflate'
    
    response = requests.request(
        method=method,
        url=url,
        headers=request_headers,
        params=params,
        json=body,
        timeout=30
    )
    response.raise_for_status()
    print(f"API call to {url} succeeded with status code {response.status_code}")
    return response.json()
