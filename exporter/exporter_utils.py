from urllib.parse import parse_qs
from fastapi import Request, Response
from prometheus_client.exposition import _bake_output


def make_scrape_endpoint(collector_registry):
    def scrape_endpoint(request: Request, response: Response):
        accept_header = request.headers.get('Accept')
        params = parse_qs(str(request.query_params))
        status, header, output = _bake_output(collector_registry, accept_header, params)
        response.headers[header[0]] = header[1]
        response.status_code = int(status.split(' ')[0])
        response.body = output
        return response
    return scrape_endpoint
