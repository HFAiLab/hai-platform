import binascii

import multiprocessing
import os
import re
import urllib.parse
import uuid
import base64
import aiohttp
from datetime import datetime

import prometheus_fastapi_instrumentator as pfi
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse as StarletteJSONResponse
from conf import CONF
from logm import logger
from db import MarsDB
from roman_parliament import register_parliament
from server_model.user_data import initialize_user_data_roaming

try:
    with os.popen('git describe --abbrev=0 2>/dev/null') as p:
        client_tag = p.read().strip()
except:
    client_tag = ''
if not client_tag:
    client_tag = 'NotFound'
print(f'client_tag: {client_tag}', flush=True)

module_name = os.environ.get('MODULE_NAME', '')
if module_name == 'server':  # 理论上应该在server.py里写，但uvicorn有bug，先这么写
    num_worker = CONF.server_workers.operating
    is_main_process = multiprocessing.current_process().name == 'MainProcess'
    worker_rank = int(multiprocessing.current_process().name.split('-')[-1]) if not is_main_process else 0
    if num_worker == 1 or not is_main_process:  # 单进程的server 或者多个进程且不是主进程，都参与会议
        if os.environ.get('REPLICA_RANK') == '0' and (num_worker == 1 or worker_rank == 1):
            os.environ['USER_DATA_SYNC_POINT'] = '1'  # 固定 server-0 的第一个 worker 进程是 USER_DATA 的同步点
        register_parliament()


app = FastAPI()


if module_name == 'server':
    # 在 server 上启动 swagger-ui
    from swagger_ui import api_doc
    api_doc(
        app,
        config_path='api/openapi_specification/user_api.yaml',
        url_prefix='/swagger',
        title='MarsV2 User API',
        parameters={
            'persistAuthorization': 'true',
            'tryItOutEnabled': 'true',
        }
    )


ALL_USERS = {}


def update_all_users(token):
    global ALL_USERS
    for user_name, token in MarsDB().execute("""select "user_name", "token" from "user" where "token" = %s """, (token, )).fetchall():
        ALL_USERS[token] = user_name


def query_mask(url: str, query_key: str, keep: float = 0.2):
    if query_key in url:
        query_key_index = url.index(query_key)
        if '&' in url[query_key_index:]:
            next_query_index = url[query_key_index:].index('&') + query_key_index
        else:
            next_query_index = len(url)
        query_value = url[query_key_index + len(query_key):next_query_index]
        keep_chars = int(len(query_value) * keep)
        query_value = query_value[:keep_chars] + '???' + query_value[-keep_chars:]
        return url[:query_key_index] + query_key + query_value + query_mask(url[next_query_index:], query_key, keep)
    else:
        return url

# 无实际业务功能的api
log_ignore_api = ['/metrics', '/api_server_status']
# 耗时的api
warning_ignore_api = [
    '/list_cluster_files', '/sync_to_cluster', '/sync_from_cluster', '/delete_files',
    '/ugc/list_cluster_files', '/ugc/sync_to_cluster', '/ugc/sync_from_cluster', '/ugc/delete_files',
    '/ugc/cloud/cluster_files/list'
]

bff_report_url = CONF.try_get('server_url.raw_bff.external')
bff_report_url = f'{bff_report_url}/agg_fetion/report/cluster' if bff_report_url else None


@app.middleware("http")
@logger.catch
async def log_requests(request: Request, call_next):
    if request.url.query:
        url = urllib.parse.unquote(f'{request.url.path}?{request.url.query}')
    else:
        url = urllib.parse.unquote(request.url.path)
    if 'token=' in url:
        url = query_mask(url, 'token=')
    if 'access_token=' in url:
        url = query_mask(url, 'access_token=')
    # 日志中不暴露 token
    if 'get_user_info/' in url:
        url = re.sub(r'get_user_info/([^/]+)/(.+)', r'get_user_info/\2?token=\1', url)
        url = query_mask(url, 'token=')
    if 'get_worker_user_info/' in url:
        url = re.sub(r'get_worker_user_info/(.+)', r'get_worker_user_info?token=\1', url)
        url = query_mask(url, 'token=')
    if 'set_user_gpu_quota/' in url:
        url = re.sub(r'set_user_gpu_quota/([^/]+)/(.+)', r'set_user_gpu_quota/\2?token=\1', url)
        url = query_mask(url, 'token=')
    log = logger.info
    if url in log_ignore_api:
        log = logger.trace
    start_time = datetime.now()
    log(f'[REQ] - {request.method} - "{url}"')
    try:
        response = await call_next(request)
        exc = None
    except Exception as e:
        exc = e
    end_time = datetime.now()
    seconds = (end_time - start_time).total_seconds()
    if request.url.path not in warning_ignore_api:
        if seconds > 5:
            log = logger.warning
        if seconds > 10:
            log = logger.error
    if exc is None:
        log(f'[RES] - {request.method} - "{url}" - [{response.status_code}] - {seconds * 1000:.2f}ms')
    if bff_report_url and os.environ.get('DISABLE_BFF_ALERT', 'False') != 'True':
        if exc is not None or seconds > 5:
            report_data = {
                "payload": {
                    "method": request.method,
                    "url": request.url.path,
                    "status_code": response.status_code if exc is None else 500,
                    "response_time": round(seconds * 1000, 2)
                }
            }
            try:
                async with aiohttp.ClientSession() as session:
                    async with await session.post(url=bff_report_url, json=report_data, timeout=1) as r:
                        pass
            except Exception as e:
                print('上报信息出现错误：', e)
    if exc is not None:
        raise exc
    return response


@app.middleware("http")
async def add_client_version(request: Request, call_next):
    response = await call_next(request)
    response.headers['client-version'] = client_tag
    return response

host = os.environ.get('HOSTNAME', 'no_env')
try:
    host = host.split('-')[1]
except Exception:
    pass


@app.middleware('http')
async def add_log_context(request: Request, call_next):
    token = None
    if request.url.query:
        req_query = {q.split('=')[0]: q.split('=')[1] for q in request.url.query.split('&') if '=' in q}
        token = req_query.get('token')
    if token is not None and ALL_USERS.get(token) is None:
        update_all_users(token)
    req_user = ALL_USERS.get(token, 'NA') if token else 'NA'
    if token is not None and token.startswith('ACCESS-'):
        split_tokens = token.split('-')
        try:
            decoded_users = base64.b16decode(split_tokens[1].upper().encode()).decode()
        except binascii.Error:
            decoded_users = '(Invalid Token)'
        for sep_char in ['#', '-']:
            try:
                from_user_name, access_user_name = decoded_users.split(sep_char)
                if access_user_name != from_user_name:
                    req_user = f'{access_user_name}({from_user_name})'
                else:
                    req_user = access_user_name
                break
            except Exception:
                pass
        else:
            req_user = decoded_users
    # pass to call_next
    request.state.mars_token = token
    request.state.mars_user = req_user

    uid = f"{str(uuid.uuid4())[:6]}-{datetime.now().strftime('%m%d_%H%M')}-{host}-{req_user}"
    with logger.contextualize(uuid=uid):
        try:
            response = await call_next(request)
            return response
        except Exception as e:
            logger.error(e)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    if isinstance(exc.detail, str):
        _res_json = {
            'success': 0,
            'msg': exc.detail
        }
    elif isinstance(exc.detail, dict):
        _res_json = exc.detail
    else:
        raise Exception('无法处理的 HTTPException 错误类型：', exc.detail)
    return StarletteJSONResponse(_res_json, status_code=exc.status_code)


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

instrumentator = pfi.Instrumentator(
    should_group_status_codes=True,
    should_ignore_untemplated=True,
    should_instrument_requests_inprogress=True,
    inprogress_labels=True,
)


@app.on_event("startup")
async def startup_event():
    if module_name == 'server':
        initialize_user_data_roaming(tables_to_subscribe='*')
    instrumentator.instrument(app).expose(app)


@app.get('/api_server_status')
async def api_server_status():
    """
    判断 api server 是不是 work 了
    @return:
    """
    return {
        'success': 1,
        'status': 'running'
    }
