import enum
import aiohttp
from fastapi import HTTPException


class RequestMethod(enum.Enum):
    GET = 0
    POST = 1


async def async_requests(method: RequestMethod,
                         url: str,
                         assert_success: list = None,
                         **kwargs):
    """async request"""
    if assert_success is None:
        assert_success = [1]
    async with aiohttp.ClientSession() as session:
        action = session.get if method == RequestMethod.GET else session.post
        async with action(url=url, **kwargs) as response:
            if response.status != 200:
                detail = await response.text()
                raise HTTPException(status_code=response.status, detail=detail)
            result = await response.json()
            # 先assert拿到正确的返回，再assert success字段
            assert result.get('success', None), result
            if result['success'] not in assert_success:
                raise HTTPException(status_code=400, detail=result['msg'])
            return result


def failed_response(msg):
    # status=200 但是失败的 response
    return {
        'success': 0,
        'msg': msg
    }
