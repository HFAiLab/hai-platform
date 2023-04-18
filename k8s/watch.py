import ujson
import http
from kubernetes.watch import Watch
from kubernetes import client

HTTP_STATUS_GONE = http.HTTPStatus.GONE


def iter_resp_lines(resp):
    prev = ""
    for seg in resp.stream(amt=None, decode_content=False):
        if isinstance(seg, bytes):
            seg = seg.decode('utf8')
        seg = prev + seg
        lines = seg.split("\n")
        if not seg.endswith("\n"):
            prev = lines[-1]
            lines = lines[:-1]
        else:
            prev = ""
        for line in lines:
            if line:
                yield line

class MyWatch(Watch):

    def __init__(self, return_type=None):
        super().__init__(return_type)
        self.received_book_mark =False

    def unmarshal_event(self, data, return_type):
        js = ujson.loads(data)
        # js['raw_object'] = js['object']
        # BOOKMARK event is treated the same as ERROR for a quick fix of
        # decoding exception
        # TODO: make use of the resource_version in BOOKMARK event for more
        # efficient WATCH
        # if return_type and js['type'] != 'ERROR' and js['type'] != 'BOOKMARK':
        if return_type and js['type'] != 'ERROR':
            if js['type'] == 'BOOKMARK':
                # treat BOOKMARK as a custom object, so that resource_version
                # is updated
                self.received_book_mark = True
            if hasattr(js['object'], 'metadata'):
                self.resource_version = js['object'].metadata.resource_version
            # For custom objects that we don't have model defined, json
            # deserialization results in dictionary
            elif (isinstance(js['object'], dict) and 'metadata' in js['object']
                  and 'resourceVersion' in js['object']['metadata']):
                self.resource_version = js['object']['metadata']['resourceVersion']
        return js

    def stream(self, func, *args, **kwargs):
        """Watch an API resource and stream the result back via a generator.

        Note that watching an API resource can expire. The method tries to
        resume automatically once from the last result, but if that last result
        is too old as well, an `ApiException` exception will be thrown with
        ``code`` 410. In that case you have to recover yourself, probably
        by listing the API resource to obtain the latest state and then
        watching from that state on by setting ``resource_version`` to
        one returned from listing.

        :param func: The API function pointer. Any parameter to the function
                     can be passed after this parameter.

        :return: Event object with these keys:
                   'type': The type of event such as "ADDED", "DELETED", etc.
                   'raw_object': a dict representing the watched object.
                   'object': A model representation of raw_object. The name of
                             model will be determined based on
                             the func's doc string. If it cannot be determined,
                             'object' value will be the same as 'raw_object'.

        Example:
            v1 = kubernetes.client.CoreV1Api()
            watch = kubernetes.watch.Watch()
            for e in watch.stream(v1.list_namespace, resource_version=1127):
                type = e['type']
                object = e['object']  # object is one of type return_type
                raw_object = e['raw_object']  # raw_object is a dict
                ...
                if should_stop:
                    watch.stop()
        """

        self._stop = False
        return_type = self.get_return_type(func)
        watch_arg = self.get_watch_argument_name(func)
        kwargs[watch_arg] = True
        kwargs['_preload_content'] = False
        if 'resource_version' in kwargs:
            self.resource_version = kwargs['resource_version']

        # Do not attempt retries if user specifies a timeout.
        # We want to ensure we are returning within that timeout.
        disable_retries = ('timeout_seconds' in kwargs)
        retry_after_410 = False
        while True:
            resp = func(*args, **kwargs)
            try:
                for line in iter_resp_lines(resp):
                    # unmarshal when we are receiving events from watch,
                    # return raw string when we are streaming log
                    if watch_arg == "watch":
                        event = self.unmarshal_event(line, return_type)
                        if isinstance(event, dict) and event['type'] == 'ERROR':
                            obj = event['object']
                            # Current request expired, let's retry, (if enabled)
                            # but only if we have not already retried.
                            if not disable_retries and not retry_after_410 and \
                                    obj['code'] == HTTP_STATUS_GONE:
                                # for too old resourversion and not bookmark received, reset it to 0 and retry
                                if not self.received_book_mark:
                                    self.resource_version = '0'
                                retry_after_410 = True
                                break
                            else:
                                reason = "%s: %s" % (
                                    obj['reason'], obj['message'])
                                raise client.rest.ApiException(
                                    status=obj['code'], reason=reason)
                        else:
                            retry_after_410 = False
                            yield event
                    else:
                        yield line
                    if self._stop:
                        break
            finally:
                resp.close()
                resp.release_conn()
                self.received_book_mark = False
                if self.resource_version is not None:
                    kwargs['resource_version'] = self.resource_version
                else:
                    self._stop = True

            if self._stop or disable_retries:
                break
