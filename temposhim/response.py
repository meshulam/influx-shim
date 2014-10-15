import json


SUCCESS = 0
FAILURE = 1
PARTIAL = 2


class ResponseException(Exception):
    """Exception class for HTTP responses"""

    def __init__(self, reason, code):
        self.response = Response(code, reason, reason)
        self.msg = 'TempoDB response returned status: %d' % code

    def __repr__(self):
        return self.msg

    def __str__(self):
        return self.msg


class MockRequestsResponse(object):
    def __init__(self, status_code, reason="", body=""):
        self.status_code = status_code
        self.reason = reason
        self.text = body


class Response(object):
    def __init__(self, status_code=200, reason="", body=""):
        self.resp = MockRequestsResponse(status_code, reason, body)
        self.session = None
        self.status = self.resp.status_code
        self.reason = self.resp.reason
        if self.status == 200:
            self.successful = SUCCESS
            self.error = None
        elif self.status == 207:
            self.successful = PARTIAL
            self.error = self.resp.text
        else:
            self.successful = FAILURE
            self.error = self.resp.text

        self.resp.encoding = "UTF-8"
        self.body = self.resp.text
        self.data = None

