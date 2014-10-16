import json
from temposhim.response import Response
from temposhim.protocol.objects import MultiPoint
from temposhim.temporal.validate import convert_iso_stamp


class Cursor(object):
    def __init__(self, data=[]):
        self.response = Response()
        self.data = data

    def __iter__(self):
        return iter(self.data)


class MultiPointCursor(Cursor):
    def __init__(self):
        self.response = Response()
        #self.tz = tz
        #self.rollup = data.get('rollup')
        #self.start = convert_iso_stamp(data.get('start'))
        #self.end = convert_iso_stamp(data.get('end'))
        self._data_dict = {}
        self.data = []

    def add_point(self, series_key, ts, value):
        multi_point = self._data_dict.setdefault(ts, MultiPoint(ts))
        multi_point.v[series_key] = value

    def finalize(self):
        self.data = self._data_dict.values()
        self.data.sort(key=lambda x: x.t)
        return self


class SeriesCursor(Cursor):
    """An iterable cursor over a collection of Series objects"""


