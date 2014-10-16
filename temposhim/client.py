from response import Response, ResponseException
from temposhim.protocol.cursor import MultiPointCursor
import influxdb
import datetime
import dateutil.parser
import pytz
import re


def datetime_to_ms(t):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
    if t is None:
        return None

    if type(t) is str:
        t = dateutil.parser.parse(t)

    if t.tzinfo is None:
        t = t.replace(tzinfo=pytz.utc)

    epoch_ms = (t - epoch).total_seconds() * 1000
    return int(epoch_ms)


def ms_to_datetime(epoch_ms):
    return datetime.datetime.fromtimestamp(epoch_ms / 1000.0, tz=pytz.utc)


def key_to_attributes(key):
    attrs = {}
    for att in key.split('.'):
        att_split = att.split(':')
        if len(att_split) == 2:
            attrs[att_split[0]] = att_split[1]
    return attrs


class Client(object):
    def __init__(self, *args, **kwargs):
        self.influx = influxdb.InfluxDBClient(*args, **kwargs)

    def key_to_influx_metadata(self, tempo_key):
        """ Takes a TempoIQ series key and returns a tuple of:
        ("influx-series-name", [column names], [column values])

        Column names and values will need to be augmented with the time and value
        cols."""
        attrs = key_to_attributes(tempo_key)

        name = "{}.{}.{}".format(attrs['deviceid'], attrs['sensor'], attrs['parameter'])
        cols = ['source', 'instance', 'type']
        vals = []
        for key in cols:
            vals.append(attrs[key])

        return (name, cols, vals)

    def influx_metadata_to_key(self, name, fields):
        """ Takes an influx series name and dict of column name=> value,
        and returns the corresponding TempoDB series key"""

        device, sensor, parameter = name.split('.')
        key = "deviceid:{}.sensor:{}.source:{}.instance:{}.type:{}.parameter:{}.HelmSmart" \
            .format(device, sensor, fields['source'], fields['instance'],
                    fields['type'], parameter)
        return key

    def write_multi(self, data):
        """data is a list of DataPoint with key, t, and v

        influx wants list of
            {"points":[[1.1,4.3,2.1],[1.2,2.0,2.0]],
                "name":"web_devweb01_load",
                "columns":["min1", "min5", "min15"]
            }
        """
        influx_data = []
        for point in data:
            (name, cols, vals) = self.key_to_influx_metadata(point.key)

            cols.append('time')
            vals.append(datetime_to_ms(point.t))
            cols.append('value')
            vals.append(point.v)
            influx_data.append({"points": [vals], "name": name, "columns": cols})

        try:
            self.influx.write_points_with_precision(influx_data, time_precision='ms')
        except influxdb.InfluxDBClientError as e:
            raise ResponseException(e.code, e.content)

        return Response()

    def read_multi(self, start, end, keys=None, rollup=None, period=None,
                   tz=None):
        period_seconds = re.search(r'\d+', period).group(0)   # Assume any numbers in period string specify # of seconds
        attr_set = [self.key_to_influx_metadata(key) for key in keys]
        names = [name for name, c, v in attr_set]
        query = ('select {}(value) from {} '
                 'where time > {}ms and time < {}ms '
                 'group by time({}s), source, instance, type') \
            .format(rollup, ', '.join(names),
                    datetime_to_ms(start), datetime_to_ms(end),
                    period_seconds)

        print("running influx query: " + query)
        response = self.influx.query(query, time_precision='ms')

        cursor = MultiPointCursor()

        for series in response:
            for point in series['points']:
                fields = {}
                for key, val in zip(series['columns'], point):
                    fields[key] = val
                tempo_key = self.influx_metadata_to_key(series['name'], fields)
                ts = ms_to_datetime(fields['time'])
                cursor.add_point(tempo_key, ts, fields[rollup])

        return cursor.finalize()

    def list_series(self, attrs=None, limit=1000):
        pass

