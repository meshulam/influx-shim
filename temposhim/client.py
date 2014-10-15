from response import Response, ResponseException
import influxdb
import datetime
import dateutil.parser
import pytz


def ts_to_epoch_ms(t):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
    if t is None:
        return None

    if type(t) is str:
        t = dateutil.parser.parse(t)

    epoch_ms = (t - epoch).total_seconds() * 1000
    return epoch_ms


def key_to_attributes(key):
    attrs = {}
    a_list = key.split('.')
    for att in a_list:
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

    def create_series(self, key=None, tags=[], attrs={}):
        """Pretty sure this just shouldn't do anything"""
        return Response()

    def write_multi(self, data):
        """data is a list of {"t": "2012-...", "key": "foo", "v": 1},

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
            vals.append(ts_to_epoch_ms(point.t))
            cols.append('value')
            vals.append(point.v)
            influx_data.append({"points": [vals], "name": name, "columns": cols})

        print("writing data: " + repr(influx_data))
        try:
            self.influx.write_points_with_precision(influx_data, time_precision='ms')
        except influxdb.InfluxDBClientError as e:
            raise ResponseException(e.code, e.content)

        return Response()

