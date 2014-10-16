from tempodb.client import Client
from temposhim.protocol.objects import DataPoint
from temposhim.client import Client as Shim
import datetime
import config

start = datetime.datetime(2014, 10, 10, 1, 10)
end = datetime.datetime(2014, 10, 10, 1, 45)

shim = Shim(database='example2')

device = '001EC010AD69'
#device = '001EC0BB3D27'

tempo = Client(config.API_KEY, config.API_KEY, config.API_SECRET)


def write_data():
    all_series = tempo.list_series(attrs={'deviceid': device})
    points = []

    for series in all_series:
        print("getting data for series {}".format(series.key))
        try:
            data = tempo.read_data(key=series.key, start=start, end=end)
        except Exception as e:
            print("Exception reading data: " + repr(e))
            continue

        for point in data:
            points.append(DataPoint.from_data(point.t, point.v, key=series.key))
            if len(points) > 200:
                print("Writing data thru shim")
                shim.write_multi(points)
                points = []

    if len(points) > 0:
        print("Writing data thru shim")
        shim.write_multi(points)


def read_data():
    keys = [
        "deviceid:001EC010AD69.sensor:wind_data.source:23.instance:0.type:TWIND True North.parameter:wind_direction.HelmSmart",
        "deviceid:001EC010AD69.sensor:wind_data.source:23.instance:0.type:NULL.parameter:wind_speed.HelmSmart",
        "deviceid:001EC010AD69.sensor:wind_data.source:23.instance:0.type:Apparent Wind.parameter:wind_direction.HelmSmart",
        "deviceid:001EC010AD69.sensor:wind_data.source:23.instance:0.type:NULL.parameter:wind_direction.HelmSmart",
        "deviceid:001EC010AD69.sensor:wind_data.source:23.instance:0.type:Apparent Wind.parameter:wind_speed.HelmSmart",
        "deviceid:001EC010AD69.sensor:wind_data.source:23.instance:0.type:TWIND True North.parameter:wind_speed.HelmSmart"]

    influx_data = shim.read_multi(start, end, keys=keys, rollup="mean", period="PT60S")
    tempo_data = tempo.read_multi(start, end, keys=keys, rollup="mean", period="PT60S")

    for t, i in zip(tempo_data.data, influx_data.data):
        print(t.to_json())

        if (t.t != i.t):
            print("!!! Mismatched timestamp: DB: {}, InF: {}".format(t.t, i.t))
        else:
            print("==== Timestamp looks ok: {}".format(t.t.isoformat()))

        for key in t.v.iterkeys():
            tempoVal = t.get(key)
            influxVal = i.get(key)
            if (tempoVal != influxVal):
                print("!!! Different values for {} at {}:  tempo: {}  influx: {}"
                      .format(key, t.t, tempoVal, influxVal))


read_data()
