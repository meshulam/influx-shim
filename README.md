Shim to map TempoDB Python interface into equivalent Influx calls.

Limitations:

* Rollup period must be specified as # of seconds, e.g. "PT300S". In particular,
  any sequence of digits will be interpreted as a number of seconds.
* Rollup function must be a supported InfluxDB aggregation. Notably, "avg" must be 
  replaced with "mean"
* read_multi may return data for more series than provided in the list of keys. It
  reads all necessary InfluxDB series, which may include multiple equivalent Tempo series.
* list_series isn't yet implemented
* All datetimes are returned in UTC
* In some of my tests, queries didn't return the same results. Could have been import-related, though.



