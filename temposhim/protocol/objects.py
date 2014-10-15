import json
from temposhim.temporal.validate import convert_iso_stamp, check_time_param
#from cursor import DataPointCursor, SeriesCursor, SingleValueCursor


class JSONSerializable(object):
    """Base class for objects that are serializable to and from JSON.
    This class defines default methods for serializing each way that use
    the class's "properties" class variable to determine what should be
    serialized or deserialized.  For example::

        class MySerialized(JSONSerializable)
            properties = ['foo', 'bar']

    This would define a class that expects to have the 'foo' and 'bar'
    keys in JSON data and would likewise serialize a JSON object with
    those keys.

    The base constructor calls the :meth:`from_json` method, which
    enforces these constraints for object construction.  If you override
    this constructor (for example, to provide static initialization of
    some variables), it is highly recommended that the subclass constructor
    call this constructor at some point through super().

    :param string json_text: the JSON string to deserialize from"""

    properties = []

    def __init__(self, json_text, response):
        self.from_json(json_text)
        self.response = response

    def from_json(self, json_text):
        """Deserialize a JSON object into this object.  This method will
        check that the JSON object has the required keys and will set each
        of the keys in that JSON object as an instance attribute of this
        object.

        :param json_text: the JSON text or object to deserialize from
        :type json_text: dict or string
        :raises ValueError: if the JSON object lacks an expected key
        :rtype: None"""

        #due to the architecture of response parsing, particularly
        #where the API returns lists, the JSON might have already been
        #parsed by the time it gets here
        if type(json_text) in [str, unicode]:
            j = json.loads(json_text)
        else:
            j = json_text

        try:
            for p in self.properties:
                setattr(self, p, j[p])
        except KeyError, e:
            msg = 'Expected key %s in JSON object, found None' % str(e)
            raise ValueError(msg)

    def to_json(self):
        """Serialize an object to JSON based on its "properties" class
        attribute.

        :rtype: string"""

        j = {}
        for p in self.properties:
            j[p] = getattr(self, p)

        return json.dumps(j)

    def to_dictionary(self):
        """Serialize an object into dictionary form.  Useful if you have to
        serialize an array of objects into JSON.  Otherwise, if you call the
        :meth:`to_json` method on each object in the list and then try to
        dump the array, you end up with an array with one string."""

        j = {}
        for p in self.properties:
            j[p] = getattr(self, p)

        return j


#PLACEHOLDER FOR EMPTY RESPONSES
class Nothing(object):
    """Used to represent empty responses.  This class should not be
    used directly in user code."""

    def __init__(self, *args, **kwargs):
        pass


class Series(JSONSerializable):
    """Represents a Series object from the TempoDB API.  Series objects
    are serialized to and from JSON using the :meth:`to_json` and
    :meth:`from_json` methods.

    Domain object attributes:

        * key: string
        * name: string
        * tags: list
        * attributes: dictionary"""

    properties = ['key', 'name', 'tags', 'attributes']

    def __init__(self, json_text, response):
        #the formatting of the series object returned from the series by key
        #endpoint is slightly different
        if isinstance(json_text, basestring):
            j = json.loads(json_text)
        else:
            j = json_text
        if 'series' in j:
            self.from_json(j['series'])
        else:
            self.from_json(json_text)
        self.response = response


class Rollup(JSONSerializable):
    """Represents the rollup information returned from the TempoDB API when
    the API calls demands it."""

    properties = ['interval', 'function', 'tz']


class DataPoint(JSONSerializable):
    """Represents a single data point in a series.  To construct these objects
    in user code, use the class method :meth:`from_data`.

    Domain object attributes:

        * t: DateTime object
        * v: int or float
        * key: string (only present when writing DataPoints)
        * id: string (only present when writing DataPoints)"""

    properties = ['t', 'v', 'key', 'id']

    def __init__(self, json_text, response, tz=None):
        self.tz = tz
        super(DataPoint, self).__init__(json_text, response)

    @classmethod
    def from_data(self, time, value, series_id=None, key=None, tz=None):
        """Create a DataPoint object from data, rather than a JSON object or
        string.  This should be used by user code to construct DataPoints from
        Python-based data like Datetime objects and floats.

        The series_id and key arguments are only necessary if you are doing a
        multi write, in which case those arguments can be used to specify which
        series the DataPoint belongs to.

        If needed, the tz argument should be an Olsen database compliant string
        specifying the time zone for this DataPoint.  This argument is most
        often used internally when reading data from TempoDB.

        :param time: the point in time for this reading
        :type time: ISO8601 string or Datetime
        :param value: the value for this reading
        :type value: int or float
        :param string series_id: (optional) a series ID for this point
        :param string key: (optional) a key for this point
        :param string tz: (optional) a timezone for this point
        :rtype: :class:`DataPoint`"""

        t = check_time_param(time)
        if type(value) in [float, int]:
            v = value
        else:
            raise ValueError('Values must be int or float. Got "%s".' %
                             str(value))

        j = {
            't': t,
            'v': v,
            'id': series_id,
            'key': key
        }
        return DataPoint(j, None, tz=tz)

    def from_json(self, json_text):
        """Deserialize a JSON object into this object.  This method will
        check that the JSON object has the required keys and will set each
        of the keys in that JSON object as an instance attribute of this
        object.

        :param json_text: the JSON text or object to deserialize from
        :type json_text: dict or string
        :raises ValueError: if the JSON object lacks an expected key
        :rtype: None"""

        if type(json_text) in [str, unicode]:
            j = json.loads(json_text)
        else:
            j = json_text

        try:
            for p in self.properties:
                if p == 't':
                    val = convert_iso_stamp(j[p], self.tz)
                    setattr(self, p, val)
                else:
                    setattr(self, p, j[p])
        #overriding this exception allows us to handle optional values like
        #id and key which are only present during particular API calls like
        #multi writes
        except KeyError:
            pass

    def to_json(self):
        """Serialize an object to JSON based on its "properties" class
        attribute.

        :rtype: string"""

        j = {}
        for p in self.properties:
            #this logic change allows us to work with optional values for
            #this data type
            try:
                v = getattr(self, p)
            except AttributeError:
                continue
            if v is not None:
                if p == 't':
                    j[p] = getattr(self, p).isoformat()
                else:
                    j[p] = getattr(self, p)

        return json.dumps(j)

    def to_dictionary(self):
        """Serialize an object into dictionary form.  Useful if you have to
        serialize an array of objects into JSON.  Otherwise, if you call the
        :meth:`to_json` method on each object in the list and then try to
        dump the array, you end up with an array with one string."""

        j = {}
        for p in self.properties:
            try:
                v = getattr(self, p)
            except AttributeError:
                continue
            if v is not None:
                if p == 't':
                    j[p] = getattr(self, p).isoformat()
                else:
                    j[p] = getattr(self, p)

        return j


class MultiPoint(JSONSerializable):
    """Represents a data point with values for multiple series at a single
    timestamp. Returned when performing a multi-series query.  The v attribute
    is a dictionary mapping series key to value.

    Domain object attributes:

        * t: DateTime object
        * v: dictionary"""

    properties = ['t', 'v']

    def __init__(self, json_text, response, tz=None):
        self.tz = tz
        super(MultiPoint, self).__init__(json_text, response)

    def from_json(self, json_text):
        """Deserialize a JSON object into this object.  This method will
        check that the JSON object has the required keys and will set each
        of the keys in that JSON object as an instance attribute of this
        object.

        :param json_text: the JSON text or object to deserialize from
        :type json_text: dict or string
        :raises ValueError: if the JSON object lacks an expected key
        :rtype: None"""

        if type(json_text) in [str, unicode]:
            j = json.loads(json_text)
        else:
            j = json_text

        try:
            for p in self.properties:
                if p == 't':
                    t = convert_iso_stamp(j[p], self.tz)
                    setattr(self, 't', t)
                else:
                    setattr(self, p, j[p])
        #overriding this exception allows us to handle optional values like
        #id and key which are only present during particular API calls like
        #multi writes
        except KeyError:
            pass

    def to_json(self):
        """Serialize an object to JSON based on its "properties" class
        attribute.

        :rtype: string"""

        j = {}
        for p in self.properties:
            try:
                v = getattr(self, p)
            except AttributeError:
                continue
            if v is not None:
                if p == 't':
                    j[p] = getattr(self, p).isoformat()
                else:
                    j[p] = getattr(self, p)

        return json.dumps(j)

    def get(self, k):
        """Convenience method for getting values for individual series out of
        the MultiPoint.  This is equivalent to calling::

            >>> point.v.get('foo')

        :param string k: the key to read
        :rtype: number"""

        return self.v.get(k)
