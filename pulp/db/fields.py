import re
import json
from datetime import datetime, date
from pulp.db.custom import JsonPayloadObject

TYPE_ERROR_STRING = "Invalid %s value '%s' for %s: '%s'!"
CHOICE_ERROR_STRING = "Value '%s' not in choices '%s' for %s: %s!"

# Fields Definition
class Field(object):

    def __init__(self, src=None, des=None, default=None, child=None, required=True, is_key=False, is_agg=False):
        self.src = src
        self.des = des
        self.child = child
        self.required = required
        self.default = default
        self.is_key = is_key
        self.is_agg = is_agg

        self.validated_value = None

    def __str__(self):
        return '%s.%s.%s' % ('Field', self.src, self.des)

    def __repr__(self):
        """
        Displays the module, class and name of the field.
        """
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        name = getattr(self, 'name', None)
        if name is not None:
            return '<%s: %s>' % (path, name)
        return '<%s>' % path

    def validate(self):
        return True


class IntField(Field):

    def validate(self, value):
        try:
            self.validated_value = int(value)
        except:
            raise TypeError(TYPE_ERROR_STRING % ("integer", value, self.__class__.__name__, self.src))


class FloatField(Field):

    def validate(self, value):
        try:
            self.validated_value = float(value)
        except:
            raise TypeError(TYPE_ERROR_STRING % ("float", value, self.__class__.__name__, self.src))


class CharField(Field):

    def validate(self, value):
        try:
            self.validated_value = str(value)
        except:
            raise TypeError(TYPE_ERROR_STRING % ("character", value, self.__class__.__name__, self.src))


class BooleanField(Field):

    def validate(self, value):
        valid_check = False
        try:
            if value == 'True':
                value = True
                pass
            elif value == 'False':
                value = False
                pass
            value = int(value)
            if value in [0, 1]:
                valid_check = True
        except:
            pass

        if not valid_check:
            raise TypeError(TYPE_ERROR_STRING % ("boolean", value, self.__class__.__name__, self.src))

        self.validated_value = value


class UrlField(Field):

    URL_REGEX = re.compile(
                            r'^(?:http|ftp)s?://' # http:// or https://
                            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
                            r'localhost|' #localhost...
                            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
                            r'(?::\d+)?' # optional port
                            r'(?:/?|[/?]\S+)$', re.IGNORECASE
                            )

    def validate(self, value):
        if not self.URL_REGEX.match(value):
            raise TypeError(TYPE_ERROR_STRING % ("url", value, self.__class__.__name__, self.src))

        self.validated_value = value


class IpField(Field):

    IPV4_REGEX = re.compile('^(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$')
    IPV6_REGEX = re.compile('^(?:(?:[0-9A-Fa-f]{1,4}:){6}(?:[0-9A-Fa-f]{1,4}:[0-9A-Fa-f]{1,4}|(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))|::(?:[0-9A-Fa-f]{1,4}:){5}(?:[0-9A-Fa-f]{1,4}:[0-9A-Fa-f]{1,4}|(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))|(?:[0-9A-Fa-f]{1,4})?::(?:[0-9A-Fa-f]{1,4}:){4}(?:[0-9A-Fa-f]{1,4}:[0-9A-Fa-f]{1,4}|(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))|(?:[0-9A-Fa-f]{1,4}:[0-9A-Fa-f]{1,4})?::(?:[0-9A-Fa-f]{1,4}:){3}(?:[0-9A-Fa-f]{1,4}:[0-9A-Fa-f]{1,4}|(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))|(?:(?:[0-9A-Fa-f]{1,4}:){,2}[0-9A-Fa-f]{1,4})?::(?:[0-9A-Fa-f]{1,4}:){2}(?:[0-9A-Fa-f]{1,4}:[0-9A-Fa-f]{1,4}|(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))|(?:(?:[0-9A-Fa-f]{1,4}:){,3}[0-9A-Fa-f]{1,4})?::[0-9A-Fa-f]{1,4}:(?:[0-9A-Fa-f]{1,4}:[0-9A-Fa-f]{1,4}|(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))|(?:(?:[0-9A-Fa-f]{1,4}:){,4}[0-9A-Fa-f]{1,4})?::(?:[0-9A-Fa-f]{1,4}:[0-9A-Fa-f]{1,4}|(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))|(?:(?:[0-9A-Fa-f]{1,4}:){,5}[0-9A-Fa-f]{1,4})?::[0-9A-Fa-f]{1,4}|(?:(?:[0-9A-Fa-f]{1,4}:){,6}[0-9A-Fa-f]{1,4})?::)$')

    def validate(self, value):
        if not self.IPV4_REGEX.match(value) and not self.IPV6_REGEX.match(value):
            raise TypeError(TYPE_ERROR_STRING % ("ip", value, self.__class__.__name__, self.src))

        self.validated_value = value


class DateTimeField(Field):

    def __init__(self, time_format='%Y-%m-%d %H:%M:%S.%f', *args, **kwargs):
        self.time_format = time_format
        super(DateTimeField, self).__init__(*args, **kwargs)

    def validate(self, value):
        if not isinstance(value, datetime):
            try:
                value = value[:26] # For python limitation to read only till 6 decimal values
                self.validated_value = datetime.strptime(value, self.time_format)
            except ValueError:
                raise TypeError(TYPE_ERROR_STRING % ("datetime", value, self.__class__.__name__, self.src))
        else:
            self.validated_value = value


class DateField(Field):

    def __init__(self, date_format='%Y-%m-%d %H:%M:%S.%f', *args, **kwargs):
        self.date_format = date_format
        super(DateField, self).__init__(*args, **kwargs)

    def validate(self, value):
        if not isinstance(value, date):
            try:
                self.validated_value = datetime.strptime(value, self.date_format).date()
            except ValueError:
                raise TypeError(TYPE_ERROR_STRING % ("date", value, self.__class__.__name__, self.src))
        else:
            self.validated_value = value


class ListField(Field):

    def validate(self, value):
        if type(value) != list:
            raise TypeError(TYPE_ERROR_STRING % ("list", value, self.__class__.__name__, self.src))
        else:
            if self.child:
                childInstance = self.child()
                for v in value:
                    childInstance.validate(v)

            self.validated_value = value


class ChoiceField(Field):

    choice_field = ListField(src="ChoiceField")

    def __init__(self, choices, **kwargs):
        super(ChoiceField, self).__init__(**kwargs)
        
        self.choice_field.validate(choices)
        self.choices = choices

    def validate(self, value):
        if value not in self.choices:
            raise TypeError(CHOICE_ERROR_STRING % (value, self.choices, self.__class__.__name__, self.src))

        self.validated_value = value


class EpochField(Field):

     def validate(self, value):
        positive_flag = False
        try:
            value = int(value)
            if value > 0:
                positive_flag = True
        except:
            pass

        if not positive_flag:
            raise TypeError(TYPE_ERROR_STRING % ("epoch", value, self.__class__.__name__, self.src))

        self.validated_value = value


class EmbeddedJsonField(Field):

    def __init__(self, extras=False, **kwargs):
        super(EmbeddedJsonField, self).__init__(**kwargs)

        self.extras = extras
        self.read_columns = []
        self.write_columns = []
        self.fields = {}

        for k, v in self.__class__.__dict__.items():
            if not isinstance(v, Field):
                continue

            self.fields[k] = v

            if v.src:
                self.read_columns.append((k, v.src))
            if v.des:
                self.write_columns.append((k, v.des))

    def _source_to_native(self, d):
        expected = []
        src_object = {}

        for to_key, from_key in self.read_columns:
            f = self.fields[to_key]
            v = d.get(from_key, None)

            if v is None:
                if not f.required:
                    continue
                elif f.default is not None:
                    v = f.default
                else:
                    expected.append(from_key)
                    continue

            f.validate(v)
            src_object[to_key] = f.validated_value

        if len(expected) > 0:
            src_object = {}
            msg = "Expected '%s', but not found from source" % (','.join(expected))
            raise Exception(msg)

        return JsonPayloadObject(src_object)

    def validate(self, value):
        is_dict = False
        if type(value) != dict:
            try:
                value = json.loads(value)
            except ValueError:
                value = eval(value)


        if type(value) == dict:
            is_dict = True

        # Raise error if given value is not convertible to dict
        if not is_dict:
            raise TypeError(TYPE_ERROR_STRING % ("json", value, self.__class__.__name__, self.src))

        value = self._source_to_native(value)

        self.validated_value = value


class JsonField(Field):

    def validate(self, value):
        is_dict = False
        if type(value) != dict:
            try:
                value = json.loads(value)
            except ValueError:
                value = eval(value)

        if type(value) == dict:
            is_dict = True

        # Raise error if given value is not convertible to dict
        if not is_dict:
            raise TypeError(TYPE_ERROR_STRING % ("json", value, self.__class__.__name__, self.src))        

        self.validated_value = value

