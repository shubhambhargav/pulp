from datetime import datetime
from collections import namedtuple

from pulp.conf import settings


class Utilities:

    @staticmethod
    def get_reader_instance(reader_name, **kwargs):
        # Reader Import from settings
        reader_from_settings = settings.READERS.get(reader_name)

        if not reader_from_settings:
            raise Exception("No reader '%s' defined in %s" % (reader_from_settings, settings))

        reader_args = reader_from_settings.get('config', {}).copy()
        reader_args['is_testing'] = kwargs.get('is_testing', False)

        reader_args.update(kwargs['reader'])
        return reader_from_settings.get('engine')(**reader_args)

    @staticmethod
    def get_writer_instance(writer_name, **kwargs):
        # Writer Import from settings
        writer_from_settings = settings.WRITERS.get(writer_name)

        if not writer_from_settings:
            raise Exception("No writer '%s' defined in %s" % (writer_from_settings, settings))

        writer_args = writer_from_settings.get('config', {}).copy()
        writer_args['is_testing'] = kwargs.get('is_testing', False)

        writer_args.update(kwargs['writer'])
        return writer_from_settings.get('engine')(**writer_args)

    @staticmethod
    def get_module_abs_path(module_name):
        try:
            abs_path = module_name.__file__
        except Exception as ex:
            raise Exception("Package/Module location access error: %s" % (ex, ))

        return abs_path.replace('__init__.pyc', '')

    # timestamp conversion
    @staticmethod
    def ts_convert(time_val):
        """Time in ms if epoch is provided
            Else in format either '%Y-%m-%d %H:%M:%S.%f' or '%Y-%m-%d %H:%M:%S'"""
        if isinstance(time_val, datetime):
            return time_val

        try:
            return datetime.fromtimestamp(float(time_val)/1000) # Try for epoch conversion
        except:
            time_val = time_val[:26] # For python limitation to read only till 6 decimal values
            patterns = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"] # Try for server type timestamp conversions
            for pattern in patterns:
                try:
                    _format = datetime.strptime(time_val, pattern)
                    return _format
                except:
                    pass

        raise Exception("Invalid Time Format!")

    @staticmethod
    def epoch_to_datetime(ep):
        return datetime.fromtimestamp(float(ep))

    @staticmethod
    def RepresentsInt(s):
        try: 
            int(s)
            return True
        except ValueError:
            return False