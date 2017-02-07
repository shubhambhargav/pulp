import os
import sys
import abc
import importlib
from time import sleep
from multiprocessing import Process, Queue, Manager

from pulp.conf import settings    
from pulp.utils.error import ErrorHandler
from pulp.db.fields import Field
from pulp.db.custom import MissingDefaultDict, JsonPayloadObject
from pulp.utils.util import Utilities

DEFAULT_QUEUE_SIZE = 100


class SingularModel(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs):
        #scan all member field which is from Field
        d = self.__class__.__dict__
        self.fields = {}
        self.read_columns = []
        self.write_columns = []
        for key, value in d.iteritems():
            if isinstance(value, Field):
                self.fields[key] = value
                if value.src:
                    self.read_columns.append((key, value.src))

                if value.des:
                    self.write_columns.append((key, value.des))

        # Reader Import from settings
        reader_from_settings = settings.READERS.get(self.__class__.Meta.reader)

        if not reader_from_settings:
            raise Exception("No reader '%s' defined in %s" % (reader_from_settings, settings))

        # Writer Import from settings
        writer_from_settings = settings.WRITERS.get(self.__class__.Meta.writer)

        if not writer_from_settings:
            raise Exception("No writer '%s' defined in %s" % (writer_from_settings, settings))

        # Reader/Writer initialization
        """
        Steps:
        1. Get connection details from settings for given reader
        2. Update connection details with the arguments for given instance
        3. Pass final connection details to specified reader/writer
        """
        self.is_testing = kwargs.get('is_testing', False)

        # get connection details from settings
        reader_args = reader_from_settings.get('config', {}).copy()
        reader_args['is_testing'] = self.is_testing
        # override/add additional details
        reader_args.update(kwargs['reader'])
        self.reader = reader_from_settings.get('engine')(**reader_args)

        writer_args = writer_from_settings.get('config', {}).copy()
        writer_args['is_testing'] = self.is_testing
        writer_args.update(kwargs['writer'])
        self.writer = writer_from_settings.get('engine')(**writer_args)

        # Mailer initialization
        if 'mailer' in kwargs:
            mailer = kwargs['mailer']
            self.is_mailer_active = mailer.get('is_active', False)
            self.mail_subject = mailer.get('subject', '[Test] Error Mailer')
        else:
            self.is_mailer_active = False

        self.is_debug = kwargs.get('is_debug', False)

        self.batch = []
        self.error_log = []

    def open(self):
        return self.writer.open() is not None and self.reader.open() is not None

    def close(self):
        self.writer.close()
        self.reader.close()

        if self.is_mailer_active and self.error_log:
            ErrorHandler().send_error_mail(self.error_log, self.mail_subject, settings.ERROR_MAIL_RECEIVER)

    def _source_to_native(self, obj):
        if not obj:
            return None

        src_object = {}
        self.is_valid = True

        expected = []
        for col in self.read_columns:
            new_key, old_key = col
            
            f = self.fields[new_key]
            v = obj.get(old_key, None)

            if v is None:
                if not f.required:
                    continue
                elif f.default is not None:
                    v = f.default
                else:
                    expected.append(old_key)
                    continue
            
            f.validate(v)

            src_object[new_key] = f.validated_value

        if len(expected) > 0:
            src_object = {}
            msg = "Expected '%s', but not found from source" % (','.join(expected))
            raise Exception(msg)

        return JsonPayloadObject(src_object)


    def _native_to_target(self, src_object):
        src_object = src_object.convertToDict()

        d = {}
        expected = []
        for col in self.write_columns:
            old_key, new_key = col
            
            f = self.fields[old_key]
            v = src_object.get(old_key, None)

            if v is None:
                if not f.required:
                    continue
                elif f.default is not None:
                    v = f.default
                else:
                    expected.append(old_key)
            
            d[new_key] = v

        if len(expected) > 0:
            msg = "Expected '%s', but not found for target" % (','.join(expected))
            raise Exception(msg)

        return d
        
    def _ingest_one(self, obj):
        add_obj = {
                    'data': obj,
                    'target_attrs': getattr(self.writer, 'target_attrs', {})
                }
        self.batch.append(add_obj)

        if len(self.batch) >= self.writer.batchsize:
            fetched_error = self.writer.write(self.batch)
            if fetched_error:
                self.error_log += fetched_error
            self.batch = []

    def _batch_check_before_close(self):
        if self.batch:
            fetched_error = self.writer.write(self.batch)
            if fetched_error:
                self.error_log += fetched_error
        self.batch = []


    def _ingest(self):
        d = self.reader.read_one()
        d = self._source_to_native(d)
        
        if self.reader.eof:
            return

        d = self.process_data(d)

        if not self.is_valid:
            return

        d = self._native_to_target(d)
        self._ingest_one(d)


    def start(self):
        if self.open():
            while not self.reader.eof:
                try:
                    self._ingest()
                except Exception as ex:
                    self.error_log.append(ex)

            self._batch_check_before_close()
            
            self.close()

    @abc.abstractmethod
    def process_data(self, clean_data):
        """
        Prcocess data abstract class
        """
        return clean_data


class BatchModel(object):
    
    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs):
        #scan all member field which is from Field
        d = self.__class__.__dict__
        self.kwargs = kwargs
        self.fields = {}
        self.read_columns = []
        self.write_columns = []
        for key, value in d.iteritems():
            if isinstance(value, Field):
                self.fields[key] = value
                if value.src:
                    self.read_columns.append((key, value.src))

                if value.des:
                    self.write_columns.append((key, value.des))

        # Reader/Writer initialization
        """
        Steps:
        1. Get connection details from settings for given reader
        2. Update connection details with the arguments for given instance
        3. Pass final connection details to specified reader/writer
        """
        self.is_testing = kwargs.get('is_testing', False)

        self.reader = Utilities.get_reader_instance(self.__class__.Meta.reader, **self.kwargs)
        self.writer = Utilities.get_writer_instance(self.__class__.Meta.writer, **self.kwargs)

        # Mailer initialization
        if 'mailer' in kwargs:
            mailer = kwargs['mailer']
            self.is_mailer_active = mailer.get('is_active', False)
            self.mail_subject = mailer.get('subject', '[Test] Error Mailer')
        else:
            self.is_mailer_active = False

        self.is_debug = kwargs.get('is_debug', False)

        self.batch = []
        self.error_log = []

        self.queue = Queue(kwargs.get('queue_size', 10))
        self.workers = kwargs.get('n_workers', 4)

    def open(self):
        return self.writer.open() is not None and self.reader.open() is not None

    def close(self):
        self.writer.close()
        self.reader.close()

        if self.is_mailer_active and self.error_log:
            ErrorHandler().send_error_mail(self.error_log, self.mail_subject, settings.ERROR_MAIL_RECEIVER)

    @staticmethod
    def _source_to_native(read_columns, fields, obj):
        if not obj:
            return None

        src_object = {}
        is_valid = True

        expected = []
        for col in read_columns:
            new_key, old_key = col
            
            f = fields[new_key]
            v = obj.get(old_key, None)

            if v is None:
                if not f.required:
                    continue
                elif f.default is not None:
                    v = f.default
                else:
                    expected.append(old_key)
                    continue
            
            f.validate(v)

            src_object[new_key] = f.validated_value

        if len(expected) > 0:
            src_object = {}
            msg = "Expected '%s', but not found from source" % (','.join(expected))
            raise Exception(msg)

        return JsonPayloadObject(src_object)

    @staticmethod
    def _native_to_target(write_columns, fields, src_object):
        src_object = src_object.convertToDict()

        d = {}
        expected = []
        for col in write_columns:
            old_key, new_key = col
            
            f = fields[old_key]
            v = src_object.get(old_key, None)

            if v is None:
                if not f.required:
                    continue
                elif f.default is not None:
                    v = f.default
                else:
                    expected.append(old_key)
            
            d[new_key] = v

        if len(expected) > 0:
            msg = "Expected '%s', but not found for target" % (','.join(expected))
            raise Exception(msg)

        return d
        
    def _ingest_one(self, obj):
        add_obj = {
                    'data': obj,
                    'target_attrs': getattr(self.writer, 'target_attrs', {})
                }
        self.batch.append(add_obj)

        if len(self.batch) >= self.writer.batchsize:
            fetched_error = self.writer.write(self.batch)
            if fetched_error:
                self.error_log += fetched_error
            self.batch = []

    @staticmethod
    def _ingest(n_worker, instance):
        batch = []
        writer_instance = Utilities.get_writer_instance(instance.__class__.Meta.writer, **instance.kwargs)
        writer_instance.open()

        while True:
            obj, counter = instance.queue.get()

            if not obj or not counter:
                if batch:
                    fetched_error = writer_instance.write(batch)
                    if fetched_error:
                        instance.error_log += fetched_error

                writer_instance.close()
                break

            try:
                native_object = instance._source_to_native(instance.read_columns, instance.fields, obj)
                processed_object = instance.process_data(native_object, writer_instance)
                target_object = instance._native_to_target(instance.write_columns, instance.fields, processed_object)

                add_obj = {
                    'data': target_object,
                    'target_attrs': getattr(writer_instance, 'target_attrs', {})
                }
                batch.append(add_obj)

                if len(batch) >= writer_instance.batchsize:
                    fetched_error = writer_instance.write(batch)
                    if fetched_error:
                        instance.error_log += fetched_error
                    batch = []
            except Exception as ex:
                instance.error_log.append(ex)

    def start(self):
        manager = Manager()
        self.error_log = manager.list()

        if self.open():
            processes = []
            for i in range(self.workers):
                p = Process(target=BatchModel._ingest, args=(i, self))
                processes.append(p)
                p.start()

            count = 0
            while not self.reader.eof:
                try:
                    d = self.reader.read_one()
                    if self.reader.eof:
                        continue

                    count += 1
                    self.queue.put((d, count))
                except Exception as ex:
                    self.error_log.append(ex)

            for i in range(self.workers):
                self.queue.put((None, None))
            
            while not self.queue.empty():
                sleep(1)
            
            self.close()

    @abc.abstractmethod
    def process_data(self, clean_data, writer_instance):
        """
        Prcocess data abstract class
        """
        return clean_data


class AggregateModel(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs):
        #scan all member field which is from Field
        d = self.__class__.__dict__
        self.fields = {}
        self.read_columns = []
        self.write_columns = []
        self.key_columns = []
        self.agg_columns = []
        for key, value in d.iteritems():
            if isinstance(value, Field):
                self.fields[key] = value
                if value.src:
                    self.read_columns.append((key, value.src))

                if value.des:
                    self.write_columns.append((key, value.des))

                if value.is_key:
                    self.key_columns.append(key)

                if value.is_agg:
                    self.agg_columns.append(key)

        # Reader Import from settings
        reader_from_settings = settings.READERS.get(self.__class__.Meta.reader)

        if not reader_from_settings:
            raise Exception("No reader '%s' defined in %s" % (reader_from_settings, settings))

        # Writer Import from settings
        writer_from_settings = settings.WRITERS.get(self.__class__.Meta.writer)

        if not writer_from_settings:
            raise Exception("No writer '%s' defined in %s" % (writer_from_settings, settings))

        # Create Key
        self.key_format = None
        self._define_key()

        # Reader/Writer initialization
        """
        Steps:
        1. Get connection details from settings for given reader
        2. Update connection details with the arguments for given instance
        3. Pass final connection details to specified reader/writer
        """
        self.is_testing = kwargs.get('is_testing', False)

        # get connection details from settings
        reader_args = reader_from_settings.get('config', {}).copy()
        reader_args['is_testing'] = self.is_testing
        # override/add additional details
        reader_args.update(kwargs['reader'])
        self.reader = reader_from_settings.get('engine')(**reader_args)

        writer_args = writer_from_settings.get('config', {}).copy()
        writer_args['is_testing'] = self.is_testing
        writer_args.update(kwargs['writer'])
        self.writer = writer_from_settings.get('engine')(**writer_args)

        # Mailer initialization
        if 'mailer' in kwargs:
            mailer = kwargs['mailer']
            self.is_mailer_active = mailer.get('is_active', False)
            self.mail_subject = mailer.get('subject', '[Test] Error Mailer')
        else:
            self.is_mailer_active = False

        self.is_debug = kwargs.get('is_debug', False)

        self.batch = []
        self.error_log = []

    def open(self):
        return self.writer.open() is not None and self.reader.open() is not None

    def close(self):
        self.writer.close()
        self.reader.close()

        if self.is_mailer_active and self.error_log:
            ErrorHandler().send_error_mail(self.error_log, self.mail_subject, settings.ERROR_MAIL_RECEIVER)

    def _define_key(self):
        self.key_format = '|'.join(['%%(%s)s' % k for k in self.key_columns])

    def _source_to_native(self, obj):
        if not obj:
            return None

        src_object = {}
        self.is_valid = True

        expected = []
        for col in self.read_columns:
            new_key, old_key = col
            
            f = self.fields[new_key]
            v = obj.get(old_key, None)

            if v is None:
                if not f.required:
                    continue
                elif f.default is not None:
                    v = f.default
                else:
                    expected.append(old_key)
                    continue
            
            f.validate(v)

            src_object[new_key] = f.validated_value

        if len(expected) > 0:
            src_object = {}
            msg = "Expected '%s', but not found from source" % (','.join(expected))
            raise Exception(msg)

        return JsonPayloadObject(src_object)

    def _native_to_target(self, src_object):
        d = {}
        expected = []
        for col in self.write_columns:
            old_key, new_key = col
            
            f = self.fields[old_key]
            v = src_object.get(old_key, None)

            if v in [None, '']:
                if not f.required:
                    continue
                elif f.default is not None:
                    v = f.default
                else:
                    expected.append(old_key)

            f.validate(v)
            
            d[new_key] = f.validated_value

        if len(expected) > 0:
            msg = "Expected '%s', but not found for target" % (','.join(expected))
            raise Exception(msg)

        return d
        
    def _ingest_one(self, obj):
        add_obj = {
                    'data': obj,
                    'target_attrs': getattr(self.writer, 'target_attrs', {})
                }
        self.batch.append(add_obj)

        if len(self.batch) >= self.writer.batchsize:
            fetched_error = self.writer.write(self.batch)
            if fetched_error:
                self.error_log += fetched_error
            self.batch = []

    def _batch_check_before_close(self):
        if self.batch:
            fetched_error = self.writer.write(self.batch)
            if fetched_error:
                self.error_log += fetched_error
        self.batch = []

    def _increment_values(self, data, agg_data):
        key = self.key_format % MissingDefaultDict(data)

        if not data:
            return agg_data

        if key not in agg_data:
            agg_data[key] = {}

        for e in self.agg_columns:
            if not data.get(e):
                continue
            
            if e not in agg_data[key]:
                agg_data[key][e] = 0

            agg_data[key][e] += data[e]

        return agg_data

    def _dict_to_list(self, agg_data):
        l = []

        for kval, cdict in agg_data.items():
            if not cdict:
                continue

            kdict = dict(zip(self.key_columns, kval.split('|')))
            cdict.update(**kdict)

            l.append(cdict)

        return l

    def _ingest(self):
        agg_data = {}

        while not self.reader.eof:
            try:
                d = self.reader.readOne()

                if not d:
                    continue

                d = self._source_to_native(d)
            
                if not self.is_valid:
                    continue

                d = self.process_data(d)
                d = d.convertToDict()
                
                if not self.is_valid or not d:
                    continue

                agg_data = self._increment_values(d, agg_data)

            except Exception as ex:
                self.error_log.append(ex)
        
        l = self._dict_to_list(agg_data)

        for e in l:
            try:
                e = self._native_to_target(e)
                self._ingest_one(e)
            except Exception as ex:
                self.error_log.append(ex)


    def start(self):
        if self.open():
            self._ingest()
            self._batch_check_before_close()
            self.close()

    @abc.abstractmethod
    def process_data(self, clean_data):
        """
        Process data abstract class
        """
        return clean_data


class SparkDirect(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs):

        self.spark_submit_args = ''
        self.spark_args = kwargs.get('spark_args', {})

        # Import Custom Modules
        if 'custom_modules' in self.spark_args:
            self.spark_submit_args = '--jars %s pyspark-shell' % ','.join(self.spark_args['custom_modules'])
            os.environ.setdefault('PYSPARK_SUBMIT_ARGS', self.spark_submit_args)

        # Reader Import from settings
        reader_from_settings = settings.READERS.get(self.__class__.Meta.reader)

        if not reader_from_settings:
            raise Exception("No reader '%s' defined in %s" % (reader_from_settings, settings))

        # Writer Import from settings
        writer_from_settings = settings.WRITERS.get(self.__class__.Meta.writer)

        if not writer_from_settings:
            raise Exception("No writer '%s' defined in %s" % (writer_from_settings, settings))

        # Reader/Writer initialization
        """
        Steps:
        1. Get connection details from settings for given reader
        2. Update connection details with the arguments for given instance
        3. Pass final connection details to specified reader/writer
        """
        self.is_testing = kwargs.get('is_testing', False)

        # get connection details from settings
        reader_args = reader_from_settings.get('config', {}).copy()
        reader_args['is_testing'] = self.is_testing
        # override/add additional details
        reader_args.update(kwargs['reader'])
        self.reader = reader_from_settings.get('engine')(**reader_args)

        writer_args = writer_from_settings.get('config', {}).copy()
        writer_args['is_testing'] = self.is_testing
        writer_args.update(kwargs['writer'])
        self.writer = writer_from_settings.get('engine')(**writer_args)

        # Mailer initialization
        if 'mailer' in kwargs:
            mailer = kwargs['mailer']
            self.is_mailer_active = mailer.get('is_active', False)
            self.mail_subject = mailer.get('subject', '[Test] Error Mailer')
        else:
            self.is_mailer_active = False

        self.is_debug = kwargs.get('is_debug', False)

        self.error_log = []

    def get_sc_instance(self):
        from pyspark import SparkContext
        # Get a new context given the app_name
        return SparkContext.getOrCreate(appName=self.spark_args.get('app_name'))

    def get_sc_conf(self):
        from pyspark import SparkConf
        return SparkConf().setAppName(self.spark_args.get('app_name'))

    def get_session_instance(self):
        from pyspark.sql import SparkSession
        # Create a new session for spark
        return SparkSession.builder.config(conf=self.get_sc_conf())\
                                    .getOrCreate()

    def open(self):
        return self.writer.open() is not None and self.reader.open() is not None

    def close(self):
        self.writer.close()
        self.reader.close()

        if self.is_mailer_active and self.error_log:
            ErrorHandler().send_error_mail(self.error_log, self.mail_subject, settings.ERROR_MAIL_RECEIVER)

    def start(self):
        if self.open():
            input_data = self.reader.read()
            output_data = self.process_data(input_data)

            if output_data:
                self.writer.write(output_data)
                output_data.sql_ctx.sparkSession.stop()
                output_data.sql_ctx.sparkSession.sparkContext.stop()
            elif input_data:
                input_data.context.stop()
            
            self.close()

    @abc.abstractmethod
    def process_data(self, input_data):
        """
        Process data abstract class
        """
        return input_data
