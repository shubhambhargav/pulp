
class SparkCsvWriter(object):

    def __init__(self, filepath, schema, is_append=False, is_testing=False):
        self.filepath = filepath
        self.is_append = is_append
        self.schema = schema
        self.is_testing = is_testing

    def open(self):
        return True

    def write(self, df):
        if self.is_append:
            df.write.csv(path=self.filepath, mode='append')
        else:
            df.write.csv(path=self.filepath)

    def close(self):
        return