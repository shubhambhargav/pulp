from pulp.db.fields import CharField, EmbeddedJsonField
from pulp.db.models import SingleProcessModel


class SampleField(EmbeddedJsonField):

    embtest1 = CharField(src='test1', des='test1')


class SampleIngestor(SingleProcessModel):

    test1 = CharField(src='test1', des='test2')
    test2 = CharField(src='test2', des='test1')
    test3 = SampleField(src='test3', des='test3')

    class Meta:
        reader = 'log_reader'
        writer = 'log_writer'

    def process_data(self, d):
        return d