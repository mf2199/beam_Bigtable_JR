from __future__ import absolute_import
import argparse
import datetime
import time
import uuid

import math

from sys import platform

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms import core
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from google.cloud.bigtable import Client
from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC

# from beam_bigtable import BigtableSource

STAGING_LOCATION = 'gs://mf2199/stage'
TEMP_LOCATION = 'gs://mf2199/temp'


class BigtableSource(BoundedSource):
  def __init__(self, project_id, instance_id, table_id, filter_=None):
    """ Constructor of the Read connector of Bigtable

    Args:
      project_id: [string] GCP Project of to write the Rows
      instance_id: [string] GCP Instance to write the Rows
      table_id: [string] GCP Table to write the `DirectRows`
      filter_: [RowFilter] Filter to apply to cells in a row.
    """
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'filter_': filter_}
    self.sample_row_keys = None
    self.table = None
    self.read_row = Metrics.counter(self.__class__.__name__, 'read_row')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = options
    self.table = None
    self.sample_row_keys = None
    self.read_row = Metrics.counter(self.__class__.__name__, 'read_row')

  def _get_table(self):
    if self.table is None:
      self.table = Client(project=self.beam_options['project_id'])\
                      .instance(self.beam_options['instance_id'])\
                      .table(self.beam_options['table_id'])
    return self.table

  def get_sample_row_keys(self):
    """ Get a sample of row keys in the table.

    The returned row keys will delimit contiguous sections of the table of
    approximately equal size, which can be used to break up the data for
    distributed tasks like mapreduces.
    :returns: A cancel-able iterator. Can be consumed by calling ``next()``
    			  or by casting to a :class:`list` and can be cancelled by
    			  calling ``cancel()``.
    """
    if self.sample_row_keys is None:
      self.sample_row_keys = list(self._get_table().sample_row_keys())
    return self.sample_row_keys

  def get_range_tracker(self, start_position=b'', stop_position=b''):
    if stop_position == b'':
      return LexicographicKeyRangeTracker(start_position)
    else:
      return LexicographicKeyRangeTracker(start_position, stop_position)

  def estimate_size(self):
    return list(self.get_sample_row_keys())[-1].offset_bytes

  def split(self, desired_bundle_size=None, start_position=None, stop_position=None):
    """ Splits the source into a set of bundles, using the row_set if it is set.

    *** At this point, only splitting an entire table into samples based on the sample row keys is supported ***

    :param desired_bundle_size: the desired size (in bytes) of the bundles returned.
    :param start_position: if specified, the position must be used as the starting position of the first bundle.
    :param stop_position: if specified, the position must be used as the ending position of the last bundle.
    Returns:
    	an iterator of objects of type 'SourceBundle' that gives information about the generated bundles.
    """

    if desired_bundle_size is not None or start_position is not None or stop_position is not None:
      raise NotImplementedError

    # TODO: Use the desired bundle size to split accordingly
    # TODO: Allow users to provide their own row sets

    sample_row_keys = list(self.get_sample_row_keys())
    bundles = []
    if len(sample_row_keys) > 0 and sample_row_keys[0] != b'':
        bundles.append(SourceBundle(sample_row_keys[0].offset_bytes, self, b'', sample_row_keys[0].row_key))
    for i in range(1, len(sample_row_keys)):
      pos_start = sample_row_keys[i - 1].offset_bytes
      pos_stop = sample_row_keys[i].offset_bytes
      bundles.append(SourceBundle(pos_stop - pos_start, self,
                                  sample_row_keys[i - 1].row_key,
                                  sample_row_keys[i].row_key))

    # Shuffle is needed to allow reading from different locations of the table for better efficiency
    shuffle(bundles)
    return bundles

  def read(self, range_tracker):
    for row in self._get_table().read_rows(start_key=range_tracker.start_position(),
                                           end_key=range_tracker.stop_position(),
                                           filter_=self.beam_options['filter_']):
      if range_tracker.try_claim(row.row_key):
        self.read_row.inc()
        yield row
      else:
        # TODO: Modify the client ot be able to cancel read_row request
        break

  def display_data(self):
    ret = {'projectId': DisplayDataItem(self.beam_options['project_id'],
                                        label='Bigtable Project Id',
                                        key='projectId'),
           'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                         label='Bigtable Instance Id',
                                         key='instanceId'),
           'tableId': DisplayDataItem(self.beam_options['table_id'],
                                      label='Bigtable Table Id',
                                      key='tableId')}
    return ret

  def to_runner_api_parameter(self, unused_context):
    pass


class BigtableRead(beam.PTransform):
  def __init__(self, project_id, instance_id, table_id):
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pbegin):
    from apache_beam.options.pipeline_options import DebugOptions
    from apache_beam.transforms import util

    assert isinstance(pbegin, pvalue.PBegin)
    self.pipeline = pbegin.pipeline

    debug_options = self.pipeline._options.view_as(DebugOptions)
    if debug_options.experiments and 'beam_fn_api' in debug_options.experiments:
      source = BigtableSource(project_id=self.beam_options['project_id'],
                              instance_id=self.beam_options['instance_id'],
                              table_id=self.beam_options['table_id'])
      return (
          pbegin
          | 'Impulse' >> core.Impulse()
          | 'Split' >> core.FlatMap(source.split())
          # | 'Reshuffle' >> util.Reshuffle()
          | 'Read' >> core.FlatMap(lambda split: split.source.read(
              split.source.get_range_tracker(split.start_position, split.stop_position))))
    else:
      # Treat Read itself as a primitive.
      return pvalue.PCollection(self.pipeline)

# def get_rows(project_id, instance_id, table_id):
#   return Client(project=project_id).instance(instance_id).table(table_id).read_rows()


def run(argv=[]):

  project_id = 'grass-clump-479'
  instance_id = 'python-write-2'

  table_id = 'test-2kkk-write-20190417-125057'
  jobname = 'test-2kkk-read-{}'.format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S'))

  argv.extend([
    '--experiments=beam_fn_api',
    '--project={}'.format(project_id),
    '--instance={}'.format(instance_id),
    '--table={}'.format(table_id),
    '--projectId={}'.format(project_id),
    '--instanceId={}'.format(instance_id),
    '--tableId={}'.format(table_id),
    '--job_name={}'.format(jobname),
    '--requirements_file=requirements.txt',
    '--disk_size_gb=100',
    '--region=us-central1',
    '--runner=dataflow',
    '--autoscaling_algorithm=NONE',
    '--num_workers=300',
    '--staging_location={}'.format(STAGING_LOCATION),
    '--temp_location={}'.format(TEMP_LOCATION),
    '--setup_file=C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py',
    # '--extra_package=C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\beam_bigtable-0.3.117.tar.gz'
    '--extra_package=C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\bigtableio-0.3.120.tar.gz'
  ])
  parser = argparse.ArgumentParser(argv)
  parser.add_argument('--projectId')
  parser.add_argument('--instanceId')
  parser.add_argument('--tableId')
  (known_args, pipeline_args) = parser.parse_known_args(argv)

  print('ProjectID: ', project_id)
  print('InstanceID:', instance_id)
  print('TableID:   ', table_id)
  print('JobID:     ', jobname)

  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=pipeline_options) as p:
    second_step = (p
                   | 'BigtableFromRead' >> ReadFromBigTable_Read(project_id=project_id,
                                                                 instance_id=instance_id,
                                                                 table_id=table_id))
    count = (second_step
             | 'Count' >> beam.combiners.Count.Globally())
    row_count = 100000
    assert_that(count, equal_to([row_count]))
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
  run()
