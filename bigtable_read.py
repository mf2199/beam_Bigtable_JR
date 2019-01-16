import copy
import math

from google.cloud import bigtable
from google.cloud.bigtable.batcher import MutationsBatcher

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.metrics import Metrics
from apache_beam.io.iobase import SourceBundle
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display import HasDisplayData
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker

class ReadFromBigtable(iobase.BoundedSource):
  def __init__(self, beam_options):
    super(ReadFromBigtable, self).__init__()
    self.beam_options = beam_options
    self.table = None
    self.read_row = Metrics.counter(self.__class__, 'read')

  def _getTable(self):
    if self.table is None:
      options = self.beam_options
      client = bigtable.Client(
        project=options.project_id,
        credentials=self.beam_options.credentials)
      instance = client.instance(options.instance_id)
      self.table = instance.table(options.table_id)
    return self.table

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = options
    self.table = None
    self.read_row = Metrics.counter(self.__class__, 'read')

  def estimate_size(self):
    size = [k.offset_bytes for k in self._getTable().sample_row_keys()][-1]
    return size

  def get_sample_row_keys(self):
    return self._getTable().sample_row_keys()

  def get_range_tracker(self, start_position, stop_position):
    return LexicographicKeyRangeTracker(start_position,
                                        stop_position)

  def split(self,
            desired_bundle_size,
            start_position=None,
            stop_position=None):

    if self.beam_options.row_set is not None:
      for sample_row_key in self.beam_options.row_set.row_ranges:
        sample_row_keys = self.get_sample_row_keys()
        for row_split in self.split_range_size(desired_bundle_size,
                                               sample_row_keys,
                                               sample_row_key):
          yield row_split
    else:
      suma = 0
      last_offset = 0
      current_size = 0

      start_key = b''
      end_key = b''

      sample_row_keys = self.get_sample_row_keys()
      for sample_row_key in sample_row_keys:
        current_size = sample_row_key.offset_bytes-last_offset
        if suma >= desired_bundle_size:
          end_key = sample_row_key.row_key
          for fraction in self.range_split_fraction(suma,
                                                    desired_bundle_size,
                                                    start_key, end_key):
            yield fraction
          start_key = sample_row_key.row_key

          suma = 0
        suma += current_size
        last_offset = sample_row_key.offset_bytes

  def split_range_size(self, desired_size, sample_row_keys, range_):
    print('Start Key', range_.start_key)
    print('End Key', range_.end_key)
    start, end = None, None
    l = 0
    for sample_row in sample_row_keys:
      current = sample_row.offset_bytes - l
      if sample_row.row_key == b'':
        continue

      if(range_.start_key <= sample_row.row_key and
         range_.end_key >= sample_row.row_key):
        if start is not None:
          end = sample_row.row_key
          range_tracker = LexicographicKeyRangeTracker(start, end)

          for split_key_range in self.split_range_sized_subranges(current,
                                                                  desired_size,
                                                                  range_tracker):
            yield split_key_range
        start = sample_row.row_key
      l = sample_row.offset_bytes

  def range_split_fraction(self,
                           current_size,
                           desired_bundle_size,
                           start_key,
                           end_key):
    range_tracker = LexicographicKeyRangeTracker(start_key, end_key)
    return self.split_range_sized_subranges(current_size,
                                            desired_bundle_size,
                                            range_tracker)

  def fraction_to_position(self, position, range_start, range_stop):
    return LexicographicKeyRangeTracker.fraction_to_position(position,
                                                             range_start,
                                                             range_stop)

  def split_range_sized_subranges(self,
                                  sample_size_bytes,
                                  desired_bundle_size,
                                  ranges):
    print('split_range_sized_subranges start', ranges.start_position())
    print('split_range_sized_subranges stop', ranges.stop_position())

    last_key = copy.deepcopy(ranges.stop_position())
    s = ranges.start_position()
    e = ranges.stop_position()

    split_ = float(desired_bundle_size) / float(sample_size_bytes)
    split_count = int(math.ceil(sample_size_bytes / desired_bundle_size))

    for i in range(split_count):
      estimate_position = ((i + 1) * split_)
      position = self.fraction_to_position(estimate_position,
                                           ranges.start_position(),
                                           ranges.stop_position())
      e = position
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, e)
      s = position
    if not s == last_key:
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, last_key )

  def read(self, range_tracker):
    if range_tracker.start_position() is not None:
      if not range_tracker.try_claim(range_tracker.start_position()):
        # there needs to be a way to cancel the request.
        return
    read_rows = self._getTable().read_rows(start_key=range_tracker.start_position(),
      end_key=range_tracker.stop_position(),
      filter_=self.beam_options.filter_)

    for row in read_rows:
      self.read_row.inc()
      yield row

  def display_data(self):
    ret = {
      'projectId': DisplayDataItem(self.beam_options.project_id,
                                   label='Bigtable Project Id',
                                   key='projectId'),
      'instanceId': DisplayDataItem(self.beam_options.instance_id,
                                    label='Bigtable Instance Id',
                                    key='instanceId'),
      'tableId': DisplayDataItem(self.beam_options.table_id,
                                 label='Bigtable Table Id',
                                 key='tableId')}
    if self.beam_options.row_set is not None:
      i = 0
      for value in self.beam_options.row_set.row_keys:
        label = 'Bigtable Row Set {}'.format(i)
        key = 'rowSet{}'.format(i)
        ret[key] = DisplayDataItem(str(value),
                                   label=label,
                                   key=key)
        i = i+1
      for (i,value) in enumerate(self.beam_options.row_set.row_ranges):
        key = 'rowSet{}'.format(i)
        label = 'Bigtable Row Set {}'.format(i)
        ret[key] = DisplayDataItem(str(value.get_range_kwargs()),
                                   label=label,
                                   key=key)
        i = i+1
    if self.beam_options.filter_ is not None:
      for (i,value) in enumerate(self.beam_options.filter_.filters):
        key = 'rowFilter{}'.format(i)
        label = 'Bigtable Row Filter {}'.format(i)
        ret[key] = DisplayDataItem(str(value.to_pb()),
                                   label=label,
                                   key=key)
    return ret
