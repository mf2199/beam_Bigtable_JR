#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""BigTable connector

This module implements writing to BigTable tables.
The default mode is to set row data to write to BigTable tables.
The syntax supported is described here:
https://cloud.google.com/bigtable/docs/quickstart-cbt

BigTable connector can be used as main outputs. A main output
(common case) is expected to be massive and will be split into
manageable chunks and processed in parallel. In the example below
we created a list of rows then passed to the GeneratedDirectRows
DoFn to set the Cells and then we call the BigTableWriteFn to insert
those generated rows in the table.

  main_table = (p
				| beam.Create(self._generate())
				| WriteToBigTable(project_id,
								  instance_id,
								  table_id))
"""
from __future__ import absolute_import

# import copy
import math

import apache_beam as beam
# from apache_beam import pvalue
from apache_beam.io.iobase import BoundedSource
from apache_beam.io.iobase import SourceBundle
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.metrics import Metrics
from apache_beam.transforms.display import DisplayDataItem
# from apache_beam.transforms import ptransform
# from apache_beam.transforms import core
# from apache_beam.portability import common_urns
# from apache_beam.transforms import window
# from apache_beam.portability.api import beam_runner_api_pb2
# from apache_beam.io.iobase import BoundedSource

try:
	from google.cloud._helpers import _microseconds_from_datetime
	from google.cloud._helpers import UTC
	from google.cloud.bigtable.batcher import FLUSH_COUNT, MAX_ROW_BYTES
	from google.cloud.bigtable import Client
	from google.cloud.bigtable import column_family
	from google.cloud.bigtable import enums
	from google.cloud.bigtable.row_set import RowSet
	from google.cloud.bigtable.row_set import RowRange
	from grpc import StatusCode
	from google.api_core.retry import if_exception_type
	from google.api_core.retry import Retry
	from google.cloud.bigtable.instance import Instance
	from google.cloud.bigtable.batcher import MutationsBatcher
	from google.cloud.bigtable.table import Table
except ImportError:
	Client = None
	RowSet = None
	RowRange = None
	FLUSH_COUNT = 1000
	MAX_ROW_BYTES = 0
	MAX_MUTATIONS = 100000

__all__ = ['WriteToBigTable', 'ReadFromBigTable', 'BigTableSource']


# class _OverlapRowSet(RowSet):
#     def __init__(self):
#         super(self.__class__, self).__init__()
#         self.row_keys = []
#         self.row_ranges = []
#
#     def _defragment(self):
#         length = len(self.row_ranges)
#         if length < 2:
#             return
#         for i in range(1, length):
#             if self.row_ranges[i-1].start_key > self.row_ranges[i].start_key:
#                 # If the list is not sorted, sort it first
#                 self.row_ranges.sort(key=lambda tup: tup.start_key)
#                 self._defragment()
#                 return
#             elif self.row_ranges[i-1].end_key >= self.row_ranges[i].start_key:
#                 self.row_ranges[i].start_key = self.row_ranges[i-1].start_key
#                 del self.row_ranges[i-1]
#                 self._defragment()
#                 return
#
#     def add_row_range(self, row_range):
#         # overlaped = True
#         #
#         # def overlap(start1, end1, start2, end2):
#         #     overlaps = start1 <= end2 and end1 >= start2
#         #     if not overlaps:
#         #         return False, None, None
#         #     return True, min(start1, start2), max(end1, end2)
#         # for (i, ranges) in enumerate(self.row_ranges):
#         #     over = overlap(row_range.start_key, row_range.end_key, ranges.start_key, ranges.end_key)
#         #     if over[0]:
#         #         self.row_ranges[i] = RowRange(over[1], over[2])
#         #         overlaped = False
#         #         break
#         # if overlaped:
#         #     self.row_ranges.append(row_range)
#
#         # for i, rng in enumerate(self.row_ranges):
#         #     if row_range.start_key <= rng.end_key and row_range.end_key >= rng.start_key:
#         #         self.row_ranges[i] = RowRange(min(row_range.start_key, rng.start_key),
#         #                                       max(row_range.end_key, rng.end_key))
#         #         return
#         self.row_ranges.append(row_range)
#         # self.row_ranges.sort(key=lambda tup: tup.start_key)
#         self._defragment()
#
#     def add_row_range_from_keys(self, start_key=None, end_key=None, start_inclusive=True, end_inclusive=False):
#         # row_range = RowRange(start_key, end_key, start_inclusive, end_inclusive)
#         # self.add_row_range(row_range)
#         self.add_row_range(RowRange(start_key, end_key, start_inclusive, end_inclusive))


class _BigTableSource(BoundedSource):
	def __init__(self, project_id, instance_id, table_id, row_set=None, filter_=None):
		""" Constructor of the Read connector of Bigtable

		Args:
			project_id(str): GCP Project ID
			instance_id(str): GCP Instance ID
			table_id(str): GCP Table ID to write the `DirectRows`
			row_set(RowSet): Represents the RowRanges needed for use; used to set the split only in that range.
			filter_(RowFilter): Filter to apply to cells in a row.
		"""
		super(_BigTableSource, self).__init__()
		self.beam_options = {'project_id': project_id,
							 'instance_id': instance_id,
							 'table_id': table_id,
							 'row_set': row_set,
							 'filter_': filter_}
		self.sample_row_keys = None
		self.table = None
		self.read_row = Metrics.counter(self.__class__.__name__, 'read_row')
		self.row_set = self._row_set(self.beam_options['row_set'])

	def __getstate__(self):
		return self.beam_options

	def __setstate__(self, options):
		self.beam_options = options
		self.sample_row_keys = None
		self.table = None
		self.read_row = Metrics.counter(self.__class__.__name__, 'read_row')
		self.row_set = self._row_set(self.beam_options['row_set'])

	def _get_table(self):
		if self.table is None:
			self.table = Client(project=self.beam_options['project_id'])\
				.instance(self.beam_options['instance_id'])\
				.table(self.beam_options['table_id'])
		return self.table

	def _defragment(self, ranges):
		""" [an auxiliary method] Sorts, if necessary, and defragments the list of row ranges

		:param ranges: [list] A list of row ranges
		:return: [void]
		"""
		length = len(ranges)
		if length < 2:
			return
		for i in range(1, length):
			if ranges[i-1].start_key > ranges[i].start_key:
				# If the list is not sorted, sort it first
				ranges.sort(key=lambda tup: tup.start_key)
				self._defragment(ranges)
				return
			elif ranges[i-1].end_key >= ranges[i].start_key:
				ranges[i].start_key = ranges[i-1].start_key
				del ranges[i-1]
				self._defragment(ranges)
				return

	def _row_set(self, row_set):
		""" [an auxiliary method] Creates a local deep copy of of the input RowSet reference

		:param row_set: [RowSet()] Reference to a RowSet() class
		:return: New RowSet() with non-overlapping row ranges
		"""
		if row_set is None:
			return None
		new_set = RowSet()
		# for sets in row_set.row_keys:
		#     new_set.add_row_key(sets)
		for sets in row_set.row_ranges:
			new_set.add_row_range(sets)
		self._defragment(new_set.row_ranges)

		keys = list(dict.fromkeys(row_set.row_keys)) # This removes duplicate keys, if any
		for key in keys:
			found = False
			for row_range in new_set.row_ranges:
				if row_range.start_key <= key <= row_range.end.key:  # Check whether the key is contained in the ranges
					found = True
			if not found:
				new_set.add_row_key(key)

		return new_set

	def estimate_size(self):
		# size = [k.offset_bytes for k in self.get_sample_row_keys()][-1]
		# return size
		return list(self.get_sample_row_keys())[-1].offset_bytes

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

	def get_range_tracker(self, start_position, stop_position):
		if stop_position == b'':
			return LexicographicKeyRangeTracker(start_position)
		else:
			return LexicographicKeyRangeTracker(start_position, stop_position)

	def _split(self, desired_bundle_size, start_position=b'', stop_position=b''):
		""" Splits the source into a set of bundles.

		Bundles should be approximately of size ``desired_bundle_size`` bytes.

		:param desired_bundle_size: [int] the desired size (in bytes) of the bundles returned.
		:param start_position: if specified, must be used as the starting position of the first bundle.
		:param stop_position: if specified, must be used as the ending position of the last bundle.
		:return: an iterator of objects of type 'SourceBundle' that gives information about the generated bundles.
		"""
		if self.beam_options['row_set'] is None:
			for bundle in self._split_bulk(desired_bundle_size, start_position, stop_position):
				yield bundle
		else:
			for bundle in self._split_itemized(desired_bundle_size, start_position, stop_position):
				yield bundle

	def _split_bulk(self, desired_bundle_size, start_position, stop_position):
		if start_position == b'' and stop_position == b'':  # special case 1
			table_size = self.estimate_size()
			if table_size <= desired_bundle_size:  # special case 1.1
				yield SourceBundle(long(table_size), self, 0, table_size)
			else:
				bundle_count = table_size // desired_bundle_size + 1
				bundle_size = table_size // bundle_count + 1
				for i in range(bundle_count):
					pos_start = i * bundle_size
					pos_stop = min(table_size, pos_start + bundle_size)
					yield SourceBundle(long(pos_stop - pos_start), self, pos_start, pos_stop)

	def _split_itemized(self, desired_bundle_size, start_position, stop_position):
		yield None

	def split(self, desired_bundle_size, start_position=b'', stop_position=b''):
		""" Splits the source into a set of bundles, using the row_set if it is set.

		Bundles should be approximately of ``desired_bundle_size`` bytes, if this
		bundle its bigger, it use the ``range_split_fraction`` to split the bundles
		in fractions.
		:param desired_bundle_size: the desired size (in bytes) of the bundles returned.
		:param start_position: if specified, the position must be used as the starting position of the first bundle.
		:param stop_position: if specified, the position must be used as the ending position of the last bundle.
		Returns:
		  an iterator of objects of type 'SourceBundle' that gives information
		  about the generated bundles.
		"""

		if start_position == b'' and stop_position == b'':
			if self.beam_options['row_set'] is not None:
				for row_range in self.row_set.row_ranges:
					for row_split in self.split_range_size(desired_bundle_size, self.get_sample_row_keys(), row_range):
						yield row_split
			else:
				addition_size = 0
				last_offset = 0
				start_key = b''

				for sample_row_key in self.get_sample_row_keys():
					addition_size += sample_row_key.offset_bytes - last_offset
					if addition_size >= desired_bundle_size:
						# for fraction in self.range_split_fraction(addition_size, desired_bundle_size, start_key, sample_row_key.row_key):
						for fraction in self.split_range_subranges(addition_size, desired_bundle_size, self.get_range_tracker(start_key, sample_row_key.row_key)):
							yield fraction
						start_key = sample_row_key.row_key
						addition_size = 0
					last_offset = sample_row_key.offset_bytes
		elif start_position is not None or stop_position is not None:
			for row_split in self.split_range_size(desired_bundle_size, self.get_sample_row_keys(), RowRange(start_position, stop_position)):
				yield row_split

	def split_range_size(self, desired_size, sample_row_keys, range_):
		""" This method split the row_set ranges using the desired_bundle_size you get.

		:param desired_size: [int]  The size you need to split the ranges.
		:param sample_row_keys: [list] A list of row keys with a end size.
		:param range_: A RowRange Element, to split if necessary.
		"""
		start = None
		last_offset = 0
		for sample_row in sample_row_keys:
			# Skip first and last sample_row in the sample_row_keys parameter
			if sample_row.row_key != b'' and range_.start_key <= sample_row.row_key <= range_.end_key:
				if start is not None:
					for fraction in self.split_range_subranges(sample_row.offset_bytes - last_offset,
															   desired_size,
															   LexicographicKeyRangeTracker(start, sample_row.row_key)):
						yield fraction
				start = sample_row.row_key
			last_offset = sample_row.offset_bytes

	# def range_split_fraction(self, current_size, desired_bundle_size, start_key, end_key):
	#     """ This method is used to send a range[start_key, end_key) to the ``split_range_subranges`` method.
	#
	#     :param current_size: the size of the range.
	#     :param desired_bundle_size: the size you want to split.
	#     :param start_key: [byte] The start key row in the range.
	#     :param end_key: [byte] The end key row in the range.
	#     """
	#     return self.split_range_subranges(current_size, desired_bundle_size, self.get_range_tracker(start_key, end_key))

	def split_range_subranges(self, sample_size_bytes, desired_bundle_size, range_tracker):
		""" This method split the range you get using the 'desired_bundle_size' as a limit size,

		It compares the size of the range and the ``desired_bundle size`` if it is necessary
		to split a range, it uses the ``fraction_to_position`` method.
		:param sample_size_bytes: The size of the Range.
		:param desired_bundle_size: The desired size to split the Range.
		:param ranges: the Range to split.
		"""
		split_ = math.floor(float(desired_bundle_size) / float(sample_size_bytes) * 100) / 100  # Why 2 decimal points?
		pos_start = range_tracker.start_position()
		pos_stop = range_tracker.stop_position()
		if split_ == 1 or pos_start == b'' or pos_stop == b'':
			yield SourceBundle(sample_size_bytes, self, pos_start, pos_stop)
		else:
			bundle_size = int(sample_size_bytes * split_)
			offset = bundle_size
			start_key = pos_start
			end_key = pos_stop
			while offset < sample_size_bytes:
				end_key = self.fraction_to_position(float(offset)/float(sample_size_bytes), pos_start, pos_stop)
				yield SourceBundle(long(bundle_size), self, start_key, end_key)
				start_key = end_key
				offset += bundle_size
			yield SourceBundle(long(sample_size_bytes - offset + bundle_size), self, end_key, pos_stop)

	def read(self, range_tracker):
		read_rows = self._get_table().read_rows(start_key=range_tracker.start_position(),
												end_key=range_tracker.stop_position(),
												filter_=self.beam_options['filter_'])
		for row in read_rows:
			if range_tracker.try_claim(row.row_key):
				self.read_row.inc()
				yield row
			else:
				break

	def fraction_to_position(self, position, range_start, range_stop):
		""" We use the ``fraction_to_position`` method in ``LexicographicKeyRangeTracker`` class to split a
		range into two chunks.

		:param position:
		:param range_start:
		:param range_stop:
		:return:
		"""
		return LexicographicKeyRangeTracker.fraction_to_position(position, range_start, range_stop)

	def display_data(self):
		return {'projectId': DisplayDataItem(self.beam_options['project_id'],
											 label='Bigtable Project Id',
											 key='projectId'),
				'instanceId': DisplayDataItem(self.beam_options['instance_id'],
											  label='Bigtable Instance Id',
											  key='instanceId'),
				'tableId': DisplayDataItem(self.beam_options['table_id'],
										   label='Bigtable Table Id',
										   key='tableId')}

	def to_runner_api_parameter(self, unused_context):
		pass


class BigTableSource(_BigTableSource):
	def __init__(self, project_id, instance_id, table_id, row_set=None, filter_=None):
		""" Constructor of the Read connector of Bigtable

		Args:
		  project_id(str): GCP Project of to write the Rows
		  instance_id(str): GCP Instance to write the Rows
		  table_id(str): GCP Table to write the `DirectRows`
		  row_set(RowSet): This variable represents the RowRanges
		  you want to use, It used on the split, to set the split
		  only in that ranges.
		  filter_(RowFilter): Get some expected rows, bases on
		  certainly information in the row.
		"""
		super(BigTableSource, self).__init__(project_id, instance_id, table_id, row_set=row_set, filter_=filter_)


class ReadFromBigTable(beam.PTransform):
	def __init__(self, project_id, instance_id, table_id):
		super(ReadFromBigTable, self).__init__()
		self.beam_options = {'project_id': project_id,
							 'instance_id': instance_id,
							 'table_id': table_id}

	def expand(self, pvalue):
		project_id = self.beam_options['project_id']
		instance_id = self.beam_options['instance_id']
		table_id = self.beam_options['table_id']
		return (pvalue
				| 'ReadFromBigtable' >> beam.io.Read(BigTableSource(project_id, instance_id, table_id)))


class _BigTableWriteFn(beam.DoFn):
	""" Creates the connector can call and add_row to the batcher using each
	row in beam pipe line
	Args:
		project_id(str): GCP Project ID
		instance_id(str): GCP Instance ID
		table_id(str): GCP Table ID
	"""

	def __init__(self, project_id, instance_id, table_id, flush_count=FLUSH_COUNT, max_row_bytes=MAX_ROW_BYTES):
		""" Constructor of the Write connector of Bigtable
		Args:
		  project_id(str): GCP Project of to write the Rows
		  instance_id(str): GCP Instance to write the Rows
		  table_id(str): GCP Table to write the `DirectRows`
		"""
		super(_BigTableWriteFn, self).__init__()
		self.beam_options = {'project_id': project_id,
							 'instance_id': instance_id,
							 'table_id': table_id,
							 'flush_count': flush_count,
							 'max_row_bytes': max_row_bytes}
		self.table = None
		self.batcher = None
		self.written = Metrics.counter(self.__class__, 'Written Row')

	def __getstate__(self):
		return self.beam_options

	def __setstate__(self, options):
		self.beam_options = options
		self.table = None
		self.batcher = None
		self.written = Metrics.counter(self.__class__, 'Written Row')

	def start_bundle(self):
		if self.table is None:
			client = Client(project=self.beam_options['project_id'])
			instance = client.instance(self.beam_options['instance_id'])
			self.table = instance.table(self.beam_options['table_id'])
		flush_count = self.beam_options['flush_count']
		max_row_bytes = self.beam_options['max_row_bytes']
		self.batcher = self.table.mutations_batcher(flush_count, max_row_bytes)

	def process(self, element, *args, **kwargs):
		self.written.inc()
		# You need to set the timestamp in the cells in this row object,
		# when we do a retry we will mutating the same object, but, with this
		# we are going to set our cell with new values.
		# Example:
		# direct_row.set_cell('cf1',
		#                     'field1',
		#                     'value1',
		#                     timestamp=datetime.datetime.now())
		self.batcher.mutate(element)

	def finish_bundle(self):
		self.batcher.flush()
		self.batcher = None

	def display_data(self):
		return {'projectId': DisplayDataItem(self.beam_options['project_id'],
											 label='Bigtable Project Id'),
				'instanceId': DisplayDataItem(self.beam_options['instance_id'],
											  label='Bigtable Instance Id'),
				'tableId': DisplayDataItem(self.beam_options['table_id'],
										   label='Bigtable Table Id')}

	def to_runner_api_parameter(self, unused_context):
		pass


class WriteToBigTable(beam.PTransform):
	""" A transform to write to the Bigtable Table.
	A PTransform that write a list of `DirectRow` into the Bigtable Table
	"""
	def __init__(self, project_id=None, instance_id=None, table_id=None,
				 flush_count=FLUSH_COUNT, max_row_bytes=MAX_ROW_BYTES):
		""" The PTransform to access the Bigtable Write connector
		Args:
		  project_id(str): GCP Project of to write the Rows
		  instance_id(str): GCP Instance to write the Rows
		  table_id(str): GCP Table to write the `DirectRows`
		"""
		super(WriteToBigTable, self).__init__()
		self.beam_options = {'project_id': project_id,
							 'instance_id': instance_id,
							 'table_id': table_id,
							 'flush_count': flush_count,
							 'max_row_bytes': max_row_bytes}

	def expand(self, pvalue):
		beam_options = self.beam_options
		return (pvalue
				| beam.ParDo(_BigTableWriteFn(beam_options['project_id'],
											  beam_options['instance_id'],
											  beam_options['table_id'],
											  beam_options['flush_count'],
											  beam_options['max_row_bytes'])))
