from collections import defaultdict, Counter
from datatable_util import AttributeDict, CSV_GivenHeaders, FIXEDWIDTH, JoinType, sortKey
from hierarchies import Hierarchy
import os
from datatable import DataTable, DataColumn
from itertools import chain
from functools import reduce

def createColumnFilter(criteria):
	if criteria is None:
		return lambda value: value is None
	if '__call__' in dir(criteria):
		return lambda value: criteria(value)
	if isinstance(criteria, DataColumnStream):
		otherValues = set(criteria)
		return lambda value: value in otherValues
	if '__contains__' in dir(criteria) and not isinstance(criteria, str):
		return lambda value: value in criteria
	return lambda value: value == criteria

class KeyParamedDefaultDict(dict):
	def __init__(self, defaultMethod, *args, **kwargs):
		super(KeyParamedDefaultDict, self).__init__(*args, **kwargs)
		self.defaultMethod = defaultMethod
	def __getitem__(self, key):
		if key not in self:
			self[key] = self.defaultMethod(key)
		return super(KeyParamedDefaultDict, self).__getitem__(key)

class DataColumnStream(object):
	def __init__(self, dataTableStream, header):
		self.__dataTableStream = dataTableStream
		if isinstance(header, (DataColumnStream, DataColumn)):
			self.header = header.header
		else:
			self.header = header
	def __iter__(self):
		for row in self.__dataTable:
			yield row[self.header]
	def __getitem__(self, index):
		'''Gets the index'th row of data'''
		if '__iter__' in dir(index) or isinstance(index, slice):
			return self.__dataTableStream[index].column(self.header)
		for i, v in enumerate(self):
			if i == index:
				return v
		return None
	def toList(self):
		return list(self)
	def first(self):
		for value in self:
			return value
		return None
	def reduce(self, reductionMethod, startingValue=None):
		if startingValue is None:
			return reduce(reductionMethod, self, startingValue)
		return reduce(reductionMethod, self)
	def last(self):
		return self.reduce(lambda a, b: b)
	def max(self):
		return max(self)
	def min(self):
		return min(self)
	def filter(self, value):
		'''
	Filter the table by matching this column with the given value
Value may be one of the following:
	None - returns rows where this column is None
	DataColumn (same table) - returns rows where the two columns are equal
	DataColumn (other table) - returns rows where this column value is in the other column
	method - returns rows where method returns true for column value
	collection - returns rows where column value is in the collection
	value - returns rows where column value equals the given value
'''
		criteria = createColumnFilter(value)
		return DataTableStream((row for row in self.__dataTableStream if criteria(row[self.header])), self.__dataTableStream.headers())
	def set(self, value):
		'''
	sets the items in this column to the given value
	If value is a function then sets each item to the result of calling value on the item
	returns the modified datatable
'''
		if hasattr(value, '__call__'):
			transform = lambda row: row + {self.header: value(row[self.header])}
		else:
			transform = lambda row: row + {self.header: value}
		return DataTableStream((transform(row) for row in self.__dataTableStream), self.__dataTableStream.headers())
	def sizeOfGroups(self):
		return Counter(self)
	def __repr__(self):
		return "DataColumnStream(<dataTable>, '%s')" % self.header
	def __str__(self):
		return ','.join(map(str, self))

class DataTableStream(object):
	def __init__(self, rows, headers):
		self.__rows = rows
		self.__headers = headers
		self.__columns = KeyParamedDefaultDict(lambda header: DataColumnStream(self, header))
	def __iter__(self):
		'''Gets an iterator over the data rows'''
		return iter(self.__rows)
	def __getitem__(self, index):
		'''Gets the index'th row of data'''
		if '__iter__' in dir(index):
			criteria = lambda i: i in set(index)
		elif isinstance(index, slice):
			criteria = lambda i: index.start <= i <= index.stop and (i - index.start) % index.step == 0
		else:
			for i, row in enumerate(self):
				if i == index:
					return row
			return None
		return DataTableStream((row for i, row in enumerate(self) if criteria(i)), self.__headers)
	def column(self, header):
		'''Gets the column named 'header' (same as dataTable.<header>)'''
		return self.__columns[header]
	def __getattr__(self, header):
		return self.column(header)
	def columns(self):
		'''Returns the DataColumn objects associated with this DataTable'''
		return [self.column(header) for header in self.__headers]
	def headers(self):
		'''Returns this table's header strings'''
		return self.__headers
	def filter(self, filterFunction):
		'''Returns a DataTable containing the lines in self filtered by the given filterFunciton
	Accepts either a dictionary of header -> value which does exact matching on the pairs,
	or a filter function which takes a dict as input and returns if that row should be included'''
		if isinstance(filterFunction, dict):
			filters = {k: createColumnFilter(v) for k, v in filterFunction.items()}
			criteria = lambda row: all(v(row[k]) for k, v in filters)
		else:
			criteria = filterFunction
		return DataTableStream((row for row in self if criteria(row)), self.__headers)
	def transform(self, transformFunction, newHeaders=None):
		return DataTableStream((transformFunction(row) for row in self), newHeaders or set())
	def index(self, keyHeaders, leafHeaders=None):
		if leafHeaders is None:
			leafHeaders = set(self.headers()).difference(keyHeaders)
		return Hierarchy.fromTable(self, keyHeaders, leafHeaders)
	def __str__(self):
		return self | FIXEDWIDTH
	def __repr__(self):
		return 'DataTableStream(<dataTable>)\nHeaders:\n%s' % self.headers()
	def augment(self, other):
		'''append the rows in other to the rows in this
	if the headers don't match between the two instances then it adds blank columns to each with the headers from the other'''
		if isinstance(other, DataTableStream, DataTable):
			headers = set(self.__headers).union(other.headers())
		else:
			headers = self.__headers
		if isinstance(other, dict):
			stream = chain(self, [other])
		else:
			stream = chain(self, other)
		return DataTableStream(({header: row.get(header, None) for header in headers} for row in stream), headers)
	def append(self, other):
		'''append the rows in the other to the rows in this
	requires that the headers match (or that one of self or other be empty)'''
		if isinstance(other, dict):
			stream = chain(self, [other])
		else:
			stream = chain(self, other)
		return DataTableStream(stream, self.__headers)
	def remove(self, other):
		'''remove the rows from other that are in self - uses exact match of rows'''
		if isinstance(other, dict):
			criteria = lambda row: row != other
		else:
			criteria = lambda row: row not in other
		return self.filter(criteria)
	def extend(self, other):
		'''Add columns to the data using the dictionary keys from other as the new headers and their values as fields on each row
Overwrites existing columns'''
		if hasattr(other, '__call__'):
			transform = lambda row: row + other(row)
		else:
			def transform(row):
				def it():
					for header, value in other.items():
						if hasattr(value, '__call__'):
							yield header, value(row)
						else:
							yield header, value
				return row + dict(it())
		return self.transform(transform)
	def __or__(self, other):
		'''Pipes the data into other
	Calls other with an iterator for the rows in self'''
		return other(iter(self))
	def exclude(self, other): #not compatible with existing functions
		'''remove column(s) from the data table
	other may be either a header or list of headers,
		or a predicate which takes a header and the set of data in that column'''
		if '__call__' in dir(other):
			data = list(self)
			length = len(data)
			data = {header: [row[header] for row in data] for header in self.__headers}
			data = {header: values for header, values in data.items() if not other(header, values)}
			return DataTableStream(({header: values[i] for header, values in data.items()} for i in range(length)), data.keys())
		if other in self.__headers:
			other = {other}
		else:
			other = set(other)
		transform = lambda row: {header: value for header, value in row.items() if header not in other}
		return self.transform(transform, {header for header in self.__headers if header not in other})
	def project(self, other): #not compatible with existing functions
		'''filter columns in the data table
	other may be either a header or list of headers,
		or a predicate which takes a header and the set of data in that column'''
		if '__call__' in dir(other):
			data = list(self)
			length = len(data)
			data = {header: [row[header] for row in data] for header in self.__headers}
			data = {header: values for header, values in data.items() if other(header, values)}
			return DataTableStream(({header: values[i] for header, values in data.items()} for i in range(length)), data.keys())
		if other in self.__headers:
			other = {other}
		else:
			other = set(other)
		transform = lambda row: {header: value for header, value in row.items() if header in other}
		return self.transform(transform, {header for header in self.__headers if header in other})
	def removeBlankColumns(self):
		'''returns a copy of this DataTable with all of the blank columns removed'''
		return self.project(lambda header, values: any(values))
	def sorted(self, *fields):
		def key(row):
			return tuple(sortKey(row.get(field, None)) for field in fields)
		return DataTable(sorted(self, key=key))
	def iterBucket(self, *fields):
		copy = self.sorted(*fields)
		currentKey = None
		currentBucket = []
		for data in copy:
			key = tuple(data[field] for field in fields)
			if currentKey is not None and key != currentKey:
				yield currentKey, DataTable(currentBucket)
				currentBucket = []
			currentKey = key
			currentBucket.append(data)
		yield currentKey, DataTable(currentBucket)
	def sizeOfBuckets(self, *fields):
		'''Returns a dict of bucket -> number of items in the bucket'''
		return Counter(tuple(row[field] for field in fields) for row in self)
	def bucket(self, *fields):
		'''Returns a dict of bucket -> DataTable of rows matching that bucket'''
		buckets = defaultdict(lambda:[])
		for data in self:
			key = tuple(data[field] for field in fields)
			buckets[key].append(data)
		return AttributeDict((key, DataTable(bucket)) for key, bucket in buckets.items())
	def filterBucket(self, predicate, *fields):
		'''Filter the datatable using an aggregate predicate
fields specifies how the data will be grouped
predicate is a method which takes a bucket of data and returns if the bucket should be included in the result
'''
		return DataTableStream(row for key, bucket in self.iterBucket(*fields) if predicate(bucket) for row in bucket)
	def join(self, other, joinParams=None, otherFieldPrefix='', joinType=JoinType.LEFT_OUTER_JOIN):
		'''
dataTable.join(otherTable, joinParams, otherFieldPrefix='')
	returns a new table with rows in the first table joined with rows in the second table, using joinParams to map fields in the first to fields in the second
Parameters:
	other - the table to join
	joinParams - a dictionary of <field in self> to <field in other>. Defaults to "natural join", merging common headers
	otherFieldPrefix - a string to prepend to the fields added from the second table
	joinType - the instance of JoinType which indicates if items should be included in one data table which aren't in the other
		'''
		if joinParams is None:
			joinParams = {h: h for h in self.headers() if h in other.headers()}
		elif not isinstance(joinParams, dict):
			raise Exception("joinParams must be a dictionary of <field in self> to <field in other>")
		selfJoinHeaders = list(joinParams.values())
		otherJoinHeaders = [joinParams[h] for h in selfJoinHeaders]

		newOtherHeaders = {(v if v in joinParams.values() else otherFieldPrefix + v) for v in otherJoinHeaders}
		otherBuckets = other.extend(
			lambda row: {otherFieldPrefix + v: row[v] for v in other.headers() if v not in otherJoinHeaders}
		).project(newOtherHeaders
		).bucket(*otherJoinHeaders)
		emptyOtherRow = AttributeDict({otherFieldPrefix + v: None for v in other.headers() if v not in otherJoinHeaders})
		emptySelfRow = AttributeDict({header: None for header in self.headers() if header not in selfJoinHeaders})
		otherKeysSeen = set()
		def it():
			for row in self:
				rowKey = tuple(row[selfHeader] for selfHeader, otherHeader in joinParams)
				otherKeysSeen.add(rowKey)
				if rowKey in otherBuckets:
					for otherRow in otherBuckets[rowKey]:
						yield row + otherRow
				elif joinType.leftOuter:
					yield emptyOtherRow + row
			if joinType.rightOuter:
				for otherKey, otherBucket in otherBuckets.items():
					if otherKey not in otherKeysSeen:
						for row in otherBucket:
							yield emptySelfRow + row

		return DataTableStream(it(), set(self.headers()).union(newOtherHeaders))
	def writeTo(self, fileName, *headers):
		'''Write the contents of this DataTable to a file with the given name in the standard csv format'''
		if not headers:
			headers = self.headers()
		with open(os.path.expanduser(fileName), 'w') as f:
			f.write(self | CSV_GivenHeaders(*headers))
	def duplicates(self, *fields):
		'''given a list of fields as keys, return a DataTable instance with the rows for which those fields are not unique'''
		matches = {}
		def it():
			for row in self:
				key = tuple(row[field] for field in fields)
				if key in matches:
					if matches[key]:
						yield matches[key]
						yield row
						matches[key] = None
					else:
						yield row
				else:
					matches[key] = row
		return DataTableStream(it(), self.headers())
	def _distinct(self):
		rows = set()
		headers = self.headers()
		for row in self:
			items = tuple(row[h] for h in headers)
			if items not in rows:
				yield row
				rows.add(items)
	def distinct(self):
		'''return a new DataTable with only unique rows'''
		return DataTableStream(self._distinct(), self.headers())
	def fillDownBlanks(self, *fields):
		'''fills in the blanks in the current table such that each blank field in a row is filled in with the first non-blank entry in the column before it'''
		if not fields:
			fields = self.headers()
		populatedRow = {field: None for field in fields}
		def it():
			for row in self:
				copy = dict(row)
				for field in fields:
					if row[field]:
						populatedRow[field] = row[field]
					else:
						copy[field] = populatedRow[field]
				yield copy
		return DataTableStream(it(), self.headers())
	def pivot(self, rowID=None):
		'''Returns a new DataTable with the rows and columns swapped
In the resulting table, the headers from the previous table will be in the 'Field' column,
	then each row will be in the column Row0, Row1, ... RowN
	optional rowID is either a column to be used as the row identifier (fields in that column become new column headers),
or a method which takes a table (this table) and the row index and returns the column header corresponding with that row
		'''
		origData = list(self)
		if rowID is None:
			digits = len(str(len(origData)))
			fmt = 'Row%0' + str(digits) + 'd'
			rowIDs = [fmt % i for i in range(len(origData))]
		elif rowID in self.__headers:
			rowIDs = [row[rowID] for row in origData]
		else:
			rowIDs = [rowID(row, i) for i, row in enumerate(origData)]
		def tempIterRows():
			for header in sorted(self.__headers, key=sortKey):
				row = {rowId: row[header] for rowId, row in zip(rowIDs, origData)}
				row['Field'] = header
				yield row
		return DataTableStream(tempIterRows(), rowIDs)
	def aggregate(self, groupBy, aggregations={}):
		'''return an aggregation of the data grouped by a given set of fields.
	Must processe the whole stream before it will start streaming resulting rows
Parameters:
	groupBy - the set of fields to group
	aggregations - a dict of field name -> aggregate method, where the method takes an intermediate DataTable
		and returns the value for that field for that row.
		'''
		if not aggregations:
			return self.project(groupBy).distinct()
		def tempIterRows():
			accumulatedRows = {}
			for row in self:
				key = tuple(row[field] for field in groupBy)
				if key not in accumulatedRows:
					accumulatedRows[key] = {a: agg.newBucket(row) for a, agg in aggregations.items()}
				accRow = accumulatedRows[key]
				for a, agg in aggregations.items():
					accRow[a] = agg.addRow(row, accRow[a])
			for key, accRow in sorted(accumulatedRows.items()):
				yield AttributeDict(zip(groupBy, key)) + {a: agg.finalize(accRow[a]) for a, agg in aggregations.items()}
		return DataTableStream(tempIterRows(), set(groupBy).union(aggregations.keys()))
	def renameColumn(self, column, newName):
		'''rename the column in place'''
		swap = lambda h: h if h != column else newName
		transform = lambda row: AttributeDict((swap(k), v) for k, v in row.items())
		return self.transform(transform, {swap(header) for header in self.__headers})
	def reduce(self, reduction, startingValue = None):
		if startingValue is None:
			return reduce(reduction, self)
		return reduce(reduction, self, startingValue)
	def minRow(self, *fields):
		'''return the row with the minimum value(s) in the given field(s)'''
		getValue = lambda row: tuple(row[field] for field in fields)
		return min(self, key=getValue)
	def maxRow(self, *fields):
		'''return the row with the maximum value(s) in the given field(s)'''
		getValue = lambda row: tuple(row[field] for field in fields)
		return min(self, key=getValue)
	def toTable(self):
		return DataTable(self)

DataTable.stream = lambda self: DataTableStream(self, self.headers())
