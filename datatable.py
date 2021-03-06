from collections import defaultdict
from datatable_util import AttributeDict, DataTableException, CSV_GivenHeaders, FIXEDWIDTH, JoinType, sortKey
from hierarchies import Hierarchy
from functools import total_ordering
import os

@total_ordering
class DataColumn(object):
	def __init__(self, dataTable, header):
		self.__dataTable = dataTable
		if isinstance(header, DataColumn):
			self.header = header.header
		else:
			self.header = header
	def __eq__(self, other):
		if not isinstance(other, DataColumn):
			return False
		return self.__dataTable == other.__dataTable and self.header == other.header
	def __lt__(self, other):
		if not isinstance(other, DataColumn) or self.__dataTable is not other.__dataTable:
			raise NotImplemented()
		return sortKey(self.header) < sortKey(other.header)
	def __iter__(self):
		for row in self.__dataTable:
			yield row[self.header]
	def __getitem__(self, index):
		'''Gets the index'th row of data'''
		return self.__dataTable[index][self.header]
	def __contains__(self, value):
		return value in iter(self)
	def __filter(self, value):
		if value is None:
			for row in self.__dataTable:
				if row[self.header] is None:
					yield row
		elif isinstance(value, DataColumn):
			if value.__dataTable == self.__dataTable:
				for row in self.__dataTable:
					if row[self.header] == row[value.header]:
						yield row
			else:
				otherValues = set(value)
				for row in self.__dataTable:
					if row[self.header] in otherValues:
						yield row
		elif '__call__' in dir(value):
			for row in self.__dataTable:
				if value(row[self.header]):
					yield row
		elif '__contains__' in dir(value) and not isinstance(value, str):
			for row in self.__dataTable:
				if row[self.header] in value:
					yield row
		else:
			for row in self.__dataTable:
				if row[self.header] == value:
					yield row
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
		return DataTable(self.__filter(value))
	def set(self, value):
		'''
	sets the items in this column to the given value
	If value is a function then sets each item to the result of calling value on the item
	returns the modified datatable
'''
		if hasattr(value, '__call__'):
			for row in self.__dataTable:
				row[self.header] = value(row[self.header])
		else:
			for row in self.__dataTable:
				row[self.header] = value
		return self.__dataTable
	def sort(self):
		self.__dataTable.sort(self.header)
	def sizeOfGroups(self):
		groups = defaultdict(lambda:0)
		for v in self:
			groups[v] += 1
		return dict(groups)
	def fillDownBlanks(self):
		prev = None
		for i in range(len(self)):
			if self[i]:
				prev = self[i]
			else:
				self.__dataTable[i][self.header] = prev
	def __repr__(self):
		return "DataColumn(<dataTable>, '%s')" % self.header
	def __str__(self):
		return ','.join(map(str, self))

class NullColumn(DataColumn):
	def __filter(self, value):
		return iter([])
	def sort(self):
		pass
	def sizeOfGroups(self):
		return {}
	def __iter__(self):
		return iter([])
	def __repr__(self):
		return "NullColumn(<dataTable>, '%s')" % self.header

def _copyAndApplyOp(op):
	def copyOp(self, *args, **kwargs):
		newData = DataTable(self)
		op(newData, *args, **kwargs)
		return newData
	copyOp.__doc__ = op.__doc__
	return copyOp

class DataTable(object):
	@staticmethod
	def collect(tables):
		'''
	Concatenates the tables together into one big data table
	essentially performs:
table = tables.next()
for t in tables:
	table.augment(t)
		'''
		return DataTable(row for table in tables for row in table)
	def __init__(self, data=None, parseMethod=None):
		'''Create a data table from the given data
	data may be one of the following:
A sequence of dictionaries, where all of the dictionaries share common keys
A sequence of sequences where the first item is the list of headers
Another DataTable instance, which will create a deep copy
A string which may be parsed into one of the previous by calling parseMethod on the string.
'''
		if isinstance(data, DataTable):
			self.__headers = {h: DataColumn(self, c) for h, c in data.__headers.items()}
			self.__data = [AttributeDict((h.header, row[h.header]) for h in self.__headers.values()) for row in data]
			return
		if isinstance(data, str):
			data = parseMethod(data)
		if not data:
			self.__data = []
			self.__headers = {}
			return
		data = [row for row in data]
		if not data:
			self.__data = []
			self.__headers = {}
			return
		if isinstance(data[0], dict):
			headers = {k for row in data for k in row.keys()}
			self.__headers = {h: DataColumn(self, h) for h in headers}
			for row in data:
				for header in self.__headers.keys():
					if header not in row:
						row[header] = None
			self.__data = [AttributeDict(row) for row in data]
		else:
			headers = data.pop(0)
			self.__headers = {h: DataColumn(self, h) for h in headers}
			self.__data = [AttributeDict(zip(headers, row)) for row in data]
	def __iter__(self):
		'''Gets an iterator over the data rows'''
		return iter(self.__data)
	def __getitem__(self, index):
		'''Gets the index'th row of data'''
		if '__iter__' in dir(index):
			return DataTable(self[i] for i in index)
		data = self.__data[index]
		if isinstance(data, list):
			return DataTable(data)
		return data
	def column(self, header):
		'''Gets the column named 'header' (same as dataTable.<header>)'''
		if header in self.__headers:
			return self.__headers[header]
		return NullColumn(self, header)
	def __getattr__(self, header):
		return self.column(header)
	def columns(self):
		'''Returns the DataColumn objects associated with this DataTable'''
		return sorted(self.__headers.values())
	def headers(self):
		'''Returns this table's header strings'''
		return sorted(self.__headers.keys(), key=sortKey)
	def filter(self, filterFunction):
		'''Returns a DataTable containing the lines in self filtered by the given filterFunciton
	Accepts either a dictionary of header -> value which does exact matching on the pairs,
	or a filter function which takes a dict as input and returns if that row should be included'''
		if isinstance(filterFunction, dict):
			return DataTable(line for line in self.__data if all(line[k] == v for k, v in filterFunction.items()))
		return DataTable(line for line in self.__data if filterFunction(line))
	def __len__(self):
		'''The number of rows'''
		return len(self.__data)
	def index(self, keyHeaders, leafHeaders=None):
		if leafHeaders is None:
			leafHeaders = set(self.headers()).difference(keyHeaders)
		return Hierarchy.fromTable(self, keyHeaders, leafHeaders)
	def __str__(self):
		return self | FIXEDWIDTH
	def __repr__(self):
		return 'Rows:%d\nHeaders:\n%s' % (len(self), self.headers())
	def __eq__(self, other):
		return isinstance(other, DataTable) and len(self) == len(other) and all(a == b for a, b in zip(self, other))
	def __ne__(self, other):
		return not isinstance(other, DataTable) or len(self) != len(other) or any(a != b for a,b in zip(self, other))
	def augment(self, other):
		'''Join two DataTable instances (concatenate their rows)
	if the headers don't match between the two instances then it adds blank columns to each with the headers from the other'''
		if not other or not len(other):
			return self
		if isinstance(other, list):
			other = DataTable(other)
		if isinstance(other, dict):
			other = DataTable([other])
		if not len(self):
			return other
		selfNewHeaders = {h: None for h in other.headers() if h not in self.headers()}
		otherNewHeaders = {h: None for h in self.headers() if h not in other.headers()}
		return (self & selfNewHeaders) + (other & otherNewHeaders)
	def __iadd__(self, other):
		'''Join two DataTable instances (concatenate their rows)
	requires that the headers match (or that one of self or other be empty)'''
		if other is None:
			return self
		if isinstance(other, DataTable):
			if self.headers() and other.headers() and self.headers() != other.headers():
				raise DataTableException("headers don't match.  Expected: " + str(self.headers()) + "\nFound: " + str(other.headers()))
			self.__data += other.__data
		elif isinstance(other, list):
			if other and self.headers() != sorted(other[0].keys(), key=sortKey):
				raise DataTableException("headers don't match.  Expected: " + str(self.headers()) + "\nFound: " + str(sorted(other[0].keys(), key=sortKey)))
			self.__data += other
		elif isinstance(other, dict):
			if self.headers() and other and self.headers() != sorted(other.keys(), key=sortKey):
				raise DataTableException("headers don't match.  Expected: " + str(self.headers()) + "\nFound: " + str(sorted(other.keys(), key=sortKey)))
			elif other:
				self.__data.append(other)
		else:
			print("other instance unknown: %s" % other.__class__)
			raise NotImplemented()
		return self
	append = __add__ = _copyAndApplyOp(__iadd__)
	def __isub__(self, other):
		'''remove the rows from other that are in self - uses exact match of rows'''
		if isinstance(other, dict):
			if other in self.__data:
				self.__data.remove(other)
		else:
			for row in other:
				if row in self.__data:
					self.__data.remove(row)
		return self
	remove = __sub__ = _copyAndApplyOp(__isub__)
	def __iand__(self, other):
		'''Add columns to the data tabel using the dictionary keys from other as the new headers and their values as fields on each row
Overwrites existing columns'''
		if hasattr(other, '__call__'):
			if not self:
				return self
			for newHeader in other(self[0]).keys():
				self.__headers[newHeader] = DataColumn(self, newHeader)
			for row in self.__data:
				row += other(row)
			return self
		for header, value in other.items():
			if header not in self.__headers:
				self.__headers[header] = DataColumn(self, header)
			if hasattr(value, '__call__'):
				for row in self.__data:
					row[header] = value(row)
			else:
				for row in self.__data:
					row[header] = value
		return self
	extend = __and__ = _copyAndApplyOp(__iand__)
	def __or__(self, other):
		'''Pipes the DataTable into other
	Calls other with an iterator for the rows in self'''
		return other(iter(self))
	def __removeColumn(self, header):
		del self.__headers[header]
		for row in self.__data:
			del row[header]
	def __ixor__(self, other):
		'''remove column(s) from the data tabel'''
		if not self.__data:
			return self
		if '__call__' in dir(other):
			for column in list(self.__headers.values()):
				if other(column):
					self.__removeColumn(column.header)
			return self
		if other in self.__headers:
			other = [other]
		for key in other:
			if key not in self.__headers:
				continue
			self.__removeColumn(key)
		return self
	exclude = __xor__ = _copyAndApplyOp(__ixor__)
	def __itruediv__(self, other):
		'''return new DataTable with only the columns listed in other'''
		if not self.__data:
			return self
		if '__call__' in dir(other):
			for column in list(self.__headers.values()):
				if not other(column):
					self.__removeColumn(column.header)
			return self
		if other in self.__headers:
			other = [other]
		for key in list(self.__headers.keys()):
			if key in other:
				continue
			self.__removeColumn(key)
		return self
	project = __truediv__ = _copyAndApplyOp(__itruediv__)
	def removeBlankColumns(self):
		'''returns a copy of this DataTable with all of the blank columns removed'''
		headers = set(self.headers())
		for row in self:
			nonBlanks = set()
			for header in headers:
				if row[header]:
					nonBlanks.add(header)
			if nonBlanks:
				headers.difference_update(nonBlanks)
				if not headers:
					return
		return self.exclude(headers)
	def sort(self, *fields):
		def key(row):
			return tuple(sortKey(row.get(field, None)) for field in fields)
		self.__data.sort(key=key)
	sorted = _copyAndApplyOp(sort)
	sorted.__doc__ = '''returns a new copy of the data table sorted'''
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
		buckets = defaultdict(lambda:0)
		for data in self.__data:
			key = tuple(data[field] for field in fields)
			buckets[key] += 1
		return buckets
	def bucket(self, *fields):
		'''Returns a dict of bucket -> DataTable of rows matching that bucket'''
		buckets = defaultdict(lambda:[])
		for data in self.__data:
			key = tuple(data[field] for field in fields)
			buckets[key].append(data)
		return AttributeDict((key, DataTable(bucket)) for key, bucket in buckets.items())
	def filterBucket(self, predicate, *fields):
		'''Filter the datatable using an aggregate predicate
fields specifies how the data will be grouped
predicate is a method which takes a bucket of data and returns if the bucket should be included in the result
'''
		return DataTable.collect(bucket for key, bucket in self.iterBucket(*fields) if predicate(bucket))
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

		if not other:
			if not self or not joinType.leftOuter:
				return DataTable()
			return self & {otherFieldPrefix + v: None for v in other.headers() if v not in joinParams.values()}
		if not self:
			if not joinType.rightOuter:
				return DataTable()
			other = DataTable(other)
			for header in other.headers():
				if header not in joinParams.values():
					other.renameColumn(header, otherFieldPrefix + header)
			return other & {header: None for header in self.headers() if header not in joinParams}

		newHeaders = [h for h in other.headers() if h not in joinParams.values()]

		otherBuckets = other.bucket(*joinParams.values())
		def tempJoin():
			seenKeys = set()
			for row in self:
				newRow = dict(row)
				key = tuple(row[field] for field in joinParams.keys())
				seenKeys.add(key)
				if key not in otherBuckets:
					if joinType.leftOuter:
						for header in newHeaders:
							newRow[otherFieldPrefix+header] = None
						yield newRow
					continue
				otherRows = otherBuckets[key]
				for otherRow in otherRows:
					for header in newHeaders:
						newRow[otherFieldPrefix+header] = otherRow[header]
					yield dict(newRow)
			if joinType.rightOuter:
				joinParamReversed = {v: k for k,v in joinParams.items()}
				for otherRow in other:
					key = tuple(otherRow[field] for field in joinParams.values())
					if key not in seenKeys:
						newRow = {joinParamReversed[k] if k in joinParamReversed else otherFieldPrefix+k: v for k, v in otherRow.items()}
						for header in self.headers():
							if header not in joinParams:
								newRow[header] = None
						yield newRow
		return DataTable(tempJoin())
	def writeTo(self, fileName, *headers):
		'''Write the contents of this DataTable to a file with the given name in the standard csv format'''
		if not headers:
			headers = self.headers()
		with open(os.path.expanduser(fileName), 'w') as f:
			f.write(self | CSV_GivenHeaders(*headers))
	def duplicates(self, *fields):
		'''given a list of fields as keys, return a DataTable instance with the rows for which those fields are not unique'''
		matchCount = {}
		for row in self.__data:
			key = tuple(row[field] for field in fields)
			if key not in matchCount:
				matchCount[key] = 0
			else:
				matchCount[key] += 1
		return self.filter(lambda row: matchCount[tuple(row[field] for field in fields)])
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
		return DataTable(self._distinct())
	def fillDownBlanks(self, *fields):
		'''fills in the blanks in the current table such that each blank field in a row is filled in with the first non-blank entry in the column before it'''
		if not fields:
			fields = self.headers()
		for field in fields:
			self.__headers[field].fillDownBlanks()
	def pivot(self, rowID=None):
		'''Returns a new DataTable with the rows and columns swapped
In the resulting table, the headers from the previous table will be in the 'Field' column,
	then each row will be in the column Row0, Row1, ... RowN
	optional rowID is either a column to be used as the row identifier (fields in that column become new column headers),
or a method which takes a table (this table) and the row index and returns the column header corresponding with that row
		'''
		if rowID is None:
			digits = len(str(len(self)))
			fmt = 'Row%0' + str(digits) + 'd'
			getRowID = lambda dataTable, i: fmt % i
		elif isinstance(rowID, str):
			getRowID = lambda dataTable, i: dataTable[i][rowID]
		else:
			getRowID = rowID
		rowIDs = [getRowID(self, i) for i in range(len(self))]
		def tempIterRows():
			for column in self.columns():
				row = AttributeDict(zip(rowIDs, column))
				row['Field'] = column.header
				yield row
		return DataTable(tempIterRows())
	def aggregate(self, groupBy, aggregations={}):
		'''return an aggregation of the data grouped by a given set of fields.
Parameters:
	groupBy - the set of fields to group
	aggregations - a dict of field name -> aggregate method, where the method takes an intermediate DataTable
		and returns the value for that field for that row.
		'''
		if not aggregations:
			return self.project(groupBy).distinct()
		accumulatedRows = {}
		for row in self:
			key = tuple(row[field] for field in groupBy)
			if key not in accumulatedRows:
				accumulatedRows[key] = {a: agg.newBucket(row) for a, agg in aggregations.items()}
			accRow = accumulatedRows[key]
			for a, agg in aggregations.items():
				accRow[a] = agg.addRow(row, accRow[a])
		newData = []
		for key, accRow in sorted(accumulatedRows.items()):
			newData.append(AttributeDict(zip(groupBy, key)) + {a: agg.finalize(accRow[a]) for a, agg in aggregations.items()})
		return DataTable(newData)
	def renameColumn(self, column, newName):
		'''rename the column in place'''
		for row in self:
			v = row[column]
			del row[column]
			row[newName] = v
		del self.__headers[column]
		self.__headers[newName] = DataColumn(self, newName)
	def minRow(self, *fields):
		'''return the row with the minimum value(s) in the given field(s)'''
		if not self:
			return None
		return self.sorted(*fields)[0]
	def maxRow(self, *fields):
		'''return the row with the maximum value(s) in the given field(s)'''
		if not self:
			return None
		return self.sorted(*fields)[-1]

def diffToTable(diffResults, keyHeaders):
	data = []
	for k, v in diffResults.items():
		if isinstance(v, dict):
			for i in range(len(list(v.values())[0])):
				d = dict(zip(keyHeaders, k))
				d.update({h: r[i] for h, r in v.items()})
				data.append(d)
		else:
			d = dict(zip(keyHeaders, k))
			d['_results'] = v
			data.append(d)
	return DataTable(data)
