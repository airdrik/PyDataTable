import types
from collections import defaultdict
from datatable_util import AttributeDict, DataTableException, CSV, FIXEDWIDTH
from hierarchies import makeHierarchyFromTable

class JoinType:
	def __init__(self, leftOuter, rightOuter):
		self.leftOuter = leftOuter
		self.rightOuter = rightOuter

INNER_JOIN = JoinType(False, False)
RIGHT_OUTER_JOIN = JoinType(False, True)
LEFT_OUTER_JOIN = JoinType(True, False)
OUTER_JOIN = JoinType(True, True)

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
		elif '__contains__' in dir(value) and not isinstance(value, str) and not isinstance(value, unicode):
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
		if isinstance(value, types.FunctionType):
			for row in self.__dataTable:
				row[self.header] = value(row[self.header])
		else:
			for row in self.__dataTable:
				row[self.header] = value
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
			self.__headers = AttributeDict((h, DataColumn(self, c)) for h, c in data.__headers.items())
			self.__data = [AttributeDict((h.header, row[h.header]) for h in self.__headers.values()) for row in data]
			return
		if isinstance(data, str) or isinstance(data, unicode):
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
			headers = reduce(set.union, (row.keys() for row in data), set())
			self.__headers = AttributeDict((h, DataColumn(self, h)) for h in sorted(headers))
			for row in data:
				for header in self.__headers.keys():
					if header not in row:
						row[header] = None
			self.__data = [AttributeDict(row) for row in data]
		else:
			headers = data.pop(0)
			self.__headers = AttributeDict((h, DataColumn(self, h)) for h in headers)
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
		return sorted(self.__headers.keys())
	def filter(self, filterFunction):
		'''Returns a DataTable containing the lines in self filtered by the given filterFunciton
	Accepts either a dictionary of header -> value which does exact matching on the pairs, 
	or a filter function which takes a dict as input and returns if that row should be included'''
		if isinstance(filterFunction, dict):
			return DataTable(line for line in self.__data if all(line[k] == v for k, v in filterFunction.iteritems()))
		return DataTable(line for line in self.__data if filterFunction(line))
	def __len__(self):
		'''The number of rows'''
		return len(self.__data)
	def toHierarchy(self, *headers):
		return makeHierarchyFromTable(self, *headers)
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
		selfNewHeaders = dict((h, None) for h in other.headers() if h not in self.headers())
		otherNewHeaders = dict((h, None) for h in self.headers() if h not in other.headers())
		return (self & selfNewHeaders) + (other & otherNewHeaders)
	def append(self, other):
		'''Join two DataTable instances (concatenate their rows)
	requires that the headers match (or that one of self or other be empty)'''
		if other is None:
			return self
		if isinstance(other, DataTable):
			if self.headers() and other.headers() and self.headers() != other.headers():
				raise DataTableException("headers don't match.  Expected: " + str(self.headers()) + "\nFound: " + str(other.headers()))
			self.__data += other.__data
		elif isinstance(other, list):
			if other and self.headers() != sorted(other[0].keys()):
				raise DataTableException("headers don't match.  Expected: " + str(self.headers()) + "\nFound: " + str(sorted(other[0].keys())))
			self.__data += other
		elif isinstance(other, dict):
			if self.headers() and other and self.headers() != sorted(other.keys()):
				raise DataTableException("headers don't match.  Expected: " + str(self.headers()) + "\nFound: " + str(sorted(other.keys())))
			elif other:
				self.__data.append(other)
		else:
			print "other instance unknown: %s" % other.__class__
			raise NotImplemented
		return self
	__iadd__ = append
	__add__ = _copyAndApplyOp(append)
	def remove(self, other):
		'''remove the rows from other that are in self - uses exact match of rows'''
		if isinstance(other, dict):
			if other in self.__data:
				self.__data.remove(other)
		else:
			for row in other:
				if row in self.__data:
					self.__data.remove(row)
		return self
	__isub__ = remove
	__sub__ = _copyAndApplyOp(remove)
	def extend(self, other):
		'''Add columns to the data tabel using the dictionary keys from other as the new headers and their values as fields on each row
Overwrites existing columns'''
		for header, value in other.items():
			if header not in self.__headers:
				self.__headers[header] = DataColumn(self, header)
			if isinstance(value, types.FunctionType):
				for row in self.__data:
					row[header] = value(row)
			else:
				for row in self.__data:
					row[header] = value
		return self
	__iand__ = extend
	__and__ = _copyAndApplyOp(extend)
	def __or__(self, other):
		'''Pipes the DataTable into other
	Calls other with an iterator for the rows in self'''
		return other(iter(self))
	def exclude(self, other):
		'''remove column(s) from the data tabel'''
		if not self.__data:
			return self
		if '__call__' in dir(other):
			for column in self.__headers.values():
				if other(column):
					del self.__headers[column.header]
					for row in self.__data:
						del row[column.header]
			return self
		if other in self.__headers:
			other = [other]
		for key in other:
			if key not in self.__headers:
				continue
			del self.__headers[key]
			for row in self.__data:
				del row[key]
		return self
	__ixor__ = exclude
	__xor__ = _copyAndApplyOp(exclude)
	def project(self, other):
		'''return new DataTable with only the columns listed in other'''
		if not self.__data:
			return self
		if '__call__' in dir(other):
			for column in self.__headers.values():
				if not other(column):
					del self.__headers[column.header]
					for row in self.__data:
						del row[column.header]
			return self
		if other in self.__headers:
			other = [other]
		for key in self.__headers.keys():
			if key in other:
				continue
			del self.__headers[key]
			for row in self.__data:
				del row[key]
		return self
	__idiv__ = project
	__div__ = _copyAndApplyOp(project)
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
		return self ^ headers
	def sort(self, *fields):
		def mycmp(row1, row2):
			for field in fields:
				if row1[field] != row2[field]:
					if row1[field] is None:
						return -1
					if row2[field] is None:
						return 1
					return cmp(row1[field], row2[field])
			return 0
		self.__data.sort(cmp = mycmp)
	sorted = _copyAndApplyOp(sort)
	sorted.__doc__ = '''returns a new copy of the data table sorted'''
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
		return AttributeDict((key, DataTable(bucket)) for key, bucket in buckets.iteritems())
	def join(self, other, joinParams=None, otherFieldPrefix='', joinType=LEFT_OUTER_JOIN):
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
			joinParams = dict((h,h) for h in self.headers() if h in other.headers())
		elif not isinstance(joinParams, dict):
			raise Exception("joinParams must be a dictionary of <field in self> to <field in other>")
		
		newHeaders = other.headers()
		for header in joinParams.values():
			newHeaders.remove(header)

		otherBuckets = other.bucket(*joinParams.values())
		def tempJoin():
			seenKeys = set()
			for row in self:
				newRow = AttributeDict(row)
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
					yield AttributeDict(newRow)
			if joinType.rightOuter:
				joinParamReversed = dict((v,k) for k,v in joinParams.items())
				for otherRow in other:
					key = tuple(otherRow[field] for field in joinParams.values())
					if key not in seenKeys:
						newRow = AttributeDict((joinParamReversed[k] if k in joinParamReversed else otherFieldPrefix+k, v) for k, v in otherRow.iteritems())
						for header in self.headers():
							if header not in joinParams:
								newRow[header] = None
						yield newRow
		return DataTable(tempJoin())
	def writeTo(self, fileName):
		'''Write the contents of this DataTable to a file with the given name in the standard csv format'''
		with open(fileName, 'w') as f:
			f.write(self | CSV)
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
			rowID = lambda dataTable, i: fmt % i
		if isinstance(rowID, str):
			rowID = lambda dataTable, i: dataTable[i][rowID]
		rowIDs = [rowID(self, i) for i in range(len(self))]
		def tempIterRows():
			for header,  column in sorted(self.__headers.iteritems()):
				row = AttributeDict(zip(rowIDs, column))
				row['Field'] = header
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
			return (self / groupBy).distinct()
		def tempIter():
			for key, bucket in self.bucket(*groupBy).iteritems():
				row = dict(zip(groupBy, key))
				for field, aggMethod in aggregations.iteritems():
					row[field] = aggMethod(bucket)
				yield row
		return DataTable(tempIter())
	def renameColumn(self, column, newName):
		'''rename the column in place'''
		for row in self:
			row[newName] = row[column]
			del row[column]
		self.__headers[newName] = DataColumn(self, newName)
		del self.__headers[column]
	def minRow(self, *fields):
		'''return the row with the minimum value(s) in the given field(s)'''
		return self.sorted(*fields)[0]
	def maxRow(self, *fields):
		'''return the row with the maximum value(s) in the given field(s)'''
		return self.sorted(*fields)[-1]

def diffToTable(diffResults, keyHeaders):
	data = []
	for k, v in diffResults.iteritems():
		if isinstance(v, dict):
			for i in range(len(v.values()[0])):
				d = dict(zip(keyHeaders, k))
				d.update(dict((h, r[i]) for h, r in v.iteritems()))
				data.append(d)
		else:
			d = dict(zip(keyHeaders, k))
			d['_results'] = v
			data.append(d)
	return DataTable(data)
