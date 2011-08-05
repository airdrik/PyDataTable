from datatable_util import types, AttributeDict, defaultdict, makeHierarchyFromTable, DataTableException, CSV, FIXEDWIDTH

class DataColumn(object):
	def __init__(self, dataTable, header, data=[]):
		self.__dataTable = dataTable
		if isinstance(header, DataColumn):
			self.header = header.header
			self.__data = list(header.__data) 
		else:
			self.header = header
			self.__data = []
			for item in data:
				if isinstance(item, dict):
					if header in item:
						self.__data.append(item[header])
					else:
						self.__data.append(None)
				else:
					self.__data.append(item)
	def __eq__(self, other):
		if not isinstance(other, DataColumn):
			return False
		return self.__data == other.__data
	def __iter__(self):
		return iter(self.__data)
	def __len__(self):
		return len(self.__data)
	def __getitem__(self, index):
		'''Gets the index'th row of data'''
		return self.__data[index]
	def __contains__(self, value):
		return value in self.__data
	def __filter(self, value):
		if value is None:
			for i in range(len(self)):
				if self[i] is None:
					yield i
		elif isinstance(value, DataColumn):
			if value.__dataTable == self.__dataTable:
				for i in range(len(self)):
					if self[i] == value[i]:
						yield i
			else:
				otherValues = set(value)
				for i in range(len(self)):
					if self[i] in otherValues:
						yield i
		elif '__call__' in dir(value):
			for i in range(len(self)):
				if value(self[i]):
					yield i
		elif '__contains__' in dir(value) and not isinstance(value, str) and not isinstance(value, unicode):
			for i in range(len(self)):
				if self[i] in value:
					yield i
		else:
			for i in range(len(self)):
				if self[i] == value:
					yield i
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
		return self.__dataTable.select(self.__filter(value))
	def set(self, value):
		self.__dataTable &= {self.header: value}
	def sort(self):
		self.__dataTable.sort(self.header)
	def sizeOfGroups(self):
		groups = defaultdict(lambda:0)
		for v in self:
			groups[v] += 1
		return dict(groups)
	def fillDownBlanks(self):
		prev = None
		for i in range(len(self.__data)):
			if self.__data[i]:
				prev = self.__data[i]
			elif prev:
				self.__data[i] = prev
	def __repr__(self):
		return "DataColumn(<dataTable>, '%s')" % self.header

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
		data = {}
		size = 0
		for table in tables:
			for column in table.columns():
				if column.header not in data:
					data[column.header] = [None]*size
				data[column.header] += list(column)
			for h in data.keys():
				if h not in table.headers():
					data[h] += [None] * len(table)
			size += len(table)
		return DataTable(DataColumn(None, header, columnData) for header, columnData in data.iteritems())
	def __init__(self, data=None, parseMethod=None):
		'''Create a data table from the given data
	data may be one of the following:
A sequence of dictionaries, where all of the dictionaries share common keys
A sequence of sequences where the first item is the list of headers
Another DataTable instance, which will create a deep copy
A string which may be parsed into one of the previous by calling parseMethod on the string.
'''
		if isinstance(data, DataTable):
			self.__headers = AttributeDict((h,DataColumn(self, c)) for h,c in data.__headers.items())
			self.__length = len(data)
			return
		if isinstance(data, str) or isinstance(data, unicode):
			data = parseMethod(data)
		if not data:
			self.__headers = {}
			self.__length = 0
			return
		data = [row for row in data]
		if not data:
			self.__headers = {}
			self.__length = 0
			return
		if isinstance(data[0], DataColumn):
			self.__headers = AttributeDict((column.header, DataColumn(self, column)) for column in data)
			self.__length = len(data[0])
			return
		if isinstance(data[0], dict):
			headers = reduce(set.union, (row.keys() for row in data), set())
			self.__headers = AttributeDict((h,DataColumn(self, h, data)) for h in sorted(headers))
		else:
			headers = data.pop(0)
			data = [AttributeDict((headers[i], row[i]) for i in range(len(headers))) for row in data]
			self.__headers = AttributeDict((h,DataColumn(self, h, data)) for h in headers)
		self.__length = len(data)
	def getRow(self, index):
		return AttributeDict((h, self.__headers[h][index]) for h in self.__headers.keys())
	def select(self, indices):
		if isinstance(indices, slice):
			start, stop, step = indices.indices(len(self)) #calculate the actual start, stop, step given the current length
			indices = range(start, stop)[::step]
		else:
			indices = list(indices)
		return DataTable(DataColumn(None, c.header, (c[i] for i in indices)) for c in self.__headers.itervalues())
	def __iter__(self):
		'''Gets an iterator over the data rows'''
		for i in range(len(self)):
			yield self.getRow(i)
	def __getitem__(self, index):
		'''Gets the index'th row of data'''
		if isinstance(index, int):
			return self.getRow(index)
		return self.select(index)
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
			return self.select(i for i in range(len(self)) if all(self.column(k)[i] == v for k,v in filterFunction.iteritems()))
		return DataTable(line for line in self if filterFunction(line))
	def __len__(self):
		'''The number of rows'''
		return self.__length
	def toHierarchy(self, *headers):
		return makeHierarchyFromTable(self, *headers)
	def __str__(self):
		return self | FIXEDWIDTH
	def __repr__(self):
		return 'Rows:%d\nHeaders:\n%s' % (len(self), self.headers())
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
	def __add__(self, other):
		'''Join two DataTable instances (concatenate their rows)
	requires that the headers match (or that one of self or other be empty)'''
		newData = DataTable(self)
		newData += other
		return newData
	def __iadd__(self, other):
		'''Join two DataTable instances (concatenate their rows)
	requires that the headers match (or that one of self or other be empty)'''
		if other is None:
			return self
		if isinstance(other, DataTable):
			if self.headers() and other.headers() and self.headers() != other.headers():
				raise DataTableException("headers don't match.  Expected: " + str(self.headers()) + "\nFound: " + str(other.headers()))
			for h in self.headers():
				self.__headers[h] = DataColumn(self, h, list(self.__headers[h]) + list(other.__headers[h]))
			self.__length = self.__length + len(other)
		elif isinstance(other, list):
			if other and self.headers() != sorted(other[0].keys()):
				raise DataTableException("headers don't match.  Expected: " + str(self.headers()) + "\nFound: " + str(sorted(other[0].keys())))
			for h in self.headers():
				self.__headers[h] = DataColumn(self, h, list(self.__headers[h]) + [row[h] for row in other])
			self.__length = self.__length + len(other)
		elif isinstance(other, dict):
			if self.headers() and other and self.headers() != sorted(other.keys()):
				raise DataTableException("headers don't match.  Expected: " + str(self.headers()) + "\nFound: " + str(sorted(other.keys())))
			if other:
				for h in self.headers():
					self.__headers[h] = DataColumn(self, h, list(self.__headers[h]) + [other[h]])
			self.__length = self.__length + 1
		else:
			print "other instance unknown: %s" % other.__class__
			raise NotImplemented
		return self
	def __sub__(self, other):
		'''remove the rows from other that are in self - uses exact match of rows'''
		newData = DataTable(self)
		newData -= other
		return newData
	def __isub__(self, other):
		'''remove the rows from other that are in self - uses exact match of rows'''
		indices = [i for i in range(len(self)) if self.getRow(i) not in other]
		for c in self.__headers.values():
			self.__headers[c.header] = DataColumn(self, c.header, [c[i] for i in indices])
		self.__length = len(indices)
		return self
	def __and__(self, other):
		'''Add columns to the data tabel using the dictionary keys from other as the new headers and their values as fields on each row
Overwrites existing columns'''
		if isinstance(other, dict):
			newData = DataTable(self)
			newData &= other
			return newData
	def __iand__(self, other):
		'''Add columns to the data tabel using the dictionary keys from other as the new headers and their values as fields on each row
Overwrites existing columns'''
		for header,value in other.items():
			data = []
			if isinstance(value, types.FunctionType):
				data = [value(row) for row in self]
			else:
				data = [value] * len(self)
			self.__headers[header] = DataColumn(self, header, data)
		return self
	def __or__(self, other):
		'''Pipes the DataTable into other
	Calls other with an iterator for the rows in self'''
		return other(iter(self))
	def __xor__(self, other):
		'''remove column(s) from the data tabel'''
		newData = DataTable(self)
		newData ^= other
		return newData
	def __ixor__(self, other):
		'''remove column(s) from the data tabel'''
		if '__call__' in dir(other):
			for column in self.__headers.values():
				if other(column):
					del self.__headers[column.header]
			return self
		if isinstance(other, str):
			other = [other]
		for key in other:
			if key not in self.__headers:
				continue
			del self.__headers[key]
		return self
	def __div__(self, other):
		'''return new DataTable with only the columns listed in other'''
		newData = DataTable(self)
		newData /= other
		return newData
	def __idiv__(self, other):
		'''return new DataTable with only the columns listed in other'''
		if '__call__' in dir(other):
			for column in self.__headers.values():
				if not other(column):
					del self.__headers[column.header]
			return self
		if isinstance(other, str):
			other = [other]
		for key in self.__headers.keys():
			if key in other:
				continue
			del self.__headers[key]
		return self
	def removeBlankColumns(self):
		'''returns a copy of this DataTable with all of the blank columns removed'''
		blanks = [h for h,col in self.__headers.iteritems() if not any(col)]
		return self ^ blanks
	def sorted(self, *fields):
		data = list(self)
		def mycmp(row1, row2):
			for field in fields:
				if row1[field] != row2[field]:
					if row1[field] is None:
						return -1
					if row2[field] is None:
						return 1
					return cmp(row1[field], row2[field])
			return 0
		data.sort(cmp = mycmp)
		return DataTable(data)
	def sort(self, *fields):
		'''returns a new copy of the data table sorted'''
		other = self.sorted(*fields)
		for h in self.__headers.keys():
			self.__headers[h] = DataColumn(self, h, other.column(h))
		return newData

	def sizeOfBuckets(self, *fields):
		'''Returns a dict of bucket -> number of items in the bucket'''
		buckets = defaultdict(lambda:0)
		for i in range(len(self)):
			key = tuple(self.__headers[field][i] for field in fields)
			buckets[key] += 1
		return buckets
	def bucket(self, *fields):
		'''Returns a dict of bucket -> DataTable of rows matching that bucket'''
		buckets = defaultdict(lambda:[])
		for i in range(len(self)):
			key = tuple(self.__headers[field][i] for field in fields)
			buckets[key].append(i)
		return AttributeDict((key, self.select(indices)) for key,indices in buckets.iteritems())
	def join(self, other, joinParams,  otherFieldPrefix='',  leftJoin=True,  rightJoin=False):
		'''
dataTable.join(otherTable, joinParams, otherFieldPrefix='')
	returns a new table with rows in the first table joined with rows in the second table, using joinParams to map fields in the first to fields in the second
Parameters:
	other - the table to join
	joinParams - a dictionary of <field in self> to <field in other>
	otherFieldPrefix - a string to prepend to the fields added from the second table
	leftJoin - whether to include items in self which are not in other (default: True)
	rightJoin - whether to include items in other which are not in self (default: False)
		'''
		if not isinstance(joinParams, dict):
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
					if leftJoin:
						for header in newHeaders:
							newRow[otherFieldPrefix+header] = None
						yield newRow
					continue
				otherRows = otherBuckets[key]
				for otherRow in otherRows:
					for header in newHeaders:
						newRow[otherFieldPrefix+header] = otherRow[header]
					yield AttributeDict(newRow)
			if rightJoin:
				for otherRow in other:
					key = tuple(otherRow[field] for field in joinParams.values())
					if key not in seenKeys:
						newRow = AttributeDict((otherFieldPrefix+k, v) for k,v in otherRow.iteritems())
						for header in self.headers():
							newRow[header] = None
						yield newRow
		return DataTable(tempJoin())
	def writeTo(self, fileName):
		'''Write the contents of this DataTable to a file with the given name in the standard csv format'''
		f = open(fileName, 'w')
		f.write(self | CSV)
		f.close()
	def duplicates(self, *fields):
		'''given a list of fields as keys, return a DataTable instance with the rows for which those fields are not unique'''
		matchCount = {}
		for i in range(len(self)):
			key = tuple(self.__headers[field][i] for field in fields)
			if key not in matchCount:
				matchCount[key] = 0
			else:
				matchCount[key] += 1
		return self.filter(lambda row: matchCount[tuple(row[field] for field in fields)])
	def _distinct(self):
		rows = set()
		for i, row in enumerate(zip(self.__headers.values())):
			if row not in rows:
				yield i
				rows.add(row)
	def distinct(self):
		'''return a new DataTable with only unique rows'''
		return self.select(self._distinct())
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
			format = 'Row%0' + str(digits) + 'd'
			rowID = lambda dataTable, i: format % i
		if isinstance(rowID, str):
			rowID = lambda dataTable, i: dataTable.column(rowID)[i]
		return DataTable([DataColumn(None, 'Field', sorted(self.__headers.iterkeys()))] + 
						[DataColumn(None, 
								rowID(self, i), 
								(self.__headers[h][i] for h in sorted(self.__headers.iterkeys()))) for i in range(len(self))])
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
		self.__headers[newName] = self.__headers[column]
		del self.__headers[column]
		self.__headers[newName].header = newName