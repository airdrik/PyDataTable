from collections import defaultdict
from datatable import DataTable

class Result:
	'''
	Result class representing the difference between rows for a given bucket
Contains the key for this bucket (may be used to find the rows in the original files), 
	those fields which changed with the from and to values, and the actual from and to rows
	'''
	def __init__(self,  key,  keyFields, fromRow, toRow):
		self.key = key
		self.fromRow = fromRow
		self.toRow = toRow
		self.__dict__.update(dict(zip(keyFields,  key)))
		self.__data = {}
		if fromRow and toRow and len(fromRow) == 1 and len(toRow) == 1:
			#extract the fields that are different between the two runs
			f = fromRow[0]
			t = toRow[0]
			for h in f.keys():
				if f[h] != t[h]:
					self.__data[h] = {'From':f[h], 'To':t[h]}
	def __eq__(self,  other):
		if isinstance(other,  Result):
			return self.key == other.key
		if isinstance(other,  tuple):
			return self.key == other
		if isinstance(other,  dict):
			return not any(self.__dict__[k] != other[k] for k in other.keys())
		raise NotImplementedError
	def __cmp__(self,  other):
		if self == other:
			return 0
		if isinstance(other, Result):
			return cmp(self.key, other.key)
		if isinstance(other, tuple):
			return cmp(self.key, other)
		if isinstance(other, dict):
			return cmp(self.keyDict, other)
	def comparable(self):
		return bool(self.__data)
	def __nonzero__(self):
		return bool(self.__data or self.fromRow is None or self.toRow is None or len(self.fromRow) != len(self.toRow))
	def __getitem__(self, field):
		return self.__data[field]
	def __contains__(self, field):
		return field in self.__data
	def __delitem__(self, field):
		del self.__data[field]
	def ignoreField(self, field):
		if field in self.__data:
			del self.__data[field]
	def checkRemove(self, field, filterMethod):
		'''
		remove the field from the result if filterMethod returns true for the fromRow, toRow pairs
field is the field to check
filterMethod is a method which takes two parameters (the fromRow and toRow versions of the field) and returns if they can be removed from the result
		'''
		if field in self.__data:
			f, t = self.__data[field]['From'], self.__data[field]['To']
			if filterMethod(a,b):
				del self.__data[field]
	def checkRemove_multiField(self, filterMethod, *fields):
		'''
		remove the set of fields from the result if filterMethod returns true for those entries
filterMethod is a method which takes two dicts: fromRow and toRow, with those fields specified by the fields parameter and returns if those values can be removed from the result
fields is a list of fields to check and possibly remove
		'''
		if any(field not in self.__data for field in fields):
			return
		f, t = (AttributeDict((field, self.__data[field]['From']) for field in fields),AttributeDict((field, self.__data[field]['To']) for field in fields))
		if filterMethod(fromRow,toRow):
			for field in fields:
				del self.__data[field]
	def __repr__(self):
		return 'Result(%s) # from rows: %d, to rows: %d' % (repr(self.key), len(self.fromRow) if self.fromRow else 0, len(self.toRow) if self.toRow else 0)
	def __str__(self):
		if self.__data:
			return '%s\t\t%s' % (self.key, self.__data)
		return '%s\t\From: %s\tTo: %s' % (self.key, len(self.fromRow) if self.fromRow else 0, len(self.toRow) if self.toRow else 0)
	def dataKeys(self):
		return self.__data.keys()
	def getLengths(self):
		return [len('%s' % k) for k in self.key]
	def formatKeys(self, lengths):
		return ', '.join(('% ' + str(l) + 's') % k for l,k in zip(lengths, self.key)) + ' |'
		
class ResultSet:
	'''
	ResultSet class representing the complete set of diff results.  
Each bucket in either table is represented by a Result instance.  
Provides filtering, iterating over the results and pretty-printing.
	'''
	def __init__(self, keyFields):
		self.__data = defaultdict(lambda : [])
		self.keyFields = keyFields
	def __iadd__(self,  result):
		if isinstance(result,  Result) and result:
			self.__data[result.key].append(result)
		return self
	def filter(self,  criteria):
		newResults = ResultSet(self.keyFields)
		for result in self:
			if criteria(result):
				newResults += result
		return newResults
	def __len__(self):
		return len(self.__data)
	def __iter__(self):
		for rList in self.__data.values():
			for result in rList:
				yield result
	def __getitem__(self,  key):
		return self.__data[key]
	def __delitem__(self,  key):
		if isinstance(key,  Result):
			self.__data[key.key].remove(key)
			if not self.__data[key.key]:
				del self.__data[key.key]
		else:
			del self.__data[key]
	def __repr__(self):
		return 'ResultSet() # length: %d' % len(self.__data)
	def __str__(self):
		def tempIter():
			yield 'Results:'
			for result in self:
				yield str(result)
		return '\n'.join(tempIter())
	def printFormatted(self):
		for line in _formatResults(self):
			print line
	def maxKeyLengths(self):
		candidates = [self.keyFields] + [result.key for result in self]
		return [max(len('%s' % row[i]) for row in candidates) for i in range(len(self.keyFields))]
	def formatKeyFields(self, lengths):
		return ', '.join(('% ' + str(l) + 's') % k for l,k in zip(lengths, self.keyFields)) + ' |'
	def pick(self):
		'''Returns a (somewhat) random result object'''
		return self.__data.itervalues().next()[0]
	def ignoreField(self, field):
		for result in self:
			result.ignoreField(field)
			if not result:
				del self[result]
	def checkRemove(self, field, filterMethod):
		'''
		remove the field from each result if filterMethod returns true for the fromRow, toRow pairs.  Removes any result which has no more inline differences
field is the field to check
filterMethod is a method which takes two parameters (the fromRow and toRow versions of the field) and returns if they can be removed from the result
		'''
		for result in self:
			result.checkRemove(field, filterMethod)
			if not result:
				del self[result]
	def checkRemove_multiField(self, filterMethod, *fields):
		'''
		remove the set of fields from each result if filterMethod returns true for those entries.  Removes any result which has no more inline differences
filterMethod is a method which takes two dicts: fromRow and toRow, with those fields specified by the fields parameter and returns if those values can be removed from the result
fields is a list of fields to check and possibly remove
		'''
		for result in self:
			result.checkRemove_multiField(filterMethod, *fields)
			if not result:
				del self[result]
	def originalFromRows(self):
		'''return the original rows being diffed from'''
		def getRows():
			for result in self:
				if not result.fromRow:
					continue
				for row in result.fromRow:
					yield row
		return DataTable(getRows())
	def originalToRows(self):
		'''return the original rows being diffed to'''
		def getRows():
			for result in self:
				if not result.toRow:
					continue
				for row in result.toRow:
					yield row
		return DataTable(getRows())

def diff(fromTable, toTable, buckets):
	'''The base diff method - buckets the data and ships it off to the Result and ResultSet classes to check for in-line differences'''
	#split the data into buckets
	fromBuckets, toBuckets = (table.bucket(*(b for b in buckets if b in table.headers())) for table in (fromTable, toTable))
	allKeys = set(fromBuckets.keys()).union(toBuckets.keys())
	
	results = ResultSet(buckets)
	for key in allKeys:
		if key in fromBuckets:
			fromBucket = fromBuckets[key]
		else:
			fromBucket = None
		if key in toBuckets:
			toBucket = toBuckets[key]
		else:
			toBucket = None
		if fromBucket and toBucket and len(fromBucket) == len(toBucket):
			for fromRow, toRow in zip(fromBucket, toBucket):
				results += Result(key, buckets, DataTable(fromRow), DataTable(toRow))
		else:
			results += Result(key, buckets, fromBucket, toBucket)
	
	return results

def _formatResults(results):
	'''Produce a pretty string for printing to the screen
	format:
header line: "bucket", Field, , Field, ...
data lines:   bucket, field_from, field_to, field_from, field_to...
	'''
	if not results:
		yield 'No results to compare'
		return
	mismatch = sorted(result for result in results if result.fromRow is None or result.toRow is None or len(result.fromRow) != len(result.toRow))
	
	keyMaxLengths = results.maxKeyLengths()
	keyTotalSize = len(results.formatKeyFields(keyMaxLengths))
	if mismatch:
		yield "Buckets don't match number of rows:"
		yield results.formatKeyFields(keyMaxLengths) + ' From Rows    To Rows'
		for result in mismatch:
			yield result.formatKeys(keyMaxLengths) + ' %-12d %-12d' % (len(result.fromRow) if result.fromRow else 0, len(result.toRow) if result.toRow else 0)
	results = results.filter(lambda result: result.fromRow and result.toRow and len(result.fromRow) == len(result.toRow))
	if not results:
		yield 'No inline differences'
		return
	yield 'Changes in common buckets:'
	headers = results.changedFields()
	resultList = []
	maxLens = [keyTotalSize] + [0]*(len(headers)*2)
	for i in range(len(headers)):
		maxLens[i*2+1] = len(headers[i])
	for result in results:
		buckets = [result.formatKeys(keyMaxLengths)]
		for i,h in enumerate(headers):
			if h in result:
				maxLens[i*2+1] = max(maxLens[i*2+1], len(str(result[h]['From'])))
				maxLens[i*2+2] = max(maxLens[i*2+2], len(str(result[h]['To'])))
				buckets += [result[h]['From'], result[h]['To']]
			else:
				buckets += ['','']
		resultList.append(buckets)
	maxLens = [str(m+1) for m in maxLens]
	linePattern = '%-' + 's%-'.join(maxLens) + 's'
	yield linePattern % ((results.formatKeyFields(keyMaxLengths),) + sum(((h,'') for h in headers), ()))
	for result in resultList:
		yield linePattern % tuple(result)

def formatResults(results):
	return '\n'.join(_formatResults(results))
