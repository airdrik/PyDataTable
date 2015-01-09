'''
a 'hierarchy' is simply a nested dict where the keys of each dict are the values for that row in the hierarchy
For instance the following table:
[{'a':10023, 'b':25.3, 'c':52},
 {'a':10023, 'b':25.3, 'c':156},
 {'a':10023, 'b':4.123, 'c':48},
 {'a':10023, 'b':65.8, 'c':52}
 {'a':10024, 'b':2.8, 'c':23},
 {'a':10024, 'b':2.8, 'c':31}]
would be represented by the following hierarchy:

{10023:
	{25.3:
		{52:{},
		 156:{}
		},
	 4.123: { 48:{} },
	 65.8: { 52:{} }
	},
 10024:
	{2.8:
		{23:{},
		 31:{}
		}
	}
}

'''
from bs4 import BeautifulSoup
from datatable_util import AttributeDict
import inspect

def _getFixedWidthFormat(widths):
	return ('{!s:<%d} ' * len(widths)) % widths

class HierarchyLeaf(list):
	'''HierarchyLeaf represents the base of a hierarchy, and is a list of rows that have been added to the hierarchy that match a hierarchy key
	'''
	def copy(self):
		return HierarchyLeaf(AttributeDict(row) for row in self)
	def project(self, newLeafHeaders):
		return HierarchyLeaf(AttributeDict((k, v) for k, v in row.items() if k in newLeafHeaders) for row in self)
	def merge(self, other):
		return HierarchyLeaf(row1+row2 for row1 in self for row2 in other)
	def mergeInPlace(self, other):
		super(HierarchyLeaf, self).__init__([row1+row2 for row1 in self for row2 in other])
	def _collect_column_maximums(self):
		return tuple(max(len(str(value)) for _, value in columnKeyValues) for columnKeyValues in zip(*(sorted(row.items()) for row in self)))
	def _str(self, widths):
		formatStr = _getFixedWidthFormat(widths)
		for row in self:
			yield formatStr.format(*(str(value) for _, value in sorted(row.items())))
	def toXML(self):
		soup = BeautifulSoup('<hierarchyleaf/>', 'xml')
		for row in self:
			soup.hierarchyleaf.append(soup.new_tag('leaf', **row))
		return soup
	def toXMLString(self):
		return ''.join((str(c) for c in self.toXML().children))
	def __getitem__(self, criteria):
		if isinstance(criteria, tuple):
			if not criteria:
				return self
			elif len(criteria) == 1:
				criteria = criteria[0]
		if hasattr(criteria, '__call__'):
			return HierarchyLeaf(row for row in self if criteria(row))
		return super(HierarchyLeaf, self).__getitem__(criteria)
	def rows(self):
		return iter(self)
	def aggregate(self, aggregations={}, parentKey=AttributeDict()):
		if not aggregations:
			return HierarchyLeaf()
		return HierarchyLeaf(	[AttributeDict((field, aggMethod(parentKey, self)) for field, aggMethod in aggregations.items())])
	def renameHeaders(self, reassignments):
		for row in self:
			for fromHeader, toHeader in reassignments.items():
				if fromHeader in row:
					row[toHeader] = row[fromHeader]
					del row[fromHeader]

def _maybeSet(d, k, v):
	if v:
		d[k] = v

class Hierarchy():
	'''Hierarchy class, providing methods for handling data in a hierarchical structure
'''
	@staticmethod
	def fromRow(values, keyHeaders, leafHeaders=None):
		'''Create a new Hierarchy with the data in the values dict'''
		if leafHeaders is None:
			leafHeaders = set(values.keys()).difference(keyHeaders)
		hierarchy = Hierarchy(keyHeaders, leafHeaders)
		hierarchy.addValues(values)
		return hierarchy
	@staticmethod
	def fromTable(table, keyHeaders, leafHeaders=None):
		'''Create a new Hierarchy with the data in the table'''
		if leafHeaders is None:
			table = list(table)
			if not table:
				return Hierarchy(keyHeaders, [])
			leafHeaders = set(table[0].keys()).difference(keyHeaders)
		hierarchy = Hierarchy(keyHeaders, leafHeaders)
		for row in table:
			hierarchy.addValues(row)
		return hierarchy
	def __init__(self, keyHeaders, leafHeaders):
		'''Create a new Hierarchy with the given key and leaf headers
The key headers defines the depth of the hierarchy, while the leaf headers defines the keys retained in the leaf nodes
'''
		self.keyHeaders = tuple(keyHeaders)
		self.leafHeaders = tuple(leafHeaders)
		self._data = {}
	def copy(self):
		'''make a new deep copy of this hierarchy'''
		new = Hierarchy(self.keyHeaders, self.leafHeaders)
		for k, v in self:
			new[k] = v.copy()
		return new
	def reindex(self, newKeyHeaders, newLeafHeaders=None):
		if newLeafHeaders is None:
			newLeafHeaders = set(self.keyHeaders).union(self.leafHeaders).difference(newKeyHeaders)
		new = Hierarchy(newKeyHeaders, newLeafHeaders)
		for row in self.rows():
			new.addValues(row)
		return new
	def addValues(self, values):
		'''Adds the values from the values dict to this hierarchy'''
		val = values[self.keyHeaders[0]]
		if len(self.keyHeaders) == 1:
			if val not in self._data:
				self._data[val] = HierarchyLeaf()
			self._data[val].append(AttributeDict((k, values[k]) for k in self.leafHeaders))
			return
		if val not in self._data:
			self._data[val] = Hierarchy(self.keyHeaders[1:], self.leafHeaders)
		self[val].addValues(values)
	def keys(self):
		return tuple(self._data.keys())
	def _filterByFunction(self, f, rest):
		new = Hierarchy(self.keyHeaders, self.leafHeaders)
		if not inspect.ismethod(f) and not inspect.isfunction(f):
			f = f.__call__
		args, vargs, kwargs, defaults = inspect.getargspec(f)
		if len(args) == 1:
			if args[0] in ('v', 'value', 'h', 'hierarchy'):
				fltr = lambda _, v: f(v)
			else:
				fltr = lambda k, _: f(k)
		elif len(args) == 2:
			fltr = f
		else:
			raise ValueError("invalid filter method.  Expected function with one or two parameters.  Given function %r had %d parameters" % (f, len(args)))
		for k, v in self:
			if fltr(k, v):
				_maybeSet(new._data, k, rest(v))
		return new
	def __getitem__(self, key):
		'''hierarchy[key]
returns the hierarchy or leaf node from the given key
key may be a list or tuple of keys which walk down the hierarchy for each key and retrieve the resulting hierarchy, leaf or scalar'''
		if isinstance(key, tuple):
			if not key:
				return self
			new = Hierarchy(self.keyHeaders, self.leafHeaders)
			if key[0] in (True, all, any):
				for k, v in self:
					_maybeSet(new._data, k, v[key[1:]])
			elif hasattr(key[0], '__call__'):
				return self._filterByFunction(key[0], lambda v: v[key[1:]])
			elif isinstance(key[0], tuple):
				first, last = key[0]
				for k, v in self:
					if first <= k and k <= last:
						_maybeSet(new._data, k, v[key[1:]])
			elif isinstance(key[0], (list, set)):
				for k, v in self:
					if k in key[0]:
						_maybeSet(new._data, k, v[key[1:]])
			else:
				_maybeSet(new._data, key[0], self._data[key[0]][key[1:]])
			return new
		if hasattr(key, '__call__'):
			return self._filterByFunction(key, lambda v: v)
		if isinstance(key, (list, set)):
			new = Hierarchy(self.keyHeaders, self.leafHeaders)
			for k, v in self:
				if k in key:
					new._data[k] = v
			return new
		return self._data[key]
	def __setitem__(self, key, value):
		'''hierarchy[key] = value
adds value to the hierarchy retrieved by hierarchy[key]'''
		if isinstance(key, (list, tuple)):
			if not key:
				self.addValues(value)
			else:
				self[key[0]][key[1:]] = value
		else:
			self[key][()] = value
	def __delitem__(self, key):
		if isinstance(key, (list, tuple)):
			if len(key) == 1:
				key = key[0]
			else:
				if not key:
					return
				if hasattr(key[0], '__call__'):
					for k, v in self:
						if key[0](k):
							del v[key[1:]]
				else:
					del self._data[key[0]][key[1:]]
				return
		if hasattr(key, '__call__'):
			for k in self._data.keys():
				if key(k):
					del self._data[k]
		else:
			del self._data[k]
	def __len__(self):
		return sum(len(child) for child in self._data.values())
	def __contains__(self, key):
		return key in self._data
	def __iter__(self):
		return iter(self._data.items())
	def rows(self):
		'''iter(hierarchy)
iterate over the "rows" in the hierarchy, where a row is defined as a leaf node merged with all parents in the hierarchy'''
		for key, child in sorted(self):
			keyDict = {self.keyHeaders[0]: key}
			for row in child.rows():
				yield row + keyDict
	def _collect_column_maximums(self):
		if not self:
			return (0,) * (len(self.leafHeaders) + len(self.keyHeaders))
		return (max(len(str(k)) for k in self._data.keys()),) + tuple(max(columnMaximums) for columnMaximums in zip(*(child._collect_column_maximums() for _, child in self)))
	def _str(self, widths):
		fmt = _getFixedWidthFormat(widths[:1])
		widths = widths[1:]
		for k,v in sorted(self):
			current = fmt.format(k)
			for line in v._str(widths):
				yield current + line
				current = ' ' * len(current)
	def __str__(self):
		widths = self._collect_column_maximums()
		headers = [str(h) for hdrs in (self.keyHeaders, sorted(self.leafHeaders)) for h in hdrs]
		widths = tuple(max((width, len(header))) for width, header in zip(widths, headers))
		return _getFixedWidthFormat(widths).format(*headers) + '\n' + '\n'.join(self._str(widths))
	def __repr__(self):
		return 'Hierarchy; headers : %s; keys : %r' % (list(self.keyHeaders) + sorted(self.leafHeaders), sorted(self._data.keys()))
	def sizeOfGroups(self):
		return {k: len(v) for k, v in self._data.items()}
	def extend(self, moreColumns):
		new = Hierarchy(self.keyHeaders, set(self.leafHeaders).union(moreColumns.keys()))
		for row in self.rows():
			for k, v in moreColumns.items():
				if hasattr(v, '__call__'):
					v = v(row)
				new.addValues(row + {k: v})
		return new
	def project(self, newLeafHeaders):
		new = Hierarchy(self.keyHeaders, set(newLeafHeaders))
		for k, v in self:
			new[k] = v.project(newLeafHeaders)
		return new
	def merge(self, other):
		'''recursively merge two hierarchies'''
		if self.keyHeaders != other.keyHeaders:
			raise Exception('Cannot merge hierarchies with different keys')
		ret = Hierarchy(self.keyHeaders, set(self.leafHeaders).union(other.leafHeaders))
		for k, v in self:
			if k in other:
				ret._data[k] = v.merge(other[k])
			else:
				ret._data[k] = v.copy()
		for k, v in other:
			if k not in self:
				ret._data[k] = v.copy()
		return ret
	def mergeInPlace(self, other):
		if self.keyHeaders != other.keyHeaders:
			raise Exception('Cannot merge hierarchies with different keys')
		self.leafHeaders = set(self.leafHeaders).union(other.leafHeaders)
		for k, v in self:
			if k in other:
				v.mergeInPlace(other[k])
		for k, v in other:
			if k not in self:
				self._data[k] = v.copy()

	def aggregate(self, aggregations={}, parentKey=AttributeDict()):
		'''return an aggregation of the hiararchy leaf tables
	the resulting Hierarchy will have the same structure, except that the leaf tables will be collapsed to single rows
	containing the results of applying the aggregations to the original leaf tables
Parameters:
	aggregations - a dict of field name -> aggregate method, where the method takes an intermediate HierarchyLeaf
		and returns the value for that field for that row.
		'''
		if not aggregations:
			return self.reindex(self.keyHeaders, ())
		new = Hierarchy(self.keyHeaders, aggregations.keys())
		for key, child in self:
			new._data[key] = child.aggregate(aggregations, parentKey=parentKey+{self.keyHeaders[0]: key})
		return new
	def renameHeaders(self, reassignments):
		self.keyHeaders = [(header if header not in reassignments else reassignments[header]) for header in self.keyHeaders]
		self.leafHeaders = {(header if header not in reassignments else reassignments[header]) for header in self.leafHeaders}
		for _, child in self:
			child.renameHeaders(reassignments)
	def toXML(self):
		soup = BeautifulSoup('<hierarchy/>', 'xml')
		for key, value in self:
			node = soup.new_tag(self.keyHeaders[0], key=key)
			for childNode in value.toXml().children:
				node.append(childNode)
			soup.hierarchy.append(node)
		return soup
	def toXMLString(self):
		return str(self.toXML())

def diffTables(fromTable, toTable, buckets=None):
	'''
	compares the data in two tables, returning a hierarchichal view of the resulting data

	buckets is the fields to bucket the results (should be the set of fields which uniquely identifies an entity)
	'''
	res = (fromTable & {'_results':'From'}).augment(toTable & {'_results':'To'})
	diffHeaders = tuple(b for b in buckets if b in res.headers()) + ('_results',)
	h = Hierarchy.fromTable(res, diffHeaders)
	def scrubResults(h):
		new = Hierarchy(h.keyHeaders, h.leafHeaders)
		if h.keyHeaders == ('_results',):
			if 'From' in h and 'To' in h and h['From'] == h['To']:
				return None
			return h
		for k, v in h:
			_maybeSet(new._data, k, scrubResults(h[k]))
		return new
	return scrubResults(h)
