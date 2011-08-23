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
from myxml import XmlNode
from datatable_util import AttributeDict

def __flatten(l):
	for i in l:
		if isinstance(i, tuple) or isinstance(i, list):
			for v in __flatten(i):
				yield v
		else:
			yield i

def flatten(*l):
	return tuple(__flatten(l))

class HierarchyKey(object):
	'''
	class used by herarchies for the dict keys
	key is the header of the item (should match the key of the other HierarchyKey in the same level)
	value is used for the actual key in the dict.
example:
d = {HierarchyKey('A','a'):{}, {HierarchyKey('A','b'):{}}
d['a'] == {}
d[HierarchyKey('A','a')] == {}

	primary use for the key is that when iterating over the hierarchy, the it is used as the key in the resulting key-value pairs
examples:
d = {HierarchyKey('A','a'):{}, {HierarchyKey('A','b'):{}}
[row for row in iterHierarchy(d)] == [{'A':'a'}, {'A','b'}]

	Should preserve the following
table = # some list of dicts where the dicts have common keys
h =  makeHierarchyFromTable(table, *table[0].keys())
table == [row for row in iterHierarchy(h)]
	'''
	def __init__(self, key, value):
		self.key = key
		self.value = value
	def __repr__(self):
		return '<%s:%s>' % (self.key, self.value)
	def __str__(self):
		return '%s:%s' % (self.key, self.value)
	def __hash__(self):
		return hash(self.value)
	def __eq__(self, other):
		return self.value == other
	def __req__(self, other):
		return self.value == other
	def __cmp__(self, other):
		return cmp(self.value, other)
	def __rcmp__(self, other):
		return cmp(self.value, other)
	def __add__(self, other):
		return HierarchyKey(flatten(self.key,other.key), flatten(self.value, other.value))
	def asDict(self):
		if isinstance(self.key, tuple):
			return AttributeDict(zip(self.key, self.value))
		return AttributeDict({self.key: self.value})

class Hierarchy(AttributeDict):
	@staticmethod
	def fromRow(values, *headers):
		h = hierarchy = Hierarchy()
		for header in headers:
			h[HierarchyKey(header, values[header])] = Hierarchy()
			h = h[values[header]]
		return hierarchy
	@staticmethod
	def fromTable(table, *headers):
		hierarchy = Hierarchy()
		for row in table:
			hierarchy.addValues(row, headers)
		return hierarchy
	def addValues(self, values, headers):
		'''Adds the values from the values dictionary (key-value pairs) to the hierarchy (tree of dictionaries) using headers to determine the order'''
		if not headers:
			return
		header, rest = headers[0], headers[1:]
		val = values[header]
		if val not in self:
			self[HierarchyKey(header, val)] = Hierarchy()
		self[val].addValues(values, rest)
	def merge(self, other):
		'''
		recursively merge two hierarchies
		'''
		ret = Hierarchy()
		for k,v in self.iteritems():
			if k in other:
				ret[k] = v.merge(other[k])
			else:
				ret[k] = v
		for k,v in other.iteritems():
			if k not in self:
				ret[k] = v
		return ret
	def cleanHierarchy(self, cleanLeaf):
		'''
	cleans up a hierarchy by removing branches that may be cleaned
When cleanHierarchy is called on a leaf node (defined as a node where all of its children are empty dicts, e.g.: {'a':{}, 'b':{}}), it returns tailReturn(h)
for all other nodes, for each of its children where cleanHierarchy returns nothing (None, empty dict, etc) then don't set that child in the return value
so, to trim all of the branches of a hierarchy where the leaf node = {'a':{}} then do the following:

def myCleanHierarchy(hierarchy):
	def cleanLeaf(h):
		if h == {'a':{}}:
			return None
		return h
	return cleanHierarchy(hierarchy, cleanLeaf)

Then calling the following:

myCleanHierarchy({'b':{'a':{}}, 'c':{'a':{}, 'd':{}}})

would return:

{'c':{'a':{}, d':{}}}

and calling:

myCleanHierarchy({'b':{'a':{}}, 'c':{'a':{}, 'd':{'a':{}}}})

would return:

{}
'''
		if self == {}:
			return self
		ret = Hierarchy()
		for k,v in self.iteritems():
			clean = v.cleanHierarchy(cleanLeaf)
			if clean is not None:
				ret[k] = clean
		if not ret:
			return None
		if not any(ret.values()):
			return cleanLeaf(ret)
		return ret

	def __iter__(self):
		for k,v in sorted(self.iteritems()):
			h = k.asDict()
			if not v:
				yield AttributeDict(h)
			else:
				for d in v:
					yield d + h
	def __str(self, currentDepth=0):
		for k,v in sorted(self.iteritems()):
			yield '\t'*currentDepth + str(k)
			for line in v.__str(currentDepth+1):
				yield line
	def __str__(self):
		return '\n'.join(self.__str())
	def __repr__(self):
		return 'Hierarchy : ' + repr(sorted(self.keys()))
	def _toXML(self):
		s = ''
		for k,v in self.iteritems():
			s += '<%s value="%s"' % (k.key, k.value)
			if v:
				s += '>' + v._toXML() + '</%s' % k.key
			else:
				s += '/'
			s +='>'
		return s
	def toXML(self):
		s = self._toXML()
		if s:
			return '<Hierarchy>' + s + '</Hierarchy>'
		return '<Hierarchy/>'
	@staticmethod
	def fromXML(theXML):
		if not isinstance(theXML, XmlNode):
			theXML = XmlNode(theXML)
		hierarchy = Hierarchy()
		# @type theXML XmlNode
		for node in theXML.children():
			hierarchy[HierarchyKey(node.name, node['value'])] = Hierarchy.fromXML(node)
		return hierarchy
	def combineKeys(self):
		'''
		reduces the depth of the hierarchy by merging the nodes with one child with that child
		e.g.
		combineKeys({'a':{'b':{...b}}, 'c':{'d':{...d},'e':{...e}}}) -> {('a','b'):{...b}, 'c':{'d':{...d},'e':{...e}}}
		combineKeys({'a':{'b':{'c':{'d':{'e':{}}}}}}) -> {('a','b','c','d','e'):{}}
		'''
		copy = Hierarchy()
		for k,v in self.items():
			if len(v) != 1:
				copy[k] = v.combineKeys()
			else:
				t = k
				while len(v) == 1:
					t = t + v.keys()[0]
					v = v.values()[0]
				copy[t] = v.combineKeys()
		return copy

def makeHierarchy(values, *headers):
	h = hierarchy = AttributeDict()
	for header in headers:
		h[HierarchyKey(header, values[header])] = AttributeDict()
		h = h[values[header]]
	return hierarchy

def addToHierarchy(hierarchy, values, headers):
	'''Adds the values from the values dictionary (key-value pairs) to the hierarchy (tree of dictionaries) using headers to determine the order'''
	if not headers:
		return
	header, rest = headers[0], headers[1:]
	val = values[header]
	if val not in hierarchy:
		hierarchy[HierarchyKey(header, val)] = AttributeDict()
	addToHierarchy(hierarchy[val], values, rest)

def mergeHierarchies(h1, h2):
	'''
	recursively merge two hierarchies
	'''
	ret = AttributeDict()
	for k,v in h1.iteritems():
		if k in h2:
			ret[k] = mergeHierarchies(v, h2[k])
		else:
			ret[k] = v
	for k,v in h2.iteritems():
		if k not in h1:
			ret[k] = v
	return ret

def makeHierarchyFromTable2(table, *headers):
	hierarchy = AttributeDict()
	for row in table:
		hierarchy = mergeHierarchies(hierarchy, makeHierarchy(row, *headers))
	return hierarchy

def makeHierarchyFromTable(table, *headers):
	hierarchy = AttributeDict()
	for row in table:
		addToHierarchy(hierarchy, row, headers)
	return hierarchy

def cleanHierarchy(h, cleanLeaf):
	'''
	cleans up a hierarchy by removing branches that may be cleaned
When cleanHierarchy is called on a leaf node (defined as a node where all of its children are empty dicts, e.g.: {'a':{}, 'b':{}}), it returns tailReturn(h)
for all other nodes, for each of its children where cleanHierarchy returns nothing (None, empty dict, etc) then don't set that child in the return value
so, to trim all of the branches of a hierarchy where the leaf node = {'a':{}} then do the following:

def myCleanHierarchy(hierarchy):
	def cleanLeaf(h):
		if h == {'a':{}}:
			return None
		return h
	return cleanHierarchy(hierarchy, cleanLeaf)

Then calling the following:

myCleanHierarchy({'b':{'a':{}}, 'c':{'a':{}, 'd':{}}})

would return:

{'c':{'a':{}, d':{}}}

and calling:

myCleanHierarchy({'b':{'a':{}}, 'c':{'a':{}, 'd':{'a':{}}}})

would return:

{}
	'''
	if h == {}:
		return h
	ret = AttributeDict()
	for k,v in h.iteritems():
		clean = cleanHierarchy(v, cleanLeaf)
		if clean is not None:
			ret[k] = clean
	if not ret:
		return None
	if not any(v for v in ret.values()):
		return cleanLeaf(ret)
	return ret

def iterHierarchy(hierarchy):
	for k,v in sorted(hierarchy.iteritems()):
		h = k.asDict()
		if not v:
			yield AttributeDict(h)
		else:
			for d in iterHierarchy(v):
				yield d + h

def printHierarchy(hierarchy, currentDepth=0):
	for k,v in sorted(hierarchy.iteritems()):
		print '\t'*currentDepth + str(k)
		printHierarchy(v, currentDepth+1)

def toXML(hierarchy):
	s = ''
	for k,v in hierarchy.iteritems():
		s += '<%s value="%s"' % (k.key, k.value)
		if v:
			s += '>' + toXML(v) + '</%s' % k.key
		else:
			s += '/'
		s +='>'
	return s

def combineKeys(hierarchy):
	'''
	reduces the depth of the hierarchy by merging the nodes with one child with that child
	e.g.
	combineKeys({'a':{'b':{...b}}, 'c':{'d':{...d},'e':{...e}}}) -> {('a','b'):{...b}, 'c':{'d':{...d},'e':{...e}}}
	combineKeys({'a':{'b':{'c':{'d':{'e':{}}}}}}) -> {('a','b','c','d','e'):{}}
	'''
	copy = AttributeDict()
	for k,v in hierarchy.items():
		if len(v) != 1:
			copy[k] = combineKeys(v)
		else:
			t = k
			while len(v) == 1:
				t = t + v.keys()[0]
				v = v.values()[0]
			copy[t] = combineKeys(v)
	return copy

def diff(fromTable, toTable, buckets=None):
	'''
	p is the production (from) profile (or the latest execution from such a profile - may be the profileID)
	t is the test (to) profile (or latest execution from such a profile).  If t is None, expects p to be a profileID and will populate p and t from production and test accordingly
	buckets is the fields to bucket the results (should be the set of fields which uniquely identifies an entity).  May be left out if p is a profile or profileID
	ignored is a list of fields to filter out of the results completely (defaults to ID and NormalizationExecutionID)
	reconDate is an optional date if p (and t) are profiles (or profileIDs)
	'''
	if fromTable is None:
		return 'No Production data'
	if toTable is None:
		return 'No test data'
	res = (fromTable & {'_results':'From'}).augment(toTable & {'_results':'To'})
	diffHeaders = (tuple(b for b in buckets if b in res.headers())+ tuple(h for h in res.headers() if h not in buckets + ('_results',)) + ('_results',))
	h = Hierarchy.fromTable(res, *diffHeaders)
	def cleanLeaf(h):
		if h == {'From':{},'To':{}}:
			return None
		return h
	return h.cleanHierarchy(cleanLeaf) or {}
