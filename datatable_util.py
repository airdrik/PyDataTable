import os
import enum

class JoinType(enum.Enum):
	def __init__(self, leftOuter, rightOuter):
		self.leftOuter = leftOuter
		self.rightOuter = rightOuter
	INNER_JOIN = False, False
	RIGHT_OUTER_JOIN = False, True
	LEFT_OUTER_JOIN = True, False
	OUTER_JOIN = True, True

def sortKey(it):
	return hash(type(it)), it

class AttributeDict(dict):
	'''
	Subclass of dict that allows access to items using dot-notation:
d = AttributeDict()
d['a'] = 1
print d.a # 1
	'''
	def __init__(self, *args, **vargs):
		super(AttributeDict, self).__init__(*args, **vargs)
	def __getattr__(self, attr):
		try:
			return self[attr]
		except KeyError as e:
			raise AttributeError(*e.args)
	__setattr__ = dict.__setitem__
	def __dir__(self):
		return list(self.__dict__.keys()) + [str(key) for key in self.keys()]
	def __iadd__(self, other):
		self.update(other)
		return self
	def __add__(self, other):
		d = AttributeDict(self)
		d += other
		return d
	def filter(self, filterFunction):
		return AttributeDict({k:v for k, v in self.items() if filterFunction(k, v)})

class DataTableException(Exception):
	pass

def _quoteField(field):
	f = str(field)
	if ',' in f:
		return '"%s"' % f
	return f


def CSV_GivenHeaders(*headers):
	'''takes a list of headers and returns a 'pipable' object, similar to CSV, but using the column order as specified'''
	return lambda it: ','.join(_quoteField(h) for h in headers) + '\n' + '\n'.join(','.join(_quoteField(line[header]) for header in headers) for line in it)

def CSV(it):
	'''Takes an iterator which yields dicts with common keys and returns a CSV string for that data'''
	l = list(it)
	if not l:
		return ''
	headers = sorted(l[0].keys())
	return CSV_GivenHeaders(*headers)(l)

def FIXEDWIDTH(it):
	'''Takes an iterator which yields dicts with common keys and returns a fixed-width formatted string (primarily for printing)'''
	l = list(it)
	if not l:
		return ''
	headers = sorted(l[0].keys())
	l = [tuple(str(v) for v in row) for row in [tuple(headers)] + [tuple(row[h] for h in headers) for row in l]]
	maxLengths = [str(max(len(row[i]) for row in l)) for i in range(len(headers))]
	if maxLengths:
		formatStr = '%-' + 's %-'.join(maxLengths) + 's'
	else:
		formatStr = '<no data>'
	return '\n'.join((formatStr % row) for row in l)

from bs4 import BeautifulSoup
def XML(it):
	'''Takes an iterator which yields dicts and returns an xml formatted string
	The root node is named 'table', the rows are represented by 'row' nodes, whose attributes are the key-value pairs from the dict
	'''
	x = BeautifulSoup('<table/>', 'xml')
	for row in it:
		x.table.append(x.new_tag('row', **{str(k): str(v) for k, v in row.items() if v is not None}))
	return x.prettify()

import json
def JSON(it):
	'''Takes an iterater of dicts and returns a json string'''
	return json.dumps([{str(k): str(v) for k, v in row.items()} for row in it])

def writeTableAsCsv(table, fileName, *headers):
	'''Write the contents of this DataTable to a file with the given name in the standard csv format'''
	if not headers:
		headers = table.headers()
	with open(os.path.expanduser(fileName), 'w') as f:
		f.write(table | CSV_GivenHeaders(*headers))


#The following are column filters.  Typical usage:
# dt = DataTable(...)
# withoutEmptyColumns = dt ^ emptyColumns
noneColumns = lambda c: set(c) == {None}
emptyColumns = lambda c: not any(c)
hasValueColumns = lambda c: any(c)
singleValueColumns = lambda c: len(set(c)) == 1

def convertColumns(columnReplacements):
	'''given a map of column -> convertMethod, returns the appropriate dict which replaces each value for that column with convertMethod(value)
e.g.
dt = DataTable(...)
print dt & convertColumns({'accountId': int, 'value': float, 'startDate': parseDate})

instead of:
print dt & {'accountId': lambda row: int(row.accountId), 'value': lambda row: float(row.value), 'startDate': lambda row: parseDate(row.startDate)}
	'''
	return {k: (lambda k, v: lambda row: v(row[k]))(k, v) for k,v in columnReplacements.items()}

def replaceNewLines(header, replacement='|'):
	'''replaceNewLines(header)
A convenience method for stripping out the newlines in a field - replaces new lines with pipe characters. Typical usage:
dt = DataTable(...)
print dt & replaceNewLines('field name')
'''
	return convertColumns({header: lambda value: value.replace('\n', replacement)})

def makeXml(header):
	'''makeXml(header)
swaps a column containing xml strings with BeautifulSoup nodes
usage: dt = DataTable(...) & makeXml('xml column')
'''
	return convertColumns({header: BeautifulSoup})

class AS_IS:
	'''Used for datatable.join otherFieldPrefix parameter if the datatable headers aren't strings and you want to preserve the headers as-is'''
	def __add__(self, other):
		return other
AS_IS = AS_IS()
