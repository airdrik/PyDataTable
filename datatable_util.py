class AttributeDict(dict):
	'''
	Subclass of dict that allows access to items using dot-notation:
d = AttributeDict()
d['a'] = 1
print d.a # 1
	'''
	def __init__(self, *args, **vargs):
		super(AttributeDict, self).__init__(*args, **vargs)
		self.__dict__ = self
	def __iadd__(self, other):
		self.update(other)
		return self
	def __add__(self, other):
		d = AttributeDict(self)
		d += other
		return d

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

import myxml
def XML(it):
	'''Takes an iterator which yields dicts and returns an xml formatted string
	The root node is named 'table', the rows are represented by 'row' nodes, whose attributes are the key-value pairs from the dict
	'''
	x = myxml.XmlNode(name='table')
	for row in it:
		x.appendChild(myxml.XmlNode(name='row', **dict((unicode(k), unicode(v)) for k, v in row.iteritems() if v is not None)))
	return x.prettyPrint()

import json
def JSON(it):
	'''Takes an iterater of dicts and returns a json string'''
	return json.dumps([dict((unicode(k), unicode(v)) for k, v in row.iteritems()) for row in it])

#The following are column filters.  Typical usage:
# dt = DataTable(...)
# withoutEmptyColumns = dt ^ emptyColumns
noneColumns = lambda c: set(c) == set([None])
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
	return dict((lambda k, v: (k, lambda row: v(row[k])))(k, v) for k,v in columnReplacements.items())

def replaceNewLines(header, replacement='|'):
	'''replaceNewLines(header)
A convenience method for stripping out the newlines in a field - replaces new lines with pipe characters. Typical usage:
dt = DataTable(...)
print dt & replaceNewLines('field name')
'''
	return convertColumns({header: lambda value: value.replace('\n', replacement)})

def makeXml(header):
	'''makeXml(header)
swaps a column containing xml strings with myxml XmlNodes
usage: dt = DataTable(...) & makeXml('xml column')
'''
	return convertColumns({header: myxml.XmlNode})

class AddsNothing:
	'''Used for datatable.join otherFieldPrefix parameter if the datatable headers aren't strings and you want to preserve the headers as-is'''
	def __add__(self, other):
		return other
