from collections import defaultdict
from hierarchies import AttributeDict, makeHierarchyFromTable

class DataTableException(Exception):
	pass

import types

def CSV(it):
	'''Takes an iterator which yields dicts with common keys and returns a CSV string for that data'''
	l = [line for line in it]
	if not l:
		return ''
	def quoteField(field):
		f = str(field)
		if ',' in f:
			return '"%s"' % f
		return str(f)
	headers = sorted(l[0].keys())
	return '\n'.join([','.join(quoteField(h) for h in headers)] + [','.join(quoteField(line[header]) for header in headers) for line in l])

def FIXEDWIDTH(it):
	'''Takes an iterator which yields dicts with common keys and returns a fixed-width formatted string (primarily for printing)'''
	l = [row for row in it]
	if not l:
		return ''
	headers = sorted(l[0].keys())
	l = [tuple(headers)] + [tuple(str(row[h]) for h in headers) for row in l]
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
		x.appendChild(myxml.XmlNode(name='row', **dict((k,unicode(v)) for k,v in row.iteritems() if v is not None)))
	return x.prettyPrint()

#The following are column filters.  Typical usage:
# dt = DataTable(...)
# withoutEmptyColumns = dt ^ emptyColumns
noneColumns = lambda c: set(c) == set([None])
emptyColumns = lambda c: not any(c)
hasValueColumns = lambda c: any(f for f in c)
singleValueColumns = lambda c: len(set(c)) == 1

#The following is a convenience method for stripping out the newlines in a field. Typical usage:
# dt = DataTable(...)
# print dt & replaceNewLines('field name')
replaceNewLines = lambda header: {header: lambda row: row[header].replace('\n','|')}

