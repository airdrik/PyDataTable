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

def fromXML(s):
	'''Expects s to be an xml string
	For each child of the root node named "row", adds a datatable row and pulls the attributes into that row
	'''
	x = myxml.XmlNode(s)
	return DataTable(row.attributes() for row in x.row)

def fromCursor(cur, scrub=None):
	'''Expects cur to be a pysql 2.0 - style cursor and returns a (list of) DataTable(s) with the results
	optional parameter scrub is a method which is called for each header (row from cursor.description) to return a replace method 
		which is then called on each value for that header
		return None to do nothing on that header
	
	example - using adodbapi to connect to MS SQL server, the following will normalize smalldatetime fields to date objects and datetime fields to datetime objects:
	
	def parseCursor(cursor):
		def scrub(header):
			if header[1] == 135 and header[5] == 0: #135 is the sql datetime type, header[5] is the size of the field
				def toDate(dt):
					if isinstance(dt, datetime.datetime):
						return dt.date()
					return dt
				return toDate
			elif header[1] == 135:
				def toDateTime(dt):
					if isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime):
						return datetime.datetime(dt.year, dt.month, dt.day)
					return dt
				return toDateTime
			return None
		return fromCursor(cursor, scrub)
	'''
	if not cur.description:
		return DataTable()
	def result():
		headers = [h[0] for h in cur.description]
		theData = [AttributeDict(zip((h for h in headers), row)) for row in cur.fetchall()]
		if scrub is not None:
			for desc in cur.description:
				replace = scrub(desc)
				if replace is not None:
					for row in theData:
						row[desc[0]] = replace(row[desc[0]])
		return DataTable(theData)
	results = [result()]
	while cur.nextset():
		results.append(result())
	if len(results) == 1:
		return results[0]
	return results

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

