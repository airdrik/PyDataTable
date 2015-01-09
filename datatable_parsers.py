from datatable_util import AttributeDict
from datatable import DataTable
from hierarchies import Hierarchy
from bs4 import BeautifulSoup
from urllib.request import urlopen
import csv

def parseFixedWidth(f, headers):
	'''
Parses a Fixed Width file using the given headers
headers are a list of the tuples: (name, start, end),
	where name is the hame of the column,
	start is the (1-based) index of the beginning of the column and
	end is the index of the end of the column
'''
	headers = [(header[0], int(header[1])-1, int(header[2])) for header in headers]
	def parse():
		for line in f:
			d = AttributeDict()
			for header in headers:
				d[header[0]] = line[header[1]:header[2]]
			yield d
	return DataTable(parse())

def parseFixedWidthSpaceDelimited(f):
	'''
Same as parseFixedWidth, but guesses at the headers by assuming that each field has a single space before it (except the first column)
	'''
	lines = f.read().splitlines()
	minlen = min(len(line) for line in lines)
	splits = [col for col in range(minlen) if all(row[col] == ' ' for row in lines)]
	return parseFixedWidth(lines[1:], [(lines[0][start+1:end].strip(), start+2, end) for start, end in zip([-1]+splits, splits+[len(lines[0])])])

def parseCsv(f, headers=None, sep=',', quot='"'):
	return DataTable(AttributeDict(line) for line in csv.DictReader(f, fieldnames=headers, delimiter=sep, quotechar=quot))

def fromXML(s):
	'''Expects s to be an xml string
	For each child of the root node named "row", adds a datatable row and pulls the attributes into that row
	'''
	return DataTable(row.attrs for row in BeautifulSoup(s).find_child('row'))

def fromCursor(cur, scrub=None, customScrub=None, indexedResults=False, index=None):
	'''Expects cur to be a pysql 2.0 - style cursor and returns a (list of) DataTable(s) with the results
	optional parameter scrub is a method which is called for each header (row from cursor.description) to return a replace method
		which is then called on each value for that header
		return None to do nothing on that header
	optional parameters indexedResults and index are used to determine if the results should be collected in an indexed Hierarchy and what index to use in that Hierarchy

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
		if not cur.description:
			return None
		headers = [h[0] for h in cur.description]
		def zipHeaders(row):
			d = {}
			for i, h in enumerate(headers):
				if h not in d or d[h] is None:
					d[h] = row[i]
				elif row[i] and row[i] != d[h]:
					print 'WARNING: query returned multiple columns with the same name (%s) with conflicting data.  Taking the data from the first returned column' % h
			return d
		theData = [zipHeaders(row) for row in cur.fetchall()]
		if scrub is not None:
			for desc in cur.description:
				replace = scrub(desc)
				if replace is not None:
					for row in theData:
						row[desc[0]] = replace(row[desc[0]])
		if customScrub:
			for row in theData:
				for header, fromDbValue in customScrub.items():
					if row[header] is not None:
						try:
							row[header] = fromDbValue(row[header])
						except:
							pass
		if indexedResults:
			return Hierarchy.fromTable(theData, index, set(headers).difference(index))
		return DataTable(theData)
	results = [result()]
	while cur.nextset():
		r = result()
		if r:
			results.append(r)
	if len(results) == 1:
		return results[0]
	return results

def fromDataUrl(url):
	u = urlopen(url)
	s = BeautifulSoup(u.read())
	u.close()
	headers = [td.string for td in s.find('thead').find_children('td')]
	data = [[td.string for td in tr.find_children('td')] for tr in s.find('tbody').find_children('tr')]
	return DataTable(dict(zip(headers, row)) for row in data)
