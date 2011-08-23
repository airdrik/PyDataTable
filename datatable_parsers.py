from datatable_util import AttributeDict
from datatable import DataTable
from myxml import XmlNode
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

def parseCsv(f, headers=None, sep=',', quot='"'):
	return DataTable(AttributeDict(line) for line in csv.DictReader(f, fieldnames=headers, delimiter=sep, quotechar=quot))

def fromXML(s):
	'''Expects s to be an xml string
	For each child of the root node named "row", adds a datatable row and pulls the attributes into that row
	'''
	return DataTable(row.attributes() for row in XmlNode(s).row)

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
