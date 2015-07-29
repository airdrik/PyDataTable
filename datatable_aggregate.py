'''Set of aggregations to be used with DataTable's aggregate method
You are welcome to define your own classes, so long as they conform to the AggregateMethod interface
'''

def first(it):
	try:
		return next(it)
	except StopIteration:
		return None

class AggregateMethod(object):
	def newBucket(self, row):
		'''called the first time a row for a given aggregate key is encountered.
	The result of this method is passed into addRow for the current row
	Override this to set any starting values used within the aggregation
		'''
		return None
	def addRow(self, row, accumValue):
		'''called for each row, using the accumulated value associated with the row's aggregate key.
	The result is passed into addRow the next time a row for the same aggregate key is encountered
	Override this to provide accumulation logic (add the things, multiply the things, count the things, etc)
		'''
		return accumValue
	def finalize(self, accumValue):
		'''called for each aggregate key after all rows have been aggregated with the result from the last call to addRow for that aggregate key
	The result is added to the resulting table row in the column specified
	Override this to perform final calculations on the accumulated values to produce the final result (calculate averages, convert intermediate values into final results, etc.)
		'''
		return accumValue

class SingleFieldAggregateMethod(AggregateMethod):
	def __init__(self, field):
		self.field = field
	def newBucket(self, row):
		return row[self.field]

def makeAggMethod(fn):
	'''Wrapper function for converting an accumulation function into an aggregate method.
	the passed-in function fn should accept 2 parameters: row and accumValue and return the new accumValue for that aggregate key
	Uses the default newBucket (return None) and finalize (return value) methods and uses fn as the addRow method
	'''
	class ThisAggregateMethod(AggregateMethod):
		def addRow(self, row, accumValue):
			return fn(row, accumValue)
	return ThisAggregateMethod()

class First(SingleFieldAggregateMethod):
	# relies on the default implementation of SingleFieldAggregateMethod which is 
	pass

class FirstNonBlank(SingleFieldAggregateMethod):
	def addRow(self, row, accumValue):
		return accumValue or row[self.field]

class Sum(SingleFieldAggregateMethod):
	def newBucket(self, row):
		return 0
	def addRow(self, row, accumValue):
		return accumValue + row[self.field]

class Count(AggregateMethod):
	def newBucket(self, row):
		return 0
	def addRow(self, row, accumValue):
		return accumValue + 1

class CountDistinct(SingleFieldAggregateMethod):
	'''Count the number of distinct values in a given field'''
	def newBucket(self, row):
		return set()
	def addRow(self, row, accumValue):
		accumValue.add(row[self.field])
		return accumValue
	def finalize(self, accumValue):
		return len(accumValue)

class DistinctValues(SingleFieldAggregateMethod):
	'''return a set of distinct values for a given field'''
	def newBucket(self, row):
		return set()
	def addRow(self, row, accumValue):
		accumValue.add(row[self.field])
		return accumValue

class AllValues(SingleFieldAggregateMethod):
	'''return a list (in current order) of values for a given field'''
	def newBucket(self, row):
		return []
	def addRow(self, row, accumValue):
		accumValue.append(row[self.field])
		return accumValue

class ConcatDistinct(AggregateMethod):
	'''String-concatenate the distinct set of values using the given string to join the values'''
	def __init__(self, field, joinStr=','):
		self.joinStr = joinStr
		self.field = field
	def newBucket(self, row):
		return set()
	def addRow(self, row, accumValue):
		accumValue.add(row[self.field])
		return accumValue
	def finalize(self, accumValue):
		return self.joinStr.join(accumValue)

class Concat(AggregateMethod):
	'''String-concatenate all of the values using the given string to join the values'''
	def __init__(self, field, joinStr=','):
		self.joinStr = joinStr
		self.field = field
	def newBucket(self, row):
		return ''
	def addRow(self, row, accumValue):
		if not accumValue:
			return row[self.field]
		return accumValue + self.joinStr + row[self.field]
	def finalize(self, accumValue):
		return accumValue

class Value(SingleFieldAggregateMethod):
	'''returns the given value'''
	def newBucket(self, row):
		return self.field

class Average(SingleFieldAggregateMethod):
	'''returns the average value for a given field'''
	def newBucket(self, row):
		return (0, 0)
	def addRow(self, row, accumValue):
		return (accumValue[0] + row[self.field], accumValue[1] + 1)
	def finalize(self, accumValue):
		return accumValue[0] / accumValue[1]

class WeightedAverage(AggregateMethod):
	'''returns the average value for a given field, weighted by another column'''
	def __init__(self, averageField, weightField):
		self.averageField = averageField
		self.weightField = weightField
	def newBucket(self, row):
		return 0, 0
	def addRow(self, row, accumValue):
		weighting, totalWeight = accumValue
		return weighting + row[self.averageField] * row[self.weightField], totalWeight + row[self.weightField]
	def finalize(self, accumValue):
		weighting, totalWeight = accumValue
		return weighting / totalWeight

class Min(SingleFieldAggregateMethod):
	def addRow(self, row, accumValue):
		return accumValue if accumValue < row[self.field] else row[self.field]

class Max(SingleFieldAggregateMethod):
	def addRow(self, row, accumValue):
		return accumValue if accumValue > row[self.field] else row[self.field]

class Span(SingleFieldAggregateMethod):
	'''return the difference between the greatest and the least'''
	def newBucket(self, row):
		return row[self.field], row[self.field]
	def addRow(self, row, accumValue):
		minValue, maxValue = accumValue
		return minValue if minValue < row[self.field] else row[self.field], maxValue if maxValue > row[self.field] else row[self.field]
	def finalize(self, accumValue):
		minValue, maxValue = accumValue
		return minValue - maxValue

