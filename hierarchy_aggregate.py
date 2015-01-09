'''Set of methods to be used when reducing Hierarchy rows
You are welcome to define your own methods (or callable classes), so long as they support the same call parameters
In particuler, the first parameter is the dict of keyHeader : key for the current hierarchy leaf (bucket)
	and the second parameter is the current hierarchy leaf (which is a list of AttributeDicts)
'''

def first(it):
	for i in it:
		return i
	return None

class AggregateMethod(object):
	'''convenience parent class for various aggregation methods which operate on a single key
provides a default implementation of __call__ which checks if the field is in the parentKey or not and 
	dispatches to aggregateParentKey or aggregateRows accordingly
	'''
	def __init__(self, field):
		self.field = field
	def __call__(self, parentKey, rows):
		if self.field in parentKey:
			return self.aggregateParentKey(parentKey[self.field], len(rows))
		return self.aggregateRows(rows)
	def aggregateParentKey(self, parentKey, rowCount):
		return parentKey
	def aggregateRows(self, rows):
		return None

class First(AggregateMethod):
	def aggregateRows(self, rows):
		return first(row[self.field] for row in rows)

class FirstNonBlank(AggregateMethod):
	def aggregateRows(self, rows):
		return first(row[self.field] for row in rows if row[self.field])

class Sum(AggregateMethod):
	def aggregateParentKey(self, parentKey, rowCount):
		return rowCount * parentKey
	def aggregateRows(self, rows):
		return sum(row[self.field] for row in rows)

class Count:
	def __init__(self):
		pass
	def __call__(self, parentKey, rows):
		return len(rows)

class CountDistinct(AggregateMethod):
	'''Count the number of distinct values in a given field'''
	def aggregateParentKey(self, parentKey, rowCount):
		return 1
	def aggregateRows(self, rows):
		return len(set(row[self.field] for row in rows))

class DistinctValues(AggregateMethod):
	'''return a sorted list of distinct values for a given field'''
	def aggregateParentKey(self, parentKey, rowCount):
		return [parentKey]
	def aggregateRows(self, rows):
		return sorted({row[self.field] for row in rows})

class AllValues(AggregateMethod):
	'''return a list (in current order) of values for a given field'''
	def aggregateParentKey(self, parentKey, rowCount):
		return [parentKey] * rowCount
	def aggregateRows(self, rows):
		return [row[self.field] for row in rows]

class ConcatDistinct:
	'''String-concatenate the distinct set of values using the given string to join the values'''
	def __init__(self, field, joinStr=','):
		self.joinStr = joinStr
		self.field = field
	def aggregateParentKey(self, parentKey, rowCount):
		return str(parentKey)
	def aggregateRows(self, rows):
		return self.joinStr.join(set(str(row[self.field]) for row in rows))

class Concat:
	'''String-concatenate all of the values using the given string to join the values'''
	def __init__(self, field, joinStr=','):
		self.joinStr = joinStr
		self.field = field
	def aggregateParentKey(self, parentKey, rowCount):
		return self.joinStr.join([str(parentKey)] * rowCount)
	def aggregateRows(self, rows):
		return self.joinStr.join(str(row[self.field]) for row in rows)

class Value(AggregateMethod):
	'''returns the given value'''
	def __call__(self, parentKey, rows):
		return self.field

class Average(AggregateMethod):
	'''returns the average value for a given field'''
	def aggregateRows(self, rows):
		return sum(row[self.field] for row in rows) / len(rows)

class WeightedAverage:
	'''returns the average value for a given field, weighted by another column'''
	def __init__(self, averageField, weightField):
		self.averageField = averageField
		self.weightField = weightField
	def __call__(self, parentKey, rows):
		if self.averageField in parentKey: # weighted average of x = x
			return parentKey[self.averageField]
		if self.weightField in parentKey: # straight average
			return sum(row[self.averageField] for row in rows) / len(rows)
		totalWeight = sum(row[self.weightField] for row in rows)
		weightedAverage = sum(row[self.averageField] * row[self.weightField] for row in rows)
		return weightedAverage / totalWeight

class Min(AggregateMethod):
	def aggregateRows(self, rows):
		return min(row[self.field] for row in rows)

class Max(AggregateMethod):
	def aggregateRows(self, rows):
		return max(row[self.field] for row in rows)

class Span(AggregateMethod):
	'''return the difference between the greatest and the least'''
	def aggregateParentKey(self, parentKey, rowCount):
		return 0
	def aggregateRows(self, rows):
		return max(row[self.field] for row in rows) - min(row[self.field] for row in rows)
