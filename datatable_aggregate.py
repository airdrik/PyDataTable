'''Set of methods to be used when reducing DataTable buckets
You are welcome to define your own methods (or callable classes), so long as they support the same call parameters
'''

def first(it):
	try:
		return it.next()
	except StopIteration:
		return None

class AggregateMethod(object):
	def __init__(self, field):
		self.field = field
	def __call__(self, bucket):
		return None

class First(AggregateMethod):
	def __call__(self, bucket):
		return bucket.column(self.field)[0]

class FirstNonBlank(AggregateMethod):
	def __call__(self, bucket): 
		return (b for b in bucket.column(self.field) if b).next()

class Sum(AggregateMethod):
	def __call__(self, bucket):
		return sum(bucket.column(self.field))

class Count:
	def __call__(self, bucket):
		return len(bucket)

class CountDistinct(AggregateMethod):
	'''Count the number of distinct values in a given field'''
	def __call__(self, bucket):
		return len(set(bucket.column(self.field)))

class DistinctValues(AggregateMethod):
	'''return a sorted list of distinct values for a given field'''
	def __call__(self, bucket):
		return sorted(set(bucket.column(self.field)))

class AllValues(AggregateMethod):
	'''return a list (in current order) of values for a given field'''
	def __call__(self, bucket):
		return list(bucket.column(self.field))

class ConcatDistinct:
	'''String-concatenate the distinct set of values using the given string to join the values'''
	def __init__(self, field, joinStr=','):
		self.joinStr = joinStr
		self.field = field
	def __call__(self, bucket):
		return self.joinStr.join(set(bucket.column(self.field)))

class Concat:
	'''String-concatenate all of the values using the given string to join the values'''
	def __init__(self, field, joinStr=','):
		self.joinStr = joinStr
		self.field = field
	def __call__(self, bucket):
		return self.joinStr.join(bucket.column(self.field))

class Value(AggregateMethod):
	'''returns the given value'''
	def __call__(self, bucket):
		return self.field

class Average(AggregateMethod):
	'''returns the average value for a given field'''
	def __call__(self, bucket):
		return sum(bucket.column(self.field)) / len(bucket)

class WeightedAverage:
	'''returns the average value for a given field, weighted by another column'''
	def __init__(self, averageField, weightField):
		self.averageField = averageField
		self.weightField = weightField
	def __call__(self, bucket):
		weightColumn = bucket.column(self.weightField)
		averageColumn = bucket.column(self.averageField)
		totalWeight = sum(weightColumn)
		weightedAverage = sum(averageColumn[i] * weightColumn[i] for i in range(len(bucket)))
		return weightedAverage / totalWeight

class Min(AggregateMethod):
	def __call__(self, bucket):
		return min(bucket.column(self.field))

class Max(AggregateMethod):
	def __call__(self, bucket):
		return max(bucket.column(self.field))

class Span:
	'''return the difference between the greatest and the least'''
	def __call__(self, bucket):
		return max(bucket.column(self.field)) - min(bucket.column(self.field))
