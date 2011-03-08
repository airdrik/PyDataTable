'''myxml.py
Author: Eric Francis

Contents:
	XmlParseError
		Exception raised when something goes wrong parsing the Xml document
	splitAllTags(string)
		helper function which splits up the xml tags, including the space between each tag
		used only during the construction of an XmlNode
	splitTag(string)
		helper function which splits up a tag into the name, attributes as a dict, tail ('/' if 
		self-closing tag, '' otherwise)
		used only during the construction of an XmlNode
	indent(string)
		helper function which adds a tab character to the beginning of the string and the 
		beginning of every line (replaces all end-of-lines with \n\t)
	XmlNode
		The Xml class.
	XmlNodeList
		returned by most of XmlNode's methods that return a collection of nodes

Ideals:
	I wanted something relatively quick and simple for representing an xml document in python.  
The parsing preserves everything between tags for reference, and to preserve fairely closely: 
str(XmlNode(xmlDocumentString)) == xmlDocumentString.  I say fairly closely because in the 
process of parsing, whitespace inside the nodes themselves gets reset to a single space 
between each attribute and no spaces between the last attribute and the closing bracket of
the tag.  
	I did this because XML is a Markup Language, meaning the tags are meant to mark up the
text inside them, and so the XmlNode class preserves the inner text of the original document.
If you want to ignore the whitespace in the inner text of the document, or otherwise want to 
do nice xml formatting, use the prettyPrint function (which currently only supports \n for new
lines and \t for indentation).

	More important than document content preservation, I wanted a simple way of representing
xml in python.  XmlNode ignores w3c and is a quick-and-dirty way of parsing xml so you can 
do stuff with it.  
'''

entities = [('quot', '"'),
	('apos', "'"),
	('lt', '<'),
	('gt', '>'),
	('amp', '&')]

def xmlEscape(s):#TODO: don't escape xml comments (<!-- -->, <![CDATA[ ]]>)
	for name, c in entities[::-1]:
		s = s.replace(c, '&%s;' % name)
	return s
def xmlUnEscape(s):
	for name, c in entities:
		s = s.replace('&%s;' % name, c)
	return s

def attrQuote(attr):
	attr = xmlEscape(attr)
	if '"' not in attr:
		return '"%s"' % attr
	if "'" not in attr:
		return "'%s'" % attr
	return '"%s"' % attr.replace('"', r'\"')
def attrUnQuote(attr):
	q = attr[0]
	attr = xmlUnEscape(attr)[1:-1]
	return attr.replace('\\'+q, q)

class XmlParseError(Exception):
	pass

class XmlTagList(list):
	pass
def splitAllTags(xmlString):
	'''splitAllTags(xmlString)
	splits up the tags and the inner text.  Returns a list of tagString,innerText,tagString,,, etc.
	If there is nothing between two tags splitAllTags will insert a '' between the two for consistency
'''
	#simulate a state machine with 5 states for walking through the xml string
	OUTSIDE, INSIDE, INQUOTE, INCOMMENT, INCDATA = 1,2,3,4,5
	escape = False
	j = 0
	tags = XmlTagList()
	state = OUTSIDE
	for i in range(len(xmlString)):
		if xmlString[i] == '\\':
			escape = not escape
		else:
			escape = False
		if state == OUTSIDE and xmlString[i] == '<':
			tag = xmlString[j:i]
			tags.append(tag)
			j = i
			state = INSIDE
		elif state == INSIDE and xmlString[j:i+1] == '<!--':
			state = INCOMMENT
		elif state == INCOMMENT and xmlString[j:i+1].endswith('-->'):
			tag = xmlString[j:i+1]
			tags.append(tag)
			j = i+1
			state = OUTSIDE
		elif state == INSIDE and xmlString[j:i+1] == '<![CDATA[':
			state = INCDATA
		elif state == INCDATA and xmlString[j:i+1].endswith(']]>'):
			tag = xmlString[j:i+1]
			tags.append(tag)
			j = i+1
			state = OUTSIDE
		elif state == INSIDE and xmlString[i] in '"\'':
			state = INQUOTE
			quoteChar = xmlString[i]
		elif state == INQUOTE and xmlString[i] == quoteChar and not escape:
			state = INSIDE
		elif state == INSIDE and xmlString[i] == '>':
			tag = xmlString[j:i+1]
			tags.append(tag)
			j = i+1
			state = OUTSIDE
	tag = xmlString[j:]
	if tag.strip():
		tags.append(tag)
	return tags

def splitTag(xmlTagString):
	'''splitTag(xmlTagString)
splits up a tag into tagName, attributeDict, tail
	tagName is everything from (not including) the < to the first space
	attributeDict is a dictionary resulting from evaluating the attribute="value" fields in the tag
	tail is '/' if the tag ends in '/>' or '' otherwise
	
	throws XmlParseError if things don't fit
'''
	try:
		parts = xmlTagString.split()
		name = parts.pop(0)
		if parts:
			parts_tmp = [name, parts.pop(0)]
		else:
			parts_tmp = [name]
		while parts:
			part = parts_tmp[-1]
			next = parts.pop(0)
			if part[-1] == '/':
				parts_tmp[-1] = part + next
			elif '=' not in part and next.startswith('='):
				parts_tmp[-1] = part + next
			elif '=' not in part:
				print part
				print parts_tmp
				print parts
				raise XmlParseError("Error parsing xml tag.  Found garbage between attributes: [%s] in tag: %s" % (part, xmlTagString))
			elif part[-1] == '=':
				parts_tmp[-1] = part + next
			elif (part.split('=')[1].startswith('"') and part.replace(r'\"', '').count('"') % 2) or (part.split('=')[1].startswith("'") and part.replace(r"\'", '').count("'") % 2):
				parts_tmp[-1] = part + ' ' + next
			else:
				parts_tmp.append(next)
		parts = parts_tmp
		if parts[-1].rstrip('/>'):
			tail = parts.pop()
			lastAttr = tail.rstrip('/>')
			parts += [lastAttr, tail[tail.rindex(lastAttr[-1])+1:]]
		attrs = {}
		for part in parts[1:-1]:
#			if '=' not in part:
#				raise XmlParseError("Error parsing xml tag.  Found garbage between attributes: [%s] in tag: %s" % (part, xmlTagString))
			attr, val = part.split('=', 1)
			if val[0] in '"\'':
				q = val[0]
				if q in val[1:-1].replace('\\'+q, ''):
					raise XmlParseError("Error parsing xml tag: extra quote found in attribute value: " + xmlTagString)
			elif '"' in val or "'" in val:
				raise XmlParseError("Error parsing xml tag: quote found in unquoted attribute value: " + xmlTagString)
			if '"' in attr or "'" in attr:
				raise XmlParseError("Error parsing xml tag: quote found in attribute name: " + xmlTagString)
			attrs[attr] = attrUnQuote(val)
		return parts[0][1:], attrs, parts[-1][:-1]
	except XmlParseError:
		raise
	except Exception, e:
		print parts
		raise XmlParseError("Error parsing xml tag: %s:%s" % (e.message, xmlTagString), e)

def indent(xmlString, newLineStr = '\n', indentation = '\t'):
	'''indent(string, newLineStr = '\n', indentation = '\t')
indents each line in the string'''
	return newLineStr.join(indentation + line for line in xmlString.split(newLineStr))

class XmlNode(object):
	'''XmlNode class
	The xml node class.  Allows access to node attributes using the . notation (e.g. node.attribute), 
and to lists of children using subscription (e.g. node['childName']).  Note: subscription returns the 
list of all child nodes of that name.
	Also defines the following special attributes: 
		innerText:	The concatenation of all text inside between this node's begin and end tag
stripped of markup
		innerXml:	Everything between this node's begin and end tags, including all markup, 
exluding this node's tags
		outerXml:	The innerXml + this node's begin and end tags.  
			Note: node.outerXml is equivalent to str(node) (in fact, node.outerXml returns str(node)
		numChildren:	Returns how many direct child nodes belong to this node
		numAttributes:	Returns how many attributes this node has
	'''
	def __new__(cls, *args, **kwds):
		self = super(XmlNode, cls).__new__(cls)
		self.__attributes = {}
		self.name = ''
		self.__pre = ''
		self.__post = ''
		self.__childNodes = []
		self.__innerText = []
		self.parent = None
		return self
		
	def __init__(self, xmlDocument = None, *nodes, **attributes):
		'''XmlNode(xmlDocument = None)
	Initializes a new XmlNode object and parses xmlDocument (if provided)
	xmlDocument may be a file-like object with a 'read' method, in which case it will parse whatever 
		string is obtained by calling read on that object
	xmlDocument may otherwise be a list of strings (as obtained by the splitAllTags function), or a single string
		'''
		if xmlDocument:
			if isinstance(xmlDocument, XmlNode):
				nodes = (xmlDocument,) + nodes
			elif 'read' in dir(xmlDocument):
				self.parse(xmlDocument.read())
			elif isinstance(xmlDocument, str) or isinstance(xmlDocument, unicode) or isinstance(xmlDocument, XmlTagList):
				self.parse(xmlDocument)
			elif '__iter__' in dir(xmlDocument):
				nodes = tuple(xmlDocument) + nodes
			else:
				raise TypeError("Unrecognized type passed into XmlNode constructor, Type: %s, Object: %s" % (type(xmlDocument), str(xmlDocument)))
		for node in nodes:
			if not isinstance(node, XmlNode):
				raise Exception("Passed in node wasn't an instance of an XmlNode: " + node)
			node.parent = self
		self.__childNodes += nodes
		self.__innerText += [''] * (len(self.__childNodes) - len(self.__innerText) + 1)
		if 'name' in attributes:
			self.name = attributes['name']
			del attributes['name']
		if 'parent' in attributes:
			raise XmlParseError("Can't set parent of xml node in XmlNode constructor.  Use parent.appendChild(newChildNode) instead")
		self.__attributes.update(attributes)
	def parse(self, xmlTags):
		'''parse(self, xmlTags)
	expects xmlTags to be an XmlTagList (as returned by the splitAllTags function)
	parses name and attributes from the first string, replacing the name and updating the attributes
of the XmlNode instance.  Then if the first tag isn't self-closing, it appends innerText and child nodes 
from the string until it encounters the appropriate closing tag.
		'''
		if not isinstance(xmlTags, XmlTagList):
			xmlTags = splitAllTags(xmlTags)
		if not xmlTags:
			return
		tag = xmlTags.pop(0)
		while not tag.startswith('<') or tag.startswith('<!DOCTYPE') or tag.startswith('<?xml') or tag.startswith('<!--')  or xmlTags[0].startswith('<![CDATA['):
			self.__pre += tag
			tag = xmlTags.pop(0)
		name, attributes, tail = splitTag(tag)
		self.name = name
		self.__attributes.update(attributes)
		if not tail:
			while True:
				if not xmlTags[0].startswith('<') or xmlTags[0].startswith('<!--') or xmlTags[0].startswith('<![CDATA['):
					self.__innerText.append(xmlUnEscape(xmlTags.pop(0)))
					while not xmlTags[0].startswith('<') or xmlTags[0].startswith('<!--') or xmlTags[0].startswith('<![CDATA['):
						self.__innerText[-1] += xmlUnEscape(xmlTags.pop(0))
				else:
					self.__innerText.append('')
				if xmlTags[0].startswith('</' + self.name):
					break
				self.__childNodes.append(XmlNode(xmlTags))
			xmlTags.pop(0)
		if len(xmlTags) == 1:
			self.__post = xmlTags[0]
		for child in self.__childNodes:
			child.parent = self
	def innerText(self):
		'''innerText()
	returns the concatenation of all text inside this node, stripped of all xml tags.'''
		if not len(self.__innerText):
			return None
		s = ''
		for i in range(len(self.__childNodes)):
			s += self.__innerText[i]
			t = self.__childNodes[i].innerText()
			if t:
				s += t
		return s + self.__innerText[-1]
	def innerXml(self):
		'''innerXml()
	returns the concatenation of all text inside this node, including all inner xml tags.'''
		if not len(self.__innerText):
			return None
		s = ''
		for i,child in enumerate(self.__childNodes):
			s += xmlEscape(self.__innerText[i]) + str(child)
		return s + xmlEscape(self.__innerText[-1])
	def outerXml(self):
		'''outerXml()
	returns the concatenation of all of this node's text including this node's tag(s) and everything inside.
		equivalent to str(node)'''
		return str(self)
	def numChildren(self):
		'''numChildren()
	returns the number of direct inner nodes'''
		return len(self.__childNodes)
	def numAttributes(self):
		'''numAttributes()
	returns the number of attributes belonging to this node'''
		return len(self.__attributes)
	def __getitem__(self, attribute):
		'''xmlNode[attribute]
	Allow access to the node's attributes and name through the subcript notation.'''
		if attribute in self.__attributes:
			return self.__attributes[attribute]
		raise AttributeError(attribute)
	def __setitem__(self, attribute, value):
		'''xmlNode[attribute] = value
	Allow access to the node's attributes and name through the subscript notation.'''
		self.__attributes[attribute] = value
	def __getattr__(self, nodeName):
		'''xmlNode.nodeName
	returns this node's child node(s) matching the given node name.
	If there are no matching nodes, throws AttributeError
	If there are one or more matching nodes, returns the list of matching nodes'''
		children = [node for node in self.__childNodes if node.name == nodeName]
		if not len(children):
			raise AttributeError(nodeName)
		return XmlNodeList(children)
	def __ne__(self, other):
		return not self == other
	def __eq__(self, other):
		return self.name == other.name and self.__attributes == other.__attributes and self.__innerText == other.__innerText and self.__childNodes == other.__childNodes
	def __contains__(self, other):
		if isinstance(other, str) or isinstance(other, unicode):
			return other in self.__attributes
		if isinstance(other, XmlNode):
			if other in self.__childNodes:
				return True
			for node in self.__childNodes:
				if other in node:
					return True
			return False
		raise TypeError
	def __repr__(self):
		return 'XmlNode: ' + self.name + '.\tAttributes: ' + str(self.__attributes)
	def __str__(self):
		'''str(xmlNode)
	returns the entire contents of this node, including this node's tags and all inner text and tags
	The following condition should hold up:
	node = XmlNode(some string)
	node == XmlNode(str(node))
	'''
		s = ''
		if self.__pre:
			s = self.__pre
		s += '<' + self.name
		if self.__attributes:
			s += ' ' + ' '.join('%s=%s' % (attribute, attrQuote(self.__attributes[attribute])) for attribute in self.__attributes)
		innerXml = self.innerXml()
		if innerXml:
			s += '>%s</%s>' % (innerXml, self.name)
		else:
			s += '/>'
		s += self.__post
		return s
	def normalizeSpaces(self, newLineStr = '\n', indentStr = '\t', indentLevel = 0):
		'''normalizeSpaces(newLineStr = '\n', indentStr = '\t', indentLevel = 0)
	normalizes the whitespace around all of the tags using the given newline and indent strings.
	indentLevel is the starting indentation for this node's inner text, primarily used for successive indentation'''
		for i in range(len(self.__innerText)):
			self.__innerText[i] = self.__innerText[i].strip() + newLineStr + indentStr * indentLevel
		for node in self.iterChildren():
			node.normalizeSpaces(newLineStr, indentStr, indentLevel + 1)
	def prettyPrint(self, newLineStr = '\n', indentStr = '\t'):
		'''prettyPrint(newLine = '\n', indent = '\t')
	returns a version of the xmlNode with all whitespace around tags normalized so that new tags are always on new lines, 
as are closing tags if there is at least one newLineStr in the innerXml of that node.
	Child nodes are indented 1 indentStr further than their parent node'''
		s = self.__pre + '<' + self.name
		if self.__attributes:
			s += ' ' + ' '.join('%s=%s' % (attribute, attrQuote(self.__attributes[attribute])) for attribute in self.__attributes)
			s = ''.join(s.splitlines())
		if not len(self.__innerText):
			return s + '/>'
		innerXml = xmlEscape(self.__innerText[0].strip())
		for i,child in enumerate(self.__childNodes):
			innerXml += newLineStr + child.prettyPrint()
			if self.__innerText[i+1].strip():
				innerXml += newLineStr + self.__innerText[i + 1].strip()
		if innerXml:
			if newLineStr in innerXml:
				s += '>%s\n</%s>' % (indent(innerXml, newLineStr, indentStr), self.name)
			else:
				s += '>%s</%s>' % (innerXml, self.name)
		else:
			s += '/>'
		return s + self.__post
	def children(self):
		'''children()
	returns a list of this node's immediate children'''
		return XmlNodeList(self.__childNodes)
	def attributes(self):
		'''attributes()
	returns a copy of this node's attributes'''
		return dict(self.__attributes)
	def find(self, criteria=None):
		'''find(criteria=None)
	recursively collects nodes matching the given criteria.
	if criteria is a string, checks if the node's name = criteria
	otherwise assumes criteria is a callable (predicate) which is checked for each node'''
		return XmlNodeList(self.__find(criteria))
	def __find(self, criteria=None):
		if criteria is None:
			searchFunction = lambda node: True
		elif isinstance(criteria, str) or isinstance(criteria, unicode):
			searchFunction = lambda node: node.name == criteria
		else:
			searchFunction = criteria
		for node in self.__childNodes:
			if searchFunction(node):
				yield node
			for descendant in node.find(searchFunction):
				yield descendant
	def containsAttribute(self, attribute):
		return attribute in self.__attributes
	def appendChild(self, childNode):
		'''appendChild(childNode)
	adds the node as a direct child node after this node's last direct child node with empty inner text between the new node and this node's closing tag'''
		if not isinstance(childNode, XmlNode):
			childNode = XmlNode(childNode)
		self.__innerText.append('')
		self.__childNodes.append(childNode)
		childNode.parent = self

def bucket(pairs):
	buckets = {}
	for k,v in pairs:
		buckets.setdefault(k, []).append(v)
	return buckets

class XmlDiff(XmlNode):
	'''XmlDiff(XmlNode)
	The result of diffing two XmlNode instances in a hierarchical manner.
	If the nodes have different names than the whole tree is considered different.
	Otherwise the attributes are compared and the differences stored as attributes on this node:
		If an attribute has different names in the two nodes, the two values are stored in a tuple
		If an attribute which is present in one node is missing in the other, the entry for the missing attribute is stored as None
	Then child nodes are compared.  
		Child nodes in one node lacking nodes with the same name in the other are stored with an added attribute: _XmlDiff_ with the value of "added" or "removed"
		Child nodes with the same name in both nodes will be compared in order (and any extras in one will be handled as if there isn't a corresponding node in the other)
	'''
	def __init__(self, xml1, xml2):
		self._different = False
		if xml1 is None and xml2 is None:
			raise Exception("Can't diff two None's")
		if xml1 is None or xml2 is None or xml1.name != xml2.name:
			self.name = 'xml_diff'
			self.__children = (xml1,xml2)
			self._different = True
			return
		self.name = xml1.name
		for attribute in xml1.attributes():
			if not xml2.containsAttribute(attribute):
				self.__attributes[attribute] = xml1[attribute],None
				self._different = True
			elif xml1[attribute] != xml2[attribute]:
				self.__attributes[attribute] = xml1[attribute],xml2[attribute]
				self._different = True
		for attribute in xml2.attributes():
			if not xml1.containsAttribute(attribute):
				self.__attributes[attribute] = None,xml2[attribute]
				self._different = True
		bucket1 = bucket((node.name, node) for node in xml1.children())
		bucket2 = bucket((node.name, node) for node in xml2.children())
		for name in set(bucket1.keys()).union(bucket2.keys()):
			if name not in bucket1:
				for node in bucket2[name].values():
					self.appendChild(XmlDiff(None, node))
			elif name not in bucket2:
				for node in bucket1[name].values():
					self.appendChild(XmlDiff(node, None))
			else:
				children1 = iter(bucket1[name])
				children2 = iter(bucket2[name])
				for child1 in children1:
					try:
						child2 = children2.next()
						self.appendChild(XmlDiff(child1, child2))
					except StopIteration:
						self.appendChild(XmlDiff(child1, None))
				for child2 in children2:
					self.appendChild(XmlDiff(None, child2))
				

class XmlNodeList(object):
	def __init__(self, nodeList):
		self.__nodeList = [node for node in nodeList]
	def __repr__(self):
		return 'XmlNodeList.  Length: %d' % len(self)
	def find(self, criteria=None):
		'''find(criteria=None)
	recursively collects nodes matching the given criteria.
	if criteria is a string, checks if the node's name = criteria
	otherwise assumes criteria is a callable (predicate) which is checked for each node'''
		return XmlNodeList(self.__find(criteria))
	def __find(self, criteria=None):
		if criteria is None:
			searchFunction = lambda node: True
		elif isinstance(criteria, str) or isinstance(criteria, unicode):
			searchFunction = lambda node: node.name == criteria
		else:
			searchFunction = criteria
		for node in self.__nodeList:
			if searchFunction(node):
				yield node
			for descendant in node.find(searchFunction):
				yield descendant
	def withAttribute(self, attribute, attributeValue = None):
		if attributeValue is not None:
			searchFunction = lambda node: node.containsAttribute(attribute) and node[attribute] == attributeValue
		else:
			searchFunction = lambda node: node.containsAttribute(attribute)
		return self.where(searchFunction)
	def withChildNode(self, nodeName = None, searchFunction = None):
		if nodeName is None and searchFunction is None:
			searchFunction = lambda node: True
		if nodeName is not None:
			searchFunction = lambda node: node.name == nodeName and searchFunction(node)
		return self.where(lambda node: any(child for child in node.children() if searchFunction(child)))
	def where(self, searchFunction):
		return XmlNodeList(node for node in self if searchFunction(node))
	def named(self, nodeName):
		return self.where(lambda node: node.name == nodeName)
	def parent(self):
		return XmlNodeList(set(node.parent for node in self))
	def __iter__(self):
		return iter(self.__nodeList)
	def __len__(self):
		return len(self.__nodeList)
	def __getitem__(self, key):
		'''
		if key is an int or a slice, return that view into the list
		otherwise, return the attributes of those nodes in the list that have that attribute
		'''
		if isinstance(key, int):
			return self.__nodeList[key]
		elif isinstance(key, slice):
			return XmlNodeList(self.__nodeList[key])
		return [node[key] for node in self if key in node]
	def __getattr__(self, attr):
		return XmlNodeList(node for node in self.__children() if node.name == attr)
	def children(self):
		return XmlNodeList(self.__children())
	def __children(self):
		for node in self:
			for child in node.children():
				yield child
	def __add__(self, other):
		newList = XmlNodeList(self)
		newList += other
		return newList
	def __iadd__(self, other):
		self.__nodeList += [node for node in other]
	def __contains__(self, node):
		return node in self.__nodeList
	def sort(self, cmp=None, key=None):
		self.__nodeList.sort(cmp, key)
