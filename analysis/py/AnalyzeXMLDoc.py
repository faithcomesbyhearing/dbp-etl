#
# This program reads through the input file for lptsmanager, and
# lists the values found there for each data field.
#
from xml.dom import minidom
import io

XML_FILENAME = "FCBH Intro/qry_dbp4_Regular_and_NonDrama.xml"
OUT_FILENAME = "DBP4_Analyze_XML.txt"

out = io.open(OUT_FILENAME, mode="w", encoding="utf-8")
xmldoc = minidom.parse(XML_FILENAME)
nodeList = xmldoc.getElementsByTagName('qry_dbp4_Regular_and_NonDrama')

nodeDict = {}
for node in nodeList:
	for child in node.childNodes:
		if child.nodeType == 1:
			tagName = child.nodeName
			#print tagName, child.firstChild.nodeValue
			if tagName not in nodeDict:
				nodeDict[tagName] = set()
			nodeDict[tagName].add(child.firstChild.nodeValue)

#for key in nodeDict:
#	print key
keys = nodeDict.keys()
#print keys
keys2 = sorted(keys)
#print keys2
for key in keys2:
	#print key
	out.write("** %s **\n" % key)
	values = sorted(list(nodeDict[key]))
	for value in values:
		#print "\t", value
		out.write("\t\t-- %s\n" % value)