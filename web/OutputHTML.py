# OutputHTML.py
#
# This class outputs an HTML page as title headings and tables.
#

class OutputHTML:

	def __init__(self):
		styles = [ "<style>",
			"<!--",
			"caption {",
			"font-size: 16pt;",
			"padding-top: 1em;",
			"padding-bottom: 0.5em;",
			"}",
			"table {",
			"border: 1px solid black;",
			"border-collapse: collapse;",
			"}",
			"th {",
			"border: 1px solid black;",
			"background-color: #00CCFF;",
			"padding-left: 1em;",
			"padding-right: 1em;",
			"padding-top: 0.5em;",
			"padding-bottom: 0.5em;",
			"}",
			"td {",
			"border: 1px solid black;",
			"padding-top: 0.5em;",
			"padding-bottom: 0.5em;",
			"text-align: center",
			"}",
			"-->",
			"</style>" ]
		form = [ '<form action="sample.html">',
			'<p><input type="radio" name="ident" value="bibleid" checked>BibleId</input>',
			'<input type="radio" name="ident" value="filesetid">FilesetId</input>',
			'<input type="text" name="type" /></p>',
			'<p><br/><label>Detail:</label>',
			'<input type="radio" name="detail" value="no" checked>No</input>',
			'<input type="radio" name="detail" value="yes">Yes</input></p>',
			'<p><br/><label>Output:</label>',
			'<input type="radio" name="output" value="html" checked>HTML</input>',
			'<input type="radio" name="output" value="json">JSON</input></p>',
			'<p><br/><input type="submit"></p>',
			'</form>' ]

		self.output = []
		self.output.append("<html>\n")
		self.output.append("<head>\n")
		self.output.append("\n".join(styles))
		self.output.append("</head>\n")
		self.output.append("<body>\n")
		self.output.append("\n".join(form))
		
		
	def title(self, level, text):
		if level == 1:
			self.output.append("<h1>%s</h1>\n" % (text))
		elif level == 2:
			self.output.append("<h2>%s</h2>\n" % (text))
		elif level == 3:
			self.output.append("<h3>%s</h3>\n" % (text))
		else:
			self.output.append("<p>%s</p>\n" % (text))
			
			
	def table(self, name, columns, rows):
		self.output.append("<table>\n")
		if name != None:
			self.output.append("<caption>%s</caption>\n" % (name))
		self.output.append("<thead>\n")
		self.output.append("<tr>")
		for col in columns:
			self.output.append("<th>%s</th>" % (col))
		self.output.append("</tr>\n")
		self.output.append("</thead>\n")
		self.output.append("<tbody>\n")
		for row in rows:
			self.output.append("<tr>")
			for value in row:
				self.output.append("<td>%s</td>" % (value))
			self.output.append("</tr>\n")
		self.output.append("</tbody>\n")
		self.output.append("</table>\n")
		
		
	def json(self, name, columns, rows):
		self.output.append('table: "%s", rows: [ ' % (name))
		for row in rows:
			self.output.append(' { ')
			for index in range(len(columns)):
				if index > 0:
					self.output.append(', ')
				self.output.append('"%s": "%s"' % (columns[index]), row[index])
			self.output.append(' } ')
		self.append(' ] ')
		
			
			
	def close(self):
		self.output.append("</body>\n")
		self.output.append("</html>\n")
			
			
	def stdout(self):
		for line in self.output:
			print(line)
			
			
	def file(self, filename):
		fp = open(filename, "w")
		for line in self.output:
			fp.write(line)
		fp.close()
		

if __name__ == "__main__":
	html = OutputHTML()
	#html.title(1, "What?")
	html.table("caption", ["col1", "col2", "col3"], [[1, 2, 3], ["a", "b", "c"]])
	html.table("caption2", ["col4", "col5", "col6"], [[1, 2, 3], ["a", "b", "c"]])
	html.close()
	html.file("sample.html")
	
