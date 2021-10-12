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
		styles22 = ('// sets\n' +
			'$gl-ms         : "screen and (max-width: 23.5em)"; // up to 360px\n' +
			'$gl-xs         : "screen and (max-width: 35.5em)"; // up to 568px\n' +
			'$gl-sm         : "screen and (max-width: 48em)";   // max 768px\n' +
			'$gl-md         : "screen and (max-width: 64em)";   // max 1024px\n' +
			'$gl-lg         : "screen and (max-width: 80em)";   // max 1280px\n' +
			'// table style\n' +
			'table {\n' +
			'	border-spacing: 1;\n' +
			'	border-collapse: collapse;\n' +
			'	background:white;\n' +
			'	border-radius:6px;\n' +
			'	overflow:hidden;\n' +
			'	max-width:800px;\n' +
			'	width:100%;\n' +
			'	margin:0 auto;\n' +
			'	position:relative;\n' +
			'	* { position:relative }\n' +
			'	td,th { padding-left:8px}\n' +
			'	thead tr {\n' +
			'		height:60px;\n' +
			'		background:#FFED86;\n' +
			'		font-size:16px;\n' +
			'	}\n' +
			'	tbody tr { height:48px; border-bottom:1px solid #E3F1D5 ;\n' +
			'		&:last-child  { border:0; }\n' +
			'	}\n' +
			'	td,th { text-align:left;\n' +
			'		&.l { text-align:right }\n' +
			'		&.c { text-align:center }\n' +
			'		&.r { text-align:center }\n' +
			'	}\n' +
			'}\n' +
			'@media #{$gl-xs} {\n' +
			'table { display:block;\n' +
			'> *,tr,td,th { display:block }\n' +
			'thead { display:none }\n' +
			'tbody tr { height:auto; padding:8px 0;\n' +
			'td { padding-left:45%; margin-bottom:12px;\n' +
			'	&:last-child { margin-bottom:0 }\n' +
			'	&:before {\n' +
			'		position:absolute;\n' +
			'		font-weight:700;\n' +
			'		width:40%;\n' +
			'		left:10px;\n' +
			'		top:0\n' +
			'	}\n' +
			'	&:nth-child(1):before { content:"Code";}\n' +
			'	&:nth-child(2):before { content:"Stock";}\n' +
			'	&:nth-child(3):before { content:"Cap";}\n' +
			'	&:nth-child(4):before { content:"Inch";}\n' +
			'	&:nth-child(5):before { content:"Box Type";}\n' +
			'}\n' +      
		'}\n' +
	'}\n' +
'}\n')

#'// body style
#'body { 
#'  background:#9BC86A; 
#'  font:400 14px "Calibri","Arial";
#'  padding:20px;
#'}')
		self.output = []
		self.output.append("<html>\n")
		self.output.append("<head>\n")
		#self.output.append("<!--\n")
		self.output.append("\n".join(styles))
		#for style in styles:
		#	self.output.append(style + "\n")
		#self.output.append(styles)
		#self.output.append("\n-->\n")
		self.output.append("</head>\n")
		self.output.append("<body>\n")
		
		
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
	
