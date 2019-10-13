

class Validate:

	def __init__(self, database):
		self.database = database
		self.messages = []

	def process(self):
		for tbl in self.database.tables:
			for col in tbl.columns:
				for parm in col.parameters:
					if parm.validate != None:
						print tbl.name, col.name, parm.name, parm.validate
						func = getattr(self, parm.validate)
						# read value from hash map
						message = func(value)
						if message != None:
							self.messages.append([tbl.name, col.name, parm.type, parm.name, message])

	## All Validate functions go here.
