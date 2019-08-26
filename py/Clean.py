

class Clean:

	def __init__(self, database):
		self.database = database
		messages = []

	def process(self):
		for tbl in self.database.tables:
			for col in tbl.columns:
				for parm in col.parameters:
					# read value from hash map
					if parm.cleanup != None:
						print tbl.name, col.name, parm.name, parm.cleanup
						func = getattr(self, parm.cleanup)
						parm.value = func(value)
					else:
						parm.value = value

	## All cleanup functions go here
