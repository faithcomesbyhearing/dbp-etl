

class Clean:

	def __init__(self, database):
		self.database = database
		#messages = []

	def process(self):
		for tbl in self.database.tables:
			for col in tbl.columns:
				for parm in col.parameters:
					if parm.clean != None:
						#print(tbl.name, col.name, parm.name, parm.clean)
						func = getattr(self, parm.clean)
						parm.value = func(parm.value)

	## All cleanup functions go here

	def nullify1(self, value):
		if value == None:
			return None
		val = value.strip()
		if val == "" or val == "#N/A" or val == "N/A":
			return None
		else:
			return val

	def tbd(self, value):
		return value


