# LookupTables



class LookupTables:

"""
	def __init__(self):
		self.otBooks=[	"GEN", "EXO", "LEV", "NUM", "DEU", "JOS", "JDG", "RUT",
			"1SA", "2SA", "1KI", "2KI", "1CH", "2CH", "EZR", "NEH",
			"EST", "JOB", "PSA", "PRO", "ECC", "SNG", "ISA", "JER",
			"LAM", "EZK", "DAN", "HOS", "JOL", "AMO", "OBA", "JON",
			"MIC", "NAM", "HAB", "ZEP", "HAG", "ZEC", "MAL"]
		self.ntBooks=[	"MAT", "MRK", "LUK", "JHN", "ACT", "ROM", "1CO", "2CO",
			"GAL", "EPH", "PHP", "COL", "1TH", "2TH", "1TI", "2TI",
			"TIT", "PHM", "HEB", "JAS", "1PE", "2PE", "1JN", "2JN",
			"3JN", "JUD", "REV"]
		self.apBooks=[	"1ES", "1MA", "1MQ", "2BA", "2ES", "2MA", "2MQ", "3MA",
			"3MQ", "4BA", "4MA", "5EZ", "6EZ", "BAR", "BEL", "DAG",
			"ENO", "ESG", "EZA", "JDT", "JUB", "LAO", "LBA", "LJE",
			"MAN", "ODA", "PS2", "PS3", "PSS", "REP", "S3Y", "SIR",
			"SUS", "TOB", "WIS"]
"""

	def scriptCode(self, script):
		scriptDic = {
			"Amharic":"Ethi", 
			"Arabic":"Arab", 
			"Armenian":"Armn",
			"Bengali":"Beng", 
			"Bengali Script":"Beng",
			"Berber":"Tfng",
			"Burmese":"Mymr", 
			"Canadian Aboriginal Syllabic":"Cans", 
			"Canadian Aboriginal Syllabics":"Cans", 
			"Cherokee Sylabary":"Cher", 
			"Cyrillic":"Cyrl", 
			"Devanagari":"Deva", 
			"Devangari":"Deva", 
			"Ethiopic":"Ethi", 
			"Ethoiopic":"Ethi", 
			"Ethopic":"Ethi", 
			"Ge'ez":"Ethi", 
			"Greek":"Grek", 
			"Gujarati":"Gujr", 
			"Gurmukhi":"Guru", 
			"Han":"Hani", 
			"Hangul (Korean)":"Kore", 
			"Hebrew":"Hebr", 
			"Japanese":"Jpan", 
			"Kannada":"Knda", 
			"Khmer":"Khmr", 
			"Khmer Script":"Khmr", 
			"Lao":"Laoo", 
			"Latin":"Latn", 
			"Latin (Africa)":"Latn", 
			"Latin (African)":"Latn", 
			"Latin (Latin America)":"Latn", 
			"Latin (Latin American)":"Latn", 
			"Latin (PNG)":"Latn", 
			"Latin (SE Asia)":"Latn", 
			"Malayalam":"Mlym", 
			"NA":"Zyyy", 
			"Oriya":"Orya", 
			"Tamil":"Taml", 
			"Telugu":"Telu", 
			"Thai":"Thai", 
			"Tibetan":"Tibt"}
			# not found should be a fatal error
		return scriptDic[script]
