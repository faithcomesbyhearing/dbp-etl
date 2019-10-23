# LookupTables



class LookupTables:

	def __init__(self):
		# these are not being used yet?
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

	def bookIdBySequence(self, seqCode):
		seqDict = {
			"B01": "MAT",
			"B02": "MRK",
			"B03": "LUK",
			"B04": "JHN",
			"B05": "ACT",
			"B06": "ROM",
			"B07": "1CO",
			"B08": "2CO",
			"B09": "GAL",
			"B10": "EPH",
			"B11": "PHP",
			"B12": "COL",
			"B13": "1TH",
			"B14": "2TH",
			"B15": "1TI",
			"B16": "2TI",
			"B17": "TIT",
			"B18": "PHM",
			"B19": "HEB",
			"B20": "JAS",
			"B21": "1PE",
			"B22": "2PE",
			"B23": "1JN",
			"B24": "2JN",
			"B25": "3JN",
			"B26": "JUD",
			"B27": "REV",
			"A01": "GEN",
			"A02": "EXO",
			"A03": "LEV",
			"A04": "NUM",
			"A05": "DEU",
			"A06": "JOS",
			"A07": "JDG",
			"A08": "RUT",
			"A09": "1SA",
			"A10": "2SA",
			"A11": "1KI",
			"A12": "2KI",
			"A13": "1CH",
			"A14": "2CH",
			"A15": "EZR",
			"A16": "NEH",
			"A17": "EST",
			"A18": "JOB",
			"A19": "PSA",
			"A20": "PRO",
			"A21": "ECC",
			"A22": "SNG",
			"A23": "ISA",
			"A24": "JER",
			"A25": "LAM",
			"A26": "EZK",
			"A27": "DAN",
			"A28": "HOS",
			"A29": "JOL",
			"A30": "AMO",
			"A31": "OBA",
			"A32": "JON",
			"A33": "MIC",
			"A34": "NAM",
			"A35": "HAB",
			"A36": "ZEP",
			"A37": "HAG",
			"A38": "ZEC",
			"A39": "MAL"}
		return seqDict.get(seqCode)



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
		return scriptDic[script] # not found should be a fatal error. That is intentional