# LookupTables


## This class is deprecated.  It has been replaced by the table lpts_script_codes

class LookupTables:

	def scriptCode(script):
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
		return scriptDic.get(script)
