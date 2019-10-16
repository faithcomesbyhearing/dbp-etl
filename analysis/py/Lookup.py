# Lookup.py

class Lookup:

	def usfmBookId(self, bookName):
		books = {
			'Genesis':   	'GEN',
			'Exodus':  		'EXO',
			'Leviticus':   	'LEV',
			'Numbers':   	'NUM',
			'Deuteronomy':  'DEU',
			'Joshua':  		'JOS',
			'Judges':  		'JDG',
			'Ruth':  		'RUT',
			'1Samuel':  	'1SA',
			'2Samuel':  	'2SA',
			'1Kings':  		'1KI',
			'2Kings':  		'2KI',
			'1Chronicles':  '1CH',
			'2Chronicles':  '2CH',
			'Ezra':  		'EZR',
			'Nehemiah':   	'NEH',
			'Esther':  		'EST',
			'Job':   		'JOB',
			'Psalms':    	'PSA',
			'Proverbs':  	'PRO',
			'Ecclesiastes': 'ECC',
			'SongofSongs':  'SNG',
			'Isaiah':   	'ISA',
			'Jeremiah':   	'JER',
			'Lamentations': 'LAM',
			'Ezekiel':  	'EZK',
			'Daniel':   	'DAN',
			'Hosea':   		'HOS',
			'Joel':  		'JOL',
			'Amos':  		'AMO',
			'Obadiah':  	'OBA',
			'Jonah': 		'JON',
			'Micah':   		'MIC',
			'Nahum':   		'NAM',
			'Habakkuk':   	'HAB',
			'Zephaniah':  	'ZEP',
			'Haggai':   	'HAG',
			'Zechariah':  	'ZEC',
			'Malachi':   	'MAL',
			'Matthew':  	'MAT',
			'Mark':  		'MRK',
			'Luke':  		'LUK',
			'John':  		'JHN',
			'Acts':  		'ACT',
			'Romans':   	'ROM',
			'1Corinthians': '1CO',
			'2Corinthians': '2CO',
			'Galatians':   	'GAL',
			'Ephesians':   	'EPH',
			'Philippians':  'PHP',
			'Colossians':   'COL',
			'1Thess':		'1TH',
			'2Thess':		'2TH',
			'1Timothy':  	'1TI',
			'2Timothy':  	'2TI',
			'Titus': 		'TIT',
			'Philemon':  	'PHM',
			'Hebrews':   	'HEB',
			'James':   		'JAS',
			'1Peter':  		'1PE',
			'2Peter':  		'2PE',
			'1John': 		'1JN',
			'2John': 		'2JN',
			'3John': 		'3JN',
			'Jude':  		'JUD',
			'Revelation':   'REV',
			# Spanish
			'Exodo': 		'EXO',
			'Levitico': 	'LEV',
			'Numeros': 		'NUM',
			'Deuteronomio':	'DEU',
			'Josue': 		'JOS',
			'Jueces': 		'JDG',
			'Rut': 			'RUT',
			'1Reyes': 		'1KI',
			'2Reyes': 		'2KI',
			'1Cronicas': 	'1CH',
			'2Cronicas': 	'2CH',
			'Esdras': 		'EZR',
			'Nehemias': 	'NEH',
			'Ester': 		'EST',
			'Salmos': 		'PSA',
			'Proverbios': 	'PRO',
			'Eclesiastes': 	'ECC',
			'Cantaras': 	'SNG',
			'Isaias': 		'ISA',
			'Jeremias': 	'JER',
			'Lamentacione': 'LAM',
			'Ezequiel': 	'EZK',
			'Oseas': 		'HOS',
			'Abdias': 		'OBA',
			'Jonas': 		'JON',
			'Miqueas': 		'MIC',
			'Habacuc': 		'HAB',
			'Sofonias': 	'ZEP',
			'Hageo': 		'HAG',
			'Zacarias': 	'ZEC',
			'Malaquias': 	'MAL',
			'San_Mateo':	'MAT',
			'San_Marcos':	'MRK',
			'San_Lucas':	'LUK',
			'San_Juan':		'JHN',
			'Hechos':		'ACT',
			'Romanos':		'ROM',
			'1Corintios':	'1CO',
			'2Corintios':	'2CO',
			'Galatas':		'GAL',
			'Efesios':		'EPH',
			'Filipenses':	'PHP',
			'Colosenses':	'COL',
			'1Tes':			'1TH',
			'2Tes':			'2TH',
			'1Timoteo':		'1TI',
			'2Timoteo':		'2TI',
			'Tito':			'TIT',
			'Filemon':		'PHM',
			'Hebreos':		'HEB',
			'Santiago':		'JAS',
			'1San_Pedro':	'1PE',
			'2San_Pedro':	'2PE',
			'1San_Juan':	'1JN',
			'2San_Juan':	'2JN',
			'3San_Juan':	'3JN',
			'Judas':		'JUD',
			'Apocalipsis':	'REV',
			# Portuguese
			'S_Mateus':		'MAT',
			'S_Marcos':		'MRK',
			'S_Lucas':		'LUK',
			'S_Joao':		'JHN',
			'Atos':			'ACT',
			'Colossenses':	'COL',
			'1Tess':		'1TH',
			'2Tess':		'2TH',
			'Hebreus':		'HEB',
			'S Tiago':		'JAS',
			'1Pedro':		'1PE',
			'2Pedro':		'2PE',
			'1S_Joao':		'1JN',
			'2S_Joao':		'2JN',
			'3S_Joao':		'3JN',
			'S_Judas':		'JUD',
			'Apocalipse':	'REV',
			# Indonesian
			'Matius':		'MAT',
			'Markus':		'MRK',
			'Lukas':		'LUK',
			'Yohanes':		'JHN',
			'Kisah Rasul':	'ACT',
			'Roma':			'ROM',
			'1Korintus':	'1CO',
			'2Korintus':	'2CO',
			'Galatia':		'GAL',
			'Efesus':		'EPH',
			'Filipi':		'PHP',
			'Kolose':		'COL',
			'1Tesalonika':	'1TH',
			'2Tesalonika':	'2TH',
			'1Timotius':	'1TI',
			'2Timotius':	'2TI',
			'Ibrani':		'HEB',
			'Yakobus':		'JAS',
			'1Petrus':		'1PE',
			'2Petrus':		'2PE',
			'1Yohanes':		'1JN',
			'2Yohanes':		'2JN',
			'3Yohanes':		'3JN',
			'Yudas':		'JUD',
			'Wahyu':		'REV',
			# Maasina Fulfulde
			'Matthieu':		'MAT',
			'Marc':			'MRK',
			'Luc':			'LUK',
			'Jean':			'JHN',
			'Actes':		'ACT',
			'Romains':		'ROM',
			'1Corinthiens':	'1CO',
			'2Corinthiens':	'2CO',
			'Galates':		'GAL',
			'Ephesiens':	'EPH',
			'Philippiens':	'PHP',
			'Colossiens':	'COL',
			'1Thess':		'1TH',
			'2Thess':		'2TH',
			'1Timothee':	'1TI',
			'2Timothee':	'2TI',
			'Tite': 		'TIT',
			'Philemon': 	'PHM',
			'Hebreux': 		'HEB',
			'Jacques': 		'JAS',
			'1Pierre': 		'1PE',
			'2Pierre': 		'2PE',
			'1Jean':		'1JN',
			'2Jean':		'2JN',
			'3Jean':		'3JN',
			'Jude':			'JUD',
			'Apocalypse': 	'REV'
		}
		result = books.get(bookName, None)
		return result



