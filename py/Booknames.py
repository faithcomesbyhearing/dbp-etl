# Booknames.py

class Booknames:

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
			'1Ezra':		'EZR',
			'Nehemiah':   	'NEH',
			'2Ezra':		'NEH',
			'Esther':  		'EST',
			'Job':   		'JOB',
			'Psalms':    	'PSA',
			'PSALM':		'PSA',
			'Proverbs':  	'PRO',
			'PROVERBS':		'PRO',
			'Ecclesiastes': 'ECC',
			'SongofSongs':  'SNG',
			'Song':			'SNG',
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
			'Zachariah':  	'ZEC',
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
			'Phil':			'PHP',
			'Colossians':   'COL',
			'1Thess':		'1TH',
			'1Thessalonians':'1TH',
			'2Thess':		'2TH',
			'2Thessalonians':'2TH',
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
			'Tobit':   		'TOB',
			'Judith':   	'JDT',
			'Wisdom':   	'WIS',
			'Sirach':   	'SIR',
			'Baruch':   	'BAR',
			'EpistJeremia':	'LJE',
			'1Maccabees':   '1MA',
			'2Maccabees':   '2MA',
			'3Maccabees':	'3MA',
			'4Maccabees':	'4MA',
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
			'Salmo':		'PSA',
			'salmo':		'PSA',
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
			'San_1uan':		'JHN',
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
			'1San_1uan':	'1JN',
			'2San_Juan':	'2JN',
			'2San_1uan':	'2JN',
			'3San_Juan':	'3JN',
			'3San_1uan':	'3JN',
			'Judas':		'JUD',
			'1udas':		'JUD',
			'Apocalipsis':	'REV',
			# Spanish Capitulo
			'Genesis_Capitulo':		'GEN',
			'Exodo_Capitulo':		'EXO',
			'Levetico_Capitulo':	'LEV',
			'Numeros_Capitulo':		'NUM',
			'Deuteronomio_Capitulo':'DEU',
			'Josue_Capitulo':		'JOS',
			'Jueces_Capitulo':		'JDG',
			'Ruth_Capitulo':		'RUT',
			'1Samuel_Capitulo':		'1SA',
			'2Samuel_Capitulo':		'2SA',
			'1Reyes_Capitulo':		'1KI',
			'2Reyes_Capitulo':		'2KI',
			'Esdras_Capitulo':		'EZR',
			'Esther_Capitulo':		'EST',
			'Job_Capitulo':			'JOB',
			'Salmos_Capitulo':		'PSA',
			'Proverbios_Capitulo':	'PRO',
			'Isaias_Capitulo':		'ISA',
			'Ezequiel_Capitulo':	'EZK',
			'Daniel_Capitulo':		'DAN',
			'Oseas_Capitulo':		'HOS',
			'Joel_Capitulo':		'JOL',
			'Amos_Capitulo':		'AMO',
			'Abdias_Capitulo':		'OBA',
			'Jonas_Capitulo':		'JON',
			'Miqueas_Capitulo':		'MIC',
			'Nahum_Capitulo':		'NAM',
			'Habacuc_Capitulo':		'HAB',
			'Sofonias_Capitulo':	'ZEP',
			# Spanish Book Codes, included as names, because not standard
			'GEN':			'GEN',
			'EXO':			'EXO',
			'LEV':			'LEV',
			'NUM':			'NUM',
			'DEU':			'DEU',
			'JOS':			'JOS',
			'JUE':			'JDG',
			'1SA':			'1SA',
			'2SA':			'2SA',
			'1RE':			'1KI',
			'2RE':			'2KI',
			'1CR':			'1CH',
			'2CR':			'2CH',
			'ESD':			'EZR',
			'NEH':			'NEH',
			'JOB':			'JOB',
			'SAL':			'PSA',
			'PRO':			'PRO',
			'ISA':			'ISA',
			'EZE':			'EZK',
			'DAN':			'DAN',
			'JOE':			'JOL',
			'AMO':			'AMO',
			'MIQ':			'MIC',
			'HAB':			'HAB',
			'SOF':			'ZEP',
			'ZAC':			'ZEC',
			'MAL':			'MAL',
			# Portuguese
			'Juizes':		'JDG', 
			'1Reis':		'1KI',
			'2Reis':		'2KI',
			'Neemias':		'NEH',
			'Cantares':		'SNG',
			'Lamentacoes':	'LAM',
			'Obadias':		'OBA',
			'Naum':			'NAM',
			'Ageu':			'HAG',
			'S_Mateus':		'MAT',
			'S_Marcos':		'MRK',
			'S_Lucas':		'LUK',
			'S_Joao':		'JHN',
			'Atos':			'ACT',
			'Colossenses':	'COL',
			'1Tess':		'1TH',
			'2Tess':		'2TH',
			'Hebreus':		'HEB',
			'S_Tiago':		'JAS',
			'1Pedro':		'1PE',
			'2Pedro':		'2PE',
			'1S_Joao':		'1JN',
			'2S_Joao':		'2JN',
			'3S_Joao':		'3JN',
			'S_Judas':		'JUD',
			'Apocalipse':	'REV',
			# French
			'Genese':		'GEN',
			'Exode':		'EXO',
			'Levitique':	'LEV',
			'Nombres':		'NUM',
			'Deuteronome':	'DEU',
			'Juges':		'JDG',
			'1Rois':		'1KI',
			'2Rois':		'2KI',
			'1Chroniques':	'1CH',
			'2Chroniques':	'2CH',
			'Nehemie':		'NEH',
			'Psaumes':		'PSA',
			'Proverbes':	'PRO',
			'Ecclesiaste':	'ECC',
			'Cantiques':	'SNG',
			'Esaie':		'ISA',
			'Jeremie':		'JER',
			'Lamentation':	'LAM',
			'Osee':			'HOS',
			'Michee':		'MIC',
			'Sophonie':		'ZEP',
			'Aggee':		'HAG',
			'Zacharie':		'ZEC',
			'Malachie':		'MAL',
			# Indonesian
			'Matius':		'MAT',
			'Markus':		'MRK',
			'Lukas':		'LUK',
			'Yohanes':		'JHN',
			'Kisah_Rasul':	'ACT',
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
			'Apocalypse': 	'REV',
			# Kolibugan Subanonm Southern Phillipines skn
			'PONOGNAAN':	'GEN',
			'YUNUS':		'JON',
			'MARKUS':		'MRK',
			'LUKAS':		'LUK',
			'YAHIYA':		'JHN',
			'MGA_GINANG':	'ACT',
			'GALATIYA':		'GAL',
			'EPESUS':		'EPH',
			'PILIPI':		'PHP',
			'KOLOSAS':		'COL',
			'1TESALONIKA':	'1TH',
			'2TESALONIKA':	'2TH',
			'1TIMUTI':		'1TI',
			'2TIMUTI':		'2TI',
			'TITUS':		'TIT',
			'YAKUB':		'JAS',
			# Malay
			'Kejadian':		'GEN',
 			'Keluaran':		'EXO',
 			'Imamat':		'LEV',
			'Bilangan':		'NUM',
			'Ulangan':		'DEU',
			'Yosua':		'JOS',
			'Hakim-hakim':	'JDG',
			'1Raja-raja':	'1KI',
			'2Raja-raja':	'2KI',
			'1Tawarikh':	'1CH',
			'2Tawarikh':	'2CH',
			'Nehemia':		'NEH',
			'Ayub':			'JOB',
			'Mazmur':    	'PSA',
			'Amsal':		'PRO',
			'Pengkhotbah':	'ECC',
			'Kidung':		'SNG',
			'Yesaya':		'ISA',
			'Yeremia':		'JER',
			'Ratapan':		'LAM',
			'Yehezkiel':	'EZK',
			'Yoel':			'JOL',
			'Yunus':		'AMO',
			'Mikha':		'MIC',
			'Habakuk':		'HAB',
			'Zefanya':		'ZEP',
			'Hagai':		'HAG',
			'Zakharia':		'ZEC',
			'Maleakhi':		'MAL'
		}
		result = books.get(bookName, None)
		return result


#NT Order:
#Traditional
#Russian
#Plautdietsch
#Finnish

#OT Order:
#Masoretic-Christian
#Masoretic-Tanakh
#Septuagint
#Vulgate

	def TraditionalOT(self, sequence):
		books = {
			'A01':  'GEN',
			'A02':  'EXO',
			'A03':  'LEV',
			'A04':  'NUM',
			'A05':  'DEU',
			'A06':  'JOS',
			'A07':  'JDG',
			'A08':  'RUT',
			'A09':  '1SA',
			'A10':  '2SA',
			'A11':  '1KI',
			'A12':  '2KI',
			'A13':  '1CH',
			'A14':  '2CH',
			'A15':  'EZR',
			'A16':  'NEH',
			'A17':  'EST',
			'A18':  'JOB',
			'A19':  'PSA',
			'A20':  'PRO',
			'A21':  'ECC',
			'A22':  'SNG',
			'A23':  'ISA',
			'A24':  'JER',
			'A25':  'LAM',
			'A26':  'EZK',
			'A27':  'DAN',
			'A28':  'HOS',
			'A29':  'JOL',
			'A30':  'AMO',
			'A31':  'OBA',
			'A32': 	'JON',
			'A33':  'MIC',
			'A34':  'NAM',
			'A35':  'HAB',
			'A36':  'ZEP',
			'A37':  'HAG',
			'A38':  'ZEC',
			'A39':  'MAL',
			'B01':  'MAT',
			'B02':  'MRK',
			'B03':  'LUK',
			'B04':  'JHN',
			'B05':  'ACT',
			'B06':  'ROM',
			'B07':  '1CO',
			'B08':  '2CO',
			'B09':  'GAL',
			'B10':  'EPH',
			'B11':  'PHP',
			'B12':  'COL',
			'B13':	'1TH',
			'B14':	'2TH',
			'B15':  '1TI',
			'B16':  '2TI',
			'B17': 	'TIT',
			'B18':  'PHM',
			'B19':  'HEB',
			'B20':  'JAS',
			'B21':  '1PE',
			'B22':  '2PE',
			'B23': 	'1JN',
			'B24': 	'2JN',
			'B25': 	'3JN',
			'B26':  'JUD',
			'B27':  'REV'
			#'Tobit':   		'TOB',
			#'Judith':   	'JDT',
			#'Wisdom':   	'WIS',
			#'Sirach':   	'SIR',
			#'Baruch':   	'BAR',
			#'EpistJeremia':	'LJE',
			#'1Maccabees':   '1MA',
			#'2Maccabees':   '2MA',
			#'3Maccabees':	'3MA',
			#'4Maccabees':	'4MA',
		}
		result = books.get(sequence, None)
		return result

	def TraditionalNT(self, sequence):
		books = {
			'B01':  'MAT',
			'B02':  'MRK',
			'B03':  'LUK',
			'B04':  'JHN',
			'B05':  'ACT',
			'B06':  'ROM',
			'B07':  '1CO',
			'B08':  '2CO',
			'B09':  'GAL',
			'B10':  'EPH',
			'B11':  'PHP',
			'B12':  'COL',
			'B13':	'1TH',
			'B14':	'2TH',
			'B15':  '1TI',
			'B16':  '2TI',
			'B17': 	'TIT',
			'B18':  'PHM',
			'B19':  'HEB',
			'B20':  'JAS',
			'B21':  '1PE',
			'B22':  '2PE',
			'B23': 	'1JN',
			'B24': 	'2JN',
			'B25': 	'3JN',
			'B26':  'JUD',
			'B27':  'REV'
		}
		return books.get(sequence, None)

	def RussianNT(self, sequence):
		books = {
			'B01':  'MAT',
			'B02':  'MRK',
			'B03':  'LUK',
			'B04':  'JHN',
			'B05':  'ACT',
			'B06':  'JAS',
			'B07':  '1PE',
			'B08':  '2PE',
			'B09': 	'1JN',
			'B10': 	'2JN',
			'B11': 	'3JN',
			'B12':  'JUD',
			'B13':  'ROM',
			'B14':  '1CO',
			'B15':  '2CO',
			'B16':  'GAL',
			'B17':  'EPH',
			'B18':  'PHP',
			'B19':  'COL',
			'B20':	'1TH',
			'B21':	'2TH',
			'B22':  '1TI',
			'B23':  '2TI',
			'B24': 	'TIT',
			'B25':  'PHM',
			'B26':  'HEB',
			'B27':  'REV'
		}
		return books.get(sequence, None)


	def PlautdietschNT(self, sequence):
		books = {
			'B01':  'MAT',
			'B02':  'MRK',
			'B03':  'LUK',
			'B04':  'JHN',
			'B05':  'ACT',
			'B06':  'ROM',
			'B07':  '1CO',
			'B08':  '2CO',
			'B09':  'GAL',
			'B10':  'EPH',
			'B11':  'PHP',
			'B12':  'COL',
			'B13':	'1TH',
			'B14':	'2TH',
			'B15':  '1TI',
			'B16':  '2TI',
			'B17': 	'TIT',
			'B18':  'PHM',
			'B19':  '1PE',
			'B20':  '2PE',
			'B21': 	'1JN',
			'B22': 	'2JN',
			'B23': 	'3JN',
			'B24':  'HEB',
			'B25':  'JAS',
			'B26':  'JUD',
			'B27':  'REV'
		}
		return books.get(sequence, None)


	def FinnishNT(self, sequence):
		books = {
			'B01':  'MAT',
			'B02':  'MRK',
			'B03':  'LUK',
			'B04':  'JHN',
			'B05':  'ACT',
			'B06':  'ROM',
			'B07':  '1CO',
			'B08':  '2CO',
			'B09':  'GAL',
			'B10':  'EPH',
			'B11':  'PHP',
			'B12':  'COL',
			'B13':	'1TH',
			'B14':	'2TH',
			'B15':  '1TI',
			'B16':  '2TI',
			'B17': 	'TIT',
			'B18':  'PHM',
			'B19':  'HEB',
			'B20':  '1PE',
			'B21':  '2PE',
			'B22': 	'1JN',
			'B23': 	'2JN',
			'B24': 	'3JN',
			'B25':  'JAS',
			'B26':  'JUD',
			'B27':  'REV'
		}
		return books.get(sequence, None)
