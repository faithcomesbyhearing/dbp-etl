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
			'EpistJeremiah':'LJE',
			'1Maccabees':   '1MA',
			'2Maccabees':   '2MA',
			'3Maccabees':	'3MA',
			'4Maccabees':	'4MA',
			# Spanish
			'Exodo': 		'EXO',
			'Levitico': 	'LEV',
			'Levetico':		'LEV',
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
			'Obaja':		'OBA',
			'Yunus':		'JON',
			'Mikha':		'MIC',
			'Habakuk':		'HAB',
			'Zefanya':		'ZEP',
			'Hagai':		'HAG',
			'Zakharia':		'ZEC',
			'Maleakhi':		'MAL',
			'Segment 01':	'C01',
			'Segment 02':	'C02',
			'Segment 03':	'C03',
			'Segment 04':	'C04',
			'Segment 05':	'C05',
			'Segment 06':	'C06',
			'Segment 07':	'C07',
			'Segment 08':	'C08',
			'Segment 09':	'C09',
			'Segment 10':	'C10',
			'Segment 11':	'C11',
			'Segment 12':	'C12',
		}
		result = books.get(bookName, None)
		return result



##
## Old Testament Book orders
##

	def TraditionalOT(self, sequence):
		traditional = {
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
			'A39':  'MAL'
		}
		return traditional.get(sequence, None)

	def MasoreticChristianOT(self, sequence):
		return self.TraditionalOT(sequence)

	def HebrewOT(self, sequence):
		hebrew = {
			'A01':  'GEN',
			'A02':  'EXO',
			'A03':  'LEV',
			'A04':  'NUM',
			'A05':  'DEU',
			'A06':  'JOS',
			'A07':  'JDG',
			'A08':  '1SA',
			'A09':  '2SA',
			'A10':  '1KI',
			'A11':  '2KI',
			'A12':  'ISA',
			'A13':  'JER',
			'A14':  'EZK',
			'A15':  'HOS',
			'A16':  'JOL',
			'A17':  'AMO',
			'A18':  'OBA',
			'A19': 	'JON',
			'A20':  'MIC',
			'A21':  'NAM',
			'A22':  'HAB',
			'A23':  'ZEP',
			'A24':  'HAG',
			'A25':  'ZEC',
			'A26':  'MAL',
			'A27':  'PSA',
			'A28':  'PRO',
			'A29':  'JOB',
			'A30':  'SNG',
			'A31':  'RUT',
			'A32':  'LAM',
			'A33':  'ECC',
			'A34':  'EST',
			'A35':  'DAN',
			'A36':  'EZR',
			'A37':  'NEH',					
			'A38':  '1CH',
			'A39':  '2CH'
		}
		return hebrew.get(sequence, None)

	def MasoreticTanakhOT(self, sequence):
		return self.HebrewOT(sequence)

	def CatholicOT(self, sequence):   # This is my name for it. It must be corrected
		catholic = {
			'A01':	'GEN',
			'A02':	'EXO',
			'A03':	'LEV',
			'A04':	'NUM',
			'A05':	'DEU',
			'A06':	'JOS',
			'A07':	'JDG',
			'A08':	'RUT',
			'A09':	'1SA',
			'A10':	'2SA',
			'A11':	'1KI',
			'A12':	'2KI',
			'A13':	'1CH',
			'A14':	'2CH',
			'A15':	'EZR',
			'A16':	'NEH',
			'A17':	'TOB',
			'A18':	'JDT',
			'A19':	'EST',
			'A20':	'1MA',
			'A21':	'2MA',
			'A22':	'JOB',
			'A23':	'PSA',
			'A24':	'PRO',
			'A25':	'ECC',
			'A26':	'SNG',
			'A27':	'WIS',
			'A28':	'SIR',
			'A29':	'ISA',
			'A30':	'JER',
			'A31':	'LAM',
			'A32':	'BAR',
			'A33':	'EZK',
			'A34':	'DAG',
			'A35':	'HOS',
			'A36':	'JOL',
			'A37':	'AMO',
			'A38':	'OBA',
			'A39':	'JON',
			'A40':	'MIC',
			'A41':	'NAM',
			'A42':	'HAB',
			'A43':	'ZEP',
			'A44':	'HAG',
			'A45':	'ZEC',
			'A46':	'MAL'
		}
		return catholic.get(sequence, None)

	def VulgateOT(self, sequence):
		vulgate = {
			## To be added when sequence is known
		}
		return vulgate.get(sequence, None)

	def SeptuagintOT(self, sequence):
		septuagint = {
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
			'A15':  'EZA', # Not sure EZA is correct codeEsdras 1
			'A16':	'EZR',
			'A17':  'NEH',
			'A18': 	'TOB', # Tobit
			'A19': 	'JDT', # Judith
			'A20':  'ESG', # EST with additions
			'A21': 	'1MA', # 1Maccabees
			'A22': 	'2MA', # 2Maccabees
			'A23': 	'3MA', # 3Maccabees
			'A24':  'PSA',
			# I think prayer of Manassa goes here
			'A25':  'JOB',
			'A26':  'PRO',
			'A27':  'ECC',
			'A28':  'SNG',
			'A29':	'WIS', # Wisdom of Solomon
			'A30': 	'SIR', # Sirach
			# Does Psalm of Solomon go here
			'A31':  'HOS',
			'A32':  'AMO',	
			'A33':  'MIC',
			'A34':  'JOL',
			'A35':  'OBA',	
			'A36': 	'JON',
			'A37':  'NAM',	
			'A38':  'HAB',
			'A39':  'ZEP',
			'A40':  'HAG',
			'A41':  'ZEC',
			'A42':  'MAL',
			'A43':  'ISA',
			'A44':  'JER',
			'A45': 	'BAR', # First book of Baruch
			'A46':  'LAM',
			'A47': 	'LJE', # Epistle Jeremiah
			'A48':  'EZK',
			'A49':  'DAG', # DAN with additions
			'A50': 	'4MA' # 4Maccabees
		}
		return septuagint.get(sequence, None)

	def Septuagint2OT(self, sequence):
		septuagint = {
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
			'A15':  'EZA', # Not sure EZA is correct codeEsdras 1
			'A16':	'EZR',
			'A17':  'NEH',
			'A18': 	'TOB', # Tobit
			'A19': 	'JDT', # Judith
			'A20':  'ESG', # EST with additions
			'A21': 	'1MA', # 1Maccabees
			'A22': 	'2MA', # 2Maccabees
			'A23': 	'3MA', # 3Maccabees
			'A24':  'PSA',
			'A25':	'PS2', # special code for PSA 151
			'A26':  'JOB',
			'A27':  'PRO',
			'A28':  'ECC',
			'A29':  'SNG',
			'A30':	'WIS', # Wisdom of Solomon
			'A31': 	'SIR', # Sirach
			# Does Psalm of Solomon go here
			'A32':  'HOS',
			'A33':  'AMO',	
			'A34':  'MIC',
			'A35':  'JOL',
			'A36':  'OBA',	
			'A37': 	'JON',
			'A38':  'NAM',	
			'A39':  'HAB',
			'A40':  'ZEP',
			'A41':  'HAG',
			'A42':  'ZEC',
			'A43':  'MAL',
			'A44':  'ISA',
			'A45':  'JER',
			'A46': 	'BAR', # First book of Baruch
			'A47':  'LAM',
			'A48': 	'LJE', # Epistle Jeremiah
			'A49':  'EZK',
			'A50':  'DAG', # DAN with additions
			'A51':	'SUS', # Susanna
			'A52': 	'4MA' # 4Maccabees
		}
		return septuagint.get(sequence, None)

	def DutchTraditionalOT(self, sequence):
		dutch = {
			## To be added when sequence is known
		}
		return dutch.get(sequence, None)

	def TRNNTMOT(self, sequence):
		trnntm = {
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
			'A11':  '1CH',
			'A12':  '2CH',
			'A13':	'2KI',
			'A14':  'ISA',
			'A15':  'JER',
			'A16':  'MIC',
			'A17':  'HOS',
			'A18':  'PSA',
			'A19':  'ZEC',
			'A20': 	'JON'
		}
		return trnntm.get(sequence, None)

##
## New Testament book orders
##

	def TraditionalNT(self, sequence):
		traditional = {
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
		return traditional.get(sequence, None)

	def RussianNT(self, sequence):
		russian = {
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
		return russian.get(sequence, None)

	def PlautdietschNT(self, sequence):
		diestsch = {
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
		return diestsch.get(sequence, None)

	def FinnishNT(self, sequence):
		finnish = {
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
		return finnish.get(sequence, None)