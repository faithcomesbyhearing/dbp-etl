# Validate.py



class Validate:

	def __init__(self, config, args):



	def process(self):
		## prepare directories
		## Invoke the correct reader based upon command line options
		## build a map of types, bibleIds, filesetId, or possibly a map for each type
		## validate the LPTS data
		## filenameParser
		## prepare errors
		## report on what is accept, what is quarantine and what is duplicated


	def parseCommandLine(self):
		## parse command line, possibly this should be done outside of class
		## parse command line
		## 1. Parse profile name
		## 2. parse -d directoryName
		## 2b. or parse -b bucketList name


	def prepareDirectory(self, directory):
		## ensure that it exists
		XXos.remove(directory + os.sep + "*.csv")
		## prepare output directory, zip up prior contents of validate directories
		## create new directories


	def validateLPTSExtract(self, directory):
		required = {"Copyrightc", "Copyrightp", "Copyright_Video", "DBP_Equivalent", 
				"ISO", "LangName", "Licensor", "Reg_StockNumber", "Volumne_Name"}
		possible = {"_x0031_Orthography"}
		## build map of extract by DBP_Equivalent, include DBP_Equivalent2, but report as warning
		## check DBP2 for uniqueness also
		## look for duplicate bible_id's
		## look for duplicate fileset_id's
		## look for duplicate stock nos
		## 
		## create error messages for all duplicates
		## for each bible_id, lookup in extract.  i.e. extract must have a map by DBP_Equivanent
		## have set of names required, and set of names not-required, but expected
		## check that all required fields are present, and prepare error messages, but continue
		## check that all non-required fields are present, and prepare error messages, and continue
		## Use damid fieldname to lookup filesets, and prepare an error for any missing.
		## Possibly prepare a warning for any that are NOT included






## parse command line
## config profile is required

config = Config(profile)
validate = Validate(config, args)
validate.process()

"""
DBP_Equivalent - required  count 3805
DBP_Equivalent2 - what is this count 16
ISO - required - count 3947
LangName - required - count 3953
_x0031_Orthography - not required - count 1568
_x0032 - count 128
x_0033 - count 42
Volumne_Name - required I think - count 3468
Copyrightc - required for what? - count 4226
Copyrightp - required for what? - count 3983
Copyright_Video - required for video - count 3953
Licensor - required ? count 2804
Reg_StockNumber - required - count 3953



Compare these to damId’s being loaded
Reg_NTAudioDamID1" validate="tbd" clean="nullify1"/>
Reg_NTAudioDamID2" validate="tbd" clean="nullify1"/>
ND_NTAudioDamID1" validate="tbd" clean="nullify1"/>
ND_NTAudioDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_NTAudioDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTAudioDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTAudioDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="CAudioDAMID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="CAudioDamStockNo" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTAudioDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTAudioDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTAudioDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_NTTextDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_NTTextDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_NTTextDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_NTTextDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_NTTextDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_NTTextDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTTextDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTTextDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTTextDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTTextDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTTextDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTTextDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="Video_Matthew_DamStockNo" validate="tbd" clean="nullify1"/>
			<lptsXML name="Video_Mark_DamStockNo" validate="tbd" clean="nullify1"/>
			<lptsXML name="Video_Luke_DamStockNo" validate="tbd" clean="nullify1"/>
			<lptsXML name="Video_John_DamStockNo" validate="tbd" clean="nullify1"/>
"""