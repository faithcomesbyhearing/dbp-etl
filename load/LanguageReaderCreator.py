from LPTSExtractReader import LPTSExtractReader
from StageCLanguageReader import StageCLanguageReader
from BlimpLanguageReader import BlimpLanguageReader
from typing import Union

class LanguageReaderCreator:
	def __init__(self, stage: str = "B"):
		self.stage = stage
		self.stage_map = {
			"B": LPTSExtractReader,
			"C": StageCLanguageReader,
			"BLIMP": BlimpLanguageReader
		}

	def create(self, lpts_xml_path: Union[str, None] = None):
		print(f"migration stage: [{self.stage}]")
		reader_class = self.stage_map.get(self.stage)
		if not reader_class:
			raise ValueError(f"Unrecognized stage: {self.stage}")
		if self.stage == "B":
			return reader_class(lpts_xml_path)
		return reader_class()

if __name__ == '__main__':
	from Config import Config
	import os

	config = Config()
	migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE", "B")
	lpts_xml = config.filename_lpts_xml if migration_stage == "B" else ""

	languageReader = LanguageReaderCreator(migration_stage).create(lpts_xml)
	results = languageReader.getByStockNumber("N1SPA/PDT")
	print("Reg_StockNumber [%s]" % results.Reg_StockNumber())
