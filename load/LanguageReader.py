# LanguageReaderInterface
#
# This is an informal python interface, intended for use during Stage C of the data model transformation
# The interface will provide abstract methods for each method in LPTSExtractReader, so that DBPLoadController and related can use this interface during both 
# Stage B and Stage C.  LPTSExtractReader will be a concrete implementation of LanguageReaderInterface and will be used during Stage B. 
# A new concrete class will created to access the new Languages and Agreements databases, and will be used during Stage C.
# This interface and the associated concrete classes will be thrown away when we move to Stage D.
#

from abc import ABC, abstractmethod

class LanguageReaderInterface(ABC):
    @abstractmethod 
    def getBibleIdMap(self):
        pass 

    @abstractmethod 
    def getByStockNumber(self, stockNumber):
        pass
    
    @abstractmethod 
    def getLPTSRecord(self, typeCode, bibleId, filesetId):
        pass

    @abstractmethod
    def getFilesetRecords(filesetId):
        pass 

    @abstractmethod
    def getLPTSRecordLoose(typeCode, bibleId, dbpFilesetId):
        pass 

    @abstractmethod
    def reduceCopyrightToName(lptsCopyright):
        pass

    @abstractmethod
    def getFilesetRecords10(damId):
        pass


    # @property?
    # maybe this is a property?
    # resultSet
    # 
    # @abstractmethod 
    # def validateUSXStockNo( self, stockNo, filenames, sampleText = None):
    #     pass




