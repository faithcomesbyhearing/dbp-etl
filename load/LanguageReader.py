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

    # returns a LanguageReaderInterface record
    @abstractmethod 
    def getByStockNumber(self, stockNumber):
        pass

    @abstractmethod 
    def getBibleIdMap(self):
        pass 

    @abstractmethod 
    def getLanguageRecord(self, typeCode, bibleId, filesetId):
        pass

    @abstractmethod
    def getFilesetRecords(filesetId):
        pass 

    @abstractmethod
    def getLanguageRecordLoose(typeCode, bibleId, dbpFilesetId):
        pass 

    @abstractmethod
    def reduceCopyrightToName(lptsCopyright):
        pass

    @abstractmethod
    def getFilesetRecords10(damId):
        pass

    def getStocknumberWithFormat(self, stockNumberItem):
        return stockNumberItem[:-3] + "/" + stockNumberItem[-3:]

    # @property?
    # maybe this is a property?
    # resultSet
    # 
    # @abstractmethod 
    # def validateUSXStockNo( self, stockNo, filenames, sampleText = None):
    #     pass


class LanguageRecordInterface(ABC):

    @abstractmethod 
    def DamIdList(self, typeCode):  
        pass

    @abstractmethod 
    def DamIdMap(self, typeCode, index):
        pass

    @abstractmethod
    def Reg_StockNumber():
        pass

    @abstractmethod
    def Volumne_Name():
        pass

    @abstractmethod
    def Licensor():
        pass

    @abstractmethod
    def CoLicensor():
        pass

    @abstractmethod
    def Copyrightc():
        pass

    @staticmethod
    def transformToTextFilesetId(damId):
        return damId[:7] + "_" + damId[8:]

    ## BWF - part of LanguageRecord interface. may be a common base class method
    def ReduceTextList(self, damIdList):
        pass    
