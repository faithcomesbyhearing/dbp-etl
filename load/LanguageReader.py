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
    def LicensorList():
        pass

    @abstractmethod
    def HasLicensor(licensor):
        pass

    @abstractmethod
    def HasCoLicensor(coLicensor):
        pass

    @abstractmethod
    def CoLicensorList():
        pass

    @abstractmethod
    def Copyrightc():
        pass

    # Transform a damId or filesetId to a text damId or a text filesetId. e.g.
    # from filesetId: TNKWBTN1ET-json to text filesetId: TNKWBTN_ET-json
    # from filesetId: TNKWBTN1ET-usx to text filesetId: TNKWBTN_ET-usx
    #
    # from damId: TNKWBTN1ET to text damId: TNKWBTN_ET
    @staticmethod
    def transformToTextId(id):
        return id[:7] + "_" + id[8:]

    @staticmethod
    def GetFilesetIdLen10(filesetId):
        return filesetId[:10]

    ## BWF - part of LanguageRecord interface. may be a common base class method
    def ReduceTextList(self, damIdList):
        pass    
