from abc import ABC, abstractmethod

class MetadataReaderInterface(ABC):
    @abstractmethod
    def getResultSet(self):
        pass

    @abstractmethod
    def getMetadataRecordByName(self, name):
        pass

class MetadataRecordInterface(ABC):
    @abstractmethod
    def Name(self):
        pass

    @abstractmethod
    def NameLocal(self):
        pass

    @abstractmethod
    def DateCompleted(self):
        pass

    @abstractmethod
    def RightsHolder(self):
        pass

    @abstractmethod
    def RightsAdmin(self):
        pass

    @abstractmethod
    def RightsAdmin(self):
        pass

    @abstractmethod
    def Language(self):
        pass

    @abstractmethod
    def Countries(self):
        pass

    @abstractmethod
    def BookNames(self):
        pass

    @abstractmethod
    def BookOrder(self):
        pass
