import io
import sys
import os
from xml.dom.minidom import parse
from MetadataReaderInterface import MetadataReaderInterface, MetadataRecordInterface

class MetadataReader(MetadataReaderInterface):

    def __init__(self, metadataExtractPath):
        self.metadataExtractPath = metadataExtractPath
        self.resultSet = []

        try:
            doc = parse(metadataExtractPath)
        except Exception as err:
            print("FATAL ERROR parsing Metadata extract", err)
            sys.exit(1)
        root = doc.childNodes

        if len(root) > 0:
            for DBLMetadata in root:
                resultRow = {}

                for fldNode in DBLMetadata.childNodes:
                    if fldNode.nodeType == 1:
                        if fldNode.hasChildNodes() == True:
                            resultRow[fldNode.nodeName] = fldNode.childNodes
                        else:
                            resultRow[fldNode.nodeName] = fldNode.firstChild.nodeValue if fldNode.firstChild != None else None

                metadataRecord = MetadataRecord(resultRow)
                self.resultSet.append(MetadataRecord(resultRow))

    def getResultSet(self):
        return self.resultSet

    def getMetadataRecordByName(self, name):
        for record in self.resultSet:
            if record.Name() == name:
                return record

        return None

class MetadataRecord(MetadataRecordInterface):
    def __init__(self, record):
        self.record = record

    def getAttributes(self):
        return list(self.record.keys())

    def Name(self):
        identification = self.record.get('identification')
        resultNode = {}
        for subFldNode in identification:
            if subFldNode.nodeName == 'name':
                return subFldNode.firstChild.nodeValue

        return None

    def NameLocal(self):
        identification = self.record.get('identification')
        resultNode = {}
        for subFldNode in identification:
            if subFldNode.nodeName == 'nameLocal':
                return subFldNode.firstChild.nodeValue

        return None

    def DateCompleted(self):
        identification = self.record.get('identification')
        resultNode = {}
        for subFldNode in identification:
            if subFldNode.nodeName == 'dateCompleted':
                return subFldNode.firstChild.nodeValue

        return None

    def RightsHolder(self):
        agencies = self.record.get('agencies')

        for subFldNode in agencies:
            if subFldNode.nodeName == 'rightsHolder':
                if subFldNode.hasChildNodes() == True:
                    resultSubNode = {}

                    for subSubFldNode in subFldNode.childNodes:
                        if subSubFldNode.nodeType == 1:
                            resultSubNode[subSubFldNode.nodeName] = subSubFldNode.firstChild.nodeValue if subSubFldNode.firstChild != None else None

                    return resultSubNode
                else:
                    return subFldNode.firstChild.nodeValue if subFldNode.firstChild != None else None

        return None

    def RightsAdmin(self):
        agencies = self.record.get('agencies')

        for subFldNode in agencies:
            if subFldNode.nodeName == 'rightsAdmin':
                if subFldNode.hasChildNodes() == True:
                    resultSubNode = {}

                    for subSubFldNode in subFldNode.childNodes:
                        if subSubFldNode.nodeType == 1:
                            resultSubNode[subSubFldNode.nodeName] = subSubFldNode.firstChild.nodeValue if subSubFldNode.firstChild != None else None

                    return resultSubNode
                else:
                    return subFldNode.firstChild.nodeValue if subFldNode.firstChild != None else None

        return None

    def Language(self):
        language = self.record.get('language')
        resultNode = {}

        if len(language) > 0:
            for subFldNode in language:
                if subFldNode.nodeType == 1:
                    resultNode[subFldNode.nodeName] = subFldNode.firstChild.nodeValue if subFldNode.firstChild != None else None

        return resultNode

    def Countries(self):
        countries = self.record.get('countries')
        resultCountries = []
        resultNode = {}

        if len(countries) > 0:
            for subFldNode in countries:
                if subFldNode.nodeType == 1 and subFldNode.nodeName == "country" and subFldNode.hasChildNodes() == True:
                    resultNode = {}

                    for childFldNode in subFldNode.childNodes:
                        if childFldNode.nodeType == 1:
                            resultNode[childFldNode.nodeName] = childFldNode.firstChild.nodeValue if childFldNode.firstChild != None else None

                    resultCountries.append(resultNode)

        return resultCountries

    def BookNames(self):
        names = self.record.get('names')
        resultNames = []
        resultNode = {}

        if len(names) > 0:
            for fldNode in names:
                if fldNode.nodeType == 1 and fldNode.hasChildNodes() == True:
                    childResultNode = {}

                    for childFldNode in fldNode.childNodes:
                        if childFldNode.nodeType == 1:
                            childResultNode[childFldNode.nodeName] = childFldNode.firstChild.nodeValue if childFldNode.firstChild != None else None

                    resultNode[fldNode.getAttribute('id')] = childResultNode
                    resultNames.append(resultNode)

        return resultNames

    def getBookNameByName(self, name):
        bookNames = self.BookNames()

        for book in self.BookNames():
            for key, record in book.items():
                if key == name:
                    return book.get(key)
        return None

    def BookOrder(self):
        publications = self.record.get('publications')
        resultNode = {}
        resultPublications = []

        if len(publications) > 0:
            for subFldNode in publications:
                if subFldNode.nodeType == 1 and subFldNode.nodeName == 'publication' and subFldNode.hasChildNodes() == True:
                    for childFldNode in subFldNode.childNodes:
                        if childFldNode.nodeType == 1 and childFldNode.nodeName == 'structure' and childFldNode.hasChildNodes() == True:

                            for structure in childFldNode.childNodes:
                                if structure.nodeType == 1:
                                    resultNode = {}
                                    resultNode['name'] = structure.getAttribute('name')
                                    resultNode['src'] = structure.getAttribute('src')
                                    resultNode['role'] = structure.getAttribute('role')
                                    resultPublications.append(resultNode)

        return resultPublications

if (__name__ == '__main__'):
    from Config import *
    from MetadataReader import *
    result = {}
    config = Config()
    print(config.filename_metadata_xml)
    reader = MetadataReader(config.filename_metadata_xml)
