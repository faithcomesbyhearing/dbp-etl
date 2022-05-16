from StageCLanguageRecord import StageCLanguageRecord as LanguageRecord;

def parseMediaRow(rowTuple):
    return {
        LanguageRecord.propertiesName['id']: rowTuple[0],
        LanguageRecord.propertiesName['regStockNumber']: rowTuple[1],
        LanguageRecord.propertiesName['extent']: rowTuple[2],
        LanguageRecord.propertiesName['country']: rowTuple[3],
        LanguageRecord.propertiesName['iso']: rowTuple[7],
        LanguageRecord.propertiesName['langName']: rowTuple[8],
        LanguageRecord.propertiesName['dbpEquivalent']: rowTuple[6],
        LanguageRecord.propertiesName['licensor']: '',
        LanguageRecord.propertiesName['copyrightc']: '',
        LanguageRecord.propertiesName['copyrightp']: '',
        LanguageRecord.propertiesName['volumneName']: rowTuple[5],
        LanguageRecord.propertiesName['dbpMobile']: '',
        LanguageRecord.propertiesName['dbpWebHub']: '',
        LanguageRecord.propertiesName['mobileText']: '',
        LanguageRecord.propertiesName['hubText']: '',
        LanguageRecord.propertiesName['apiDevText']: '',
        LanguageRecord.propertiesName['apiDevAudio']: '',
        LanguageRecord.propertiesName['webHubVideo']: '',
        LanguageRecord.propertiesName['copyrightVideo']: '',
        LanguageRecord.propertiesName['apiDevVideo']: '',
        LanguageRecord.propertiesName['status']: rowTuple[9],
        LanguageRecord.propertiesName['derivativeOf']: rowTuple[10],
        LanguageRecord.propertiesName['orthography']: rowTuple[11],
        LanguageRecord.propertiesName['ethName']: rowTuple[12],
        LanguageRecord.propertiesName['altName']: rowTuple[13],
    }

def parseResult(result):
    newList = []

    for rowTuple in result:
        resultRow = parseMediaRow(rowTuple)
        newList.append(LanguageRecord(resultRow))

    return newList
