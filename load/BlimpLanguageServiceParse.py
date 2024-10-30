from BlimpLanguageRecord import BlimpLanguageRecord as LanguageRecord;

def parseMediaRow(rowTuple):
    return {
        LanguageRecord.propertiesName['id']: rowTuple[0],
        LanguageRecord.propertiesName['regStockNumber']: rowTuple[1],
        LanguageRecord.propertiesName['extent']: '',
        LanguageRecord.propertiesName['country']: '',
        LanguageRecord.propertiesName['iso']: '',
        LanguageRecord.propertiesName['langName']: '',
        LanguageRecord.propertiesName['dbpEquivalent']: rowTuple[2],
        LanguageRecord.propertiesName['licensor']: '',
        LanguageRecord.propertiesName['copyrightc']: '',
        LanguageRecord.propertiesName['copyrightp']: '',
        LanguageRecord.propertiesName['volumneName']: rowTuple[4],
        LanguageRecord.propertiesName['dbpMobile']: '',
        LanguageRecord.propertiesName['dbpWebHub']: '',
        LanguageRecord.propertiesName['mobileText']: '',
        LanguageRecord.propertiesName['hubText']: '',
        LanguageRecord.propertiesName['apiDevText']: '',
        LanguageRecord.propertiesName['apiDevAudio']: '',
        LanguageRecord.propertiesName['webHubVideo']: '',
        LanguageRecord.propertiesName['copyrightVideo']: '',
        LanguageRecord.propertiesName['apiDevVideo']: '',
        LanguageRecord.propertiesName['status']: 'Live',
        LanguageRecord.propertiesName['derivativeOf']: rowTuple[2],
        LanguageRecord.propertiesName['orthography']: '',
        LanguageRecord.propertiesName['ethName']: '',
        LanguageRecord.propertiesName['altName']: '',
        "Text": rowTuple[0],
        "Audio": rowTuple[0],
        "Video": rowTuple[0],
    }

def parseResult(result):
    newList = []

    for rowTuple in result:
        resultRow = parseMediaRow(rowTuple)
        newList.append(LanguageRecord(resultRow))

    return newList
