# PreValidateHandlerStub.py
# This is a test stub intended to provide the same interface as the AWS Lambda Handler
import boto3
import os
import sys
from LanguageReaderCreator import LanguageReaderCreator
from PreValidate import PreValidate
from Config import Config

### copied from lambda/prevalidate/Handler.py, except for s3Client
def handler(event, context):


    bucket    = os.getenv("UPLOAD_BUCKET")
    # migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE") # Should be "B" or "C"

    directory = event["prefix"] # can be filesetId or lang_stockno_USX
    filenames = event["files"] # Should be object keys
    # Should be a string if user has uploaded a file.
    # e.g. "N1KANDPI\r\nO1KANDPI"
    # each stocknumber will be separated by a line break.
    stocknumbersContents = event["stocknumbers"]

    # session = boto3.Session() # this is in prod handler since the lambda has only one session profile 
    config = Config.shared()
    session = boto3.Session(profile_name = config.s3_aws_profile)    
    s3Client = session.client("s3")
	 
    # print("Copying lpts-dbp.xml...")
    # s3Client.download_file(bucket, "lpts-dbp.xml", "/tmp/lpts-dbp.xml")  # commented out for efficiency during testing
    migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE", "B")
    lpts_xml = config.filename_lpts_xml if migration_stage == "B" else ""
    languageReader = LanguageReaderCreator(migration_stage).create(lpts_xml)


    preValidate = PreValidate(languageReader, s3Client, bucket) ## removed UnicodeScript
    messages = preValidate.validateLambda(directory, filenames, stocknumbersContents)
    return messages
#### end of copy


if (__name__ == '__main__'):

    os.environ["UPLOAD_BUCKET"] = "dbp-etl-upload-dev-ya1mbvdty8tlq15r"
    # os.environ["DATA_MODEL_MIGRATION_STAGE"] = "B"

    # general
    textOTFiles = ["xxxGEN.usx", "xxxEXO.usx", "xxxLEV.usx", "xxxNUM.usx", "xxxDEU.usx", "xxxJOS.usx", "xxxJDG.usx", "xxxRUT.usx", "xxx1SA.usx", "xxx2SA.usx", "xxx1KI.usx", "xxx2KI.usx", "xxx1CH.usx", "xxx2CH.usx", 
			   "xxxEZR.usx", "xxxNEH.usx", "xxxEST.usx", "xxxJOB.usx", "xxxPSA.usx", "xxxPRO.usx", "xxxECC.usx", "xxxSNG.usx", "xxxISA.usx", "xxxJER.usx", "xxxLAM.usx", "xxxEZK.usx", "xxxDAN.usx", "xxxHOS.usx", 
			   "xxxJOL.usx", "xxxAMO.usx", "xxxOBA.usx", "xxxJON.usx", "xxxMIC.usx", "xxxNAM.usx", "xxxHAB.usx", "xxxZEP.usx", "xxxHAG.usx", "xxxZEC.usx", "xxxMAL.usx"]
    textNTFiles = ["xxxMAT.usx", "xxxMRK.usx", "xxxLUK.usx", "xxxJHN.usx", "xxxACT.usx", "xxxROM.usx", "xxx1CO.usx", "xxx2CO.usx", "xxxGAL.usx", "xxxEPH.usx", "xxxPHP.usx", "xxxCOL.usx", "xxx1TH.usx", "xxx2TH.usx", 
			   "xxx1TI.usx", "xxx2TI.usx", "xxxTIT.usx", "xxxPHM.usx", "xxxHEB.usx", "xxxJAS.usx", "xxx1PE.usx", "xxx2PE.usx", "xxx1JN.usx", "xxx2JN.usx", "xxx3JN.usx", "xxxJUD.usx", "xxxREV.usx"]

    # textOTEvent_withFile
    textPrefix_withFile = "Kannada_N1 & O1 KANDPI_USX"
    textOTStockNumber = "O1KANDPI"
    textOTEvent_withFile = {
        "prefix": textPrefix_withFile,
        "files": textOTFiles,
        "stocknumbers": textOTStockNumber
    }
    # textOTEvent_withFile_Fail
    textOTEvent_withFile_Fail = {
        "prefix": textPrefix_withFile,
        "files": textOTFiles,
        "stocknumbers": textOTStockNumber+"foo"
    }
    # textNTEvent_withFile
    textNTStockNumber = "N1KANDPI"
    textNTEvent_withFile = {
        "prefix": textPrefix_withFile,
        "files": textNTFiles,
        "stocknumbers": textNTStockNumber
    }  
    # textNTEvent_withFile_Fail
    textNTStockNumber = "N1KANDPI"
    textNTEvent_withFile_Fail = {
        "prefix": textPrefix_withFile,
        "files": textNTFiles,
        "stocknumbers": textNTStockNumber+"foo"
    } 
    # textBOTHEvent_withFile
    textBOTHEvent_withFile = {
        "prefix": textPrefix_withFile,
        "files": textOTFiles + textNTFiles,
        "stocknumbers": textOTStockNumber + "\r"+textNTStockNumber
    }  


    # textNTEvent_withoutFile (note: this event is contrived - the files do not match the stocknumber. just fyi)
    textPrefixNT_withoutFile = "Abidji_N2ABIWBT_USX" # this configuration is contrived
    textNTEvent_withoutFile = {
        "prefix": textPrefixNT_withoutFile,
        "files": textNTFiles,
        "stocknumbers": ""
    }

    # audioEvent
    audioPrefix = "ENGESVN1DA"
    audioFiles = ""
    audioEvent = {
        "prefix": audioPrefix,
        "files": audioFiles,
        "stocknumbers": ""
    } 
    # audioEvent_Fail
    audioPrefix = "foo4567890"
    audioFiles = ""
    audioEvent_Fail = {
        "prefix": audioPrefix,
        "files": audioFiles,
        "stocknumbers": ""
    }     
    # videoEvent = {
    #     "prefix": videoPrefix,
    #     "files": videoFiles,
    #     "stocknumbers": ""
    # } 

    #defaults
    type = "text"
    extent = "OT"
    expectedOutcome = "pass"

    if len(sys.argv) < 2:
        print("FATAL command line parameters: environment text|audio|video  OT|NT|BOTH|NT_WITHOUTFILE pass|fail ")
        sys.exit()

    if len(sys.argv) > 2:
        type = sys.argv[2] # text|audio|video

    if len(sys.argv) > 3:
        extent = sys.argv[3]

    if len(sys.argv) > 4:
        expectedOutcome = sys.argv[4] # pass|fail

    print ("type: %s, extent: %s, expectedOutcome: %s" % (type,extent, expectedOutcome))
    result = None
    if (type == "text"):
        if (extent == "OT"):
            if (expectedOutcome == "pass"):
                messages = handler(textOTEvent_withFile, None)
            else:
                messages = handler(textOTEvent_withFile_Fail, None)
        elif (extent == "NT"):
            if (expectedOutcome == "pass"):
                messages = handler(textNTEvent_withFile, None)
            else:
                messages = handler(textNTEvent_withFile_Fail, None)
        elif (extent == "BOTH"):
            messages = handler(textBOTHEvent_withFile, None)
        elif (extent == "NT_WITHOUTFILE"):
            messages = handler(textNTEvent_withoutFile, None)            
        else:
            print("didn't recognize extent: [%s]" % (extent))
            sys.exit()
    elif (type == "audio"):
        if (expectedOutcome == "pass"):
            messages = handler(audioEvent, None) 
        else:
            messages = handler(audioEvent_Fail, None) 
    elif (type == "video"):
        print ("video event not implemented")     
        sys.exit()
    else:
       print("didn't recognize type: [%s]" % (type))
       sys.exit()

    if (len(messages) > 0):
        print ("validate failed: %s" % (messages))
    else:
        print("Validation passed")


# python3 load/PreValidateLambdaHandlerStub.py test text OT 
# python3 load/PreValidateLambdaHandlerStub.py test text OT fail
# python3 load/PreValidateLambdaHandlerStub.py test text NT
# python3 load/PreValidateLambdaHandlerStub.py test text NT fail
# python3 load/PreValidateLambdaHandlerStub.py test text BOTH
# python3 load/PreValidateLambdaHandlerStub.py test text NT_WITHOUTFILE
# python3 load/PreValidateLambdaHandlerStub.py test audio 
# python3 load/PreValidateLambdaHandlerStub.py test audio xx fail
# python3 load/PreValidateLambdaHandlerStub.py test video (not yet implemented)
