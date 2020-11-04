# Publisher

The Publisher program prepares Bible Text for publication in the BibleApp.  It expects the Bible text to be in USX form, and translates it into publication ready text stored in a Sqlite database.  The Publisher program prepares the following content into Sqlite tables:

1. tableContent: contains data needed to present a Table of Contents, page headings and abbreviations.
    i.e. code, heading, title, name, abbrev, lastChapter, priorBook, nextBook, chapterRowId.
1. chapters: contains the Bible text in HTML form, and a reference as a primary key.
1. concordance: contains one row for each word in the Bible, and an index to all references for that word.
1. verses: contains one row of text for each verse in the Bible including the text.  This is used to present search results.
1. styleUse: contain a list of USFM/USX styles that occurred in the translation and the references.  This is used to ensure the App supports all of the needed styles.
1. styleIndex: contains a summary list of the USFM/USX styles that occurred in the translation.
1. valPunctuation: contains a reference to each punctuation character that was dropped during preparation of the concordance.
1. valConcordance: will be used during concordance validation
1. charset: contains one row for each character that occurs.  It is used during validation.
1. identity: will contain identity and version information after the file is validated
   
## Running Publisher

At this writing the Publisher program expects a specific directory structure so that many versions can be processed quickly in an organized manner.

The source USX files are expected in the following location:

    $HOME/ShortSands/DBL/2current/<VersionId>/USX_1/<chapter_files>.usx

To execute the Publisher program you must have both the Publisher directory and the Library directory on your desktop.  The unix/mac command line script is as follows:

    ./Publisher.sh VersionId

The program will create a Sqlite database at the following location with all of the above tables as described.  The directory shown must already exist.

    $HOME/ShortSands/DBL/3prepared/






       
     
       
 
