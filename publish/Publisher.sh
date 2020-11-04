#!/bin/sh
echo \"use strict\"\; > Publisher.js
cat library/manufacture/AssetType.js >> Publisher.js
cat library/manufacture/AssetBuilder.js >> Publisher.js
cat library/manufacture/VersionStatistics.js >> Publisher.js
cat library/manufacture/ChapterBuilder.js >> Publisher.js
cat library/manufacture/VerseBuilder.js >> Publisher.js
cat library/manufacture/TOCBuilder.js >> Publisher.js
cat library/manufacture/ConcordanceBuilder.js >> Publisher.js
cat library/manufacture/StyleIndexBuilder.js >> Publisher.js
cat library/manufacture/StyleUseBuilder.js >> Publisher.js
cat library/manufacture/DOMBuilder.js >> Publisher.js
cat library/manufacture/HTMLBuilder.js >> Publisher.js
cat library/model/usx/USX.js >> Publisher.js
cat library/model/usx/Book.js >> Publisher.js
cat library/model/usx/Cell.js >> Publisher.js
cat library/model/usx/Chapter.js >> Publisher.js
cat library/model/usx/Para.js >> Publisher.js
cat library/model/usx/Row.js >> Publisher.js
cat library/model/usx/Table.js >> Publisher.js
cat library/model/usx/Verse.js >> Publisher.js
cat library/model/usx/Note.js >> Publisher.js
cat library/model/usx/Char.js >> Publisher.js
cat library/model/usx/Ref.js >> Publisher.js
cat library/model/usx/OptBreak.js >> Publisher.js
cat library/model/usx/Text.js >> Publisher.js
cat library/xml/XMLTokenizer.js >> Publisher.js
cat library/xml/USXParser.js >> Publisher.js
cat library/model/meta/Canon.js >> Publisher.js
cat library/model/meta/TOC.js >> Publisher.js
cat library/model/meta/TOCBook.js >> Publisher.js
cat library/model/meta/Concordance.js >> Publisher.js
cat library/model/meta/DOMNode.js >> Publisher.js
cat library/model/meta/PubVersion.js >> Publisher.js
cat library/io/IOError.js >> Publisher.js
cat library/io/NodeFileReader.js >> Publisher.js
cat library/io/DeviceDatabaseNode.js >> Publisher.js
cat library/io/ChaptersAdapter.js >> Publisher.js
cat library/io/VersesAdapter.js >> Publisher.js
cat library/io/ConcordanceAdapter.js >> Publisher.js
cat library/io/TableContentsAdapter.js >> Publisher.js
cat library/io/StyleIndexAdapter.js >> Publisher.js
cat library/io/StyleUseAdapter.js >> Publisher.js
cat library/io/CharsetAdapter.js >> Publisher.js
cat library/io/VersionsReadAdapter.js >> Publisher.js
cat library/util/LocalizeNumber.js >> Publisher.js
cat library/manufacture/AssetController.js >> Publisher.js
##node Publisher.js $*

##node library/manufacture/CopyBiblesDev.js $*
