"use strict";
/**
* This object of the Director pattern, it contains a boolean member for each type of asset.
* Setting a member to true will be used by the Builder classes to control which assets are built.
*/
function AssetType(location, versionCode) {
	this.location = location;
	this.versionCode = versionCode;
	this.chapterFiles = false;
	this.tableContents = false;
	this.concordance = false;
	this.styleIndex = false;
	this.statistics = false;
	Object.seal(this);
}
AssetType.prototype.getUSXPath = function(filename) {
	return(this.versionCode + '/USX_1/' + filename);
};

/**
* The Table of Contents and Concordance must be created by processing the entire text.  Since the parsing of the XML
* is a significant amount of the time to do this, this class reads over the entire Bible text and creates
* all of the required assets.
*/
function AssetBuilder(types, database, pubVersion) {
	this.types = types;
	this.database = database;
	this.builders = [];
	if (types.chapterFiles) {
		var chapterBuilder = new ChapterBuilder(this.database.chapters, pubVersion);
		this.builders.push(chapterBuilder);
		this.builders.push(new VerseBuilder(this.database.verses, chapterBuilder));
	}
	if (types.tableContents) {
		this.builders.push(new TOCBuilder(this.database.tableContents));
	}
	if (types.concordance) {
		this.builders.push(new ConcordanceBuilder(this.database.concordance, pubVersion));
	}
	if (types.styleIndex) {
		this.builders.push(new StyleIndexBuilder(this.database.styleIndex));
		this.builders.push(new StyleUseBuilder(this.database.styleUse));
	}
	if (types.statistics) {
		this.builders.push(new VersionStatistics(this.database));
	}
	this.reader = new FileReader(types.location);
	this.parser = new USXParser();
	this.filesToProcess = [];
	Object.freeze(this);
}
AssetBuilder.prototype.build = function(callback) {
	var that = this;
	if (this.builders.length > 0) {
		this.filesToProcess.splice(0);
		var canon = new Canon();
		for (var i=0; i<canon.books.length; i++) {
			this.filesToProcess.push(canon.books[i].code + '.usx');
		}
		processReadFile(this.filesToProcess.shift());
	} else {
		callback();
	}
	function processReadFile(file) {
		if (file) {
			that.reader.readTextFile(that.types.getUSXPath(file), function(data) {
				if (data.errno) {
					if (data.errno === -2) { // file not found
						console.log('File Not Found', file);
						processReadFile(that.filesToProcess.shift());
					} else {
						console.log('file read err ', JSON.stringify(data));
						callback(data);
					}
				} else {
					var rootNode = that.parser.readBook(data);
					for (var i=0; i<that.builders.length; i++) {
						that.builders[i].readBook(rootNode);
					}
					processReadFile(that.filesToProcess.shift());
				}
			});
		} else {
			processDatabaseLoad(that.builders.shift());
		}
	}
	function processDatabaseLoad(builder) {
		if (builder) {
			builder.adapter.drop(function(err) {
				if (err instanceof IOError) {
					console.log('drop error', err);
					callback(err);
				} else {
					builder.adapter.create(function(err) {
						if (err instanceof IOError) {
							console.log('create error', err);
							callback(err);
						} else {
							builder.loadDB(function(err) {
								if (err instanceof IOError) {
									console.log('load db error', err);
									callback(err);
								} else {
									processDatabaseLoad(that.builders.shift());
								}
							});
						}
					});
				}
			});
		} else {
			callback();
		}
	}
};
/**
* This program reads a version of the Bible and generates statistics about what is found there.
*/
function VersionStatistics(database) {
	this.adapter = new CharsetAdapter(database);
	this.charCounts = {};
	Object.seal(this);
}
VersionStatistics.prototype.readBook = function(usxRoot) {
	var that = this;
	readRecursively(usxRoot);

	function readRecursively(node) {
		switch(node.tagName) {
			case 'book':
				break;
			case 'chapter':
				break;
			case 'verse':
				break;
			case 'note':
				parse(node.text);
				break;
			case 'text':
				parse(node.text);
				break;
			default:
				if ('children' in node) {
					for (var i=0; i<node.children.length; i++) {
						readRecursively(node.children[i]);
					}
				}
		}
	}
	function parse(text) {
		if (text) {
			for (var i=0; i<text.length; i++) {
				var charCode = text[i].charCodeAt();
				var counts = that.charCounts[charCode];
				if (counts) {
					that.charCounts[charCode] = counts + 1;
				} else {
					that.charCounts[charCode] = 1;
				}
			}
		}	
	}
};
VersionStatistics.prototype.loadDB = function(callback) {
	var array = [];
	var codes = Object.keys(this.charCounts);
	codes.sort(function(a, b) {
		return(a - b);
	});
	for (var i=0; i<codes.length; i++) {
		var charCode = codes[i];
		var count = this.charCounts[charCode];
		var char = String.fromCharCode(charCode);
		var hex = Number(charCode).toString(16).toUpperCase();
		if (hex.length === 3) hex = '0' + hex;
		if (hex.length === 2) hex = '00' + hex;
		console.log(hex, char, count);
		array.push([ hex, char, count ]);
	}
	this.adapter.load(array, function(err) {
		if (err instanceof IOError) {
			console.log('Charset Load Failed', JSON.stringify(err));
			callback(err);
		} else {
			console.log('Charset loaded in database');
			callback();
		}
	});
};/**
* This class iterates over the USX data model, and breaks it into files one for each chapter.
*
*/
function ChapterBuilder(adapter, pubVersion) {
	this.adapter = adapter;
	this.pubVersion = pubVersion;
	this.chapters = [];
	Object.seal(this);
}
ChapterBuilder.prototype.readBook = function(usxRoot) {
	var that = this;
	var priorChildNode = null;
	var bookCode = null;
	var chapterNum = 0;
	var oneChapter = new USX({ version: usxRoot.version });
	for (var i=0; i<usxRoot.children.length; i++) {
		var childNode = usxRoot.children[i];
		switch(childNode.tagName) {
			case 'book':
				bookCode = childNode.code;
				break;
			case 'chapter':
				this.chapters.push({bookCode: bookCode, chapterNum: chapterNum, usxTree: oneChapter});
				oneChapter = new USX({ version: usxRoot.version });
				chapterNum = childNode.number;
				break;
		}
		if (priorChildNode) oneChapter.addChild(priorChildNode);
		priorChildNode = childNode;
	}
	oneChapter.addChild(priorChildNode);
	this.chapters.push({bookCode: bookCode, chapterNum: chapterNum, usxTree: oneChapter});
};
ChapterBuilder.prototype.loadDB = function(callback) {
	var array = [];
	var domBuilder = new DOMBuilder(this.pubVersion);
	var htmlBuilder = new HTMLBuilder();
	for (var i=0; i<this.chapters.length; i++) {
		var chapObj = this.chapters[i];
		var xml = chapObj.usxTree.toUSX();
		var dom = domBuilder.toDOM(chapObj.usxTree);
		var html = htmlBuilder.toHTML(dom);
		var values = [ chapObj.bookCode + ':' + chapObj.chapterNum, xml, html ];
		array.push(values);
	}
	this.adapter.load(array, function(err) {
		if (err instanceof IOError) {
			console.log('Storing chapters failed');
			callback(err);
		} else {
			console.log('store chapters success');
			callback();
		}
	});	
};
/**
* This class reads the USX files of the Bibles, and divides them up into verses
* and stores the individual verses in the verses table.
*/
function VerseBuilder(adapter, chapterBuilder) {
	this.adapter = adapter;
	this.chapterBuilder = chapterBuilder;
	this.verses = [];
	this.breakList = []; // temp used by breakChapterIntoVerses
	this.oneVerse = null; // temp used by breakChapterIntoVerses
	this.scanResult = []; // temp used by extractVerseText
	Object.seal(this); 
}
VerseBuilder.prototype.readBook = function(usxRoot) {
	// Do nothing here. Data collection is done by using chapterBuilder
};
VerseBuilder.prototype.loadDB = function(callback) {
	var that = this;
	var chapters = this.chapterBuilder.chapters;
	for (var i=0; i<chapters.length; i++) {
		var chapObj = chapters[i];
		if (chapObj.chapterNum > 0) { // excludes FRT, GLO and chapter introductions
			var verseList = breakChapterIntoVerses(chapObj.usxTree);
			for (var j=0; j<verseList.length; j++) {
				var verseUSX = verseList[j];
				var verseNum = verseUSX.children[0].number;
				var verseHTML = extractVerseText(verseUSX);
				var reference = chapObj.bookCode + ':' + chapObj.chapterNum + ':' + verseNum;
				this.verses.push([ reference, verseUSX.toUSX(), verseHTML ]);		
			}
		}
	}
	this.adapter.load(this.verses, function(err) {
		if (err instanceof IOError) {
			console.log('Storing verses failed');
			callback(err);
		} else {
			console.log('store verses success');
			callback();
		}
	});
	function breakChapterIntoVerses(chapterUSX) {
		that.breakList = [];
		that.oneVerse = null;
		breakRecursively(chapterUSX);
		that.breakList.push(that.oneVerse); // last verse in chapter
		return(that.breakList);
	}
	function breakRecursively(verseUSX) {
		switch(verseUSX.tagName) {
			case 'book':
			case 'chapter':
			case 'note':
			case 'ref':
			case 'optbreak':
				break;
			case 'usx':
			case 'para':
				for (var i=0; i<verseUSX.children.length; i++) {
					breakRecursively(verseUSX.children[i]);
				}
				break;
			case 'verse':
				if (that.oneVerse) {
					that.breakList.push(that.oneVerse);
				}
				that.oneVerse = new USX({ version: 2.0 });
				that.oneVerse.addChild(verseUSX);
				break;
			case 'char':
			case 'text':
				if (that.oneVerse) {
					that.oneVerse.addChild(verseUSX);
				}
				break;
			default:
				throw new Error('Unknown tagName ' + verseUSX.tagName + ' in VerseBuilder.breakRecursively.');
		}
	}
	function extractVerseText(verseUSX) {
		that.scanResult = [];
		scanRecursively(verseUSX);
		return(that.scanResult.join('').trim());
	}
	function scanRecursively(node) {
		if (node.tagName === 'text') {
			that.scanResult.push(node.text);
		}
		if (node.tagName !== 'note' && 'children' in node) {
			for (var i=0; i<node.children.length; i++) {
				scanRecursively(node.children[i]);
			}
		}
	}
};
/**
* This class traverses the USX data model in order to find each book, and chapter
* in order to create a table of contents that is localized to the language of the text.
*/
function TOCBuilder(adapter) {
	this.adapter = adapter;
	this.toc = new TOC(adapter);
	this.tocBook = null;
	this.chapterRowSum = 1; // Initial value for first Book
	Object.seal(this);
}
TOCBuilder.prototype.readBook = function(usxRoot) {
	this.readRecursively(usxRoot);
};
TOCBuilder.prototype.readRecursively = function(node) {
	switch(node.tagName) {
		case 'book':
			var priorBook = null;
			if (this.tocBook) {
				this.tocBook.nextBook = node.code;
				priorBook = this.tocBook.code;
			}
			this.tocBook = new TOCBook(node.code);
			this.tocBook.priorBook = priorBook;
			this.tocBook.chapterRowId = this.chapterRowSum;
			this.chapterRowSum++; // add 1 for chapter 0.
			this.toc.addBook(this.tocBook);
			break;
		case 'chapter':
			this.tocBook.lastChapter = node.number;
			this.chapterRowSum++;
			break;
		case 'para':
			switch(node.style) {
				case 'h':
					this.tocBook.heading = node.children[0].text;
					break;
				case 'toc1':
					this.tocBook.title = node.children[0].text;
					break;
				case 'toc2':
					this.tocBook.name = node.children[0].text;
					break;
				case 'toc3':
					this.tocBook.abbrev = node.children[0].text;
					break;
			}
	}
	if ('children' in node) {
		for (var i=0; i<node.children.length; i++) {
			this.readRecursively(node.children[i]);
		}
	}
};
TOCBuilder.prototype.size = function() {
	return(this.toc.bookList.length);
};
TOCBuilder.prototype.loadDB = function(callback) {
	console.log('TOC loadDB records count', this.size());
	var array = [];
	var len = this.size();
	for (var i=0; i<len; i++) {
		var toc = this.toc.bookList[i];
		var heading = toc.heading || toc.name;
		var title = toc.title || toc.name || toc.heading;
		var name = toc.name || toc.heading;
		var abbrev = toc.abbrev || toc.name || toc.heading;
		if (toc.lastChapter == null) toc.lastChapter = 0; // ERV does not have chapters in FRT and GLO
		var values = [ toc.code, heading, title, name, abbrev, toc.lastChapter, 
			toc.priorBook, toc.nextBook, toc.chapterRowId ];
		array.push(values);
	}
	this.adapter.load(array, function(err) {
		if (err instanceof IOError) {
			console.log('TOC Builder Failed', JSON.stringify(err));
			callback(err);
		} else {
			console.log('TOC loaded in database');
			callback();
		}
	});
};
TOCBuilder.prototype.toJSON = function() {
	return(this.toc.toJSON());
};/**
* This class traverses the USX data model in order to find each word, and 
* reference to that word.
*
* This solution might not be unicode safe. GNG Apr 2, 2015
*/
function ConcordanceBuilder(adapter, pubVersion) {
	this.adapter = adapter;
	this.langCode = pubVersion.langCode;
	this.bookCode = '';
	this.chapter = '0';
	this.verse = '0';
	this.position = 0;
	this.refList = {};
	this.refList.constructor = undefined;
	this.refPositions = {};
	this.refPositions.constructor = undefined;
	this.refList2 = {};
	this.refList2.constructor = undefined;
	Object.seal(this);
}
ConcordanceBuilder.prototype.readBook = function(usxRoot) {
	this.bookCode = '';
	this.chapter = '0';
	this.verse = '0';
	this.position = 0;
	this.readRecursively(usxRoot);
};
ConcordanceBuilder.prototype.readRecursively = function(node) {
	switch(node.tagName) {
		case 'book':
			this.bookCode = node.code;
			this.chapter = '0';
			this.verse = '0';
			this.position = 0;
			break;
		case 'chapter':
			this.chapter = node.number;
			this.verse = '0';
			this.position = 0;
			break;
		case 'verse':
			this.verse = node.number;
			this.position = 0;
			break;
		case 'note':
			break; // Do not index notes
		case 'text':
			var words = null;
			if (this.langCode === 'zh' || this.langCode === 'th') {
				words = node.text.split('');
			} else {
				words = node.text.split(/[\s\-\u2010-\u2015\u2043\u058A]+/);
			}
			for (var i=0; i<words.length; i++) {
				var word2 = words[i];
				//var word1 = word2.replace(/[\u2000-\u206F\u2E00-\u2E7F\\'!"#\$%&\(\)\*\+,\-\.\/:;<=>\?@\[\]\^_`\{\|\}~\s0-9]+$/g, '');
				var word1 = word2.replace(/[\u0000-\u0040\u005B-\u0060\u007B-\u00BF\u2010-\u206F]+$/g, '');
				var word = word1.replace(/^[\u0000-\u0040\u005B-\u0060\u007B-\u00BF\u2010-\u206F]+/g, '');
				//var word = word1.replace(/^[\u2000-\u206F\u2E00-\u2E7F\\'!"#\$%&\(\)\*\+,\-\.\/:;<=>\?@\[\]\^_`\{\|\}~\s0-9]+/g, '');
				//var word = words[i].replace(/[\u2000-\u206F\u2E00-\u2E7F\\'!"#\$%&\(\)\*\+,\-\.\/:;<=>\?@\[\]\^_`\{\|\}~\s0-9]$/g, '');
				if (word.length > 0 && this.chapter !== '0' && this.verse !== '0') { // excludes FRT, GLO and chapter introductions
					var reference = this.bookCode + ':' + this.chapter + ':' + this.verse;
					this.position++;
					this.addEntry(word.toLocaleLowerCase(), reference, this.position);
				}
			}
			break;
		default:
			if ('children' in node) {
				for (var child=0; child<node.children.length; child++) {
					this.readRecursively(node.children[child]);
				}
			}

	}
};
ConcordanceBuilder.prototype.addEntry = function(word, reference, index) {
	if (this.refList[word] === undefined) {
		this.refList[word] = [];
		this.refPositions[word] = [];
		this.refList2[word] = [];
	}
	var list = this.refList[word];
	var pos = this.refPositions[word];
	var list2 = this.refList2[word];
	if (reference !== list[list.length -1]) { /* ignore duplicate reference */
		list.push(reference);
		pos.push(reference + ':' + index);
		list2.push(reference + ';' + index);
	} else {
		pos[pos.length -1] = pos[pos.length -1] + ':' + index;
		list2.push(reference + ';' + index);
	}
};
ConcordanceBuilder.prototype.size = function() {
	return(Object.keys(this.refList).length); 
};
ConcordanceBuilder.prototype.loadDB = function(callback) {
	console.log('Concordance loadDB records count', this.size());
	var words = Object.keys(this.refList);
	var array = [];
	for (var i=0; i<words.length; i++) {
		var word = words[i];
		if (this.refList[word]) {
			array.push([ word, this.refList[word].length, this.refList[word].join(','), this.refPositions[word].join(','), this.refList2[word].join(',') ]);
		}
	}
	this.adapter.load(array, function(err) {
		if (err instanceof IOError) {
			console.log('Concordance Builder Failed', JSON.stringify(err));
			callback(err);
		} else {
			console.log('concordance loaded in database');
			callback();
		}
	});
};
ConcordanceBuilder.prototype.toJSON = function() {
	return(JSON.stringify(this.refList, null, ' '));
};/**
* This class traverses the USX data model in order to find each style, and 
* reference to that style.  It builds an index to each style showing
* all of the references where each style is used.
*/
function StyleIndexBuilder(adapter) {
	this.adapter = adapter;
	this.index = {};
}
StyleIndexBuilder.prototype.addEntry = function(word, reference) {
	if (this.index[word] === undefined) {
		this.index[word] = [];
	}
	if (this.index[word].length < 100) {
		this.index[word].push(reference);
	}
};
StyleIndexBuilder.prototype.readBook = function(usxRoot) {
	this.bookCode = '';
	this.chapter = null;
	this.verse = null;
	this.readRecursively(usxRoot);
};
StyleIndexBuilder.prototype.readRecursively = function(node) {
	switch(node.tagName) {
		case 'book':
			this.bookCode = node.code;
			var style = 'book.' + node.style;
			var reference = this.bookCode;
			this.addEntry(style, reference);
			break;
		case 'chapter':
			this.chapter = node.number;
			style = 'chapter.' + node.style;
			reference = this.bookCode + ':' + this.chapter;
			this.addEntry(style, reference);
			break;
		case 'verse':
			this.verse = node.number;
			style = 'verse.' + node.style;
			reference = this.bookCode + ':' + this.chapter + ':' + this.verse;
			this.addEntry(style, reference);
			break;
		case 'usx':
		case 'text':
			// do nothing
			break;
		default:
			style = node.tagName + '.' + node.style;
			reference = this.bookCode + ':' + this.chapter + ':' + this.verse;
			this.addEntry(style, reference);
	}
	if ('children' in node) {
		for (var i=0; i<node.children.length; i++) {
			this.readRecursively(node.children[i]);
		}
	}
};
StyleIndexBuilder.prototype.size = function() {
	return(Object.keys(this.index).length);
};
StyleIndexBuilder.prototype.loadDB = function(callback) {
	console.log('style index loadDB records count', this.size());
	var array = [];
	var styles = Object.keys(this.index);
	for (var i=0; i<styles.length; i++) {
		var style = styles[i];
		var styleUse = style.split('.');
		var refList = this.index[style];
		for (var j=0; j<refList.length; j++) {
			var refItem = refList[j];
			var reference = refItem.split(':');
			switch(reference.length) {
				case 1:
					var values = [ styleUse[1], styleUse[0], reference[0], null, null ];
					break;
				case 2:
					values = [ styleUse[1], styleUse[0], reference[0], reference[1], null ];
					break;
				case 3:
					values = [ styleUse[1], styleUse[0], reference[0], reference[1], reference[2] ];
			}
			array.push(values);
		}
	}
	this.adapter.load(array, function(err) {
		if (err instanceof IOError) {
			console.log('StyleIndex Builder Failed', JSON.stringify(err));
			callback(err);
		} else {
			console.log('StyleIndex loaded in database');
			callback();
		}
	});
};
StyleIndexBuilder.prototype.toJSON = function() {
	return(this.toJSON());
};
/**
* This class builds a table of already handled styles so that we can easily
* query the styleIndex table for any styles that are new in a table.
*/
function StyleUseBuilder(adapter) {
	this.adapter = adapter;
}
StyleUseBuilder.prototype.readBook = function(usxRoot) {
	// This table is not populated from text of the Bible
};
StyleUseBuilder.prototype.loadDB = function(callback) {
	var styles = [ 'book.id', 'para.ide', 'para.h', 'para.toc1', 'para.toc2', 'para.toc3', 'para.rem',
		'para.imt', 'para.is',
		'para.iot', 'para.io', 'para.io1', 'para.io2',
		'para.ip', 'para.ipi',
		'chapter.c', 'para.cl', 'verse.v',
		'para.p', 'para.m', 'para.pi', 'para.pi1', 'para.pc', 'para.mi',
		'para.nb', 'para.q', 'para.q1', 'para.q2', 'para.qc',
		'char.qs', 'para.qa', 'para.b', 
		'para.mt', 'para.mt1', 'para.mt2', 'para.mt3', 'para.mt4',
		'para.ms', 'para.ms1', 'para.mr',
		'para.s', 'para.s1', 'para.s2', 'para.r', 'para.sp', 'para.d',
		'row.tr', 'cell.th1', 'cell.th2', 'cell.tc1', 'cell.tc2', 'cell.tcr1', 'cell.tcr2', 
		'para.li', 'para.li1',
		'note.f', 'char.ft', 'char.fk', 'char.fr', 
		'char.fqa', 'char.fv', 'char.fm',
		'note.x', 'char.xt', 'char.xo', 'char.rq',
		'char.nd', 'char.tl','char.bk', 'char.pn', 'char.wj', 'char.k', 'char.add',
		'char.it', 'char.bd', 'char.sc', 
		'para.pb', 'char.w', 'para.periph', 'para.toc'
		];
	var array = [];
	for (var i=0; i<styles.length; i++) {
		var style = styles[i];
		var styleUse = style.split('.');
		var values = [ styleUse[1], styleUse[0] ];
		array.push(values);
	}
	this.adapter.load(array, function(err) {
		if (err instanceof IOError) {
			console.log('StyleUse Builder Failed', JSON.stringify(err));
			callback(err);
		} else {
			console.log('StyleUse loaded in database');
			callback();
		}
	});
};/**
* This class iterates over the USX data model, and translates the contents to DOM.
*
* This method generates a DOM tree that has exactly the same parentage as the USX model.
* This is probably a problem.  The easy insertion and deletion of nodes probably requires
* having a hierarchy of books and chapters. GNG April 13, 2015
*
* The DOMNode class this uses is not a standard class, but one defined for this project
* at Library/util/DOMNode.js
*
* NOTE: This class must be instantiated once for an entire book are all books, not just one chapter,
* because the bookCode is only present in chapter 0, but is needed by all chapters.
*/
function DOMBuilder(pubVersion) {
	this.localizeNumber = new LocalizeNumber(pubVersion.silCode);
	this.direction = pubVersion.direction;
	this.bookCode = '';
	this.chapter = 0;
	this.verse = 0;
	this.noteNum = 0;
	this.treeRoot = null;
	Object.seal(this);
}
DOMBuilder.prototype.toDOM = function(usxRoot) {
	this.chapter = 0;
	this.verse = 0;
	this.noteNum = 0;
	this.treeRoot = new DOMNode('root');
	this.readRecursively(this.treeRoot, usxRoot);
	return(this.treeRoot);
};
DOMBuilder.prototype.readRecursively = function(domParent, node) {
	var domNode;
	//console.log('dom-parent: ', domParent.nodeName, domParent.nodeType, '  node: ', node.tagName);
	switch(node.tagName) {
		case 'usx':
			domNode = node.toDOM(domParent);
			break;
		case 'book':
			this.bookCode = node.code;
			domParent.setAttribute('id', this.bookCode + ':0');
			domNode = node.toDOM(domParent);
			break;
		case 'chapter':
			this.chapter = node.number;
			domParent.setAttribute('id', this.bookCode + ':' + this.chapter);
			this.noteNum = 0;
			domNode = node.toDOM(domParent, this.bookCode, this.localizeNumber);
			break;
		case 'para':
			domNode = node.toDOM(domParent);
			break;
		case 'verse':
			this.verse = node.number;
			domNode = node.toDOM(domParent, this.bookCode, this.chapter, this.localizeNumber);
			break;
		case 'text':
			node.toDOM(domParent, this.bookCode, this.chapter);
			domNode = domParent;
			break;
		case 'char':
			domNode = node.toDOM(domParent);
			break;
		case 'note':
			domNode = node.toDOM(domParent, this.bookCode, this.chapter, ++this.noteNum, this.direction);
			break;
		case 'ref':
			domNode = node.toDOM(domParent);
			break;
		case 'optbreak':
			domNode = node.toDOM(domParent);
			break;
		case 'table':
			domNode = node.toDOM(domParent);
			break;
		case 'row':
			domNode = node.toDOM(domParent);
			break;
		case 'cell':
			domNode = node.toDOM(domParent);
			break;
		default:
			throw new Error('Unknown tagname ' + node.tagName + ' in DOMBuilder.readBook');
	}
	if ('children' in node) {
		for (var i=0; i<node.children.length; i++) {
			this.readRecursively(domNode, node.children[i]);
		}
	}
};
/**
* This class traverses a DOM tree in order to create an equivalent HTML document.
*/
function HTMLBuilder() {
	this.result = [];
	Object.seal(this);
}
HTMLBuilder.prototype.toHTML = function(fragment) {
	this.result = [];
	this.readRecursively(fragment);
	return(this.result.join(''));
};
HTMLBuilder.prototype.readRecursively = function(node) {
	var nodeName = node.nodeName.toLowerCase();
	switch(node.nodeType) {
		case 11: // fragment
			break;
		case 1: // element
			this.result.push('<', nodeName);
			var attrs = node.attrNames();
			for (var i=0; i<attrs.length; i++) {
				this.result.push(' ', attrs[i], '="', node.getAttribute(attrs[i]), '"');
			}
			if (node.emptyElement) {
				this.result.push(' />');
			} else {
				this.result.push('>');
			}
			break;
		case 3: // text
			this.result.push(node.text);
			break;
		default:
			throw new Error('Unexpected nodeType ' + node.nodeType + ' in HTMLBuilder.toHTML().');
	}
	if (node.childNodes) {
		for (var child=0; child<node.childNodes.length; child++) {
			this.readRecursively(node.childNodes[child]);
		}
	}
	if (node.nodeType === 1 && ! node.emptyElement) {
		this.result.push('</', nodeName, '>');
	}
};
HTMLBuilder.prototype.toJSON = function() {
	return(this.result.join(''));
};


/**
* This class is the root object of a parsed USX document
*/
function USX(node) {
	this.version = node.version;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	this.children = []; // includes books, chapters, and paragraphs
	Object.seal(this);
}
USX.prototype.tagName = 'usx';
USX.prototype.addChild = function(node) {
	this.children.push(node);
	node.usxParent = this;
};
USX.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	return('\r\n<usx version="' + this.version + elementEnd);
};
USX.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</usx>');
};
USX.prototype.toUSX = function() {
	var result = [];
	this.buildUSX(result);
	return(result.join(''));
};
USX.prototype.toDOM = function(parentNode) {
	var child = new DOMNode('section');
	child.emptyElement = this.emptyElement;
	parentNode.appendChild(child);
	return(child);
};
USX.prototype.buildUSX = function(result) {
	result.push(String.fromCharCode('0xFEFF'), '<?xml version="1.0" encoding="utf-8"?>');
	result.push(this.openElement());
	for (var i=0; i<this.children.length; i++) {
		this.children[i].buildUSX(result);
	}
	result.push(this.closeElement());
};

/**
* This class contains a book of the Bible
*/
function Book(node) {
	this.code = node.code;
	this.style = node.style;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	this.children = []; // contains text
	Object.seal(this);
}
Book.prototype.tagName = 'book';
Book.prototype.addChild = function(node) {
	this.children.push(node);
	node.usxParent = this;
};
Book.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	return('<book code="' + this.code + '" style="' + this.style + elementEnd);
};
Book.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</book>');
};
Book.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	for (var i=0; i<this.children.length; i++) {
		this.children[i].buildUSX(result);
	}
	result.push(this.closeElement());
};
Book.prototype.toDOM = function(parentNode) {
	var article = new DOMNode('article');
	article.setAttribute('id', this.code);
	article.setAttribute('class', this.style);
	article.emptyElement = this.emptyElement;
	parentNode.appendChild(article);
	return(article);
};
/**
* This class contains a cell (th or td) element as parsed from a USX Bible file.
* This maps perfectly to the th or td element of a table.
*/
function Cell(node) {
	this.style = node.style;
	if (this.style !== 'tc1' && this.style !== 'tc2' && this.style !== 'tcr1' && this.style !== 'tcr2'  && this.style !== 'th1' && this.style !== 'th2') {
		throw new Error('Cell style must be tc1, tc2, tcr1, tcr2, th1, th2.  It is |' + this.style + '|');
	}
	this.align = node.align;
	if (this.align !== 'start' && this.align !== 'end') {
		throw new Error('Cell align must be start. It is |' + this.align + '|');
	}
	this.usxParent = null;
	this.children = [];
	Object.seal(this);
}
Cell.prototype.tagName = 'cell';
Cell.prototype.addChild = function(node) {
	this.children.push(node);
	node.usxParent = this;
};
Cell.prototype.openElement = function() {
	return('<cell style="' + this.style + '" align="' + this.align + '">');
};
Cell.prototype.closeElement = function() {
	return('</cell>');
};
Cell.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	for (var i=0; i<this.children.length; i++) {
		this.children[i].buildUSX(result);
	}
	result.push(this.closeElement());
};
Cell.prototype.toDOM = function(parentNode) {
	var child = new DOMNode('td');
	child.setAttribute('class', this.style);
	// align is not processed here.
	parentNode.appendChild(child);
	return(child);
};
/**
* This object contains information about a chapter of the Bible from a parsed USX Bible document.
*/
function Chapter(node) {
	this.number = node.number;
	this.style = node.style;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	Object.seal(this);
}
Chapter.prototype.tagName = 'chapter';
Chapter.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	return('<chapter number="' + this.number + '" style="' + this.style + elementEnd);
};
Chapter.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</chapter>');
};
Chapter.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	result.push(this.closeElement());
};
Chapter.prototype.toDOM = function(parentNode, bookCode, localizeNumber) {
	var child = new DOMNode('p');
	child.setAttribute('class', this.style);
	child.emptyElement = false;
	child.appendText(localizeNumber.toLocal(this.number));
	parentNode.appendChild(child);
	return(child);
};
/**
* This object contains a paragraph of the Bible text as parsed from a USX version of the Bible.
*/
function Para(node) {
	this.style = node.style;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	this.children = []; // contains verse | note | char | text
	Object.seal(this);
}
Para.prototype.tagName = 'para';
Para.prototype.addChild = function(node) {
	this.children.push(node);
	node.usxParent = this;
};
Para.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	return('<para style="' + this.style + elementEnd);
};
Para.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</para>');
};
Para.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	for (var i=0; i<this.children.length; i++) {
		this.children[i].buildUSX(result);
	}
	result.push(this.closeElement());
};
Para.prototype.toDOM = function(parentNode) {
	var identStyles = [ 'ide', 'sts', 'rem', 'h', 'toc1', 'toc2', 'toc3', 'cl' ];
	var child = new DOMNode('p');
	child.setAttribute('class', this.style);
	if (identStyles.indexOf(this.style) >= 0) {
		child.setAttribute('hidden', '');	
	}
	child.emptyElement = this.emptyElement;
	parentNode.appendChild(child);
	return(child);
};

/**
* This class contains a row (tr) element as parsed from a USX Bible file.
* This maps perfectly to the tr element of a table.
*/
function Row(node) {
	this.style = node.style;
	if (this.style !== 'tr') {
		throw new Error('Row style must be tr. It is ', this.style);
	}
	this.usxParent = null;
	this.children = [];
	Object.seal(this);
}
Row.prototype.tagName = 'row';
Row.prototype.addChild = function(node) {
	this.children.push(node);
	node.usxParent = this;
};
Row.prototype.openElement = function() {
	return('<row style="' + this.style + '">');
};
Row.prototype.closeElement = function() {
	return('</row>');
};
Row.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	for (var i=0; i<this.children.length; i++) {
		this.children[i].buildUSX(result);
	}
	result.push(this.closeElement());
};
Row.prototype.toDOM = function(parentNode) {
	var child = new DOMNode('tr');
	child.setAttribute('class', 'usx');
	parentNode.appendChild(child);
	return(child);
};
/**
* This class contains a table element as parsed from a USX Bible file.
* This contains no attributes.  In fact it is not an explicit node in
* usfm, but is created at the point of the first row.
*/
function Table(node) {
	this.usxParent = null;
	this.children = [];
	Object.seal(this);
}
Table.prototype.tagName = 'table';
Table.prototype.addChild = function(node) {
	this.children.push(node);
	node.usxParent = this;
};
Table.prototype.openElement = function() {
	return('<table>');
};
Table.prototype.closeElement = function() {
	return('</table>');
};
Table.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	for (var i=0; i<this.children.length; i++) {
		this.children[i].buildUSX(result);
	}
	result.push(this.closeElement());
};
Table.prototype.toDOM = function(parentNode) {
	var child = new DOMNode('table');
	child.setAttribute('class', 'usx');
	parentNode.appendChild(child);
	return(child);
};
/**
* This chapter contains the verse of a Bible text as parsed from a USX Bible file.
*/
function Verse(node) {
	this.number = node.number;
	this.style = node.style;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	Object.seal(this);
}
Verse.prototype.tagName = 'verse';
Verse.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	return('<verse number="' + this.number + '" style="' + this.style + elementEnd);
};
Verse.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</verse>');
};
Verse.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	result.push(this.closeElement());
};
Verse.prototype.toDOM = function(parentNode, bookCode, chapterNum, localizeNumber) {
	var reference = bookCode + ':' + chapterNum + ':' + this.number;
	var child = new DOMNode('span');
	child.setAttribute('id', reference);
	child.setAttribute('class', this.style);
	child.emptyElement = false;
	child.appendText(localizeNumber.toLocal(this.number) + '&nbsp;');
	parentNode.appendChild(child);
	return(child);
};
/**
* This class contains a Note from a USX parsed Bible
*/
function Note(node) {
	this.caller = node.caller.charAt(0);
	if (this.caller !== '+' && this.caller !== '-' && this.caller !== '*') {
		console.log(JSON.stringify(node));
		throw new Error('Note caller with no + or - or *');
	}
	this.style = node.style;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	this.children = [];
	Object.seal(this);
}
Note.prototype.tagName = 'note';
Note.prototype.addChild = function(node) {
	this.children.push(node);
	node.usxParent = this;
};
Note.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	return('<note caller="' + this.caller + '" style="' + this.style + elementEnd);
};
Note.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</note>');
};
Note.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	for (var i=0; i<this.children.length; i++) {
		this.children[i].buildUSX(result);
	}
	result.push(this.closeElement());
};
Note.prototype.toDOM = function(parentNode, bookCode, chapterNum, noteNum, direction) {
	var nodeId = bookCode + chapterNum + '-' + noteNum;
	var refChild = new DOMNode('span');
	refChild.setAttribute('id', nodeId);
	refChild.setAttribute('class', 'top' + this.style);
	refChild.setAttribute('caller', this.caller);
	refChild.setAttribute('onclick', "bibleShowNoteClick('" + nodeId + "');");
	switch(this.style) {
		case 'f':
			refChild.appendText((direction === 'rtl') ? ' \u261C ' : ' \u261E '); //261C points left, 261E points right
			break;
		case 'x':
			refChild.appendText((direction === 'rtl') ? ' \u261A ' : ' \u261B '); //261A points left, 261B points right
			break;
		default:
			refChild.appendText('* ');
	}
	refChild.emptyElement = false;
	parentNode.appendChild(refChild);
	return(refChild);
};
/**
* This class contains a character style as parsed from a USX Bible file.
*/
function Char(node) {
	this.style = node.style;
	this.closed = node.closed;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	this.children = [];
	Object.seal(this);
}
Char.prototype.tagName = 'char';
Char.prototype.addChild = function(node) {
	this.children.push(node);
	node.usxParent = this;
};
Char.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	if (this.closed) {
		return('<char style="' + this.style + '" closed="' + this.closed + elementEnd);
	} else {
		return('<char style="' + this.style + elementEnd);
	}
};
Char.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</char>');
};
Char.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	for (var i=0; i<this.children.length; i++) {
		this.children[i].buildUSX(result);
	}
	result.push(this.closeElement());
};
Char.prototype.toDOM = function(parentNode) {
	var child = new DOMNode('span');
	child.setAttribute('class', this.style);
	if (this.closed) child.setAttribute('closed', this.closed);
	child.emptyElement = this.emptyElement;
	parentNode.appendChild(child);
	return(child);
};
/**
* This class contains a ref element as parsed from a USX Bible file.
* This contains one attribute loc, which contain a bible reference
* And a text node, which contains the Bible reference in text form.
*/
function Ref(node) {
	this.loc = node.loc;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	this.children = [];
	Object.seal(this);
}
Ref.prototype.tagName = 'ref';
Ref.prototype.addChild = function(node) {
	this.children.push(node);
	node.usxParent = this;
};
Ref.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	return('<ref loc="' + this.loc + elementEnd);
};
Ref.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</ref>');
};
Ref.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	for (var i=0; i<this.children.length; i++) {
		this.children[i].buildUSX(result);
	}
	result.push(this.closeElement());
};
Ref.prototype.toDOM = function(parentNode) {
	var child = new DOMNode('span');
	child.setAttribute('loc', this.loc);
	child.emptyElement = this.emptyElement;
	parentNode.appendChild(child);
	return(child);
};
/**
* This class contains a optbreak element as parsed from a USX Bible file.
* This is an empty element, which defines an optional location for a line
* break
*/
function OptBreak(node) {
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	Object.seal(this);
}
OptBreak.prototype.tagName = 'optbreak';
OptBreak.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '/>' : '>';
	return('<optbreak' + elementEnd);
};
OptBreak.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</optbreak>');
};
OptBreak.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	result.push(this.closeElement());
};
OptBreak.prototype.toDOM = function(parentNode) {
	var child = new DOMNode('wbr');
	child.emptyElement = this.emptyElement;
	parentNode.appendChild(child);
	return(child);
};
/**
* This class contains a text string as parsed from a USX Bible file.
*/
function Text(text) {
	this.text = text;
	this.usxParent = null;
	Object.seal(this);
}
Text.prototype.tagName = 'text';
Text.prototype.buildUSX = function(result) {
	result.push(this.text);
};
Text.prototype.toDOM = function(parentNode, bookCode, chapterNum) {
	var that = this;
	if (parentNode.nodeName === 'article') {
		parentNode.setAttribute('hidden', this.text);
	} else if (parentNode.nodeName === 'section') {
		parentNode.appendText(this.text);
	} else if (parentNode.hasAttribute('hidden')) {
		parentNode.setAttribute('hidden', this.text);
	} else {
		var parentClass = parentNode.getAttribute('class');
		if (parentClass === 'fr' || parentClass === 'xo') {
			parentNode.setAttribute('hidden', this.text); // permanently hide note.
		} else {
			var count = isInsideNote(this);
			var parents = ancestors(this);
			//console.log(count, parents.join(', '));
			if (count === 0) {
				parentNode.appendText(this.text);
			} else if (count === 1) {
				var textNode = new DOMNode('span');
				textNode.setAttribute('class', parentClass.substr(3));
				textNode.appendText(this.text);
				textNode.setAttribute('style', 'display:none');
				parentNode.appendChild(textNode);
			} else if (count === 2) {
				parentNode.setAttribute('style', 'display:none');
				parentNode.appendText(this.text);
			} else {
				parentNode.appendText(this.text);
			}
		}
	}

	function isInsideNote(curr) {
		var count = 0;
		while (curr) {
			if (curr.tagName === 'note') {
				return(count);
			} else {
				count++;
				curr = curr.usxParent;
			}
		}
		return(0);
	}
	
	function ancestors(curr) {
		var parents = [curr.tagName];
		while (curr && curr.usxParent) {
			parents.push(curr.usxParent.tagName);
			curr = curr.usxParent;
		}
		return(parents);
	}
	
};


/**
* This class does a stream read of an XML string to return XML tokens and their token type.
*/
var XMLNodeType = Object.freeze({ELE:'ele', ELE_OPEN:'ele-open', ATTR_NAME:'attr-name', ATTR_VALUE:'attr-value', ELE_END:'ele-end', 
			WHITESP:'whitesp', TEXT:'text', ELE_EMPTY:'ele-empty', ELE_CLOSE:'ele-close', PROG_INST:'prog-inst', END:'end'});

function XMLTokenizer(data) {
	this.data = data;
	this.position = 0;

	this.tokenStart = 0;
	this.tokenEnd = 0;

	this.state = Object.freeze({ BEGIN:'begin', START:'start', WHITESP:'whitesp', TEXT:'text', ELE_START:'ele-start', ELE_OPEN:'ele-open', 
		EXPECT_EMPTY_ELE:'expect-empty-ele', ELE_CLOSE:'ele-close', 
		EXPECT_ATTR_NAME:'expect-attr-name', ATTR_NAME:'attr-name', EXPECT_ATTR_VALUE:'expect-attr-value1', ATTR_VALUE:'attr-value', 
		PROG_INST:'prog-inst', END:'end' });
	this.current = this.state.BEGIN;

	Object.seal(this);
}
XMLTokenizer.prototype.tokenValue = function() {
	return(this.data.substring(this.tokenStart, this.tokenEnd));
};
XMLTokenizer.prototype.nextToken = function() {
	this.tokenStart = this.position;
	while(this.position < this.data.length) {
		var chr = this.data[this.position++];
		//console.log(this.current, chr, chr.charCodeAt(0));
		switch(this.current) {
			case this.state.BEGIN:
				if (chr === '<') {
					this.current = this.state.ELE_START;
					this.tokenStart = this.position;
				}
				else if (chr === ' ' || chr === '\t' || chr === '\n' || chr === '\r') {
					this.current = this.state.WHITESP;
					this.tokenStart = this.position -1;
				}
				break;
			case this.state.START:
				if (chr === '<') {
					this.current = this.state.ELE_START;
					this.tokenStart = this.position;
				}
				else if (chr === ' ' || chr === '\t' || chr === '\n' || chr === '\r') {
					this.current = this.state.WHITESP;
					this.tokenStart = this.position -1;
				}
				else {
					this.current = this.state.TEXT;
					this.tokenStart = this.position -1;
				}
				break;
			case this.state.WHITESP:
				if (chr === '<') {
					this.current = this.state.START;
					this.position--;
					this.tokenEnd = this.position;
					return(XMLNodeType.WHITESP);
				}
				else if (chr !== ' ' && chr !== '\t' && chr !== '\n' && chr !== '\r') {
					this.current = this.state.TEXT;
				}
				break;
			case this.state.TEXT:
				if (chr === '<') {
					this.current = this.state.START;
					this.position--;
					this.tokenEnd = this.position;
					return(XMLNodeType.TEXT);
				}
				break;
			case this.state.ELE_START:
				if (chr === '/') {
					this.current = this.state.ELE_CLOSE;
					this.tokenStart = this.position;
				} 
				else if (chr === '?') {
					this.current = this.state.PROG_INST;
					this.tokenStart = this.position;
				} 
				else {
					this.current = this.state.ELE_OPEN;
				}
				break;
			case this.state.ELE_OPEN:
				if (chr === ' ') {
					this.current = this.state.EXPECT_ATTR_NAME;
					this.tokenEnd = this.position -1;
					return(XMLNodeType.ELE_OPEN);
				} 
				else if (chr === '>') {
					this.current = this.state.START;
					this.tokenEnd = this.position -1;
					return(XMLNodeType.ELE);
				}
				else if (chr === '/') {
					this.current = this.state.EXPECT_EMPTY_ELE;
					this.tokenEnd = this.position -1;
					return(XMLNodeType.ELE_OPEN);
				}
				break;
			case this.state.ELE_CLOSE:
				if (chr === '>') {
					this.current = this.state.START;
					this.tokenEnd = this.position -1;
					return(XMLNodeType.ELE_CLOSE);
				}
				break;
			case this.state.EXPECT_ATTR_NAME:
				if (chr === '>') {
					this.current = this.state.START;
					this.tokenEnd = this.tokenStart;
					return(XMLNodeType.ELE_END);
				}
				else if (chr === '/') {
					this.current = this.state.EXPECT_EMPTY_ELE;
				}
				else if (chr !== ' ') {
					this.current = this.state.ATTR_NAME;
					this.tokenStart = this.position -1;		
				}
				break;
			case this.state.EXPECT_EMPTY_ELE:
				if (chr === '>') {
					this.current = this.state.START;
					this.tokenEnd = this.tokenStart;
					return(XMLNodeType.ELE_EMPTY);
				}
				break;
			case this.state.ATTR_NAME:
				if (chr === '=') {
					this.current = this.state.EXPECT_ATTR_VALUE;
					this.tokenEnd = this.position -1;
					return(XMLNodeType.ATTR_NAME);
				}
				break;
			case this.state.EXPECT_ATTR_VALUE:
				if (chr === '"') {
					this.current = this.state.ATTR_VALUE;
					this.tokenStart = this.position;
				} else if (chr !== ' ') {
					throw new Error();
				}
				break;
			case this.state.ATTR_VALUE:
				if (chr === '"') {
					this.current = this.state.EXPECT_ATTR_NAME;
					this.tokenEnd = this.position -1;
					return(XMLNodeType.ATTR_VALUE);
				}
				break;
			case this.state.PROG_INST:
				if (chr === '>') {
					this.current = this.state.START;
					this.tokenStart -= 2;
					this.tokenEnd = this.position;
					return(XMLNodeType.PROG_INST);
				}
				break;
			default:
				throw new Error('Unknown state ' + this.current);
		}
	}
	return(XMLNodeType.END);
};
/**
* This class reads USX files and creates an equivalent object tree
* elements = [usx, book, chapter, para, verse, note, char];
* paraStyle = [b, d, cl, cp, h, li, p, pc, q, q2, mt, mt2, mt3, mte, toc1, toc2, toc3, ide, ip, ili, ili2, is, m, mi, ms, nb, pi, s, sp];
* charStyle = [add, bk, it, k, fr, fq, fqa, ft, wj, qs, xo, xt];
*/
function USXParser() {
}
USXParser.prototype.readBook = function(data) {
	var reader = new XMLTokenizer(data);
	var nodeStack = [];
	var node;
	var tempNode = {};
	var count = 0;
	while (tokenType !== XMLNodeType.END && count < 300000) {

		var tokenType = reader.nextToken();

		var tokenValue = reader.tokenValue();
		//console.log('type=|' + type + '|  value=|' + value + '|');
		count++;

		switch(tokenType) {
			case XMLNodeType.ELE:
				node = this.createUSXObject({ tagName: tokenValue, emptyElement: false });
				if (nodeStack.length > 0) {
					nodeStack[nodeStack.length -1].addChild(node);
				}
				nodeStack.push(node);
			case XMLNodeType.ELE_OPEN:
				tempNode = { tagName: tokenValue };
				tempNode.whiteSpace = '';
				//console.log(tokenValue, priorType, '|' + priorValue + '|');
				break;
			case XMLNodeType.ATTR_NAME:
				tempNode[tokenValue] = '';
				break;
			case XMLNodeType.ATTR_VALUE:
				tempNode[priorValue] = tokenValue;
				break;
			case XMLNodeType.ELE_END:
				tempNode.emptyElement = false;
				node = this.createUSXObject(tempNode);
				//console.log(node.openElement());
				if (nodeStack.length > 0) {
					nodeStack[nodeStack.length -1].addChild(node);
				}
				nodeStack.push(node);
				break;
			case XMLNodeType.TEXT:
			case XMLNodeType.WHITESP:
				node = new Text(tokenValue);
				//console.log(node.text);
				if (nodeStack.length > 0) {
					nodeStack[nodeStack.length -1].addChild(node);
				}
				break;
			case XMLNodeType.ELE_EMPTY:
				tempNode.emptyElement = true;
				node = this.createUSXObject(tempNode);
				//console.log(node.openElement());
				nodeStack[nodeStack.length -1].addChild(node);
				break;
			case XMLNodeType.ELE_CLOSE:
				node = nodeStack.pop();
				//console.log(node.closeElement());
				if (node.tagName !== tokenValue) {
					throw new Error('closing element mismatch ' + node.openElement() + ' and ' + tokenValue);
				}
				break;
			case XMLNodeType.PROG_INST:
				// do nothing
				break;
			case XMLNodeType.END:
				// do nothing
				break;
			default:
				throw new Error('The XMLNodeType ' + tokenType + ' is unknown in USXParser.');
		}
		var priorValue = tokenValue;
	}
	return(node);
};
USXParser.prototype.createUSXObject = function(tempNode) {
	switch(tempNode.tagName) {
		case 'char':
			return(new Char(tempNode));
		case 'note':
			return(new Note(tempNode));
		case 'verse':
			return(new Verse(tempNode));
		case 'para':
			return(new Para(tempNode));
		case 'chapter':
			return(new Chapter(tempNode));
		case 'book':
			return(new Book(tempNode));
		case 'ref':
			return(new Ref(tempNode));
		case 'optbreak':
			return(new OptBreak(tempNode));
		case 'table':
			return(new Table(tempNode));
		case 'row':
			return(new Row(tempNode));
		case 'cell':
			return(new Cell(tempNode));
		case 'usx':
			return(new USX(tempNode));
		default:
			throw new Error('USX element name ' + tempNode.tagName + ' is not known to USXParser.');
	}
};
/**
* This class contains the Canon of Scripture as 66 books.  It is used to control
* which books are published using this App.  The codes are used to identify the
* books of the Bible, while the names, which are in English are only used to document
* the meaning of each code.  These names are not used for display in the App.
*/
function Canon() {
	this.books = [
		{ code: 'FRT', name: 'Preface' },
    	{ code: 'GEN', name: 'Genesis' },
    	{ code: 'EXO', name: 'Exodus' },
    	{ code: 'LEV', name: 'Leviticus' },
    	{ code: 'NUM', name: 'Numbers' },
    	{ code: 'DEU', name: 'Deuteronomy' },
    	{ code: 'JOS', name: 'Joshua' },
    	{ code: 'JDG', name: 'Judges' },
    	{ code: 'RUT', name: 'Ruth' },
    	{ code: '1SA', name: '1 Samuel' },
    	{ code: '2SA', name: '2 Samuel' },
    	{ code: '1KI', name: '1 Kings' },
    	{ code: '2KI', name: '2 Kings' },
    	{ code: '1CH', name: '1 Chronicles' },
    	{ code: '2CH', name: '2 Chronicles' },
    	{ code: 'EZR', name: 'Ezra' },
    	{ code: 'NEH', name: 'Nehemiah' },
    	{ code: 'EST', name: 'Esther' },
    	{ code: 'JOB', name: 'Job' },
    	{ code: 'PSA', name: 'Psalms' },
    	{ code: 'PRO', name: 'Proverbs' },
    	{ code: 'ECC', name: 'Ecclesiastes' },
    	{ code: 'SNG', name: 'Song of Solomon' },
    	{ code: 'ISA', name: 'Isaiah' },
    	{ code: 'JER', name: 'Jeremiah' },
    	{ code: 'LAM', name: 'Lamentations' },
    	{ code: 'EZK', name: 'Ezekiel' },
    	{ code: 'DAN', name: 'Daniel' },
    	{ code: 'HOS', name: 'Hosea' },
    	{ code: 'JOL', name: 'Joel' },
    	{ code: 'AMO', name: 'Amos' },
    	{ code: 'OBA', name: 'Obadiah' },
    	{ code: 'JON', name: 'Jonah' },
    	{ code: 'MIC', name: 'Micah' },
    	{ code: 'NAM', name: 'Nahum' },
    	{ code: 'HAB', name: 'Habakkuk' },
    	{ code: 'ZEP', name: 'Zephaniah' },
    	{ code: 'HAG', name: 'Haggai' },
    	{ code: 'ZEC', name: 'Zechariah' },
    	{ code: 'MAL', name: 'Malachi' },
    	{ code: 'MAT', name: 'Matthew' },
    	{ code: 'MRK', name: 'Mark' },
    	{ code: 'LUK', name: 'Luke' },
    	{ code: 'JHN', name: 'John' },
    	{ code: 'ACT', name: 'Acts' },
    	{ code: 'ROM', name: 'Romans' },
    	{ code: '1CO', name: '1 Corinthians' },
    	{ code: '2CO', name: '2 Corinthians' },
    	{ code: 'GAL', name: 'Galatians' },
    	{ code: 'EPH', name: 'Ephesians' },
    	{ code: 'PHP', name: 'Philippians' },
    	{ code: 'COL', name: 'Colossians' },
    	{ code: '1TH', name: '1 Thessalonians' },
    	{ code: '2TH', name: '2 Thessalonians' },
    	{ code: '1TI', name: '1 Timothy' },
    	{ code: '2TI', name: '2 Timothy' },
    	{ code: 'TIT', name: 'Titus' },
    	{ code: 'PHM', name: 'Philemon' },
    	{ code: 'HEB', name: 'Hebrews' },
    	{ code: 'JAS', name: 'James' },
    	{ code: '1PE', name: '1 Peter' },
    	{ code: '2PE', name: '2 Peter' },
    	{ code: '1JN', name: '1 John' },
    	{ code: '2JN', name: '2 John' },
    	{ code: '3JN', name: '3 John' },
    	{ code: 'JUD', name: 'Jude' },
    	{ code: 'REV', name: 'Revelation' },
    	{ code: 'GLO', name: 'Glossary' },
    	{ code: 'BAK', name: 'Appendix' } ];
}
Canon.prototype.sequenceMap = function() {
	var result = {};
	for (var i=0; i<this.books.length; i++) {
		var item = this.books[i];
		result[item.code] = i;
	}
	return(result);
};
/**
* This class holds data for the table of contents of the entire Bible, or whatever part of the Bible was loaded.
*/
function TOC(adapter) {
	this.adapter = adapter;
	this.bookList = [];
	this.bookMap = {};
	this.isFilled = false;
	Object.seal(this);
}
TOC.prototype.fill = function(callback) {
	var that = this;
	this.adapter.selectAll(function(results) {
		if (results instanceof IOError) {
			callback();
		} else {
			that.bookList = results;
			that.bookMap = {};
			for (var i=0; i<results.length; i++) {
				var tocBook = results[i];
				that.bookMap[tocBook.code] = tocBook;
			}
			that.isFilled = true;
		}
		Object.freeze(that);
		callback();
	});
};
TOC.prototype.addBook = function(book) {
	this.bookList.push(book);
	this.bookMap[book.code] = book;
};
TOC.prototype.find = function(code) {
	return(this.bookMap[code]);
};
TOC.prototype.rowId = function(reference) {
	var current = this.bookMap[reference.book];
	var rowid = current.chapterRowId + reference.chapter;
	return(rowid);	
};
TOC.prototype.size = function() {
	return(this.bookList.length);
};
TOC.prototype.toString = function(reference) {
	return(this.find(reference.book).name + ' ' + reference.chapter + ':' + reference.verse);
};
TOC.prototype.toJSON = function() {
	return(JSON.stringify(this.bookList, null, ' '));
};/**
* This class holds the table of contents data each book of the Bible, or whatever books were loaded.
*/
function TOCBook(code, heading, title, name, abbrev, lastChapter, priorBook, nextBook, chapterRowId) {
	this.code = code || null;
	this.heading = heading || null;
	this.title = title || null;
	this.name = name || null;
	this.abbrev = abbrev || null;
	this.lastChapter = lastChapter || null;
	this.priorBook = priorBook || null;
	this.nextBook = nextBook || null; // do not want undefined in database
	this.chapterRowId = chapterRowId || null;
	if (lastChapter) {
		Object.freeze(this);
	} else {
		Object.seal(this);
	}
}/**
* This class holds the concordance of the entire Bible, or whatever part of the Bible was available.
*/
function Concordance(adapter, wordsLookAhead) {
	this.adapter = adapter;
	this.wordsLookAhead = (wordsLookAhead) ? wordsLookAhead : 0;
	Object.freeze(this);
}
Concordance.prototype.search = function(words, callback) {
	var that = this;
	this.adapter.select(words, function(refLists) {
		if (refLists instanceof IOError) {
			callback(refLists);
		} else {
			var result = intersection(refLists);
			callback(result);
		}
	});
	function intersection(refLists) {
		if (refLists.length === 0) {
			return([]);
		}
		if (refLists.length === 1) {
			return(refLists[0]);
		}
		var mapList = [];
		for (var i=1; i<refLists.length; i++) {
			var map = arrayToMap(refLists[i]);
			mapList.push(map);
		}
		var result = [];
		var firstList = refLists[0];
		for (var j=0; j<firstList.length; j++) {
			var reference = firstList[j];
			if (presentInAllMaps(mapList, reference)) {
				result.push(reference);
			}
		}
		return(result);
	}
	function arrayToMap(array) {
		var map = {};
		for (var i=0; i<array.length; i++) {
			map[array[i]] = true;
		}
		return(map);
	}
	function presentInAllMaps(mapList, reference) {
		for (var i=0; i<mapList.length; i++) {
			if (mapList[i][reference] === undefined) {
				return(false);
			}
		}
		return(true);
	}
};
Concordance.prototype.search2 = function(words, callback) {
	var that = this;
	this.adapter.select2(words, function(refLists) {
		if (refLists instanceof IOError) {
			callback(refLists);
		} else if (refLists.length !== words.length) {
			callback([]);
		} else {
			var resultList = intersection(refLists);
			callback(resultList);
		}
	});
	function intersection(refLists) {
		if (refLists.length === 0) {
			return([]);
		}
		var resultList = [];
		if (refLists.length === 1) {
			for (var ii=0; ii<refLists[0].length; ii++) {
				resultList.push([refLists[0][ii]]);
			}
			return(resultList);
		}
		var mapList = [];
		for (var i=1; i<refLists.length; i++) {
			var map = arrayToMap(refLists[i]);
			mapList.push(map);
		}
		var firstList = refLists[0];
		for (var j=0; j<firstList.length; j++) {
			var reference = firstList[j];
			var resultItem = matchEachWord(mapList, reference);
			if (resultItem) {
				resultList.push(resultItem);
			}
		}
		return(resultList);
	}
	function arrayToMap(array) {
		var map = {};
		for (var i=0; i<array.length; i++) {
			map[array[i]] = true;
		}
		return(map);
	}
	function matchEachWord(mapList, reference) {
		var resultItem = [ reference ];
		for (var i=0; i<mapList.length; i++) {
			reference = matchWordWithLookahead(mapList[i], reference);
			if (reference == null) {
				return(null);
			}
			resultItem.push(reference);
		}
		return(resultItem);
	}
	function matchWordWithLookahead(mapRef, reference) {
		for (var look=1; look<=that.wordsLookAhead + 1; look++) {
			var next = nextPosition(reference, look);
			if (mapRef[next]) {
				return(next);
			}
		}
		return(null);
	}
	function nextPosition(reference, position) {
		var parts = reference.split(';');
		var next = parseInt(parts[1]) + position;
		return(parts[0] + ';' + next.toString());
	}
};
/**
* This class is a Faux DOM node.  The USX classes create a DOM equivalent of themselves for presentation,
* which is then converted to HTML by HTMLBuilder.  But this is now done using this Faux Dom Node class
* so that the processing can run in Node.js without any window object.
*/
function DOMNode(nodeName) {
	this.nodeName = nodeName;
	this.nodeType = 1; // Element Node
	if (nodeName == 'root') this.nodeType = 11; // Fragment Node
	this.parentNode = null;
	this.attributes = {};
	this.emptyElement = false;
	this.childNodes = [];
	Object.seal(this);
}
DOMNode.prototype.getAttribute = function(name) {
	return(this.attributes[name]);
};
DOMNode.prototype.setAttribute = function(name, value) {
	this.attributes[name] = value;
};
DOMNode.prototype.hasAttribute = function(name) {
	var attr = this.attributes[name];
	return(attr !== null && attr !== undefined);	
};
DOMNode.prototype.attrNames = function() {
	return(Object.keys(this.attributes));	
};
DOMNode.prototype.appendChild = function(node) {
	this.childNodes.push(node);	
	node.parentNode = this;
};
DOMNode.prototype.appendText = function(text) {
	var node = new DOMText(text);
	this.appendChild(node);
};

/**
* This is an inner class of DOMNode, which contains only 
* text and whitespace.  It is presented as a separate class
* so that DOMText nodes can be children of DOMNode.
*/
function DOMText(text) {
	this.nodeName = 'text';
	this.nodeType = 3; // Text Node
	this.text = text;
	this.parentNode = null;
}

/**
* This class is used to carry information about the language and version
* for the publish program.
*/
function PubVersion(row) {
	this.silCode = row.silCode;
	this.langCode = row.langCode;
	this.direction = row.direction;
	Object.freeze(this);
}/**
* This class is a wrapper for SQL Error so that we can always distinguish an error
* from valid results.  Any method that calls an IO routine, which can expect valid results
* or an error should test "if (results instanceof IOError)".
*/
function IOError(err) {
	if (err.code && err.message) {
		this.code = err.code;
		this.message = err.message;
	} else {
		this.code = 0;
		this.message = JSON.stringify(err);
	}
}
/**
* This class is a file reader for Node.  It can be used with node.js and node-webkit.
* cordova requires using another class, but the interface should be the same.
*/
function FileReader(location) {
	this.fs = require('fs');
	this.location = location;
	Object.freeze(this);
}
FileReader.prototype.fileExists = function(filepath, callback) {
	var fullPath = this.location + filepath;
	console.log('checking fullpath', fullPath);
	this.fs.stat(fullPath, function(err, stat) {
		if (err) {
			err.filepath = filepath;
			callback(err);
		} else {
			callback(stat);
		}
	});
};
FileReader.prototype.readDirectory = function(filepath, callback) {
	var fullPath = this.location + filepath;
	console.log('read directory ', fullPath);
	this.fs.readdir(fullPath, function(err, data) {
		if (err) {
			err.filepath = filepath;
			callback(err);
		} else {
			callback(data);
		}
	});
};
FileReader.prototype.readTextFile = function(filepath, callback) {
	var fullPath = this.location + filepath;
	console.log('read file ', fullPath);
	this.fs.readFile(fullPath, { encoding: 'utf8'}, function(err, data) {
		if (err) {
			err.filepath = filepath;
			callback(err);
		} else {
			callback(data);
		}
	});
};/**
* This class is a facade over a node compatible sqlite adapterdatabase that is used to store bible text, concordance,
* table of contents, history and questions.
*
* This file is DeviceDatabaseNode, which implements a much simpler interface than the WebSQL interface.
*/
function DeviceDatabase(versionFile) {
	this.code = versionFile;
    this.className = 'DeviceDatabaseNode';
    var sqlite3 = require('sqlite3'); //.verbose();
	this.database = new sqlite3.Database(versionFile);
	//this.database.on('trace', function(sql) { console.log('DO ', sql); });
	//this.database.on('profile', function(sql, ms) { console.log(ms, 'DONE', sql); });
	this.database.exec("PRAGMA foreign_keys = ON");

	this.chapters = new ChaptersAdapter(this);
    this.verses = new VersesAdapter(this);
	this.tableContents = new TableContentsAdapter(this);
	this.concordance = new ConcordanceAdapter(this);
	this.styleIndex = new StyleIndexAdapter(this);
	this.styleUse = new StyleUseAdapter(this);
	Object.seal(this);
}
DeviceDatabase.prototype.select = function(statement, values, callback) {
	this.database.all(statement, values, function(err, results) {
		if (err) callback(new IOError(err));
		callback(results);
	});
};
DeviceDatabase.prototype.executeDML = function(statement, values, callback) {
	this.database.run(statement, values, function(err) {
		if (err) callback(err);
		callback(1); // See if this causes a problem.  If not, can I eliminate the rowsAffected?
	});
};
DeviceDatabase.prototype.manyExecuteDML = function(statement, array, callback) {
	var that = this;
	executeOne(0);
	
	function executeOne(index) {
		if (index < array.length) {
			that.executeDML(statement, array[index], function(results) {
				if (results instanceof IOError) {
					callback(results);
				} else {
					executeOne(index + 1);
				}
			});
		} else {
			callback(array.length);
		}
	}	
};
DeviceDatabase.prototype.bulkExecuteDML = function(statement, array, callback) {
    var that = this;
    this.database.serialize(function() {
    	that.database.exec('BEGIN TRANSACTION', function(err) {
	    	if (err) callback(new IOError(err));
    	});
    	var stmt = that.database.prepare(statement, function(err) {
	    	if (err) callback(new IOError(err));
    	});
    	for (var i=0; i<array.length; i++) {
	    	stmt.run(array[i], function(err) {
		    	if (err) callback(new IOError(err));
	    	});
    	}
    	that.database.exec('END TRANSACTION', function(err) {
	    	if (err) callback(new IOError(err));
	    	callback(array.length);
    	});
    });
};
DeviceDatabase.prototype.executeDDL = function(statement, callback) {
	this.database.exec(statement, function(err) {
		if (err) callback(new IOError(err));
		callback();
	});
};
DeviceDatabase.prototype.close = function() {
	this.database.close();
};


/**
* This class is the database adapter for the codex table
*/
//var IOError = require('./IOError'); What needs this, Publisher does not

function ChaptersAdapter(database) {
	this.database = database;
	this.className = 'ChaptersAdapter';
	Object.freeze(this);
}
ChaptersAdapter.prototype.drop = function(callback) {
	this.database.executeDDL('drop table if exists chapters', function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('drop chapters success');
			callback();
		}
	});
};
ChaptersAdapter.prototype.create = function(callback) {
	var statement = 'create table if not exists chapters(' +
		'reference text not null primary key, ' +
		'xml text not null, ' +
		'html text not null)';
	this.database.executeDDL(statement, function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('create chapters success');
			callback();
		}
	});
};
ChaptersAdapter.prototype.load = function(array, callback) {
	var statement = 'insert into chapters(reference, xml, html) values (?,?,?)';
	this.database.bulkExecuteDML(statement, array, function(count) {
		if (count instanceof IOError) {
			callback(count);
		} else {
			console.log('load chapters success, rowcount', count);
			callback();
		}
	});
};
ChaptersAdapter.prototype.getChapters = function(values, callback) {
	var statement = 'select reference, html from chapters where';
	if (values.length === 1) {
		statement += ' rowid = ?';
	} else {
		statement += ' rowid >= ? and rowid <= ? order by rowid';
	}
	this.database.select(statement, values, function(results) {
		if (results instanceof IOError) {
			console.log('found Error', results);
			callback(results);
		} else {
			callback(results);
        }
	});
};
/**
* This class is the database adapter for the verses table
*/
function VersesAdapter(database) {
	this.database = database;
	this.className = 'VersesAdapter';
	Object.freeze(this);
}
VersesAdapter.prototype.drop = function(callback) {
	this.database.executeDDL('drop table if exists verses', function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('drop verses success');
			callback();
		}
	});
};
VersesAdapter.prototype.create = function(callback) {
	var statement = 'create table if not exists verses(' +
		'reference text not null primary key, ' +
		'xml text not null, ' +
		'html text not null)';
	this.database.executeDDL(statement, function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('create verses success');
			callback();
		}
	});
};
VersesAdapter.prototype.load = function(array, callback) {
	var statement = 'insert into verses(reference, xml, html) values (?,?,?)';
	this.database.bulkExecuteDML(statement, array, function(count) {
		if (count instanceof IOError) {
			callback(count);
		} else {
			console.log('load verses success, rowcount', count);
			callback();
		}
	});
};
VersesAdapter.prototype.getVerses = function(values, callback) {
	var that = this;
	var numValues = values.length || 0;
	var array = [numValues];
	for (var i=0; i<numValues; i++) {
		array[i] = '?';
	}
	var statement = 'select reference, html from verses where reference in (' + array.join(',') + ') order by rowid';
	this.database.select(statement, values, function(results) {
		if (results instanceof IOError) {
			console.log('VersesAdapter select found Error', results);
			callback(results);
		} else if (results.rows.length === 0) {
			callback(new IOError({code: 0, message: 'No Rows Found'}));// Is this really an error?
		} else {
			callback(results);
        }
	});
};/**
* This class is the database adapter for the concordance table
*/
function ConcordanceAdapter(database) {
	this.database = database;
	this.className = 'ConcordanceAdapter';
	Object.freeze(this);
}
ConcordanceAdapter.prototype.drop = function(callback) {
	this.database.executeDDL('drop table if exists concordance', function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('drop concordance success');
			callback();
		}
	});
};
ConcordanceAdapter.prototype.create = function(callback) {
	var statement = 'create table if not exists concordance(' +
		'word text primary key not null, ' +
    	'refCount integer not null, ' +
    	'refList text not null, ' + // comma delimited list of references where word occurs
    	'refPosition text null, ' + // comma delimited list of references with position in verse.
    	'refList2 text null)';
   	this.database.executeDDL(statement, function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('create concordance success');
			callback();
		}
	});
};
ConcordanceAdapter.prototype.load = function(array, callback) {
	var statement = 'insert into concordance(word, refCount, refList, refPosition, refList2) values (?,?,?,?,?)';
	this.database.bulkExecuteDML(statement, array, function(count) {
		if (count instanceof IOError) {
			callback(count);
		} else {
			console.log('load concordance success', count);
			callback();
		}
	});
};
ConcordanceAdapter.prototype.select = function(words, callback) {
	var values = [ words.length ];
	var questMarks = [ words.length ];
	for (var i=0; i<words.length; i++) {
		values[i] = words[i].toLocaleLowerCase();
		questMarks[i] = '?';
	}
	var statement = 'select refList from concordance where word in(' + questMarks.join(',') + ')';
	this.database.select(statement, values, function(results) {
		if (results instanceof IOError) {
			console.log('found Error', results);
			callback(results);
		} else {
			var refLists = [];
			for (i=0; i<results.rows.length; i++) {
				var row = results.rows.item(i);
				if (row && row.refList) { // ignore words that have no ref list
					var array = row.refList.split(',');
					refLists.push(array);
				}
			}
            callback(refLists);
        }
	});
};
/**
* This is similar to select, except that it returns the refList2 field, 
* and resequences the results into the order the words were entered.
*/
ConcordanceAdapter.prototype.select2 = function(words, callback) {
	var values = [ words.length ];
	var questMarks = [ words.length ];
	for (var i=0; i<words.length; i++) {
		values[i] = words[i].toLocaleLowerCase();
		questMarks[i] = '?';
	}
	var statement = 'select word, refList2 from concordance where word in(' + questMarks.join(',') + ')';
	this.database.select(statement, values, function(results) {
		if (results instanceof IOError) {
			console.log('found Error', results);
			callback(results);
		} else {
			var resultMap = {};
			for (i=0; i<results.rows.length; i++) {
				var row = results.rows.item(i);
				if (row && row.refList2) { // ignore words that have no ref list
					resultMap[row.word] = row.refList2;
				}
			}
			var refLists = []; // sequence refList by order search words were entered
			for (i=0; i<values.length; i++) {
				var ref = resultMap[values[i]];
				if (ref) {
					refLists.push(ref.split(','));
				}
			}
            callback(refLists);
        }
	});
};/**
* This class is the database adapter for the tableContents table
*/
function TableContentsAdapter(database) {
	this.database = database;
	this.className = 'TableContentsAdapter';
	Object.freeze(this);
}
TableContentsAdapter.prototype.drop = function(callback) {
	this.database.executeDDL('drop table if exists tableContents', function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('drop tableContents success');
			callback();
		}
	});
};
TableContentsAdapter.prototype.create = function(callback) {
	var statement = 'create table if not exists tableContents(' +
		'code text primary key not null, ' +
    	'heading text not null, ' +
    	'title text not null, ' +
    	'name text not null, ' +
    	'abbrev text not null, ' +
		'lastChapter integer not null, ' +
		'priorBook text null, ' +
		'nextBook text null, ' +
		'chapterRowId integer not null)';
	this.database.executeDDL(statement, function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('create tableContents success');
			callback();
		}
	});
};
TableContentsAdapter.prototype.load = function(array, callback) {
	var statement = 'insert into tableContents(code, heading, title, name, abbrev, lastChapter, priorBook, nextBook, chapterRowId) ' +
		'values (?,?,?,?,?,?,?,?,?)';
	//this.database.manyExecuteDML(statement, array, function(count) {
	this.database.bulkExecuteDML(statement, array, function(count) {
		if (count instanceof IOError) {
			callback(count);
		} else {
			console.log('load tableContents success, rowcount', count);
			callback();
		}
	});
};
TableContentsAdapter.prototype.selectAll = function(callback) {
	var statement = 'select code, heading, title, name, abbrev, lastChapter, priorBook, nextBook, chapterRowId ' +
		'from tableContents order by rowid';
	this.database.select(statement, [], function(results) {
		if (results instanceof IOError) {
			callback(results);
		} else {
			var array = [];
			for (var i=0; i<results.rows.length; i++) {
				var row = results.rows.item(i);
				var tocBook = new TOCBook(row.code, row.heading, row.title, row.name, row.abbrev, 
					row.lastChapter, row.priorBook, row.nextBook, row.chapterRowId);
				array.push(tocBook);
			}
			callback(array);
		}
	});
};/**
* This class is the database adapter for the styleIndex table
*/
function StyleIndexAdapter(database) {
	this.database = database;
	this.className = 'StyleIndexAdapter';
	Object.freeze(this);
}
StyleIndexAdapter.prototype.drop = function(callback) {
	this.database.executeDDL('drop table if exists styleIndex', function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('drop styleIndex success');
			callback();
		}
	});
};
StyleIndexAdapter.prototype.create = function(callback) {
	var statement = 'create table if not exists styleIndex(' +
		'style text not null, ' +
		'usage text not null, ' +
		'book text not null, ' +
		'chapter integer null, ' +
		'verse integer null)';
	this.database.executeDDL(statement, function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('create styleIndex success');
			callback();
		}
	});
};
StyleIndexAdapter.prototype.load = function(array, callback) {
	var statement = 'insert into styleIndex(style, usage, book, chapter, verse) values (?,?,?,?,?)';
	this.database.bulkExecuteDML(statement, array, function(count) {
		if (count instanceof IOError) {
			callback(count);
		} else {
			console.log('load styleIndex success', count);
			callback();
		}
	});
};/**
* This class is the database adapter for the styleUse table
*/
function StyleUseAdapter(database) {
	this.database = database;
	this.className = 'StyleUseAdapter';
	Object.freeze(this);
}
StyleUseAdapter.prototype.drop = function(callback) {
	this.database.executeDDL('drop table if exists styleUse', function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('drop styleUse success');
			callback();
		}
	});
};
StyleUseAdapter.prototype.create = function(callback) {
	var statement = 'create table if not exists styleUse(' +
		'style text not null, ' +
		'usage text not null, ' +
		'primary key(style, usage))';
	this.database.executeDDL(statement, function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('create styleUse success');
			callback();
		}
	});
};
StyleUseAdapter.prototype.load = function(array, callback) {
	var statement = 'insert into styleUse(style, usage) values (?,?)';
	this.database.bulkExecuteDML(statement, array, function(count) {
		if (count instanceof IOError) {
			callback(count);
		} else {
			console.log('load styleUse success', count);
			callback();
		}
	});
};/**
* This class is the database adapter for the charset table
*/
function CharsetAdapter(database) {
	this.database = database;
	this.className = 'CharsetAdapter';
	Object.freeze(this);
}
CharsetAdapter.prototype.drop = function(callback) {
	this.database.executeDDL('drop table if exists charset', function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('drop charset success');
			callback();
		}
	});
};
CharsetAdapter.prototype.create = function(callback) {
	var statement = 'create table if not exists charset(' +
		'hex text not null, ' +
		'char text not null, ' +
		'count int not null)';
	this.database.executeDDL(statement, function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('create charset success');
			callback();
		}
	});
};
CharsetAdapter.prototype.load = function(array, callback) {
	var statement = 'insert into charset(hex, char, count) values (?,?,?)';
	this.database.bulkExecuteDML(statement, array, function(count) {
		if (count instanceof IOError) {
			callback(count);
		} else {
			console.log('load charset success', count);
			callback();
		}
	});
};/**
* This class is the database adapter that is used to read the Versions.db 
* In the publishing process.  It expects to use a node sqlite interface
* not a WebSQL interface used by the App
*/
function VersionsReadAdapter() {
    var sqlite3 = require('sqlite3'); //.verbose();
	this.database = new sqlite3.Database('../Versions/Versions.db');
	this.database.exec("PRAGMA foreign_keys = ON");
	Object.freeze(this);
}
VersionsReadAdapter.prototype.readVersion = function(versionCode, callback) {
	var statement = 'SELECT l.silCode, l.langCode, l.direction FROM language l JOIN version v ON v.silCode=l.silCode WHERE versionCode=?';
	this.database.get(statement, [versionCode], function(err, row) {
		if (err) {
			console.log('SELECT ERROR IN VERSION', JSON.stringify(err));
			callback();
		} else {
			var pubVersion = new PubVersion(row);
			callback(pubVersion);
		}
	});
};

/**
* This class will convert a number to a localized representation of the same number.
* This is used primarily for converting chapter and verse numbers, since USFM and USX
* always represent those numbers in ASCII.
*/
function LocalizeNumber(silCode) {
	this.silCode = silCode;
	switch(silCode) {
		case 'arb': // Arabic
			this.numberOffset = 0x0660 - 0x0030;
			break;
		case 'nep': // Nepali
			this.numberOffset = 0x0966 - 0x0030;
			break;
		case 'pes': // Persian
		case 'urd': // Urdu
			this.numberOffset = 0x06F0 - 0x0030;
			break;

		default:
			this.numberOffset = 0;
			break;
	}
	Object.freeze(this);
}
LocalizeNumber.prototype.toLocal = function(number) {
	if ((typeof number) === 'number') {
		return(this.convert(String(number), this.numberOffset));
	} else {
		return(this.convert(number, this.numberOffset));		
	}
};
LocalizeNumber.prototype.toTOCLocal = function(number) {
	if (number == 0) {
		return('\u2744');
	} else {
		return(this.toLocal(number));
	}
};
LocalizeNumber.prototype.toHistLocal = function(number) {
	if (number == 0) {
		return('');
	} else {
		return(this.toLocal(number));
	}
};
LocalizeNumber.prototype.toAscii = function(number) {
	return(this.convert(number, - this.numberOffset));
};
LocalizeNumber.prototype.convert = function(number, offset) {
	if (offset === 0) return(number);
	var result = [];
	for (var i=0; i<number.length; i++) {
		var char = number.charCodeAt(i);
		if (char > 47 && char < 58) { // if between 0 and 9
			result.push(String.fromCharCode(char + offset));
		} else {
			result.push(number.charAt(i));
		}
	}
	return(result.join(''));
};/**
* Unit Test Harness for AssetController
*/
var FILE_PATH = process.env.HOME + '/ShortSands/DBL/2current/';
var DB_PATH = process.env.HOME + '/ShortSands/DBL/3prepared/';
	
if (process.argv.length < 3) {
	console.log('Usage: ./Publisher.sh VERSION');
	process.exit(1);
} else {
	var version = process.argv[2];
	console.log('received', version);
	if (version && version.length > 2) {
		var types = new AssetType(FILE_PATH, version.toUpperCase());
		types.chapterFiles = true;
		types.tableContents = true;
		types.concordance = true;
		types.styleIndex = true;
		types.statistics = true;
		var database = new DeviceDatabase(DB_PATH + version.toUpperCase() + '.db');
		var versionAdapter = new VersionsReadAdapter();
		versionAdapter.readVersion(version.toUpperCase(), function(pubVersion) {
			if (pubVersion) {
				var builder = new AssetBuilder(types, database, pubVersion);
				builder.build(function(err) {
					if (err instanceof IOError) {
						console.log('FAILED', JSON.stringify(err));
						process.exit(1);
					} else {
						console.log('Success, Database created');
					}
				});		
			}
			else {
				console.log('FAILED TO GET LANGUAGE');
				process.exit(1);
			}
		});
	}
}

