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
};