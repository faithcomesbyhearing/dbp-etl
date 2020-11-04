/**
* This class holds data for the table of contents of the entire Bible, or whatever part of the Bible was loaded.
*/
function TOC(adapter) {
	this.adapter = adapter;
	this.bookList = [];
	this.bookMap = {};
	this.maxRowId = 0;
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
				that.maxRowId = tocBook.chapterRowId + tocBook.lastChapter;
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
TOC.prototype.priorRowId = function(reference) {
	var rid = this.rowId(reference);
	return((rid && rid > 1) ? rid - 1: null);
};
TOC.prototype.nextRowId = function(reference) {
	var rid = this.rowId(reference);
	return((rid && rid < this.maxRowId) ? rid + 1: null);	
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
};