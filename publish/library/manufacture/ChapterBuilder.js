/**
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
