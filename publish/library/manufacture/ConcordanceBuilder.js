/**
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
};