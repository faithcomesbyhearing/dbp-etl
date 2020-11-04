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
};