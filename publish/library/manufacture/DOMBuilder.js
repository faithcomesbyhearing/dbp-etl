/**
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
