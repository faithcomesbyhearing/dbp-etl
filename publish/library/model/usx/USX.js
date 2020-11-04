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

