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
