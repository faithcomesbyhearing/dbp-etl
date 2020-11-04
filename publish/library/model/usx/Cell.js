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
