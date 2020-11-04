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
