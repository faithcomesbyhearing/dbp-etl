/**
* This class contains a optbreak element as parsed from a USX Bible file.
* This is an empty element, which defines an optional location for a line
* break
*/
function OptBreak(node) {
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	Object.seal(this);
}
OptBreak.prototype.tagName = 'optbreak';
OptBreak.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '/>' : '>';
	return('<optbreak' + elementEnd);
};
OptBreak.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</optbreak>');
};
OptBreak.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	result.push(this.closeElement());
};
OptBreak.prototype.toDOM = function(parentNode) {
	var child = new DOMNode('wbr');
	child.emptyElement = this.emptyElement;
	parentNode.appendChild(child);
	return(child);
};
