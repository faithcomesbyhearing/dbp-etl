/**
* This object contains information about a chapter of the Bible from a parsed USX Bible document.
*/
function Chapter(node) {
	this.number = node.number;
	this.style = node.style;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	Object.seal(this);
}
Chapter.prototype.tagName = 'chapter';
Chapter.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	return('<chapter number="' + this.number + '" style="' + this.style + elementEnd);
};
Chapter.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</chapter>');
};
Chapter.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	result.push(this.closeElement());
};
Chapter.prototype.toDOM = function(parentNode, bookCode, localizeNumber) {
	var child = new DOMNode('p');
	child.setAttribute('class', this.style);
	child.emptyElement = false;
	child.appendText(localizeNumber.toLocal(this.number));
	parentNode.appendChild(child);
	return(child);
};
