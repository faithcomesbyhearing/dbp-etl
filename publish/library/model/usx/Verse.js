/**
* This chapter contains the verse of a Bible text as parsed from a USX Bible file.
*/
function Verse(node) {
	this.number = node.number;
	this.style = node.style;
	this.emptyElement = node.emptyElement;
	this.usxParent = null;
	Object.seal(this);
}
Verse.prototype.tagName = 'verse';
Verse.prototype.openElement = function() {
	var elementEnd = (this.emptyElement) ? '" />' : '">';
	return('<verse number="' + this.number + '" style="' + this.style + elementEnd);
};
Verse.prototype.closeElement = function() {
	return(this.emptyElement ? '' : '</verse>');
};
Verse.prototype.buildUSX = function(result) {
	result.push(this.openElement());
	result.push(this.closeElement());
};
Verse.prototype.toDOM = function(parentNode, bookCode, chapterNum, localizeNumber) {
	var reference = bookCode + ':' + chapterNum + ':' + this.number;
	var child = new DOMNode('span');
	child.setAttribute('id', reference);
	child.setAttribute('class', this.style);
	child.emptyElement = false;
	child.appendText(localizeNumber.toLocal(this.number) + '&nbsp;');
	parentNode.appendChild(child);
	return(child);
};
