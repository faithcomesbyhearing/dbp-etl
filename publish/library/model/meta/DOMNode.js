/**
* This class is a Faux DOM node.  The USX classes create a DOM equivalent of themselves for presentation,
* which is then converted to HTML by HTMLBuilder.  But this is now done using this Faux Dom Node class
* so that the processing can run in Node.js without any window object.
*/
function DOMNode(nodeName) {
	this.nodeName = nodeName;
	this.nodeType = 1; // Element Node
	if (nodeName == 'root') this.nodeType = 11; // Fragment Node
	this.parentNode = null;
	this.attributes = {};
	this.emptyElement = false;
	this.childNodes = [];
	Object.seal(this);
}
DOMNode.prototype.getAttribute = function(name) {
	return(this.attributes[name]);
};
DOMNode.prototype.setAttribute = function(name, value) {
	this.attributes[name] = value;
};
DOMNode.prototype.hasAttribute = function(name) {
	var attr = this.attributes[name];
	return(attr !== null && attr !== undefined);	
};
DOMNode.prototype.attrNames = function() {
	return(Object.keys(this.attributes));	
};
DOMNode.prototype.appendChild = function(node) {
	this.childNodes.push(node);	
	node.parentNode = this;
};
DOMNode.prototype.appendText = function(text) {
	var node = new DOMText(text);
	this.appendChild(node);
};

/**
* This is an inner class of DOMNode, which contains only 
* text and whitespace.  It is presented as a separate class
* so that DOMText nodes can be children of DOMNode.
*/
function DOMText(text) {
	this.nodeName = 'text';
	this.nodeType = 3; // Text Node
	this.text = text;
	this.parentNode = null;
}

