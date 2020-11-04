/**
* This class traverses a DOM tree in order to create an equivalent HTML document.
*/
function HTMLBuilder() {
	this.result = [];
	Object.seal(this);
}
HTMLBuilder.prototype.toHTML = function(fragment) {
	this.result = [];
	this.readRecursively(fragment);
	return(this.result.join(''));
};
HTMLBuilder.prototype.readRecursively = function(node) {
	var nodeName = node.nodeName.toLowerCase();
	switch(node.nodeType) {
		case 11: // fragment
			break;
		case 1: // element
			this.result.push('<', nodeName);
			var attrs = node.attrNames();
			for (var i=0; i<attrs.length; i++) {
				this.result.push(' ', attrs[i], '="', node.getAttribute(attrs[i]), '"');
			}
			if (node.emptyElement) {
				this.result.push(' />');
			} else {
				this.result.push('>');
			}
			break;
		case 3: // text
			this.result.push(node.text);
			break;
		default:
			throw new Error('Unexpected nodeType ' + node.nodeType + ' in HTMLBuilder.toHTML().');
	}
	if (node.childNodes) {
		for (var child=0; child<node.childNodes.length; child++) {
			this.readRecursively(node.childNodes[child]);
		}
	}
	if (node.nodeType === 1 && ! node.emptyElement) {
		this.result.push('</', nodeName, '>');
	}
};
HTMLBuilder.prototype.toJSON = function() {
	return(this.result.join(''));
};


