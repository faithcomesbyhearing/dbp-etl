/**
* This class contains a text string as parsed from a USX Bible file.
*/
function Text(text) {
	this.text = text;
	this.usxParent = null;
	Object.seal(this);
}
Text.prototype.tagName = 'text';
Text.prototype.buildUSX = function(result) {
	result.push(this.text);
};
Text.prototype.toDOM = function(parentNode, bookCode, chapterNum) {
	var that = this;
	if (parentNode.nodeName === 'article') {
		parentNode.setAttribute('hidden', this.text);
	} else if (parentNode.nodeName === 'section') {
		parentNode.appendText(this.text);
	} else if (parentNode.hasAttribute('hidden')) {
		parentNode.setAttribute('hidden', this.text);
	} else {
		var parentClass = parentNode.getAttribute('class');
		if (parentClass === 'fr' || parentClass === 'xo') {
			parentNode.setAttribute('hidden', this.text); // permanently hide note.
		} else {
			var count = isInsideNote(this);
			var parents = ancestors(this);
			//console.log(count, parents.join(', '));
			if (count === 0) {
				parentNode.appendText(this.text);
			} else if (count === 1) {
				var textNode = new DOMNode('span');
				textNode.setAttribute('class', parentClass.substr(3));
				textNode.appendText(this.text);
				textNode.setAttribute('style', 'display:none');
				parentNode.appendChild(textNode);
			} else if (count === 2) {
				parentNode.setAttribute('style', 'display:none');
				parentNode.appendText(this.text);
			} else {
				parentNode.appendText(this.text);
			}
		}
	}

	function isInsideNote(curr) {
		var count = 0;
		while (curr) {
			if (curr.tagName === 'note') {
				return(count);
			} else {
				count++;
				curr = curr.usxParent;
			}
		}
		return(0);
	}
	
	function ancestors(curr) {
		var parents = [curr.tagName];
		while (curr && curr.usxParent) {
			parents.push(curr.usxParent.tagName);
			curr = curr.usxParent;
		}
		return(parents);
	}
	
};


