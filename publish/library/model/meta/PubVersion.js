/**
* This class is used to carry information about the language and version
* for the publish program.
*/
function PubVersion(row) {
	this.silCode = row.silCode;
	this.langCode = row.langCode;
	this.direction = row.direction;
	Object.freeze(this);
}