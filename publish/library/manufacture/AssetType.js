/**
* This object of the Director pattern, it contains a boolean member for each type of asset.
* Setting a member to true will be used by the Builder classes to control which assets are built.
*/
function AssetType(location, versionCode) {
	this.location = location;
	this.versionCode = versionCode;
	this.chapterFiles = false;
	this.tableContents = false;
	this.concordance = false;
	this.styleIndex = false;
	this.statistics = false;
	Object.seal(this);
}
AssetType.prototype.getUSXPath = function(filename) {
	return(this.versionCode + '/USX_1/' + filename);
};

