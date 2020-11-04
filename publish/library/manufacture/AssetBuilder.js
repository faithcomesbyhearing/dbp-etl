/**
* The Table of Contents and Concordance must be created by processing the entire text.  Since the parsing of the XML
* is a significant amount of the time to do this, this class reads over the entire Bible text and creates
* all of the required assets.
*/
function AssetBuilder(types, database, pubVersion) {
	this.types = types;
	this.database = database;
	this.builders = [];
	if (types.chapterFiles) {
		var chapterBuilder = new ChapterBuilder(this.database.chapters, pubVersion);
		this.builders.push(chapterBuilder);
		this.builders.push(new VerseBuilder(this.database.verses, chapterBuilder));
	}
	if (types.tableContents) {
		this.builders.push(new TOCBuilder(this.database.tableContents));
	}
	if (types.concordance) {
		this.builders.push(new ConcordanceBuilder(this.database.concordance, pubVersion));
	}
	if (types.styleIndex) {
		this.builders.push(new StyleIndexBuilder(this.database.styleIndex));
		this.builders.push(new StyleUseBuilder(this.database.styleUse));
	}
	if (types.statistics) {
		this.builders.push(new VersionStatistics(this.database));
	}
	this.reader = new FileReader(types.location);
	this.parser = new USXParser();
	this.filesToProcess = [];
	Object.freeze(this);
}
AssetBuilder.prototype.build = function(callback) {
	var that = this;
	if (this.builders.length > 0) {
		this.filesToProcess.splice(0);
		var canon = new Canon();
		for (var i=0; i<canon.books.length; i++) {
			this.filesToProcess.push(canon.books[i].code + '.usx');
		}
		processReadFile(this.filesToProcess.shift());
	} else {
		callback();
	}
	function processReadFile(file) {
		if (file) {
			that.reader.readTextFile(that.types.getUSXPath(file), function(data) {
				if (data.errno) {
					if (data.errno === -2) { // file not found
						console.log('File Not Found', file);
						processReadFile(that.filesToProcess.shift());
					} else {
						console.log('file read err ', JSON.stringify(data));
						callback(data);
					}
				} else {
					var rootNode = that.parser.readBook(data);
					for (var i=0; i<that.builders.length; i++) {
						that.builders[i].readBook(rootNode);
					}
					processReadFile(that.filesToProcess.shift());
				}
			});
		} else {
			processDatabaseLoad(that.builders.shift());
		}
	}
	function processDatabaseLoad(builder) {
		if (builder) {
			builder.adapter.drop(function(err) {
				if (err instanceof IOError) {
					console.log('drop error', err);
					callback(err);
				} else {
					builder.adapter.create(function(err) {
						if (err instanceof IOError) {
							console.log('create error', err);
							callback(err);
						} else {
							builder.loadDB(function(err) {
								if (err instanceof IOError) {
									console.log('load db error', err);
									callback(err);
								} else {
									processDatabaseLoad(that.builders.shift());
								}
							});
						}
					});
				}
			});
		} else {
			callback();
		}
	}
};
