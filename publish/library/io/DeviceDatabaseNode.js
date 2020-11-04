/**
* This class is a facade over a node compatible sqlite adapterdatabase that is used to store bible text, concordance,
* table of contents, history and questions.
*
* This file is DeviceDatabaseNode, which implements a much simpler interface than the WebSQL interface.
*/
function DeviceDatabase(versionFile) {
	this.code = versionFile;
    this.className = 'DeviceDatabaseNode';
    var sqlite3 = require('sqlite3'); //.verbose();
	this.database = new sqlite3.Database(versionFile);
	//this.database.on('trace', function(sql) { console.log('DO ', sql); });
	//this.database.on('profile', function(sql, ms) { console.log(ms, 'DONE', sql); });
	this.database.exec("PRAGMA foreign_keys = ON");

	this.chapters = new ChaptersAdapter(this);
    this.verses = new VersesAdapter(this);
	this.tableContents = new TableContentsAdapter(this);
	this.concordance = new ConcordanceAdapter(this);
	this.styleIndex = new StyleIndexAdapter(this);
	this.styleUse = new StyleUseAdapter(this);
	Object.seal(this);
}
DeviceDatabase.prototype.select = function(statement, values, callback) {
	this.database.all(statement, values, function(err, results) {
		if (err) callback(new IOError(err));
		callback(results);
	});
};
DeviceDatabase.prototype.executeDML = function(statement, values, callback) {
	this.database.run(statement, values, function(err) {
		if (err) callback(err);
		callback(1); // See if this causes a problem.  If not, can I eliminate the rowsAffected?
	});
};
DeviceDatabase.prototype.manyExecuteDML = function(statement, array, callback) {
	var that = this;
	executeOne(0);
	
	function executeOne(index) {
		if (index < array.length) {
			that.executeDML(statement, array[index], function(results) {
				if (results instanceof IOError) {
					callback(results);
				} else {
					executeOne(index + 1);
				}
			});
		} else {
			callback(array.length);
		}
	}	
};
DeviceDatabase.prototype.bulkExecuteDML = function(statement, array, callback) {
    var that = this;
    this.database.serialize(function() {
    	that.database.exec('BEGIN TRANSACTION', function(err) {
	    	if (err) callback(new IOError(err));
    	});
    	var stmt = that.database.prepare(statement, function(err) {
	    	if (err) callback(new IOError(err));
    	});
    	for (var i=0; i<array.length; i++) {
	    	stmt.run(array[i], function(err) {
		    	if (err) callback(new IOError(err));
	    	});
    	}
    	that.database.exec('END TRANSACTION', function(err) {
	    	if (err) callback(new IOError(err));
	    	callback(array.length);
    	});
    });
};
DeviceDatabase.prototype.executeDDL = function(statement, callback) {
	this.database.exec(statement, function(err) {
		if (err) callback(new IOError(err));
		callback();
	});
};
DeviceDatabase.prototype.close = function() {
	this.database.close();
};


