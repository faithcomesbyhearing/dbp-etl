/**
* This class is the database adapter that is used to read the Versions.db 
* In the publishing process.  It expects to use a node sqlite interface
* not a WebSQL interface used by the App
*/
function VersionsReadAdapter() {
    var sqlite3 = require('sqlite3'); //.verbose();
	this.database = new sqlite3.Database('../Versions/Versions.db');
	this.database.exec("PRAGMA foreign_keys = ON");
	Object.freeze(this);
}
VersionsReadAdapter.prototype.readVersion = function(versionCode, callback) {
	var statement = 'SELECT l.silCode, l.langCode, l.direction FROM language l JOIN version v ON v.silCode=l.silCode WHERE versionCode=?';
	this.database.get(statement, [versionCode], function(err, row) {
		if (err) {
			console.log('SELECT ERROR IN VERSION', JSON.stringify(err));
			callback();
		} else {
			var pubVersion = new PubVersion(row);
			callback(pubVersion);
		}
	});
};

