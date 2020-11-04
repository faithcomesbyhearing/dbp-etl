/**
* This program is responsible for copying the most currently prepared, but not
* tested Bibles to the development client BibleNW.
*/
"use strict;"
const BIBLES_SRC = process.env.HOME + "/ShortSands/DBL/3prepared/";
const VERSION_SRC = process.env.HOME + "/ShortSands/BibleApp/Versions/Versions.db";
const PROJECT = process.env.HOME + "/Library/Application\ Support/BibleAppNW/databases/"
const MASTER = PROJECT + "Databases.db"
const DATABASES = PROJECT + "file__0/";
const SETTINGS_DEST = DATABASES + "8";
const VERSIONS_DEST = DATABASES + "9";

var copyBiblesDev = function(version) {
	var filename = version + '.db';
	if (version.toLowerCase() === 'versions') {
		copyFile(VERSION_SRC, VERSIONS_DEST);
	} else {
		var database = openDatabase(MASTER);
		selectVersion(database, filename, function(row) {
			if (row == null) {
				insertVersion(database, filename, function(id) {
					insertInstalled(version, filename);
					copyFile(BIBLES_SRC + filename, DATABASES + id);
				});
			} else {
				copyFile(BIBLES_SRC + filename, DATABASES + row.id);
			}
		});
	}

	function openDatabase(filename) {
		const sqlite3 = require('sqlite3').verbose();
		var database = new sqlite3.Database(filename);
		database.exec("PRAGMA foreign_keys = ON");
		return(database);
	}
	function selectVersion(database, filename, callback) {
		const statement = 'SELECT id, origin FROM Databases WHERE name=?';
		database.get(statement, [filename], function(error, row) {
			ifErrorMessage('selectVersion ' + filename, error);
			callback(row);
		});
	}
	function insertVersion(database, filename, callback) {
		const statement = 'INSERT INTO Databases (origin, name, description, estimated_size) VALUES (?, ?, ?, 31457280)';
		database.run(statement, ['file__0', filename, filename], function(error) {
			ifErrorMessage('insertVersion ' + filename, error);
			console.log('Inserted ', filename, ' at ', this.lastID);
			callback(this.lastID);
		});
	}
	function insertInstalled(version, filename) {
		var db = openDatabase(SETTINGS_DEST);
		const statement = 'INSERT INTO Installed VALUES (?, ?, ?)';
		const now = new Date().toISOString();
		db.run(statement, [version, filename, now], function(error) {
			db.close();
			ifErrorMessage('insertInstalled ' + version, error)
		});
	}
	function copyFile(source, target) {
		const fs = require('fs');
		var sourceStream = fs.createReadStream(source);
		sourceStream.on('error', function(error) {
			ifErrorMessage('copyFile from ' + source, error);
  		});
		var targetStream = fs.createWriteStream(target); 
		targetStream.on('error', function(error) {
			ifErrorMessage('copyFile to ' + target, error);
		});
		sourceStream.pipe(targetStream);
		console.log('copied ', source, target);
	}
	function ifErrorMessage(source, error) {
		if (error) {
			console.log('***************** Failure in CopyBiblesDev *****************');
			console.log(source, JSON.stringify(error));
			process.exit(1);
		}
	}
}

if (process.argv.length < 3) {
	console.log('Usage: CopyBiblesDev.sh  version');
	process.exit(1);
}

copyBiblesDev(process.argv[2]);



