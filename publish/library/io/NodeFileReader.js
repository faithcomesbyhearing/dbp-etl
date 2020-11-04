/**
* This class is a file reader for Node.  It can be used with node.js and node-webkit.
* cordova requires using another class, but the interface should be the same.
*/
function FileReader(location) {
	this.fs = require('fs');
	this.location = location;
	Object.freeze(this);
}
FileReader.prototype.fileExists = function(filepath, callback) {
	var fullPath = this.location + filepath;
	console.log('checking fullpath', fullPath);
	this.fs.stat(fullPath, function(err, stat) {
		if (err) {
			err.filepath = filepath;
			callback(err);
		} else {
			callback(stat);
		}
	});
};
FileReader.prototype.readDirectory = function(filepath, callback) {
	var fullPath = this.location + filepath;
	console.log('read directory ', fullPath);
	this.fs.readdir(fullPath, function(err, data) {
		if (err) {
			err.filepath = filepath;
			callback(err);
		} else {
			callback(data);
		}
	});
};
FileReader.prototype.readTextFile = function(filepath, callback) {
	var fullPath = this.location + filepath;
	console.log('read file ', fullPath);
	this.fs.readFile(fullPath, { encoding: 'utf8'}, function(err, data) {
		if (err) {
			err.filepath = filepath;
			callback(err);
		} else {
			callback(data);
		}
	});
};