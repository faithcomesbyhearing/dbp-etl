/**
* This class is the database adapter for the charset table
*/
function CharsetAdapter(database) {
	this.database = database;
	this.className = 'CharsetAdapter';
	Object.freeze(this);
}
CharsetAdapter.prototype.drop = function(callback) {
	this.database.executeDDL('drop table if exists charset', function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('drop charset success');
			callback();
		}
	});
};
CharsetAdapter.prototype.create = function(callback) {
	var statement = 'create table if not exists charset(' +
		'hex text not null, ' +
		'char text not null, ' +
		'count int not null)';
	this.database.executeDDL(statement, function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('create charset success');
			callback();
		}
	});
};
CharsetAdapter.prototype.load = function(array, callback) {
	var statement = 'insert into charset(hex, char, count) values (?,?,?)';
	this.database.bulkExecuteDML(statement, array, function(count) {
		if (count instanceof IOError) {
			callback(count);
		} else {
			console.log('load charset success', count);
			callback();
		}
	});
};