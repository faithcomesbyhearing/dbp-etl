/**
* This class is the database adapter for the styleIndex table
*/
function StyleIndexAdapter(database) {
	this.database = database;
	this.className = 'StyleIndexAdapter';
	Object.freeze(this);
}
StyleIndexAdapter.prototype.drop = function(callback) {
	this.database.executeDDL('drop table if exists styleIndex', function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('drop styleIndex success');
			callback();
		}
	});
};
StyleIndexAdapter.prototype.create = function(callback) {
	var statement = 'create table if not exists styleIndex(' +
		'style text not null, ' +
		'usage text not null, ' +
		'book text not null, ' +
		'chapter integer null, ' +
		'verse integer null)';
	this.database.executeDDL(statement, function(err) {
		if (err instanceof IOError) {
			callback(err);
		} else {
			console.log('create styleIndex success');
			callback();
		}
	});
};
StyleIndexAdapter.prototype.load = function(array, callback) {
	var statement = 'insert into styleIndex(style, usage, book, chapter, verse) values (?,?,?,?,?)';
	this.database.bulkExecuteDML(statement, array, function(count) {
		if (count instanceof IOError) {
			callback(count);
		} else {
			console.log('load styleIndex success', count);
			callback();
		}
	});
};