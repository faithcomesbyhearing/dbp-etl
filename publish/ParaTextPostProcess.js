/**
* Bibles converted by Paratext contain 6 digit names that include a 3 digit number to define
* sequence.  These numbers must be removed for processing.
*/
function ParaTextPostProcess(version) {
	this.version = version;
	console.log('found', this.version);
}
ParaTextPostProcess.prototype.process = function(callback) {
	const that = this;
	const fs = require('fs');
	const fullPath = '../../DBL/1original/' + this.version + '/USX_1/';
	fs.readdir(fullPath, function(err, data) {
		if (err) {
			that.error('ParaTextPostProcess.directory', err);
		} else {
			for (var i=0; i<data.length; i++) {
				var entry = data[i];
				if (entry.length === 10) {
					var pre = parseInt(entry.substr(0, 3));
					if (pre > 0) {
						var post = entry.substr(6, 4);
						if (post === '.usx') {
							var newFile = entry.substr(3, 7);
							console.log(entry, ' to ', newFile);
							fs.renameSync(fullPath + entry, fullPath + newFile)
						}
					}
				}
			}
		}
	});
};
ParaTextPostProcess.prototype.error = function(message, err) {
	console.log(message, err);
	process.exit(1);	
};


if (process.argv.length < 3) {
	console.log('Usage: ParaTextPostProcess.sh  version');
	process.exit(1);
}
var paraTextPost = new ParaTextPostProcess(process.argv[2]);
paraTextPost.process(function() {
	console.log('PARA TEXT POST PROCESS COMPLETED SUCESSFULLY');
});
