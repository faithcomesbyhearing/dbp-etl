/**
* This class builds a table of already handled styles so that we can easily
* query the styleIndex table for any styles that are new in a table.
*/
function StyleUseBuilder(adapter) {
	this.adapter = adapter;
}
StyleUseBuilder.prototype.readBook = function(usxRoot) {
	// This table is not populated from text of the Bible
};
StyleUseBuilder.prototype.loadDB = function(callback) {
	var styles = [ 'book.id', 'para.ide', 'para.h', 'para.toc1', 'para.toc2', 'para.toc3', 'para.rem',
		'para.imt', 'para.is',
		'para.iot', 'para.io', 'para.io1', 'para.io2',
		'para.ip', 'para.ipi',
		'chapter.c', 'para.cl', 'verse.v',
		'para.p', 'para.m', 'para.pi', 'para.pi1', 'para.pc', 'para.mi',
		'para.nb', 'para.q', 'para.q1', 'para.q2', 'para.qc',
		'char.qs', 'para.qa', 'para.b', 
		'para.mt', 'para.mt1', 'para.mt2', 'para.mt3', 'para.mt4',
		'para.ms', 'para.ms1', 'para.mr',
		'para.s', 'para.s1', 'para.s2', 'para.r', 'para.sp', 'para.d',
		'row.tr', 'cell.th1', 'cell.th2', 'cell.tc1', 'cell.tc2', 'cell.tcr1', 'cell.tcr2', 
		'para.li', 'para.li1',
		'note.f', 'char.ft', 'char.fk', 'char.fr', 
		'char.fqa', 'char.fv', 'char.fm',
		'note.x', 'char.xt', 'char.xo', 'char.rq',
		'char.nd', 'char.tl','char.bk', 'char.pn', 'char.wj', 'char.k', 'char.add',
		'char.it', 'char.bd', 'char.sc', 
		'para.pb', 'char.w', 'para.periph', 'para.toc'
		];
	var array = [];
	for (var i=0; i<styles.length; i++) {
		var style = styles[i];
		var styleUse = style.split('.');
		var values = [ styleUse[1], styleUse[0] ];
		array.push(values);
	}
	this.adapter.load(array, function(err) {
		if (err instanceof IOError) {
			console.log('StyleUse Builder Failed', JSON.stringify(err));
			callback(err);
		} else {
			console.log('StyleUse loaded in database');
			callback();
		}
	});
};