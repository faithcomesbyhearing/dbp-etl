/**
* This class holds the concordance of the entire Bible, or whatever part of the Bible was available.
*/
function Concordance(adapter, wordsLookAhead) {
	this.adapter = adapter;
	this.wordsLookAhead = (wordsLookAhead) ? wordsLookAhead : 0;
	Object.freeze(this);
}
Concordance.prototype.search = function(words, callback) {
	var that = this;
	this.adapter.select(words, function(refLists) {
		if (refLists instanceof IOError) {
			callback(refLists);
		} else {
			var result = intersection(refLists);
			callback(result);
		}
	});
	function intersection(refLists) {
		if (refLists.length === 0) {
			return([]);
		}
		if (refLists.length === 1) {
			return(refLists[0]);
		}
		var mapList = [];
		for (var i=1; i<refLists.length; i++) {
			var map = arrayToMap(refLists[i]);
			mapList.push(map);
		}
		var result = [];
		var firstList = refLists[0];
		for (var j=0; j<firstList.length; j++) {
			var reference = firstList[j];
			if (presentInAllMaps(mapList, reference)) {
				result.push(reference);
			}
		}
		return(result);
	}
	function arrayToMap(array) {
		var map = {};
		for (var i=0; i<array.length; i++) {
			map[array[i]] = true;
		}
		return(map);
	}
	function presentInAllMaps(mapList, reference) {
		for (var i=0; i<mapList.length; i++) {
			if (mapList[i][reference] === undefined) {
				return(false);
			}
		}
		return(true);
	}
};
Concordance.prototype.search2 = function(words, callback) {
	var that = this;
	this.adapter.select2(words, function(refLists) {
		if (refLists instanceof IOError) {
			callback(refLists);
		} else if (refLists.length !== words.length) {
			callback([]);
		} else {
			var resultList = intersection(refLists);
			callback(resultList);
		}
	});
	function intersection(refLists) {
		if (refLists.length === 0) {
			return([]);
		}
		var resultList = [];
		if (refLists.length === 1) {
			for (var ii=0; ii<refLists[0].length; ii++) {
				resultList.push([refLists[0][ii]]);
			}
			return(resultList);
		}
		var mapList = [];
		for (var i=1; i<refLists.length; i++) {
			var map = arrayToMap(refLists[i]);
			mapList.push(map);
		}
		var firstList = refLists[0];
		for (var j=0; j<firstList.length; j++) {
			var reference = firstList[j];
			var resultItem = matchEachWord(mapList, reference);
			if (resultItem) {
				resultList.push(resultItem);
			}
		}
		return(resultList);
	}
	function arrayToMap(array) {
		var map = {};
		for (var i=0; i<array.length; i++) {
			map[array[i]] = true;
		}
		return(map);
	}
	function matchEachWord(mapList, reference) {
		var resultItem = [ reference ];
		for (var i=0; i<mapList.length; i++) {
			reference = matchWordWithLookahead(mapList[i], reference);
			if (reference == null) {
				return(null);
			}
			resultItem.push(reference);
		}
		return(resultItem);
	}
	function matchWordWithLookahead(mapRef, reference) {
		for (var look=1; look<=that.wordsLookAhead + 1; look++) {
			var next = nextPosition(reference, look);
			if (mapRef[next]) {
				return(next);
			}
		}
		return(null);
	}
	function nextPosition(reference, position) {
		var parts = reference.split(';');
		var next = parseInt(parts[1]) + position;
		return(parts[0] + ';' + next.toString());
	}
};
