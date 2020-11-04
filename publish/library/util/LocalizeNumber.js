/**
* This class will convert a number to a localized representation of the same number.
* This is used primarily for converting chapter and verse numbers, since USFM and USX
* always represent those numbers in ASCII.
*/
function LocalizeNumber(silCode) {
	this.silCode = silCode;
	switch(silCode) {
		case 'arb': // Arabic
			this.numberOffset = 0x0660 - 0x0030;
			break;
		case 'nep': // Nepali
			this.numberOffset = 0x0966 - 0x0030;
			break;
		case 'pes': // Persian
		case 'urd': // Urdu
			this.numberOffset = 0x06F0 - 0x0030;
			break;

		default:
			this.numberOffset = 0;
			break;
	}
	Object.freeze(this);
}
LocalizeNumber.prototype.toLocal = function(number) {
	if ((typeof number) === 'number') {
		return(this.convert(String(number), this.numberOffset));
	} else {
		return(this.convert(number, this.numberOffset));		
	}
};
LocalizeNumber.prototype.toTOCLocal = function(number) {
	if (number == 0) {
		return('\u2744');
	} else {
		return(this.toLocal(number));
	}
};
LocalizeNumber.prototype.toHistLocal = function(number) {
	if (number == 0) {
		return('');
	} else {
		return(this.toLocal(number));
	}
};
LocalizeNumber.prototype.toAscii = function(number) {
	return(this.convert(number, - this.numberOffset));
};
LocalizeNumber.prototype.convert = function(number, offset) {
	if (offset === 0) return(number);
	var result = [];
	for (var i=0; i<number.length; i++) {
		var char = number.charCodeAt(i);
		if (char > 47 && char < 58) { // if between 0 and 9
			result.push(String.fromCharCode(char + offset));
		} else {
			result.push(number.charAt(i));
		}
	}
	return(result.join(''));
};