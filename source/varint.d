module varint;
import utils;


/+string varintLengthCheck(uint maxLen) {
	string result;
	foreach(i;0..maxLen) {
		result ~= "if "
	}
	
}+/

struct VarInt {
	pure nothrow @safe :
	this(BigEndian!long value) {
		auto len = lengthInVarInt(value);
		auto tmp = new ubyte[](len);
		ulong beValue = value.asBigEndian;

		while(len--) {
			tmp[len] = (beValue & 0x7f) | 0x80;
			beValue >>= 7;
		}
		tmp[$-1] &= 0x7f;
		byteArray = tmp;
	}

	@nogc :
	const ubyte[] byteArray;

	alias toBeLong this;
	alias toBeLong = toBeLongImpl;

	@property BigEndian!long toBeLongImpl() {
		long tmp;

		uint length = length();
		BigEndian!long result;
		
		// The hottest loop in the whole program!
		//TODO There must be a way to speed it up!
		foreach(idx;0..length) {
			ubyte val = byteArray[idx];
			long maskedVal = (cast(long)val & 0x7fUL); // mask 8th bit
			long shiftBy = (length-idx-1UL)*7UL;

			if(idx < 8) {
				tmp |=  (maskedVal << shiftBy);
			} else {
				tmp |=  (cast(long)val << 63UL);
			}
		}
		//this evokes swapIfNeeded
		result = tmp;


		return result;
	}
	this (const ubyte[] _arr) {
		this.byteArray = _arr;
	}

	static int lengthInVarInt(BigEndian!long value) {
		if (value < 0) {
			return 9;
		} else if (value < 1<<7) {
			return 1;
		} else if (value < 1<<14) {
			return 2;
		} else if (value < 1<<21) {
			return 3;
		} else if (value < 1<<28) {
			return 4;
		} else if (value < 1L<<35) {
			return 5;
		} else if (value < 1L<<42) {
			return 6;
		} else if (value < 1L<<49) {
			return 7;
		} else if (value < 1L<<63) {
			return 8;
		}
		import std.conv;
		assert(0, "We should never get here");
	}

	@property uint length ()  {
		return _length(byteArray);
	}

	static uint _length(const ubyte[] arr) {

		foreach(idx;0..9) {
			if(arr[idx] & (1 << 7)) {
				continue;
			} else {
				return idx+1;
			}
			assert(0, "we should never get here");
		}
		return 9;
	}

	static assert(_length((cast(ubyte[])[0x6d,0x00])) == 1);
	static assert(_length((cast(ubyte[])[0x7f,0x00])) == 1);
	static assert(_length((cast(ubyte[])[0x82,0x12])) == 2);
	static assert(_length((cast(ubyte[])[0xfe,0xfe,0x00])) == 3);
	static assert(VarInt((cast(ubyte[])[0x81,0x01])).toBeLong == 129);
	static assert(VarInt((cast(ubyte[])[0x81,0x00])).toBeLong == 0x0080);
	static assert(VarInt((cast(ubyte[])[0x82,0x00])).toBeLong == 0x0100); // should be 0x0100
	static assert(_length((cast(ubyte[])[0x82,0x80,0x00])) == 3);
	static assert(VarInt((cast(ubyte[])[0x84,0x60,0x00])).toBeLong == 608);
	static assert(VarInt(bigEndian!long(265)).toBeLong == 265);
	static assert(VarInt(bigEndian!long(6421)).toBeLong == 6421);
	//static assert (VarInt().lengthInVarInt(608) == 2);
	static assert(VarInt((cast(ubyte[])[0x81,0x82,0x83,0x84,0x85,0x86,0x87,0x88,0x89])).toBeLong != 0);
}
