module varint;
import utils;


/+string varintLengthCheck(uint maxLen) {
	string result;
	foreach(i;0..maxLen) {
		result ~= "if "
	}
	
}+/

static align(1) struct VarInt {
	const ubyte[] byteArray;
	pure nothrow @nogc :
align(1):

	this (const ubyte[] _arr) {
		auto len = _length(_arr.ptr);
		byteArray = _arr[0 .. len];
	}
	
	this(const ubyte* ptr) {
		auto len = _length(ptr);
		byteArray = ptr[0 .. len];
	}

	alias toBeLong this;
	alias toBeLong = toBeLongImpl;

	@property BigEndian!long toBeLongImpl() {
		long tmp;

		uint length = length();
		BigEndian!long result;
		
		// The hottest loop in the whole program!
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
		result = tmp;


		return result;
	}

	@property uint length ()  {
		return cast(uint) byteArray.length;
	}

	static uint _length(const ubyte* ptr) {

		foreach(idx;0..9) {
			if((*(ptr + idx)) & (1 << 7)) {
				continue;
			} else {
				return idx+1;
			}
			assert(0, "we should never get here");
		}
		return 9;
	}

	static assert(_length((cast(const ubyte[])[0x6d,0x00]).ptr) == 1);
	static assert(_length((cast(const ubyte[])[0x7f,0x00]).ptr) == 1);
	static assert(_length((cast(const ubyte[])[0x82,0x12]).ptr) == 2);
	static assert(VarInt((cast(const ubyte[])[0x81,0x00]).ptr).toBeLong == 0x0080);
	static assert(VarInt((cast(const ubyte[])[0x82,0x00]).ptr).toBeLong == 0x0100); // should be 0x0100
	static assert(_length((cast(const ubyte[])[0x82,0x80,0x00]).ptr) == 3);
	static assert(VarInt((cast(const ubyte[])[0x84,0x60,0x00]).ptr).toBeLong == 608);
	//static assert (VarInt().lengthInVarInt(608) == 2);
	static assert(VarInt((cast(const ubyte[])[0x81,0x82,0x83,0x84,0x85,0x86,0x87,0x88,0x89]).ptr).toBeLong != 0);
}
