module varint;
import utils;


/+string varintLengthCheck(uint maxLen) {
	string result;
	foreach(i;0..maxLen) {
		result ~= "if "
	}
	
}+/

static struct VarInt {
	const ubyte[] byteArray;
	pure nothrow @nogc @safe :

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
	/+
	 non-functional right now :/
	static void toVarint(long val) {
		union U {
			BigEndian!long beval;
			ubyte[long.sizeof] _arr;
		}
		U u;
		uint ctr;
		ubyte[9] varIntStorage;

		u.beval = val;
		uint VarIntLength; 
		for (;;) {
			// safe the first 7 bits 
			varIntStorage[ctr] = u._arr[ctr] & (~0x7f); 
			// if the 8th bit is set in
			if (u._arr[ctr] & 7f) {

				continue;
			} else {
				break ;
			}
		}
	}+/

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
	static assert(VarInt((cast(ubyte[])[0x81,0x00])).toBeLong == 0x0080);
	static assert(VarInt((cast(ubyte[])[0x82,0x00])).toBeLong == 0x0100); // should be 0x0100
	static assert(_length((cast(ubyte[])[0x82,0x80,0x00])) == 3);
	static assert(VarInt((cast(ubyte[])[0x84,0x60,0x00])).toBeLong == 608);
	//static assert (VarInt().lengthInVarInt(608) == 2);
	static assert(VarInt((cast(ubyte[])[0x81,0x82,0x83,0x84,0x85,0x86,0x87,0x88,0x89])).toBeLong != 0);
}
