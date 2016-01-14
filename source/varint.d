module sqlite.varint;
import sqlite.utils;

auto LIKELY(T)(T t) {return t;}

version = Benchmark;

version(Benchmark) {
	import std.datetime;
}

/+string varintLengthCheck(uint maxLen) {
	string result;
	foreach(i;0..maxLen) {
		result ~= "if "
	}
	
}+/

static align(1) struct VarInt {
	ubyte *byteArray;
	// pure nothrow @nogc :
align(1):

	alias toBeLong this;
	alias toBeLong = toBeLongImpl;

	@property BigEndian!long toBeLongImpl() {
		long tmp;
		version (Benchmark) {
			static uint ctr;
			static TickDuration[512] durations;
			static StopWatch toBeLongWatch;
			toBeLongWatch.start;
		}

		uint length = length();
		BigEndian!long result;
		
		//TODO unroll this loop + SIMD
		foreach(idx;0..length) {
			ubyte val = *(byteArray + idx);
			long maskedVal = (cast(long)val & 0x7fUL); // mask 8th bit
			long shiftBy = (length-idx-1UL)*7UL;

			if(idx < 8) {
				tmp |=  (maskedVal << shiftBy);
			} else {
				tmp |=  (cast(long)val << 63UL);
			}
		}
		result = tmp;


				version (Benchmark) {
					import std.stdio;
					writeln(toBeLongWatch.peek);
				}
		return result;
	}

	static uint lengthInVarInt(long val) {
		uint length = sizeInBytes(val);
		return length;
	}

	//	@property void toBeLong(BigEndian!long val) {
	//		uint length = bsf(val) / 7;

	//	}

	@property uint length ()  {
		return _length(byteArray);
	}

	static uint _length(const ubyte* position) {
		version(benchmark) {
			static StopWatch lengthWatch;
		}

		foreach(idx;0..9) {
			if(*(cast(ubyte*)position + idx) & (1 << 7)) {
				continue;
			} else {
				return idx+1;
			}
			assert(0, "we should never get here");
		}
		return 9;

		version (benchmark) {
			scope (exit) {
				
			} 
		} 
	}

	version(Benchmark) {} 
	else {
	
	static assert(_length((cast(ubyte[])[0x6d,0x00]).ptr) == 1);
	static assert(_length((cast(ubyte[])[0x7f,0x00]).ptr) == 1);
	static assert(_length((cast(ubyte[])[0x82,0x12]).ptr) == 2);
	static assert(VarInt((cast(ubyte[])[0x81,0x00]).ptr).toBeLong == 0x0080);
	static assert(VarInt((cast(ubyte[])[0x82,0x00]).ptr).toBeLong == 0x0100); // should be 0x0100
	static assert(_length((cast(ubyte[])[0x82,0x80,0x00]).ptr) == 3);
	static assert(VarInt((cast(ubyte[])[0x84,0x60,0x00]).ptr).toBeLong == 608);
	//static assert (VarInt().lengthInVarInt(608) == 2);
	static assert(VarInt((cast(ubyte[])[0x81,0x82,0x83,0x84,0x85,0x86,0x87,0x88,0x89]).ptr).toBeLong != 0);
	}
}
