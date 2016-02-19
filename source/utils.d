module sqlite.utils;
/** this struct tries to keep it's own value as BigEndian */
struct BigEndian(T) {
	T asNative;
	alias asBigEndian this;
//	nothrow  pure :
/*@nogc bswap is not @nogc at 2.066.1 :*/
	@property T asBigEndian() {
		return swapIfNeeded(asNative);
	}
	
	@property asBigEndian(U)(U val) if(is(U == T)) {
		return asNative = swapIfNeeded(val);
	}
	
	alias isBigEndian = void;
	
	this (T val) {
		static if (is(T.isBigEndian)) {
			this.asNative = val.asNative;
		} else {
			this.asNative = val;
		}
	}
	
	BigEndian!T opAssign(BigEndian!T val) {
		this.asNative = val.asNative;
		return this;
	}
	import std.traits;
	BigEndian!T opAssign(U)(U val) if(!is(U.isBigEndian) && isIntegral!U) {
		assert(val <= T.max && val >= T.min);
		this.asNative = swapIfNeeded(cast(T)val);
		return this;
	}

	BigEndian!T opAssign(U)(U val) if(is(U : const ubyte[])) {
		assert(T.sizeof == val.length);
		T tmp;
		// (XXX) Consider swaping while reading them in.
		foreach(i;0 .. T.sizeof) {
			tmp |= (val[i] << (T.sizeof - i - 1)*8UL); 
		}
		this.asNative = swapIfNeeded(tmp);
	
		return this;
	}

	static U swapIfNeeded (U)(U val) {
		import std.bitmanip:swapEndian;
		
		version(BigEndian) {
			return val;
		} else {
			static if (is(U.isBigEndian)) {
				return val;
			} else {
				return swapEndian(val);
			}
		}
	}
}

auto bigEndian(T)(T val) {
	static if (is(T.isBigEndian)) {
		return BigEndian!(typeof(val.asNative))(val.asNative);
	} else {
		return BigEndian!T(val);
	}
}


static struct CArray(TElement) {
	alias ElementType = TElement;
	ElementType firstElement;
	
	string toString() pure nothrow @nogc {
		return "please use .toArray(size, pos)";
	}
	
	static ElementType[] toArray(void* arrayPos, long size) pure nothrow @nogc {
		if (arrayPos != null) {
			return (cast(ElementType*)arrayPos)[0 .. cast(size_t)size];
		} else {
			return [];
		}
	}
	
	
}

T[] toArray(T)(const ubyte[] _array, const size_t size) {
	T[] result;
	alias sliceType = typeof(_array[0 .. T.sizeof]);
	 
//	result.length = size;


	foreach(i; 0 .. size) {
		const pos = i * T.sizeof;
		static if (is(typeof(T.init = sliceType.init))) {
			T tmp;
			tmp = _array[pos .. pos + T.sizeof];
			result ~= tmp;
		} else static if (is(typeof(T(sliceType)))) {
			result ~= T(_array[pos .. pos + T.sizeof]);
		} else {
			import std.conv;
			static assert(0, T.stringof ~ " has to have a constructor or opAssign taking " ~ sliceType.stringof);
		}
	}

	return result;
}

uint sizeInBytes(ulong val) pure @nogc nothrow {
	foreach(n;0 .. cast(uint)ulong.sizeof) {
		if (!(val >>= 8)) {
			return n+1;
		} else {
			continue;
		}
	}
	assert(0);
}
import std.range : isRandomAccessRange, ElementType;

struct SkipArray(T) if (isRandomAccessRange!(ElementType!T)) {
	const(ElementType!T)[] arrays;
	size_t _length;

	@property const(size_t) length() const pure {
		return cast(const) _length;
	}
	
	auto opOpAssign (string op)(const T rhs) {
		static if (op == "~") {
			arrays ~= rhs;
			_length += rhs.length;
		} else {
			assert(0, "Operator " ~ op ~ " not supported");
		}
	}
	
	const auto opIndex(const size_t idx) {
		assert(idx < length);
		size_t currentPos;
		foreach(ref a;arrays) {
			if (idx >= a.length + currentPos) {
				currentPos += a.length;
			} else {
				return a[idx - currentPos];
			}
		}
		assert(0, "invalid idx");
	}

	this(T arrays) {
		this.arrays = arrays;
		foreach(a;arrays) {
			_length += a.length;
		}
	}
}

auto skipArray(T)(T t) {
	return SkipArray!T(t);
}

static immutable intArrArr = skipArray([[1,2],[3],[4,5,6],[7,8,9]]);
static assert(intArrArr.length == 9);
static assert(intArrArr[3] == 4);
static assert(intArrArr[8] == 9);
static assert(intArrArr[0] == 1);

unittest {
	auto arr = skipArray([["Hello"]]);
	arr ~= [["beautiful"], ["world"]];
	assert(arr.length == 3);
	
}

//TODO implement this!
double float64(const ubyte[] _bytes) {
	assert(_bytes.length > double.sizeof);
	enum bias = 1023;
	enum mantissa_length = 53;
	enum exponent_length = 11;
	assert(mantissa_length + exponent_length == 64);
	double result;

	foreach(i; 0 .. (mantissa_length / 8) + 1) {

	}

	return result;
}