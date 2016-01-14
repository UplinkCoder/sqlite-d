module sqlite.utils;
/** this struct tries to keep it's own value as BigEndian */
struct BigEndian(T) {
	T asNative;
	alias asBigEndian this;
	nothrow  pure :
@nogc :
	@property T asBigEndian() {
		return swapIfNeeded(asNative);
	}
	
	@property asBigEndian(U)(U v) if(is(U:T)) {
		return asNative = swapIfNeeded(v);
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
	
	BigEndian!T opAssign(U)(U val) if(!is(U.isBigEndian)) {
		assert(val <= T.max && val >= T.min);
		this.asNative = swapIfNeeded(cast(T)val);
		return this;
	}
	
	static U swapIfNeeded (U)(U val) {
		import std.bitmanip:swapEndian;
		
		version(BigEndian) {
			return val;
		} else {
			static if (is(T.isBigEndian)) {
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
	
	auto opBinary (string op)(const T rhs) {
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
	auto arr = skipArray(["Hello"]);
	arr ~= ["beautiful", "world"];
	assert(arr.length == 3);
	
}
