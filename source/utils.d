module utils;
/***************************
 * Utils used by SQLite-D *
 * By Stefan Koch 2016    *
***************************/

/** this struct tries to keep it's own value as BigEndian */
struct BigEndian(T) {
	T asNative;
	alias asBigEndian this;
	pure nothrow @safe @nogc :
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

	this(const ubyte[] _array) @trusted {
		assert(T.sizeof == _array.length);
		foreach(i;0 .. T.sizeof) {
			asNative |= (_array[i] << i*8UL); 
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
		this = BigEndian!T(val);
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
				enum _2066_cannot_handle_swapEndian = true;
				static if (_2066_cannot_handle_swapEndian) {
					static if (U.sizeof == 8) {
						return (((cast(ulong)val) & 0x00000000000000ffUL) << 56UL) | 
								(((cast(ulong)val) & 0x000000000000ff00UL) << 40UL) | 
								(((cast(ulong)val) & 0x0000000000ff0000UL) << 24UL) | 
								(((cast(ulong)val) & 0x00000000ff000000UL) <<  8UL) | 
								(((cast(ulong)val) & 0x000000ff00000000UL) >>  8UL) | 
								(((cast(ulong)val) & 0x0000ff0000000000UL) >> 24UL) | 
								(((cast(ulong)val) & 0x00ff000000000000UL) >> 40UL) | 
								(((cast(ulong)val) & 0xff00000000000000UL) >> 56UL);
					} else static if (U.sizeof == 4) {
							return ((val & 0x000000ff) << 24) |
									((val & 0x0000ff00) <<  8) |
									((val & 0x00ff0000) >>  8) |
									((val & 0xff000000) >> 24);
					} else static if (U.sizeof == 2) {
							return cast(ushort)(((val & 0xff00) >> 8) |
									((val & 0x00ff) << 8));
					} else static if (U.sizeof == 1) {
								assert(0, "you should not use BigEndian for byte-sized vaules");
					} else {
								assert(0, "cannot swap this byteSize");
					}
				} else {
					return swapEndian(val);
				}
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
	if (__ctfe) {
		T[] result;
		alias sliceType = typeof(_array[0 .. T.sizeof]);
		
		result.length = size;

		foreach(i; 0 .. size) {
			const pos = i * T.sizeof;
			static if (is(typeof(T(sliceType.init)))) {
				result[i] = T(_array[pos .. pos + T.sizeof]);
			} else {
				static assert(0, T.stringof ~ " has to have a constructor taking " ~ sliceType.stringof);
			}
		}

		return result;
	} else {
		return cast(T[])(_array);
	}
}

T fromArray(T)(const ubyte[] _array) {
	if (__ctfe) {
		uint offset;
		T result;
		static assert(T.alignof == 1, "Be sure to use this only on align(1) structures!");
		assert(_array.length >= T.sizeof,"your input array needs to be at least as long as your type.sizeof");

		///XXX this cucially depends on your type being byte aligned!
		foreach (member; __traits(derivedMembers, struct_type)) {
			alias type = typeof(__traits(getMember, instance, member));

			static if (!(is(type == function) || is(type == const))) {
				alias sliceType = typeof(_array[0 .. type.sizeof]);
				static if (is(typeof(type(sliceType.init)))) {
					__traits(getMember, result, member) = type(_array[offset .. offset + type.sizeof]);
				} else static if (type.sizeof == sliceType.init[0].sizeof && is(typeof(cast(type)(sliceType.init[0])))) {
					__traits(getMember, result, member) = type(_array[offset .. offset + type.sizeof][0]);
				} else {
					static assert(0, T.stringof ~ " has to have a constructor taking or needs to be castable to ubyte" ~ sliceType.stringof);
				}
				offset += type.sizeof;
				assert(__traits(getMember, result, member).alignof == offset);
			}
		}

		return result;
	} else {
		return *(cast(T*) _array.ptr);
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
