module writer;

import sqlited;
import varint;
import utils;
import misc;

struct CowSlice {
	const ubyte[] roData;
	alias roData this;
}



struct PathEntry {
	enum TransitionType {
		LPageToLCell, // End
		RPageToLPage,
		RPageToIPage,
		IPageToICell,
		ICellToLPage,
	}
	TransitionType transitionType;
	uint transitionDestination;
}


struct WritableDatabase {
	uint currentPosition;
	uint currentEffectivePosition;
	static struct WriteOperation {
		// ... OperationType bla bla bla bla ...
	}

	static struct WriteOperationEntry {
		WriteOperation operation;
		uint posOfNextWriteOperation;
	}
	WriteOperationEntry[] writeOperations;

	void insertInto(T...)(string tableName, T values) {

	}

//	db.table("mytable").insertRow("value",12);

	static ubyte[] rowBytes(T...)(T values) pure {
		ubyte[] pageContent;
		uint headerOffset;
		
		foreach(v;values) {
			pageContent ~= getTypeCode(v).byteArray;
		}
		
		headerOffset = cast(uint)pageContent.length;
		
		foreach(v;values) {
			import std.traits : isIntegral;
			static if (isIntegral!(typeof(v))) {
				uint len = sizeInBytes(v);
				pageContent ~= bigEndian(v).asArray[v.sizeof - len .. v.sizeof];
			} else static if (is(typeof(v) == ubyte[])) {
				pageContent ~= v;
			} else static if (is(typeof(v) == string)) {
				pageContent ~= cast(ubyte[])v;
			}
		}
		
		return pageContent;
	}


	static assert(rowBytes("hello",12,"hello") == [
			cast(ubyte)23, 0x01, cast(ubyte)23,
			cast(ubyte)'h', cast(ubyte)'e', cast(ubyte)'l', cast(ubyte)'l', cast(ubyte)'o',
			cast(ubyte)12,
			cast(ubyte)'h', cast(ubyte)'e', cast(ubyte)'l', cast(ubyte)'l', cast(ubyte)'o'
	]);

	struct WriteableTable {
		Table table;
		WritableDatabase db;
		alias table this;
	
		this(Table table, WritableDatabase db) {
			this.table = table;
			this.db = db;
		}

		void serialize(struct_type)(struct_type s) {
			uint ctr;
			Database.Payload[] rowContent;
			foreach (member; __traits(derivedMembers, struct_type)) {
				alias type = typeof(__traits(getMember, s, member));
				static if (!is(type == function)) {
					rowContent ~= Database.Payload(__traits(getMember, s, member));
				}
			}
			return instance;
		}

	}

	Database db;
	alias db this;
	ubyte[] data;

	this (const Database input) {
		data = cast (ubyte[]) CowSlice(input.data[0 .. $]);
		db = Database(data);

	}

	struct WriteablePageRange {
	//	PageStat currentPageStat;
		Database.PageRange _pages;
		alias _pages this;
	} 

	WriteablePageRange pages;


	this(string filename) {
		this(Database(filename));
	}

	WriteableTable table(string tableName) {
		auto table = db.table(tableName);
		return WriteableTable(db.table(tableName), this);
	}

	static VarInt getTypeCode(T)(T t) pure {
		import std.traits;
		static if (is(T == typeof(null))) {
			return VarInt(bigEndian!long(0));
		} else static if (is(T == ubyte[])) {
			return	VarInt(bigEndian!long(t.length*2 + 12));
		} else static if (is (T == string)) {
			return	VarInt(bigEndian!long(t.length*2 + 13));
		} else static if (isIntegral!T)  {
			// The oblivous optimisation is to mask the sign first!
			// and then just check the abs
			// check if this makes things faster (it should!)
			if (t >= -(1<<7) && t < (1<<7)) {
				return VarInt(bigEndian!long(1));
			} else if (t >= -(1<<15) && t < (1<<15)) {
				return VarInt(bigEndian!long(2));
			} else if (t >= -(1<<23) && t < (1<<23)) {
				return VarInt(bigEndian!long(3));
			} else if (t >= -(1<<31) && t < (1<<31)) {
				return VarInt(bigEndian!long(4));
			} else if (t >= -(1L<<47) && t < (1L<<47)) {
				return VarInt(bigEndian!long(5));
			} else if (t >= -(1L<<63) && t < (1L<<63)) {
				return VarInt(bigEndian!long(6));
			} else 
				assert(0);
		} else static if (isFloatingPoint!T) {
			return VarInt(bigEndian!long(7));
		} else static if (is(T == bool)) {
			return t ? VarInt(bigEndian!long(9)) : VarInt(bigEndian!long(8));
		} else static assert (0, "Payload has to be of numericType, string, ubyte[] or bool");
	}

	static assert (getTypeCode("hello") == VarInt(bigEndian!long(23)));
	static assert (getTypeCode(12) == VarInt(bigEndian!long(1)));
	static assert (getTypeCode(127) == VarInt(bigEndian!long(1)));
	static assert (getTypeCode(128) == VarInt(bigEndian!long(2)));
	static assert (getTypeCode(cast(ubyte[])[]) == VarInt(bigEndian!long(12)));
}

