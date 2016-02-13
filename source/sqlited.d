module sqlited;
import sqlite.utils;
import sqlite.varint;

version = C_compat;

version (C_compat) {
extern (C):
	Database fromBuffer(ubyte* bufPtr, uint size) {
		return Database(bufPtr[0 .. size]);
	}
}

struct Database {
	string dbFilename;
	ubyte[] data;
	uint currentOffset = 0;
	DatabaseHeader cachedHeader;

	DatabaseHeader header() pure {
		return cachedHeader;
	}

	BTreePage rootPage() {
		return BTreePage(data.ptr, usablePageSize, 100);
	}

	PageRange pages() {
		return PageRange(data.ptr, cachedHeader.pageSize,
				usablePageSize, cast(uint) data.length / cachedHeader.pageSize);
	}

	uint usablePageSize() {
		uint _usablePageSize = cachedHeader.pageSize - cachedHeader.reserved;
		return _usablePageSize;
	}

	struct PageRange {
		uint currentOffset = 0;
		ubyte* basePtr;
		const uint pageSize;
		const uint usablePageSize;

		const uint numberOfPages;
		uint pageIndex;

		@property uint length() const {
			return numberOfPages;
		}

		bool empty() {
			return (pageIndex > numberOfPages);
		}

		BTreePage opIndex(uint pageNumber) {
			assert(pageNumber <= numberOfPages,
					"Attempting to go to invalid page");
			return BTreePage(basePtr + (pageSize * pageNumber),
					usablePageSize, (pageNumber == 0 ? 100 : 0));
		}

		this(ubyte* basePtr, uint pageSize,
				uint usablePageSize, uint numberOfPages) pure {
			this.basePtr = basePtr;
			this.pageSize = pageSize;
			this.usablePageSize = usablePageSize;
			this.numberOfPages = numberOfPages;
		}

	}

	import std.range;

	static assert(hasLength!PageRange);

	static struct Payload {
		alias SerialTypeCodeEnum = SerialTypeCode.SerialTypeCodeEnum;
		static struct SerialTypeCode {
			alias SerialTypeCodeEnum this;

			static enum SerialTypeCodeEnum : long {
				NULL = (0),
				int8 = (1),
				int16 = (2),
				int24 = (3),
				int32 = (4),
				int48 = (5),
				int64 = (6),
				float64 = (7),

				bool_false = (8),
				bool_true = (9),

				blob = (12),
				_string = (13)
			}

			this(VarInt v) /*pure nothrow*/ {
				if (v > 11) {
					if (v & 1) {
						type = SerialTypeCode.SerialTypeCodeEnum._string;
						length = (v - 13) / 2;
					} else {
						type = SerialTypeCode.SerialTypeCodeEnum.blob;
						length = (v - 12) / 2;
					}
				} else {
					type = cast(SerialTypeCodeEnum) v;
					final switch (type) with (SerialTypeCodeEnum) {
					case NULL:
						length = 0;
						break;
					case int8:
						length = 1;
						break;
					case int16:
						length = 2;
						break;
					case int24:
						length = 3;
						break;
					case int32:
						length = 4;
						break;
					case int48:
						length = 6;
						break;
					case int64:
						length = 8;
						break;
					case float64:
						length = 8;
						break;
					case bool_false:
						length = 0;
						break;
					case bool_true:
						length = 0;
						break;
					case blob:
						assert(0, "SerialType Blob needs an explicit size");
					case _string:
						assert(0, "SerialType String needs an explicit size");
					}
				}
			}

			SerialTypeCodeEnum type;
			long length;
		}

		static union {
			byte int8;
			short int16;
			int int24;
			int int32;
			long int48;
			long int64;
			double float64;
			const(char)[] _string;
			ubyte[] blob;
		}

		SerialTypeCode typeCode;
		alias length = typeCode.length;
		alias type = typeCode.type;
	}

	static struct Row {
		Payload[] colums;
		//	TableSchema* schema;
		alias colums this;
	}

	static struct Table {
		TableSchema schema;
		Row[] rows;
	}

	static align(1) struct DatabaseHeader {

		bool isValid() {
			return magicString == "SQLite format 3\0";
		}

	align(1):
		// ALL NUMBERS ARE BIGENDIAN
		static {
			enum TextEncoding : uint {
				utf8 = bigEndian(1),
				utf16le = bigEndian(2),
				utf16be = bigEndian(3)
			}

			enum SchemaFormat : uint {
				_1 = bigEndian(1),
				_2 = bigEndian(2),
				_3 = bigEndian(3),
				_4 = bigEndian(4),
			}

			enum FileFormat : ubyte {
				legacy = 1,
				wal = 2,
			}

			struct Freelist {
				BigEndian!uint nextPage;
				BigEndian!uint leafPointers; // Number if leafPoinrers;
			}

		}

		const(char[16]) magicString;

		BigEndian!ushort pageSize; /// between 512 and 32768 or 1

		FileFormat FileFormatWriteVer;
		FileFormat FileFormatReadVer;

		ubyte reserved; /// unused bytes at the end of each page

		immutable ubyte maxEmbeddedPayloadFract = 64;
		immutable ubyte minEmbeddedPayloadFract = 32;
		immutable ubyte leafPayloadFract = 32;
		BigEndian!uint fileChangeCounter;
		BigEndian!uint sizeInPages; /// fileChangeCounter has to match validForVersion or sizeInPages is invalid

		BigEndian!uint firstFreelistPage; /// Page number of the first freelist trunk page. 
		BigEndian!uint freelistPages; /// Total number of freelist Pages;

		BigEndian!uint schemaCookie;
		SchemaFormat schemaFormatVer; // 1,2,3 or 4

		BigEndian!uint defaultCacheSize;
		BigEndian!uint largestRootPage; /// used in incermental and auto vacuum modes. 0 otherwise.

		TextEncoding textEncoding;

		BigEndian!uint userVersion;

		BigEndian!uint incrementalVacuum; /// Non-Zero if on, Zero otherwise;

		BigEndian!uint applicationId;

		BigEndian!uint[5] _reserved; /// Reserved space for future format expansion 

		BigEndian!uint validForVersion;
		BigEndian!uint sqliteVersion;
	}

	this(string filename, bool readEntirely = true) {
		import std.file;
		import std.stdio;

		auto data = cast(ubyte[]) read(filename);
		this(data, filename);
	}

	this(ubyte[] buffer = null, string filename = ":Memory:") {
		if (buffer is null) {
			ubyte[] myBuffer;
			data = myBuffer;
		} else {
			data = buffer;
		}
		dbFilename = filename;
		auto _header = cast(DatabaseHeader*) buffer.ptr;
		assert(_header.isValid());
		cachedHeader = *_header;
	}

	static struct MasterTableSchema {
		string type;
		string name;
		string tbl_name;
		uint rootPage;
		string sql;
	}

	static struct TableSchema {
		static struct SchemaEntry {
			//	uint colNumber;
			string columNmae;
			string TypeName;
			string defaultValue;
			bool isPrimayKey;
			bool notNull;
		}
	}

	static struct OverflowInfo {
		uint nextPageIdx;
		uint remainingBytesOfPayload;
		uint payloadOnPage;
		uint remainingTotalPayload;

		string toString() {
			import std.stdio;

			return std.format.format("
	nextPageIdx: %d
	remainingBytesOfPayload: %d
	payloadOnPage: %d
	remainingTotalPayload %d", nextPageIdx,
					remainingBytesOfPayload, payloadOnPage,
					remainingTotalPayload);
		}
	}

	/*
	 * CREATE TABLE sqlite_master(
	 *	type text,
	 *	name text,
	 *	tbl_name text,
	 *	rootpage integer,
	 *	sql text
	 * );
	 *	 
	 */

	//Schema[] schema() {

	//}

	static struct BTreePage {
		ubyte* base;
		const uint _usablePageSize;

		uint offset;
		@property const uint usablePageSize() pure {
			return _usablePageSize;
		}

		uint payloadOnPage(uint payloadSize) pure {
			auto m = ((usablePageSize - 12) * 32 / 255) - 23;
			import std.algorithm : min;

			auto x1 = m + ((payloadSize - m) % (usablePageSize - 4));
			auto x2 = ((usablePageSize - 12) * 64 / 255) - 23;

			debug {
				import std.stdio;

				writeln("M: ", m);
				writeln("payloadSize: ", payloadSize);
				writeln("x1: ", x1);
			}

			final switch (header.pageType) with (typeof(header.pageType)) {
			case emptyPage:
			case tableInteriorPage:
				assert(0, "has no payload");

			case tableLeafPage:
				return min(usablePageSize - 35, x1);

			case indexInteriorPage:
			case indexLeafPage:
				return min(x1, x2);

			}
		}

		string toString(PageRange pages) {
			import std.conv;

			auto pageType = header.pageType;
			string result = to!string(pageType);

			auto cellPointers = getCellPointerArray();
			foreach (cp; cellPointers) {
				ubyte* printPtr = cast(ubyte*) base + cp;
				//assert(printPtr - ptrToBase > 5000);
				result ~= "\ncp:" ~ to!string(cp) ~ "\n";

				final switch (header.pageType) with (
					BTreePageHeader.BTreePageType) {
				case emptyPage:
					result ~= "This page is Empty or the pointer is bogus\n";
					break;

				case tableLeafPage: {
						auto relativeBase = printPtr;
						auto singlePagePayloadSize = usablePageSize - 35;
						result ~= "singlePagePayloadSize : " ~ (
							singlePagePayloadSize).to!string ~ "\n";
						auto payloadSize = VarInt(printPtr);
						result ~= "payloadSize: " ~ (payloadSize).to!string
							~ "\n";
						printPtr += payloadSize.length;
						auto rowid = VarInt(printPtr);
						result ~= "rowid: " ~ (rowid).to!string ~ "\n";
						printPtr += rowid.length;

						auto payloadHeaderSize = VarInt(printPtr);
						result ~= "payloadHeaderSize: " ~ (payloadHeaderSize)
							.to!string ~ "\n";

						printPtr += payloadHeaderSize.length;

						auto typeCodes = processPayloadHeader(printPtr,
								payloadHeaderSize);
						printPtr += payloadHeaderSize - 1;

						result ~= "{ ";
						if (payloadSize < singlePagePayloadSize) {
							foreach (typeCode; typeCodes) {
								auto p = extractPayload(printPtr, typeCode);
								result ~= p.apply!(
									v => "\t\"" ~ to!string(v) ~ "\",\n");
								printPtr += typeCode.length;
							}
						} else {
							OverflowInfo overflowInfo;
							overflowInfo.remainingTotalPayload = cast(
								uint)(payloadSize - payloadHeaderSize);
							overflowInfo.payloadOnPage = 
								payloadOnPage(cast(uint) payloadSize);
							uint payloadOnFirstPage = payloadOnPage(cast(uint) payloadSize) - 14;							
							debug {
								import std.stdio;
								writeln("phs:" ,payloadHeaderSize);
							}

							foreach (typeCode; typeCodes) {
								debug {
									import std.stdio;

									writeln("payloadSize - typeCode.length ",
											payloadSize - typeCode.length);
								}

								auto p = extractPayload(&overflowInfo,
										&payloadOnFirstPage, pages, &printPtr, typeCode);
								result ~= p.apply!(
									v => "\t\"" ~ to!string(v) ~ "\",\n");
							}
						}

						result ~= " }\n";

						printPtr += payloadSize;
						//result ~= (payload.length) ? (payload).to!string : "";
					}
					break;

				case tableInteriorPage: {
						BigEndian!uint leftChildPointer = *(
							cast(uint*) printPtr);
						result ~= "nextPagePointer: " ~ (leftChildPointer)
							.to!string ~ "\n";
						//auto lc = BTreePage(base, usablePageSize, leftChildPointer);
						//result ~= to!string(lc);
						printPtr += uint.sizeof;
						auto integerKey = VarInt(printPtr);
						result ~= "integerKey: " ~ (integerKey).to!string
							~ "\n";
						printPtr += integerKey.length;

					}
					break;

				case indexLeafPage: {
						auto payloadSize = VarInt(cast(ubyte*) printPtr);
						result ~= "payloadSize: " ~ (payloadSize).to!string
							~ "\n";
						printPtr += payloadSize.length;
						auto payloadHeaderSize = VarInt(cast(ubyte*) printPtr);
						printPtr += payloadHeaderSize.length;
						result ~= "payloadHeaderSize: " ~ (payloadHeaderSize)
							.to!string ~ "\n";
						auto typeCodes = processPayloadHeader(printPtr,
								payloadHeaderSize);
						foreach (typeCode; typeCodes) {
							result ~= to!string(typeCode) ~ ", ";
						}
						result ~= "\n";
						result ~= "rs-phs : " ~ to!string(
							payloadSize - payloadHeaderSize) ~ "\n";
						auto payload = CArray!char.toArray(
							cast(ubyte*)(printPtr + payloadHeaderSize),
								payloadSize - payloadHeaderSize);
						printPtr += payloadSize;
						result ~= (payload.length) ? (payload).to!string : "";

					}
					break;

				case indexInteriorPage: {
						BigEndian!uint leftChildPointer = *(
							cast(uint*) printPtr);
						result ~= "leftChildPinter: " ~ (leftChildPointer)
							.to!string ~ "\n";
						VarInt payloadSize;
						CArray!ubyte _payload;
						BigEndian!uint _firstOverflowPage;
						//assert(0,"No support for indexInteriorPage");
					}
					break;

				}

			}
			return result;
		}

		static align(1) struct BTreePageHeader {
		align(1):
			//	pure :
			enum BTreePageType : ubyte {
				emptyPage = 0,
				indexInteriorPage = 2,
				tableInteriorPage = 5,
				indexLeafPage = 10,
				tableLeafPage = 13
			}

			BTreePageType _pageType;
			BigEndian!ushort firstFreeBlock;
			BigEndian!ushort cellsInPage;
			BigEndian!ushort startCellContantArea; /// 0 is interpreted as 65536
			ubyte fragmentedFreeBytes;
			BigEndian!uint _rightmostPointer;

			bool isInteriorPage() pure const {
				return (pageType == pageType.indexInteriorPage
						|| pageType == pageType.tableInteriorPage);
			}

			@property auto pageType() const pure {
				return cast(const) _pageType;
			}

			@property BigEndian!uint rightmostPointer() {
				assert(isInteriorPage,
						"the rightmost pointer is only in interior nodes");
				return _rightmostPointer;
			}

			@property void rightmostPointer(uint rmp) {
				assert(isInteriorPage,
						"the rightmost pointer is only in interior nodes");
				_rightmostPointer = rmp;
			}

			string toString() {
				import std.conv;

				string result = "pageType:\t";
				result ~= to!string(pageType) ~ "\n";
				result ~= "firstFreeBlock:\t";
				result ~= to!string(firstFreeBlock) ~ "\n";
				result ~= "cellsInPage:\t";
				result ~= to!string(cellsInPage) ~ "\n";
				result ~= "startCellContantArea:\t";
				result ~= to!string(startCellContantArea) ~ "\n";
				result ~= "fragmentedFreeBytes:\t";
				result ~= to!string(fragmentedFreeBytes) ~ "\n";
				if (isInteriorPage) {
					result ~= "_rightmostPointer";
					result ~= to!string(_rightmostPointer) ~ "\n";
				}

				return result;
			}
		}

		alias BTreePageType = BTreePage.BTreePageHeader.BTreePageType;

		bool hasPayload() const pure {
			final switch (pageType) with (BTreePageType) {
			case emptyPage:
				return false;
			case tableInteriorPage:
				return false;
			case indexInteriorPage:
				return true;
			case indexLeafPage:
				return true;
			case tableLeafPage:
				return true;
			}
		}

		auto payloadSize(BigEndian!ushort cp) {
			assert(hasPayload);
			final switch (pageType) with (BTreePageType) {
			case emptyPage:
			case tableInteriorPage:
				assert(0, "page has no payload");
			
			case indexInteriorPage:
				return VarInt(base + cp + uint.sizeof);
			case indexLeafPage:
				return VarInt(base + cp);
			case tableLeafPage:
				return VarInt(base + cp);
			}
		}

		BTreePageHeader header() const pure {
			ubyte* _offset = (cast(ubyte*) base + offset);
			return *cast(BTreePageHeader*) _offset;
		}

		uint headerSize() pure const {
			return 12 - (header.isInteriorPage ? 0 : 4);
		}

		auto getCellPointerArray() {
			ubyte* arrayPos = cast(ubyte*) base + offset + headerSize();
			return CArray!(BigEndian!ushort).toArray(arrayPos,
					header().cellsInPage);
		}

		BTreePageHeader.BTreePageType pageType() const pure {
			return (header()).pageType;
		}

		auto processPayloadHeader(ubyte* startPayloadHeader,
				ulong payloadHeaderSize) {
			Payload.SerialTypeCode[] serialTypeCodes;
			serialTypeCodes.reserve(cast(uint) payloadHeaderSize);

			ubyte* ptr = startPayloadHeader;
			while (ptr < startPayloadHeader + payloadHeaderSize - 1) {
				auto typeCode = VarInt(ptr);
				serialTypeCodes ~= Payload.SerialTypeCode(typeCode);
				ptr += typeCode.length;
			}
			return serialTypeCodes;
		}

		Payload extractPayload(OverflowInfo* overflowInfo,
				uint* payloadOnFirstPage,
				PageRange pages, ubyte** startPayload,
				Payload.SerialTypeCode typeCode) {

			uint nextPageIdx;

			static void gotoNextPage(uint* nextPageIdx,
					PageRange pages, ubyte** startPayload) {
				assert(*nextPageIdx != 0, "No next Page to go to");
		
				auto nextPage = pages[(*nextPageIdx) - 1];
				*nextPageIdx = *(cast(BigEndian!uint*) nextPage.base);

				debug {
					import std.stdio;
					writeln("going to next Page ...\n",
							nextPageIdx);
				}

				*startPayload = nextPage.base + uint.sizeof;
			}

			if (typeCode.length <= *payloadOnFirstPage) {
				(*payloadOnFirstPage) -= typeCode.length;

				auto oldStartPayload = *startPayload;
				*startPayload += typeCode.length;
				if (*payloadOnFirstPage == 0
						&& overflowInfo.remainingTotalPayload > 0) {
					assert(0, "I do not expect us to ever get here" ~
						"If we ever do, uncomment the two lines below and delete this assert");
				//	nextPageIdx = *cast(uint*)*startPayload;
				//	gotoNextPage(&nextPageIdx, pages, startPayload);
				}

				return extractPayload(oldStartPayload, typeCode);
			} else { // typeCode.length > overflowInfo.payloadOnPage
				// We need to consolidate the Payload here...
				// let's assume SQLite is sane and does not split primitive types in the middle
				alias et = Payload.SerialTypeCode.SerialTypeCodeEnum;
				assert(typeCode.type == et.blob || typeCode.type == et._string);

				auto remainingBytesOfPayload = cast(uint) typeCode.length;
				ubyte[] _payloadBuffer;
				_payloadBuffer.reserve(cast(uint) typeCode.length);

				assert(*payloadOnFirstPage != 0);

				_payloadBuffer ~= ((*startPayload)[0 .. *payloadOnFirstPage]);
				*startPayload += *payloadOnFirstPage;
				remainingBytesOfPayload -= *payloadOnFirstPage;
				nextPageIdx = 
					*(cast(BigEndian!uint*) *startPayload);

				for (;;) {
					gotoNextPage(&nextPageIdx, pages, startPayload);

					import std.algorithm : min;

					auto readBytes = cast(uint) min(_usablePageSize,
							remainingBytesOfPayload);
					debug {
						import std.stdio;

						writeln("readBytes : ", readBytes);
						writeln("remainingBytesOfPayload: ",
								remainingBytesOfPayload);
					}
					remainingBytesOfPayload -= readBytes;
					overflowInfo.remainingTotalPayload -= readBytes;

					_payloadBuffer ~= (*startPayload)[0 .. readBytes];
					debug {
						import std.stdio;

						writeln("isAddedtoPayload: ",
								cast(string)(*startPayload)[0 .. readBytes]);
					}
					*startPayload += readBytes;

					if (remainingBytesOfPayload == 0) {
						debug {
							import std.stdio;

							writeln("pB:", cast(string) _payloadBuffer);
			
						}
						return extractPayload(_payloadBuffer.ptr, typeCode);
					}
				}
			}
		}

		Payload extractPayload(ubyte* startPayload,
				Payload.SerialTypeCode typeCode) {
			Payload p;
			p.typeCode = typeCode;

			final switch (typeCode.type) {
			case typeof(typeCode).int8:
				p.int8 = *cast(byte*) startPayload;
				break;
			case typeof(typeCode).int16:
				p.int16 = *cast(short*) startPayload;
				break;
			case typeof(typeCode).int24:
				p.int24 = (*cast(int*) startPayload) & 0xfff0;
				break;
			case typeof(typeCode).int32:
				p.int32 = *cast(int*) startPayload;
				break;
			case typeof(typeCode).int48:
				p.int48 = (*cast(long*) startPayload) & 0xffffff00;
				break;
			case typeof(typeCode).int64:
				p.int64 = *cast(long*) startPayload;
				break;
			case typeof(typeCode).float64:
				p.float64 = *cast(double*) startPayload;
				break;
			case typeof(typeCode).blob:
				p.blob = cast(ubyte[]) startPayload[0 .. cast(uint) typeCode.length];
				break;
			case typeof(typeCode)._string:
				p._string = cast(string) startPayload[0 .. cast(uint) typeCode.length];
				break;

			case typeof(typeCode).NULL:
			case typeof(typeCode).bool_false:
			case typeof(typeCode).bool_true:
					break;
			}

			return p;
		}

	}

}

auto apply(alias handler)(Database.Payload p) {
	final switch (p.typeCode.type) with (typeof(p.typeCode.type)) {
	case NULL:
		static if (__traits(compiles, handler(null))) {
			return handler(null);
		} else {
			assert(0, "handler cannot take null");
		}
	case int8:
		static if (__traits(compiles, handler(p.int8))) {
			return handler(p.int8);
		} else {
			assert(0);
		}
	case int16:
		static if (__traits(compiles, handler(p.int16))) {
			return handler(p.int16);
		} else {
			assert(0);
		}
	case int24:
		static if (__traits(compiles, handler(p.int24))) {
			return handler(p.int24);
		} else {
			assert(0);
		}
	case int32:
		static if (__traits(compiles, handler(p.int32))) {
			return handler(p.int32);
		} else {
			assert(0);
		}
	case int48:
		static if (__traits(compiles, handler(p.int48))) {
			return handler(p.int48);
		} else {
			assert(0);
		}
	case int64:
		static if (__traits(compiles, handler(p.int64))) {
			return handler(p.int64);
		} else {
			assert(0);
		}
	case float64:
		static if (__traits(compiles, handler(p.float64))) {
			return handler(p.float64);
		} else {
			assert(0);
		}
	case bool_false:
		static if (__traits(compiles, handler(false))) {
			return handler(false);
		} else {
			assert(0);
		}
	case bool_true:
		static if (__traits(compiles, handler(true))) {
			return handler(true);
		} else {
			assert(0);
		}
	case blob:
		static if (__traits(compiles, handler(p.blob))) {
			return handler(p.blob);
		} else {
			assert(0);
		}
	case _string:
		static if (__traits(compiles, handler(p._string))) {
			return handler(p._string);
		} else {
			assert(0);
		}

	}
}

auto getAs(T)(Database.Payload p) {
	return p.apply!(v => cast(T) v);
}

auto getAs(T)(Database.Row r, uint columIndex) {
	return r[columIndex].getAs!T();
}

auto getAs(T)(Database.Row r, Database.TableSchema s, string colName) {
	return r.getAs!T(s.getColumIndex(colName));
}

unittest {
	Database.Payload p;
	p.typeCode.type = Database.Payload.SerialTypeCodeEnum.bool_true;
	assert(p.getAs!(int) == 1);
	p.typeCode.type = Database.Payload.SerialTypeCodeEnum.bool_false;
	assert(p.getAs!(int) == 0);
}
