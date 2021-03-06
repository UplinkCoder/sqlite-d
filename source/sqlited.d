module sqlited;

//          Copyright Stefan Koch 2015 - 2018.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE.md or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

/****************************
 * SQLite-D SQLite 3 Reader *
 * By Stefan Koch 2016      *
 ****************************/

import utils;
import varint;

struct Database {
	string dbFilename;
	const ubyte[] data;
	alias Row = BTreePage.Row;
	alias BTreePageType = BTreePage.BTreePageType;

	SQLiteHeader _cachedHeader;

	SQLiteHeader header() pure const {
		return _cachedHeader;
	}

	BTreePage rootPage() pure const {
		return pages[0];
	}

	PageRange pages() pure const {
		return PageRange(cast(const)data, header.pageSize,
			usablePageSize, cast(const uint)(data.length / header.pageSize));
	}

	const(uint) usablePageSize() pure const {
		return header.pageSize - header.reserved;
	}

	struct PageRange {
		const ubyte[] data;
		const uint pageSize;
		const uint usablePageSize;

		const uint numberOfPages;
	pure :
		@property uint length() const {
			return numberOfPages;
		}

		BTreePage opIndex(const uint pageNumber) pure const {
			assert(pageNumber <= numberOfPages,
				"Attempting to go to invalid page");

			const size_t pageBegin = 
				(pageSize * pageNumber);
			const size_t pageEnd = 
				(pageSize * pageNumber) + usablePageSize;

			return BTreePage(data[pageBegin .. pageEnd], pageNumber ? 0 : 100);
		}

		this(const ubyte[] data, const uint pageSize,
			const uint usablePageSize, const uint numberOfPages) pure {
			this.data = data;
			this.pageSize = pageSize;
			this.usablePageSize = usablePageSize;
			this.numberOfPages = numberOfPages;
			assert(usablePageSize >= 512);
		}

	}

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

			this(VarInt v) pure nothrow {
				long _v = v; // this could be an int
                                             // also the varint handling would not need to swap 8 bytes

				if (_v > 11) {
					if (_v & 1) {
						type = SerialTypeCode.SerialTypeCodeEnum._string;
						length = (_v - 13) / 2;
					} else {
						type = SerialTypeCode.SerialTypeCodeEnum.blob;
						length = (_v - 12) / 2;
					}
				} else {
					static immutable uint[11] lengthTbl = [0, 1, 2, 3, 4, 6, 8, 0, 0, -1, -1];
					type = cast(SerialTypeCodeEnum) _v;
					length = lengthTbl[_v];
				}

			}

			SerialTypeCodeEnum type;
			long length;
		}

		union {
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

	static struct Table {
		TableSchema schema;
	//	Row[] rows;
	}

	static align(1) struct SQLiteHeader {

		bool isValid() pure {
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

		char[16] magicString;

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

		static SQLiteHeader fromArray (const ubyte[] raw) pure {
			assert(raw.length >= this.sizeof);
			if (__ctfe) {
				SQLiteHeader result;

				result.magicString = cast(char[])raw[0 .. 16];
				result.pageSize = raw[16 .. 18];
				result.FileFormatWriteVer = cast(FileFormat)raw[18 .. 19][0];
				result.FileFormatReadVer = cast(FileFormat)raw[19 .. 20][0];
				result.reserved = raw[20 .. 21][0];
				// maxEmbeddedPayloadFract ff.
				// are immutable values and do not need to be read

				//	result.maxEmbeddedPayloadFract = raw[21 .. 22];
				//	result.minEmbeddedPayloadFract = raw[22 .. 23];
				//	result.leafPayloadFract = raw[23 .. 24]

				result.fileChangeCounter = raw[24 .. 28];
				result.sizeInPages = raw[28 .. 32];
				result.firstFreelistPage = raw[32 .. 36];

				return result;
			} else {
				return *(cast(SQLiteHeader*) raw);
			}
		}
	}

	this(string filename, bool readEntirely = true) {
		import std.file;

		auto data = cast(ubyte[]) read(filename);
		this(data, filename);
	}
	/// If you pass null as buffer a new one will be gc-allocated;
	this(const ubyte[] buffer, string filename = ":Memory:") pure {
		if (buffer is null) {
			ubyte[] myBuffer = new ubyte[](1024);
			data = myBuffer;
			///TODO write a suitable default header here.
		} else {
			data = cast(ubyte[])buffer;
		}
		dbFilename = filename;
		_cachedHeader = SQLiteHeader.fromArray(buffer); 
		assert(_cachedHeader.magicString[0..6] == "SQLite");
	}

	/**
	 * CREATE TABLE sqlite_master(
	 *	type text,
	 *	name text,
	 *	tbl_name text,
	 *	rootpage integer,
	 *	sql text
	 * );
	 *	 
	 */
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
			string columnName;
			string TypeName;
			string defaultValue;
			bool isPrimayKey;
			bool notNull;
		}
	}



	static struct BTreePage {
	
		enum BTreePageType : ubyte {
			emptyPage = 0,
			indexInteriorPage = 2,
			tableInteriorPage = 5,
			indexLeafPage = 10,
			tableLeafPage = 13
		}

	
		const ubyte[] page;
		const uint headerOffset;
		
		static struct Row {
			const PageRange pages;
			const uint payloadSize;
			const uint rowId;
			const uint payloadHeaderSize;
			const ubyte[] payloadHeaderBytes;
			const ubyte[] payloadStart;
			const BTreePageType pageType;

			auto column(const uint colNum) pure const {
				auto payloadHeader = PayloadHeader(payloadHeaderBytes);
				uint offset;
				
				foreach(_; 0 .. colNum) {
					offset += payloadHeader.front().length;
					payloadHeader.popFront();
				}
				
				auto typeCode = payloadHeader.front();
				uint payloadEnd = cast(uint) (offset + typeCode.length);
				
				if (payloadStart.length > payloadEnd) {
					return extractPayload(payloadStart[offset .. payloadEnd], typeCode);
				} else {
					auto overflowInfo = OverflowInfo(payloadStart, offset, payloadSize, pages, payloadHeaderSize, pageType);
					return extractPayload(&overflowInfo, typeCode, pages);
				}
			}
		}



		Row getRow(const ushort cellPointer, const PageRange pages, const BTreePageType pageType) pure const {
			uint offset = cellPointer;
			import std.algorithm : min;

			auto payloadSize = VarInt(page[offset .. $]);
			offset += payloadSize.length;

			ulong rowId;
			if (pageType == BTreePageType.tableLeafPage) {
				VarInt rowId_v = VarInt(page[offset .. $]);
				rowId = rowId_v;
				offset += rowId_v.length;
			}
			
			auto payloadHeaderSize = VarInt(page[offset ..  page.length]);
			uint _payloadHeaderSize = cast(uint)payloadHeaderSize;

			if (_payloadHeaderSize > page.length - offset) {
				assert(0, "Overflowing payloadHeaders are currently not handeled");
			}

			auto ph = page[offset + payloadHeaderSize.length .. offset + _payloadHeaderSize];
			offset += _payloadHeaderSize;
			// TODO The payloadHeader does not have to be sliced off here
			// We can potentially do better of we pass just one buffer to struct Row and slice there.

			return Row(pages, cast(uint) payloadSize, cast(uint) rowId, cast(uint) _payloadHeaderSize , ph, page[offset .. $], pageType);
		}

		//		string toString(PageRange pages) {
		//			import std.conv;
		//
		//			auto pageType = header.pageType;
		//			string result = to!string(pageType);
		//
		//			auto cellPointers = getCellPointerArray();
		//			foreach (cp; cellPointers) {
		//				ubyte* printPtr = cast(ubyte*) base + cp;
		//
		//				final switch (header.pageType) with (
		//					BTreePageHeader.BTreePageType) {
		//				case emptyPage:
		//					result ~= "This page is Empty or the pointer is bogus\n";
		//					break;
		//
		//				case tableLeafPage: {
		//						auto singlePagePayloadSize = usablePageSize - 35;
		//						result ~= "singlePagePayloadSize : " ~ (
		//							singlePagePayloadSize).to!string ~ "\n";
		//						auto payloadSize = VarInt(printPtr);
		//						result ~= "payloadSize: " ~ (payloadSize).to!string
		//							~ "\n";
		//						printPtr += payloadSize.length;
		//						auto rowid = VarInt(printPtr);
		//						result ~= "rowid: " ~ (rowid).to!string ~ "\n";
		//						printPtr += rowid.length;
		//
		//						auto payloadHeaderSize = VarInt(printPtr);
		//						result ~= "payloadHeaderSize: " ~ (payloadHeaderSize)
		//							.to!string ~ "\n";
		//
		//						printPtr += payloadHeaderSize.length;
		//
		//						auto typeCodes = processPayloadHeader(printPtr,
		//								payloadHeaderSize);
		//						printPtr += payloadHeaderSize - payloadHeaderSize.length;
		//
		//						import std.algorithm;
		//						assert(typeCodes.map!(tc => tc.length).sum == payloadSize - payloadHeaderSize);
		//
		//						result ~= "{ ";
		//						if (payloadSize < singlePagePayloadSize) {
		//							foreach (typeCode; typeCodes) {
		//								auto p = extractPayload(printPtr, typeCode);
		//								result ~= p.apply!(
		//									v => "\t\"" ~ to!string(v) ~ "\",\n");
		//								printPtr += typeCode.length;
		//							}
		//						} else {
		//							auto overflowInfo = OverflowInfo();
		//
		//							overflowInfo.remainingTotalPayload = cast(uint)payloadSize;
		//							overflowInfo.payloadOnFirstPage = 
		//								payloadOnPage(cast(uint)(payloadSize)) - cast(uint)payloadHeaderSize;
		//
		//							foreach (typeCode; typeCodes) {
		//								auto p = extractPayload(&printPtr, typeCode,
		//										&overflowInfo, pages);
		//								auto str = p.apply!(v => "\t\"" ~ to!string(v) ~ "\",\n");
		//								result ~= str;
		//
		//							}
		//						}
		//
		//						result ~= " }\n";
		//
		//						printPtr += payloadSize;
		//					}
		//					break;
		//
		//				case tableInteriorPage: {
		//						BigEndian!uint leftChildPointer = *(
		//							cast(uint*) printPtr);
		//						result ~= "nextPagePointer: " ~ (leftChildPointer)
		//							.to!string ~ "\n";
		//						//auto lc = BTreePage(base, usablePageSize, leftChildPointer);
		//						//result ~= to!string(lc);
		//						printPtr += uint.sizeof;
		//						auto integerKey = VarInt(printPtr);
		//						result ~= "integerKey: " ~ (integerKey).to!string
		//							~ "\n";
		//						printPtr += integerKey.length;
		//					}
		//					
		//					break;
		//
		//				case indexLeafPage: {
		//						auto payloadSize = VarInt(cast(ubyte*) printPtr);
		//						result ~= "payloadSize: " ~ (payloadSize).to!string
		//							~ "\n";
		//						printPtr += payloadSize.length;
		//						auto payloadHeaderSize = VarInt(cast(ubyte*) printPtr);
		//						printPtr += payloadHeaderSize.length;
		//						result ~= "payloadHeaderSize: " ~ (payloadHeaderSize)
		//							.to!string ~ "\n";
		//						auto typeCodes = processPayloadHeader(printPtr,
		//								payloadHeaderSize);
		//						foreach (typeCode; typeCodes) {
		//							result ~= to!string(typeCode) ~ ", ";
		//						}
		//						result ~= "\n";
		//						result ~= "rs-phs : " ~ to!string(
		//							payloadSize - payloadHeaderSize) ~ "\n";
		//						auto payload = CArray!char.toArray(
		//							cast(ubyte*)(printPtr + payloadHeaderSize),
		//								payloadSize - payloadHeaderSize);
		//						printPtr += payloadSize;
		//						result ~= (payload.length) ? (payload).to!string : "";
		//						
		//					}
		//					break;
		//
		//				case indexInteriorPage: {
		//						BigEndian!uint leftChildPointer = 
		//							*(cast(uint*) printPtr);
		//						result ~= "leftChildPinter: " ~ (leftChildPointer)
		//							.to!string ~ "\n";
		//						VarInt payloadSize;
		//						CArray!ubyte _payload;
		//						BigEndian!uint _firstOverflowPage;
		//						//assert(0,"No support for indexInteriorPage");
		//					}
		//					break;
		//
		//				}
		//
		//			}
		//			return result;
		//		}

		static align(1) struct BTreePageHeader {
		align(1):
			//	pure :
			BTreePageType _pageType;
			BigEndian!ushort firstFreeBlock;
			BigEndian!ushort cellsInPage;
			BigEndian!ushort startCellContantArea; /// 0 is interpreted as 65536
			ubyte fragmentedFreeBytes;
			BigEndian!uint _rightmostPointer;

			static BTreePageHeader fromArray(const ubyte[] _array) pure {
				assert(_array.length >= this.sizeof);
				if (__ctfe) {
					BTreePageHeader result;

					result._pageType = cast(BTreePageType)_array[0];
					result.firstFreeBlock = _array[1 .. 3]; 
					result.cellsInPage = _array[3 .. 5];
					result.startCellContantArea = _array[5 .. 7];
					result.fragmentedFreeBytes = _array[7];
					if (result.isInteriorPage) {
						result._rightmostPointer = _array[8 .. 12];
					}

					return result;
				} else {
					return *(cast (BTreePageHeader*) _array.ptr);
				}
			}

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

			uint length() pure const {
				return 12 - (isInteriorPage ? 0 : 4);
			}

//			string toString() {
//				import std.conv;
//
//				string result = "pageType:\t";
//				result ~= to!string(pageType) ~ "\n";
//				result ~= "firstFreeBlock:\t";
//				result ~= to!string(firstFreeBlock) ~ "\n";
//				result ~= "cellsInPage:\t";
//				result ~= to!string(cellsInPage) ~ "\n";
//				result ~= "startCellContantArea:\t";
//				result ~= to!string(startCellContantArea) ~ "\n";
//				result ~= "fragmentedFreeBytes:\t";
//				result ~= to!string(fragmentedFreeBytes) ~ "\n";
//				if (isInteriorPage) {
//					result ~= "_rightmostPointer";
//					result ~= to!string(_rightmostPointer) ~ "\n";
//				}
//
//				return result;
//			}
		}

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
					return VarInt(page[cp + uint.sizeof .. $]);
				case indexLeafPage:
					return VarInt(page[cp .. $]);
				case tableLeafPage:
					return VarInt(page[cp .. $]);
			}
		}

		BTreePageHeader header() const pure {
			return BTreePageHeader.fromArray(page[headerOffset.. headerOffset + BTreePageHeader.sizeof]);
		}

		BigEndian!ushort[] getCellPointerArray() const pure {
			auto offset = header.length + headerOffset;
			return page[offset .. offset + header.cellsInPage * ushort.sizeof]
				.toArray!(BigEndian!ushort)(header.cellsInPage);
		}

		BTreePageType pageType() const pure {
			return (header()).pageType;
		}

		struct PayloadHeader {
			const ubyte[] payloadHeader;
			uint offset;
			uint _length;
			 
			void popFront() pure {
				assert(offset < payloadHeader.length);
				offset += _length;
				_length = 0;
			}

			Payload.SerialTypeCode front() pure {
				auto v = VarInt(payloadHeader[offset .. $]);
				_length = cast(uint)v.length;
				return Payload.SerialTypeCode(v);
			}

			bool empty() pure const {
				return offset == payloadHeader.length;
			}
		}


	}
}

struct OverflowInfo {
	const(ubyte)[] pageSlice;
	uint nextPageIdx;

	this(const ubyte[] payloadStart, int offset, const uint payloadSize, const Database.PageRange pages, const uint payloadHeaderSize, const Database.BTreePageType pageType) pure {
		import std.algorithm : min;
		
		uint x;
		if (isIndex(pageType)) {
			x = ((pages.usablePageSize - 12) * 32 / 255) - 23;
		} else {
			x = pages.usablePageSize - 35; 
		}
		
		if (payloadSize > x) {
			auto m = ((pages.usablePageSize - 12) * 32 / 255) - 23;
			auto k = m + ((payloadSize - m) % (pages.usablePageSize - 4));

			auto payloadOnFirstPage = (k <= x ? k : m) - payloadHeaderSize;
	
			nextPageIdx = BigEndian!uint(payloadStart[payloadOnFirstPage .. payloadOnFirstPage + uint.sizeof]);
		
			if(offset > payloadOnFirstPage) {
				offset -= payloadOnFirstPage;
				gotoNextPage(pages);

				auto payloadOnOverflowPage = pages.usablePageSize - uint.sizeof;
				while(offset>payloadOnOverflowPage) {
					gotoNextPage(pages);
					offset -= payloadOnOverflowPage;
				}
			} else {
				pageSlice = payloadStart[0 .. payloadOnFirstPage];
			}
		} else {
			pageSlice = payloadStart;
		}
		pageSlice = pageSlice[offset .. $];
	}

	void gotoNextPage(const Database.PageRange pages) pure {
		assert(nextPageIdx != 0, "No next Page to go to");
		auto nextPage = pages[nextPageIdx - 1];
		nextPageIdx = BigEndian!uint(nextPage.page[0 .. uint.sizeof]);
		pageSlice = nextPage.page[uint.sizeof .. $];
	}

}


static Database.Payload extractPayload(
	OverflowInfo* overflowInfo,
	const Database.Payload.SerialTypeCode typeCode,
	const Database.PageRange pages,
	) pure {
	

	if (overflowInfo.pageSlice.length >= typeCode.length) {
		auto _length = cast(uint) typeCode.length;

		auto payload = overflowInfo.pageSlice[0 .. _length];
		overflowInfo.pageSlice = overflowInfo.pageSlice[_length .. $];

		if (overflowInfo.pageSlice.length == uint.sizeof) {
			assert(0, "I do not expect us to ever get here\n" ~
				"If we ever do, uncomment the two lines below and delete this assert");
		//		overflowInfo.nextPageIdx = BigEndian(overflowInfo.pageSlice[0 .. uint.sizeof]);
		//		overflowInfo.gotoNextPage(pages);
		}

		return extractPayload(payload, typeCode);
	} else { // typeCode.length > payloadOnFirstPage
		alias et = Database.Payload.SerialTypeCode.SerialTypeCodeEnum;
		// let's assume SQLite is sane and does not split primitive types in the middle
		assert(typeCode.type == et.blob || typeCode.type == et._string);
		
		auto remainingBytesOfPayload = cast(uint) typeCode.length;
		ubyte[] _payloadBuffer;
		
		for (;;) {

			import std.algorithm : min;
			
			auto readBytes = cast(uint) min(overflowInfo.pageSlice.length,
				remainingBytesOfPayload);

			remainingBytesOfPayload -= readBytes;

			_payloadBuffer ~= overflowInfo.pageSlice[0 .. readBytes];

			if (remainingBytesOfPayload == 0) {
				assert(typeCode.length == _payloadBuffer.length);

				return extractPayload(cast(const)_payloadBuffer, typeCode);
			} else {
				if (overflowInfo.nextPageIdx == 0 && !remainingBytesOfPayload) {
					//This is a very special-case 
					//Hopefully we don't hit it :)
					assert(0, "Moow!");
				}
				overflowInfo.gotoNextPage(pages);
			}
		}
	}
}

static Database.Payload extractPayload(const ubyte[] startPayload,
	const Database.Payload.SerialTypeCode typeCode) pure {
	Database.Payload p;
	p.typeCode = typeCode;
	
	final switch (typeCode.type) {
		case typeof(typeCode).int8:
			p.int8 = *cast(byte*) startPayload;
			break;
		case typeof(typeCode).int16:
			p.int16 = *cast(BigEndian!short*) startPayload;
			break;
		case typeof(typeCode).int24:
			p.int24 = (*cast(BigEndian!int*) startPayload) & 0xfff0;
			break;
		case typeof(typeCode).int32:
			p.int32 = *cast(BigEndian!int*) startPayload;
			break;
		case typeof(typeCode).int48:
			p.int48 = (*cast(BigEndian!long*) startPayload) & 0xffffff00;
			break;
		case typeof(typeCode).int64:
			p.int64 = *cast(BigEndian!long*) startPayload;
			break;
		case typeof(typeCode).float64:
			if (!__ctfe) {
				p.float64 = *cast(double*) startPayload;
				assert(0, "Not supported at runtime either it's BigEndian :)");
			} else
				assert(0, "not supporeted at CTFE yet");
		//	break;
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



auto apply(alias handler)(const Database.Payload p) {
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
				assert(0,"handler " ~ typeof(handler).stringof ~ " cannot be called with string");
			}
	}
}
pure :
auto getAs(T)(Database.Payload p) {
	return p.apply!(v => cast(T) v);
}

auto getAs(T)(Database.Row r, uint columnIndex) {
	return r.column(columnIndex).getAs!T();
}

auto getAs(T)(Database.Row r, Database.TableSchema s, string colName) {
	return r.getAs!T(s.getcolumnIndex(colName));
}

unittest {
	Database.Payload p;
	p.typeCode.type = Database.Payload.SerialTypeCodeEnum.bool_true;
	assert(p.getAs!(int) == 1);
	p.typeCode.type = Database.Payload.SerialTypeCodeEnum.bool_false;
	assert(p.getAs!(int) == 0);
}

bool isIndex(const Database.BTreePage.BTreePageType pageType) pure {
	return ((pageType & 2) ^ 2) == 0; 
}

static assert(isIndex(Database.BTreePageType.indexLeafPage) && isIndex(Database.BTreePageType.indexInteriorPage) && !isIndex(Database.BTreePageType.tableLeafPage));
