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
	const ubyte[] data;

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
		if (__ctfe) {
			return header.pageSize - header.reserved;
		}
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

			this(VarInt v) pure  {
				if (v > 11) {
					if (v & 1) {
						type = SerialTypeCode.SerialTypeCodeEnum._string;
						length = (v - 13) / 2;
					} else {
						type = SerialTypeCode.SerialTypeCodeEnum.blob;
						length = (v - 12) / 2;
					}
				} else {
					debug {
						import std.stdio;
						if (!__ctfe) writeln(v);
					}
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

	static struct Row {
		const long payloadSize;
		const Payload.SerialTypeCode[] typeCodes;
		const ubyte[] payloadStart;
//		Payload[] colums;
		//	TableSchema* schema;
//		alias colums this;
	}

	static struct Table {
		TableSchema schema;
		Row[] rows;
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
			SQLiteHeader result;

			result.magicString = cast(char[])raw[0 .. 16];
			result.pageSize = raw[16 .. 18];
			result.FileFormatWriteVer = cast(FileFormat)raw[18 .. 19][0];
			result.FileFormatReadVer = cast(FileFormat)raw[19 .. 20][0];
			result.reserved = raw[20 .. 21][0];
		//	result.maxEmbeddedPayloadFract = raw[21 .. 22];
		//	result.minEmbeddedPayloadFract = raw[22 .. 23];
		//	result.leafPayloadFract = raw[23 .. 24]
			result.fileChangeCounter = raw[24 .. 28];
			result.sizeInPages = raw[28 .. 32];
			result.firstFreelistPage = raw[32 .. 36];

			return result;
		}
	}

	this(string filename, bool readEntirely = true) {
		import std.file;

		auto data = cast(ubyte[]) read(filename);
		this(data, filename);
	}

	this(const ubyte[] buffer = null, string filename = ":Memory:") pure {
		if (buffer is null) {
			ubyte[] myBuffer;
			data = myBuffer;
		} else {
			data = cast(ubyte[])buffer;
		}
		dbFilename = filename;
		_cachedHeader = SQLiteHeader.fromArray(buffer); 
		assert(_cachedHeader.magicString[0..6] == "SQLite");
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

	static struct BTreePage {
		const ubyte[] page;
		const uint headerOffset; 

		uint payloadOnPage(const uint payloadSize) const pure {
			auto m = ((page.length - 12) * 32 / 255) - 23;
			import std.algorithm : min;

			auto x1 = cast(uint) m + ((payloadSize - m) % (page.length - 4));
			auto x2 = cast(uint) ((page.length - 12) * 64 / 255) - 23;

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
				return min(cast(uint)page.length - 35, x1);

			case indexInteriorPage:
			case indexLeafPage:
				return min(x1, x2);

			}
		}

		struct OverflowInfo {
			uint remainingTotalPayload;
			uint nextPageIdx;
			uint payloadOnFirstPage;
			uint __padding;
		}

		static assert (OverflowInfo.sizeof % 16 == 0);

		Row[] getRows(const PageRange pages) pure const {
			version(Multithreaded) {
				auto cellPointers = parallel(getCellPointerArray());
				import atomicarray;
				AtomicArray!Row rows;
			} else {
				auto cellPointers = getCellPointerArray();
				Row[] rows;
			}
			import std.parallelism;

			assert(pageType == BTreePageType.tableLeafPage, "only tableLeafPages are supported for now");

			foreach (cp; cellPointers) {
				version(Multithreaded) {
					rows ~= atomicValue(getRow(cp, pages), cast(uint)cp);
				} else {
					rows ~= getRow(cp, pages);
				}
			}
			version(Multithreaded) {
				return cast(Row[]) rows._data;
			} else {
				return rows;
			}
		}
		private
		Row getRow(const uint cellPointer, const PageRange pages) pure const {
			uint offset = cellPointer;
			
			auto payloadSize = VarInt(page[offset .. offset + 9]);
			offset += payloadSize.length;
			
			auto rowId = VarInt(page[offset .. offset + 9]);
			offset += rowId.length;

			auto payloadHeaderSize = VarInt(page[offset .. offset + 9]);

			auto typeCodes = processPayloadHeader(page[offset + payloadHeaderSize.length .. offset + payloadHeaderSize]);
			offset += payloadHeaderSize;

			return Row(payloadSize, typeCodes, page[offset .. $]);
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

			static BTreePageHeader fromArray(const ubyte[] _array) pure {
				assert(_array.length >= this.sizeof);
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
				return VarInt(page[cp + uint.sizeof .. cp + uint.sizeof + 9]);
			case indexLeafPage:
				return VarInt(page[cp .. cp + 9]);
			case tableLeafPage:
				return VarInt(page[cp .. cp + 9]);
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

		BTreePageHeader.BTreePageType pageType() const pure {
			return (header()).pageType;
		}

		auto processPayloadHeader(const ubyte[] payloadHeader) const pure {
			Payload.SerialTypeCode[] serialTypeCodes;
			if (!__ctfe) {
			//	serialTypeCodes.reserve(cast(uint) payloadHeader.length);
			}
			uint offset;

			while (offset < payloadHeader.length) {
				import std.algorithm : min;
				auto typeCode = VarInt(payloadHeader[offset .. min(offset + 9, payloadHeader.length)]);
				serialTypeCodes ~= Payload.SerialTypeCode(typeCode);
				offset += typeCode.length;
			}
			return serialTypeCodes;
		}

//		Payload extractPayload(
//			const(ubyte[])* startPayload,
//			Payload.SerialTypeCode typeCode,
//			OverflowInfo* overflowInfo,
//			PageRange pages
//		) pure {
//
//			static void gotoNextPage(uint* nextPageIdx,
//					PageRange pages, const ubyte[] startPayload) {
//
//				assert(*nextPageIdx != 0, "No next Page to go to");
//				debug {
//					import std.stdio;
//					writeln("goto page : ", *nextPageIdx);
//				}
//				auto nextPage = pages[(*nextPageIdx) - 1];
//				BigEndian!uint np;
//				np = nextPage.page[0 .. uint.sizeof];
//				*nextPageIdx = np;
//
//				startPayload = cast(ubyte[])nextPage.page[uint.sizeof .. $];
//			}
//
//			if (typeCode.length <= overflowInfo.payloadOnFirstPage) {
//				overflowInfo.payloadOnFirstPage -= typeCode.length;
//				overflowInfo.remainingTotalPayload -= typeCode.length;
//
//				auto oldPayload = startPayload;
//				startPayload = startPayload[typeCode.length .. $];
//				if (overflowInfo.payloadOnFirstPage == 0
//						&& overflowInfo.remainingTotalPayload > 0) {
//					assert(0, "I do not expect us to ever get here"
//						"If we ever do, uncomment the two lines below and delete this assert");
//				//	nextPageIdx = *cast(uint*)*startPayload;
//				//	gotoNextPage(&nextPageIdx, pages, startPayload);
//				}
//
//				return extractPayload(oldPayload, typeCode);
//			} else { // typeCode.length > payloadOnFirstPage
//				// We need to consolidate the Payload here...
//				// let's assume SQLite is sane and does not split primitive types in the middle
//				alias et = Payload.SerialTypeCode.SerialTypeCodeEnum;
//				assert(typeCode.type == et.blob || typeCode.type == et._string);
//
//				auto remainingBytesOfPayload = cast(uint) typeCode.length;
//				ubyte[] _payloadBuffer;
//			//	_payloadBuffer.reserve(cast(uint) typeCode.length);
//
//				if (auto pofp = overflowInfo.payloadOnFirstPage) {
//					_payloadBuffer ~= startPayload[0 .. pofp].dup;
//					startPayload = startPayload[pofp .. $];
//
//					remainingBytesOfPayload -= pofp;
//					overflowInfo.remainingTotalPayload -= pofp;
//
//					if (overflowInfo.nextPageIdx == 0) {
//						overflowInfo.nextPageIdx = 
//							*(cast(BigEndian!uint*) startPayload[0 .. 4]);
//					}
//					overflowInfo.payloadOnFirstPage = 0;
//				}
//				
//				for (;;) {
//					if(remainingBytesOfPayload > overflowInfo.remainingTotalPayload) {
//						debug { 
//							import std.stdio;
//							writeln(remainingBytesOfPayload, " > ", overflowInfo.remainingTotalPayload);
//						}
//						
//					}
//					gotoNextPage(&overflowInfo.nextPageIdx, pages, startPayload);
//					import std.algorithm : min;
//
//					auto readBytes = cast(uint) min(page.length - uint.sizeof,
//							remainingBytesOfPayload);
//
//					debug {
//						import std.stdio;
//
//						writeln("readBytes : ", readBytes);
//						writeln("remainingBytesOfPayload: ",
//								remainingBytesOfPayload);
//					}
//					remainingBytesOfPayload -= readBytes;
//
//					_payloadBuffer ~= startPayload[0 .. readBytes];
//					debug {
//						import std.stdio;
//
//					//	writeln("isAddedtoPayload: ",
//					//		cast(ubyte[])(*startPayload)[0 .. readBytes]);
//					//	writeln(stderr, "after Payload: ",
//					//		*cast(BigEndian!uint*)(*startPayload + readBytes), 
//					//		"remaingTotalPayload : ", overflowInfo.remainingTotalPayload,
//					//		"remaingPayload : ", remainingBytesOfPayload);
//					}
//					startPayload = startPayload[readBytes .. $];
//
//					if (remainingBytesOfPayload == 0) {
//						debug {
//							import std.stdio;
//
//							writeln("pB:", cast(ubyte[]) _payloadBuffer);
//			
//						}
//						overflowInfo.payloadOnFirstPage = cast(uint) (
//							page.length - (readBytes + uint.sizeof));
//
//						assert(_payloadBuffer.length == typeCode.length);
//						overflowInfo.remainingTotalPayload -= _payloadBuffer.length;
//
//						return extractPayload(_payloadBuffer, typeCode);
//					}
//				}
//			}
//		}
	}

	static auto extractPayload(T...)(const Row r) {
		uint offset;
		uint lastCol;
		Payload[T.length] result;

		import std.algorithm : isSorted;
		static assert(isSorted([T]));
		foreach (n,colNum;T) {
			foreach(i; lastCol .. colNum) {
				offset += r.typeCodes[i].length;
			}
		
			auto payloadEnd = offset + r.typeCodes[colNum].length;
		
			if (r.payloadStart.length > payloadEnd) {
				result[n] = extractPayload(r.payloadStart[offset .. payloadEnd], r.typeCodes[colNum]);
			} else {
				import std.conv;
				assert(0, "Overflow pages and stuff " ~ to!string(payloadEnd));
			}
			lastCol = colNum;
		}
		return result;
	}


	static auto extractPayload(const Row r, const uint colNum) {
		uint offset;


		foreach(i; 0 .. colNum) {
			offset += r.typeCodes[i].length;
		}

		auto payloadEnd = offset + r.typeCodes[colNum].length;

		if (r.payloadStart.length > payloadEnd) {
			return extractPayload(r.payloadStart[offset .. payloadEnd], r.typeCodes[colNum]);
		} else {
			import std.conv;
			assert(0, "Overflow pages and stuff " ~ to!string(payloadEnd));
		}
	}

	static Payload extractPayload(const ubyte[] startPayload,
		Payload.SerialTypeCode typeCode) pure {
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
				if (!__ctfe)
					p.float64 = *cast(double*) startPayload;
				else
					assert(0, "not supporeted at CTFE yet");
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
			assert(0,"handler " ~ typeof(handler).stringof ~ " cannot be called with string");
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
