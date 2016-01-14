import std.stdio;
import sqlited;
import sqlite.utils;
import std.conv;

void main(string[] args) {
	string filename = (args.length > 1 ? args[1] : "example/test.s3db");
	auto page = (args.length > 2 ? parse!(int)(args[2]) : 0);

	writefln("opening file %s", filename); 
	auto db = new Database(filename);
	if (db !is null) {
		writeln("it appears to be a database");
//		writeln(db.tables.schemas);
		writeln(db.pages[page].header);

		if (db.pages[page].pageType == db.BTreePage.BTreePageHeader.BTreePageType.tableLeafPage) {
			writeln(db.pages[page].toString(db));
		}

		if (db.pages[page].pageType == db.BTreePage.BTreePageHeader.BTreePageType.tableInteriorPage) {
			uint[] pageNumbers;
			auto cpa = db.pages[page].getCellPointerArray;
			pageNumbers.reserve(cpa.length);
			 
			foreach(cp;cpa) {
				auto cellPointer = (db.rootPage.base + cp);
				BigEndian!uint leftChildPage = *(cast(uint*)cellPointer);
				pageNumbers ~= leftChildPage;
			}
			pageNumbers ~= db.rootPage.header._rightmostPointer;

			foreach(pageIndex;pageNumbers) {
				auto _page = db.pages[pageIndex-1];
				writeln("page [",pageIndex, "] :\n", _page.toString(db));

				if(_page.hasPayload) {
					foreach(cp;_page.getCellPointerArray) {
						auto ps = _page.payloadSize(cp);
						if (ps > _page.usablePageSize) {
							writeln("found payload bigger then the usable pageSize ", _page.usablePageSize, " < ", ps);
						}
					}
				}
			}
		}





		//foreach(i; 1.. db.pages.length ){writeln("page [",i,"]\n",db.pages[i]);}
		writeln("pageSize : ",db.header.pageSize);
//		writeln(db.header);
//		writeln(db.tables["source_location"]);
//		writeln(db.pages.front.pageType);
//		writeln(db.pages.length);
//		writeln(db.header.largestRootPage);
//		writeln(db.pages[0]);
		int a = 12;
//		writeln(db.tables);
//		writeln(db.pages.front);
		//auto firstPage = (*cast(db.BTreePage*)(cast(ubyte*)db.data.ptr + 100));
		//writeln(cast(char[])db.data);
		//	writeln(db.BTreePage(db.data.ptr, 100).toString());
		//	writeln(firstPage.pageType(db.data.ptr));
		//	writeln(db.VarInt().lengthInVarInt(0x80));
		//	writeln(db.VarInt().lengthInVarInt(0x7f));
		//	writeln(db.VarInt().lengthInVarInt(0x7dff));
		//	writeln(db.VarInt().lengthInVarInt(0x_ffff_ffff_ffff_ffff));
		//foreach(page;db.pages) {
		//	writeln(page);
		//}
	} else {
		writeln("invalid database or header corrupted");
	}
	
	readln();
}
