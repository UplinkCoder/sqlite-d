import std.stdio;
import sqlited;
import sqlite.utils;

void main(string[] args) {
	string filename = (args.length == 2 ? args[1] : "example/test.s3db");
	writefln("opening file %s", filename); 
	auto db = new Database(filename);
	if (db !is null) {
		writeln("it appears to be a database");
//		writeln(db.tables.schemas);
		writeln(db.rootPage.header);

		if (db.rootPage.pageType == db.BTreePage.BTreePageHeader.BTreePageType.leafTablePage) {
			writeln(db.rootPage.toString(db));
		}

		if (db.rootPage.pageType == db.BTreePage.BTreePageHeader.BTreePageType.interiorTablePage) {
			uint[] pageNumbers;
			auto cpa = db.rootPage.getCellPointerArray;
			pageNumbers.reserve(cpa.length);
			 
			foreach(cp;cpa) {
				auto cellPointer = (db.rootPage.base + cp);
				BigEndian!uint leftChildPage = *(cast(uint*)cellPointer);
				pageNumbers ~= leftChildPage;
			}
			pageNumbers ~= db.rootPage.header._rightmostPointer;

			foreach(pageIndex;pageNumbers) {
				auto page = db.pages[pageIndex-1];
				writeln("page [",pageIndex, "] :\n", page.toString(db));

				if(page.hasPayload) {
					foreach(cp;page.getCellPointerArray) {
						auto ps = page.payloadSize(cp);
						if (ps > page.usablePageSize) {
							writeln("found payload bigger then the usable pageSize ", page.usablePageSize, " < ", ps);
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
