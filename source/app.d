//import std.stdio;
import sqlited;
import utils;
import misc;
import std.conv;
import std.stdio;
import core.memory;
import test;

import std.algorithm : map, filter, count;
import std.range : join, takeOne;
//import std.typecons : tuple;

void* pageHandler(Database.BTreePage page, Database.PageRange pages, void* unused) {
	string toPrint = page.header.to!string; //~ "\n" ~ page.toString(pages); 
	writeln(toPrint);
	return null;
}

void* countCellsHandler(Database.BTreePage page, Database.PageRange pages, void* currentCount) {
	*cast(uint*)currentCount += page.header.cellsInPage;
	return currentCount;
}	
static immutable ubyte[] test_s3db = cast(immutable ubyte[]) import("test4.s3db");
static immutable db = cast(immutable)Database(test_s3db, "");
static immutable pages =  cast(immutable)db.pages();
static immutable rp = cast(immutable)pages[0];

uint ct () { 
	uint cnt;
	handlePage!((page, pages) => cnt += page.header.cellsInPage) (rp, db.pages);
	return cnt;
}
auto pn_() {

	Database.Row[] rows;
	import std.algorithm : map, filter;
	import std.array : array;
	struct NumberName {
		uint pagenr;
	}
	
	auto tablesInDB = handlePage!(
		(page,pages) => 
		page.getRows(pages)
		.map!(r => r.colums(1))
		.map!(p => p.getAs!string).array
	) (db, 0);

	auto tableTypes = handlePage!(
		(page,pages) => (page.getRows(pages)).map!(r => r.colums(0).getAs!string == "table").array
	)(rp, pages);

	return tablesInDB;
}

auto getTableNames(const Database db) {
	import std.array : array;
	return handlePage!(
		(page,pages) => (page.getRows(pages))
		.filter!(r => r.colums(0).getAs!string == "table")
		.map!(r => r.colums(1).getAs!string)
	)(db, 0);
}

auto getRowsOf(const Database db, const string tableName) {
	return handlePage!(
		(page,pages) => (page.getRows(pages))
		.filter!(r => r.colums(0).getAs!string == "table")
		.filter!(r => r.colums(1).getAs!string == tableName)
		.map!(r => r.colums(3).getAs!uint - 1)
		.filter!(n => n != 0)
		.map!(n => handlePage!((pg, pages) => pg.getRows(pages))(db, n))
		.join
	)(rp, pages);
}

auto getRootPageOf1(const Database db, const string tableName) {
	return handlePage!(
		(page,pages) => (page.getRows(pages))
		.filter!(r => r.colums(0).getAs!string == "table")
		.filter!(r => r.colums(1).getAs!string == tableName)
		.map!(r => r.colums(3).getAs!uint - 1)
		)(rp, pages).join;
}

auto getRootPageOf2(const Database db, const string tableName) {
	return handlePage!(
		(page,pages) => (page.getRows(pages))
		.map!(r => r.colums(0, 1, 3))
		.filter!(cols => cols[0].getAs!string == "table")
		.filter!(cols => cols[1].getAs!string == tableName)
		.map!(cols => cols[2].getAs!uint-1)
		)(rp, pages).join;
}

pragma(msg, pn_);

int main(string[] args) {
	import std.stdio;
//	GC.disable();
//	fn_();
	string filename = (args.length > 1 ? args[1] : "example/test.s3db");
	auto pageNr = (args.length > 2 ? parse!(int)(args[2]) : 0);
//	writefln("opening file %s", filename); 
//	auto db = new Database(filename);
//	static if (is(typeof(db) : typeof(null))) {
//		auto db = db;
//	} else {
//		auto db = &db;
//	}
	if (&db !is null) {
	//	writeln("it appears to be a database");
	//	writeln(db.pages[page].header);
		writeln("pageSize : ",db.header.pageSize);

//		writeln(db.pages[page].getRows(db.pages));
//		handlePageF(db.pages[page], db.pages, &pageHandler);
		import std.datetime;
		const _page = db.pages[pageNr];
		StopWatch sw;

		foreach(_; 0 .. 2*4096) {
			string[] results;
		
			sw.start;
			foreach(rows;db.getRowsOf("Album").join) {
				import std.array : array;
				results = rows.map!(row => row.colums(1).getAs!string).array;
			}
			sw.stop();
		//	sw.reset();
		}

		writeln("Getting all entries of colum 1 in table Album 4096 times took ", sw.peek().msecs, "msecs");

	//	writeln(db.pages[1].getRows(db.pages));
		Database.MasterTableSchema[] schemas;
		//Database.Row[] rows;


//		auto rows = handlePage!(
//			(page, pages) => page.getRows(pages)
//		) (db, 0);
//		writeln(rows);

		void fn2() {
			//	uint cnt;
			//	Database.Row[][] rows; 
			//	handlePage!((a, b) =>  writeln(a.getRows(b)))(_page, db.pages);
			getRootPageOf1(db, "Artist");
			
		}

		void fn() {
			getRootPageOf1(db, "Artist");
//			uint cnt;
//			handlePageF(cast(Database.BTreePage)_page, db.pages, &countCellsHandler, &cnt);
//			return cnt;
		}



		foreach(i;0 .. 32) {	}
//	//	writefln("page has %d cells", cellCount);
//	
//foreach(i; 1.. db.pages.length ){writeln("page [",i,"]\n",db.pages[i]);}
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
		return 0;
	} else {
//		writeln("invalid database or header corrupted");
	}
	assert(0);
//	readln();
}
