//import std.stdio;
import sqlited;
import utils;
import misc;
import std.conv;
import std.stdio;
import core.memory;

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

static immutable db4 = cast(immutable) Database(cast(ubyte[]) import("test4.s3db"), "");


auto getTableNames(const Database db) {
	import std.array : array;
	return handleRow!((r) {
			if (r.colums(0).getAs!string == "table") {
				return r.colums(1).getAs!string;
			} else {
				return "";
			}
		}
	)(db.rootPage, db.pages);
}


auto getRowsOf0(const Database db, const string tableName) {
	return handleRow!(
		(r) {
			if (r.colums(0).getAs!string == "table" && 
				r.colums(1).getAs!string == tableName) {
				auto n = r.colums(3).getAs!uint - 1;
				return handleRow!((_r) {
					return _r;
				}) (db.pages[n], db.pages);
			} else {
				return null;
			}

		})(db.rootPage, db.pages);
}

auto getRowsOf1(const Database db, const string tableName) {
	auto n = getRootPageOf1(db, tableName);
	return (n ? handleRow!(r => r)(db.pages[n], db.pages) : null);
}


auto getRootPageOf1(const Database db, const string tableName) {
	uint rootPage;	
	handleRow!(
		(r) {
			if (r.colums(0).getAs!string == "table" && 
				r.colums(1).getAs!string == tableName) {
				rootPage = r.colums(3).getAs!uint - 1;
			}
	})(db.pages[0], db.pages);


	return rootPage;
}

auto getRootPageOf2(const Database db, const string tableName) {
	uint rootPage;	
	handleRow!(
		(r) {
			auto cols = r.colums(1,3);
			if (cols[0].getAs!string == tableName) {
				assert(rootPage == 0,"tableName duplicated");
				rootPage = cols[1].getAs!uint - 1;
			}
		})(db.pages[0], db.pages);

	return rootPage;
}


int main(string[] args) {
//	import std.stdio;
//	string filename = (args.length > 1 ? args[1] : "example/test4.s3db");
//	auto pageNr = (args.length > 2 ? parse!(int)(args[2]) : 0);
//	writefln("opening file %s", filename); 
//	auto db = new Database(filename);

	Database.MasterTableSchema[] schemas;

	auto test_db = Database("views/test-2.3.sqlite");
	schemas = handleRow!(r => r.deserialize!(Database.MasterTableSchema))(test_db.rootPage, test_db.pages);
	foreach(schema;schemas) {
		writeln(schema);
	}
	writeln("rootPage of Towns:", test_db.getRootPageOf2("Towns"));
	struct Town {
		uint PK_UID;
		string name;
		uint peoples;
		uint localc;
		uint country;
		uint region;
		/* Geomerty point */
	}
	Town[] towns;
	towns = handleRow!(r => r.deserialize!Town)(test_db.table("Towns"));

	foreach(town;towns) {
		//writeln(town);
	}


//	if (db !is null) {
	//	writeln("it appears to be a database");
		import std.datetime;
		StopWatch sw;
		enum times = 2*4096;
		foreach(_; 0 .. times) {
			string result;
		
			sw.start;
			auto x = result.length;
			foreach(row;(db4).table("Album")) {
			//	writeln(row.colums(1).getAs!string);
			} 
			sw.stop();
		}

		writeln("Getting all entries of colum 1 in table Album ", times, " times took ", sw.peek().msecs, "msecs");
//	writeln(db4.table("Artist"));
//	foreach(_;0..32) {
//		writeln(comparingBenchmark!((){test_db.getRootPageOf1("Towns");},(){test_db.getRootPageOf2("Towns");},4096).point);
//	}
		return 0;
//	} else {
//		writeln("invalid database or header corrupted");
//	}
	assert(0);
//	readln();
}
