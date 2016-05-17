//import std.stdio;
import sqlited;
import utils;
import misc;
import std.conv;
import std.stdio;
import core.memory;

import std.algorithm : map, filter, count, reduce, each;
import std.range : join, takeOne;
//import std.typecons : tuple;
static immutable db4 = cast(immutable) Database(cast(ubyte[]) import("test4.s3db"), "");


void test() {
	struct test_schema {
		uint result;
		string expr;
	}
	
	static immutable tests_db = cast(immutable) Database(cast(ubyte[]) import("hello.db"), "hello");
	static immutable tests = tests_db.table("hello").deserialize!test_schema;
	
	static assert(mixin(tests[0].expr) == tests[0].result);
	static assert(mixin(tests[1].expr) == tests[1].result);
}


int main(string[] args) {
	import std.stdio;
	string filename = (args.length > 1 ? args[1] : "views/test4.s3db");
	auto pageNr = (args.length > 2 ? parse!(int)(args[2]) : 0);
	writefln("opening file %s", filename); 
	auto db = new Database(filename);

	Database.MasterTableSchema[] schemas;

	auto test_db = Database("views/test-2.3.sqlite");
	schemas = handleRow!(r => r.deserialize!(Database.MasterTableSchema))(db.rootPage, db.pages);
//	if (pageNr) {
//		foreach(row;Table(db.pages, cast(RootPage)pageNr)) {
//			foreach(col;row.colums) {
//				col.apply!(c => writeln(c));
//			}
//		}
//	} else {
		foreach(schema;schemas) {
			writeln(schema);
		}
//	}
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
	//	writeln(town);
	}


//	if (db !is null) {
	//	writeln("it appears to be a database");
		import std.datetime;
		StopWatch sw;
	uint x;
		enum times = 2*4096;
		foreach(_; 0 .. times) {
			string result;
		x = 0;
			sw.start;
			foreach(row;(db4).table("Album")) {
			x++;
				row.colum(1).getAs!string;
			} 
			sw.stop();
		}

		writeln("Getting all ", x, " entries of colum 1 in table Album ", times, " times took ", sw.peek().msecs, "msecs");
//	writeln(db4.table("Artist"));
		return 0;
//	} else {
//		writeln("invalid database or header corrupted");
//	}
	assert(0);
//	readln();
}
