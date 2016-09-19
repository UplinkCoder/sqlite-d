//import std.stdio;
import sqlited;
import sqlited : isIndex;
import utils;
import misc;
import std.conv;
import std.stdio;
import core.memory;

import std.algorithm : map, filter, count, reduce, each;
import std.range : join, takeOne;
//import std.typecons : tuple;
static immutable db4 = cast(immutable) Database(cast(ubyte[]) import("test4.s3db"), "");

int main(string[] args) {
	string filename = (args.length > 1 ? args[1] : "views/test-2.3.sqlite");
	auto pageNr = (args.length > 2 ? parse!(int)(args[2]) : 0);
	writefln("opening file %s", filename); 
	auto db = new Database(filename);

	Database.MasterTableSchema[] schemas;

	auto test_db = Database("views/test-2.3.sqlite");
	schemas = readRows!(r => r.deserialize!(Database.MasterTableSchema))(db.rootPage, db.pages);
	if (pageNr) {
		uint x;
		foreach(row;Table(db.pages, cast(RootPage)pageNr)) {
//			row.column(0).apply!writeln;
//			foreach(col;row.columns) {
//				col!(pl => pl.writeln);
//			}
			x++;
		}
		x.writeln();
	} else {
		foreach(schema;schemas.filter!(s => s.type == "table")) {
			writeln(schema.name, ":", schema.rootPage);
		}

	}
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
	towns = readRows!(r => r.deserialize!Town)(test_db.table("Towns"));

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
				row.column(1).getAs!string;
			}
			sw.stop();
		}

	//	foreach(_;test_db.table("Regions")) {}

		writeln("Getting all ", x, " entries of column 1 in table Album ", times, " times took ", sw.peek().msecs, "msecs");
//	writeln(db4.table("Artist"));
		return 0;
//	} else {
//		writeln("invalid database or header corrupted");
//	}
	assert(0);

//	readln();
}
