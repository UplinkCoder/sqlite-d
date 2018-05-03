//import std.stdio;
import sqlited;
import sqlited : isIndex;
import utils;
import misc;
import std.conv;
import std.stdio;
import core.memory;
import etc.c.sqlite3;

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
	size_t sqlite3_hash;
	{
		sqlite3* dbp;
		sqlite3_open("views/test-2.3.sqlite", &dbp);
		scope(exit) sqlite3_close(dbp);
		sqlite3_stmt* stmt;
		const char* tail;
		sqlite3_prepare_v2(
			dbp,            /* Database handle */
			"SELECT * FROM Album",       /* SQL statement, UTF-8 encoded */
			"SELECT * FROM Album".sizeof, /* Maximum length of zSql in bytes. */
			&stmt,  /* OUT: Statement handle */
			&tail     /* OUT: Pointer to unused portion of zSql */
		);
		bool shouldRun = true;
		while(shouldRun)
		{
			auto rv = sqlite3_step(stmt);
			shouldRun = (rv != SQLITE_DONE || rv != SQLITE_ERROR);
			printf("rv = %d\n", rv);
			//printf("StillRuning");
			auto value = sqlite3_column_text(stmt, 0)[0 .. sqlite3_column_bytes(stmt, 0)];
			if (value.length) printf(&value[0]);
			foreach(c;value)
				 sqlite3_hash += c;
		}
	}
	writeln("sqlite3_hash: ", sqlite3_hash);

	auto test_db = Database("views/test-2.3.sqlite");
	schemas = readRows!(r => r.deserialize!(Database.MasterTableSchema))(db.rootPage, db.pages);
	if (pageNr) {
		uint x;
		foreach(row;Table(db.pages, cast(uint)pageNr)) {
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
		size_t hash;
		foreach(_; 0 .. times) {
			string result;
			x = 0;
			sw.start;
			foreach(row;(db4).table("Album")) {
				x++;
				auto album_title = row.column(1).getAs!string;
				foreach(c;album_title)
                                    hash += c;
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
