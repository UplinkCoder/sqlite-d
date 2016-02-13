import std.stdio;
import sqlited;
import sqlite.utils;
import sqlite.misc;
import std.conv;

void* pageHandler(Database.BTreePage page, Database.PageRange pages) {
	string toPrint = page.header.to!string ~ "\n" ~ page.toString(pages); 
	writeln(toPrint);
	return null;
}


void main(string[] args) {
	string filename = (args.length > 1 ? args[1] : "example/test.s3db");
	auto page = (args.length > 2 ? parse!(int)(args[2]) : 0);
	writefln("opening file %s", filename); 
	auto db = new Database(filename);
	if (db !is null) {
		writeln("it appears to be a database");
		writeln(db.pages[page].header);
		writeln("pageSize : ",db.header.pageSize);

		handlePage(db.pages[page], db.pages, &pageHandler);
	
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
	} else {
		writeln("invalid database or header corrupted");
	}
	
	readln();
}
