module test;
import sqlited;
import misc;

static immutable long_create_table = 
q{CREATE TABLE `veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongtttttttttaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbllllllllllleeeeeeeeeeeeeeeeennnnnnaaaaaaaaaaameeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee` (
	`vvvvvvvvvveeeeeeeerrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrryyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyylllllllloooooooooooooooooooooooooooonnnnnnnnnnnnnnnngggggggggggggggggggvvvvvvvvvvvvvvvvffffffffffffffffffffiiiiiiiiiiieeeeeeeeeeeeeeeeellllllllllllllddddddddddddddddddddddnnnnnnnnnnnaaaaaaaaaaaaammmmmmmmmmmmmmmmeeeeeeeeeeeeeeeeee`	INTEGER,
	`Field2`	INTEGER
)};
static immutable test_s3db = cast(immutable)Database(cast(immutable ubyte[]) import("test.s3db"));
static immutable Database.MasterTableSchema[] schemas = handleRow!(r => r.deserialize!(Database.MasterTableSchema))(test_s3db.rootPage, test_s3db.pages);
static assert(schemas[2].sql==long_create_table);

import frail_sql_parser;

static assert (parseCreateTable(
q{CREATE TABLE spatial_ref_sys (
		srid INTEGER NOT NULL PRIMARY KEY,
		auth_name VARCHAR(256) NOT NULL,
		auth_srid INTEGER NOT NULL,
		ref_sys_name VARCHAR(256),
		proj4text VARCHAR(2048) NOT NULL)}
	) == TableInfo("spatial_ref_sys", [
			ColumInfo("srid", "INTEGER", true, true),
			ColumInfo("auth_name", "VARCHAR(256)", true),
			ColumInfo("auth_srid", "INTEGER", true),
			ColumInfo("ref_sys_name","VARCHAR(256)",false),
			ColumInfo("proj4text","VARCHAR(2048)",true),
		])
);
static assert(parseCreateTable(long_create_table) != TableInfo.init);

static assert(parseCreateTable(
q{CREATE TABLE Towns (
				PK_UID INTEGER PRIMARY KEY AUTOINCREMENT,
				Name TEXT,
				Peoples INTEGER,
				LocalCounc INTEGER,
				County INTEGER,
				Region INTEGER, "Geometry" POINT)
}
) != TableInfo.init);
