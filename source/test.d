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
	) != TableInfo.init);
static assert(parseCreateTable(long_create_table) != TableInfo.init);