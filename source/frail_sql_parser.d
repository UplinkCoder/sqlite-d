// "CREATE TABLE `table_name` (

/*CREATE TABLE spatial_ref_sys (
srid INTEGER NOT NULL PRIMARY KEY,
auth_name VARCHAR(256) NOT NULL,
auth_srid INTEGER NOT NULL,
ref_sys_name VARCHAR(256),
proj4text VARCHAR(2048) NOT NULL),
*/

alias lengthType = uint;

auto getDelim(char c) {
	switch(c) {
		case '[' :
			return ']';
		case '`' : 
			return '`';
		default :
			// if we don't have a recognized delimiter... delimit by Space 
			// THIS IS A HACK!!!
			return ' ';
	}
}

import std.algorithm;
auto skipWhiteSpace(string _string) {
	static struct Result {
		string result;
		uint length;
	}

	Result result;

	import std.ascii : isWhite;
	while(isWhite(_string[result.length++])) {}

	result.length--;
	result.result = _string[result.length .. $];

	return result;
}


auto parseCreateTable(string sql) pure {
	ColumInfo[] colums;
	sql = sql["CREATE TABLE ".length .. $];
	auto delim = getDelim(sql[0]);
	size_t pos = sql[1 .. $].countUntil(delim);
/*	foreach (i,c;sql) {
		if (c == delim) {
			pos = i;
			break;
		}
	}
*/

	string tableName = sql[1 .. pos];
	sql = sql[pos .. $];

	pos = sql.countUntil('(');
	while(sql[pos] != ')') {
		auto res = parseColum(sql[pos .. $]);
		pos += res.length;
		colums ~= res.colum;
	} 

	
}

struct ColumInfo {
	string name;
	string typeName;
	bool notNull;
	bool primaryKey;		
}
	

auto parseColum(string sql) pure {
	struct Result {
		ColumInfo colum;
		uint length;
	}
	Result result;
	
	auto res = sql.skipWhiteSpace();
	sql = res.result;
	result.length = res.length;
	
	auto delim = getDelim(sql[0]);
	size_t pos = sql.countUntil(delim);
	bool whitespaceDelim = (delim == ' ') ;

	result.colum.name = (whitespaceDelim ? sql[0 .. pos] : sql[1 .. pos]);
	sql = sql[pos .. $];
	result.length += pos + !whitespaceDelim;

	res = sql.skipWhiteSpace();
	sql = res.result;
	result.length += res.length;
	
	pos = sql.countUntil(' ');
	result.colum.typeName = 	sql[0 .. pos];

	result.length += pos;

	return result;

}

