// "CREATE TABLE `table_name` (
alias lengthType = uint;

auto getDelim(char c) {
	switch(c) {
		case '[' :
			return ']';
		case '`' : 
			return '`';
		default :
			return '\0';
	}
}

import std.algorithm;
auto skipWhiteSpace(const ref string _string) {
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
/+

auto parseCreateTable(string sql) pure {
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

auto parseColum(string sql) pure {
	Colum col;

	auto res = sql.skipWhitespace();
	sql = res.result;
	auto length = res.length;
	
	auto delim = getDelim(sql[0]);
	size_t pos = sql.countUntil(delim);


	col.name = sql[1 .. pos];
	sql = sql[pos .. $];
	length += pos + 1;

	res = sql.skipWhiteSpace();
	sql = res.result;
	length += res.length;
	
	pos = sql.countUntil(' ');
	col.typeName = 	sql[0 .. pos];

	length += pos;

	static struct Result {
		lengthType length;
		
	}
	
}
+/
