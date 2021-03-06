// "CREATE TABLE `table_name` (

/*CREATE TABLE spatial_ref_sys (
srid INTEGER NOT NULL PRIMARY KEY,
auth_name VARCHAR(256) NOT NULL,
auth_srid INTEGER NOT NULL,
ref_sys_name VARCHAR(256),
proj4text VARCHAR(2048) NOT NULL),
*/
import std.algorithm;
alias lengthType = uint;
pure :
uint countUntil(SR, T...)(SR s, T matches)
{
	uint count = 0;
	foreach (c;s)
	{
		foreach(m;matches)
		{
			if (m == c) 
				return count;
		}
		++count;
	}
	return count;
}

auto getDelim(char c) {
	switch(c) {
		case '"' :
			return '"';
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
import sqlstuff;
void skipWhiteSpace(string _string, uint* pos) {

	import std.ascii : isWhite;
	uint _pos = *pos;
	while(isWhite(_string[_pos++]) && _pos < _string.length) {}
	*pos = _pos - 1;
}


auto parseCreateTable(const string sql) pure {
	ColumInfo[] columInfos;
	uint pos = cast(uint) "CREATE TABLE".length;
	sql.skipWhiteSpace(&pos);

	auto delim = getDelim(sql[pos]);
	pos += (delim == ' ' ? 0 : 1);
	auto strlen = cast(uint)sql[pos .. $].countUntil(delim);

	string tableName = sql[pos .. pos + strlen];
	pos += strlen + (delim == ' ' ? 0 : 1);
	sql.skipWhiteSpace(&pos);
	debug {
		import std.stdio;
	//	writeln("tableName :",tableName);
	//	writeln(sql[pos .. $]);
	}
	assert(sql[pos] == '(');
	pos++;
	while(sql[pos] != ')') {
		auto res = parseColum(sql[pos .. $]);
		columInfos ~= res.colum;
		pos += res.length;
		pos += (sql[pos] == ',' ? 1 : 0);
	}

	return TableInfo(tableName, columInfos);
}

	

auto parseColum(const string sql) pure {
	enum KeywordEnum {
		_,
		notNull,
		primaryKey,
		autoincrement,
		// unique,
	}
	struct Result {
		ColumInfo colum;
		uint length;
	}
	Result result;
	
	sql.skipWhiteSpace(&result.length);
	auto delim = getDelim(sql[result.length]);
	result.length += (delim == ' ' ? 0 : 1);
	int strlen = cast(int) sql[result.length .. $].countUntil(delim);

	result.colum.name = sql[result.length .. result.length + strlen];
	result.length += strlen + (delim == ' ' ? 0 : 1);
	sql.skipWhiteSpace(&result.length);

	delim = getDelim(sql[result.length]);
	strlen = cast(int) sql[result.length .. $].countUntil(delim,',','\n');

	//TODO make the ')' work as final delimiter;
	// because this is still a nasty hack!
	// a nasty nasty HACK!!!
	//so if I would eat the the final ')' truncate the strlen before the final ')'
	import std.range : retro;
	auto lastParen = cast(int)sql.retro.countUntil(')');
	if (sql.length - result.length - strlen <= lastParen) {
		strlen -= 1;
	}
	// the code above is a nasty nasty nasty HACK!!!

	result.colum.typeName =  sql[result.length .. result.length + strlen];
	result.length += strlen + (delim == ' ' ? 0 : 1);
	sql.skipWhiteSpace(&result.length);


	debug {
		import std.stdio;
		if (!__ctfe) {
				writeln("Before while:" ,result.colum , sql[result.length .. $]);
		}
	}
	while (sql[result.length] != ',' && sql[result.length] != ')') {
		import std.ascii;
		if (auto kw = cast(KeywordEnum)sql[result.length .. $].map!(c => toUpper(c)).startsWith("NOT NULL","PRIMARY KEY","AUTOINCREMENT")) {
			final switch (kw) with(KeywordEnum) {
				case _ : assert(0); // cannot ever happen
				case notNull :
					assert(result.colum.notNull == false);
					result.colum.notNull = true;
					result.length += "NOT NULL".length;
					break;
				case primaryKey :
					assert(result.colum.primaryKey == false);
					result.colum.primaryKey = true;
					result.length += "PRIMARY KEY".length;
					break;
				case autoincrement :
					assert(result.colum.autoincrement == false);
					result.colum.autoincrement = true;
					result.length += "AUTOINCREMENT".length;
					break;
			}
			sql.skipWhiteSpace(&result.length);
			debug {
				import std.stdio;
				if (!__ctfe) {
				//	writeln(result);
				}
			}
			continue ;
		} else {
			debug {
				import std.stdio;
				if (!__ctfe) {
					writeln(result);
					writeln(sql[result.length .. $]);
				}
			}
			assert(0, "Unhandeled Keyword or something");
		}
	

	}


	return result;

}
