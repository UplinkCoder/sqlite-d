//sqlstuff

struct TableInfo {
	ColumInfo[] columInfos;
	string tableName;

	this(string tableName, ColumInfo[] columInfos) pure {
		this.tableName = tableName;
		this.columInfos = columInfos;
	}
}

struct ColumInfo {
	string name;
	string typeName;
	bool primaryKey;
	bool notNull;
	bool unique;
	bool autoincrement;
}
