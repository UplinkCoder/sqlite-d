module layout2dot;
import sqlited;

string TreeLayoutToDot(Database db) {
	string result = "digraph table {";

	Database.MasterTableSchema[] schemas;
	schemas = readRows!(r => r.deserialize!(Database.MasterTableSchema))(db.rootPage,
		db.pages);
	result ~= TreeLayoutToDot(db.rootPage, db.pages, 1);
	foreach (schema; schemas) {
		if (schema.rootPage) {
			result ~= "\n" ~ "\"Root\"" ~ " -> " ~ '"' ~ toQuotedString(schema.rootPage) ~ '"' ~ "\n";
			result ~= TreeLayoutToDot(db.pages[schema.rootPage - 1], db.pages, schema.rootPage) ~ "\n";
		}
	}

	return result ~ "}\n";
}

string toQuotedString(uint i) {
	return "\"" ~ to!string(i) ~ "\"";
}

string TreeLayoutToDot(Database.BTreePage page, Database.PageRange pages, uint rootPage) {
	string pref = "\n" ~ toQuotedString(rootPage) ~ " -> ";
	string result;
	auto cpa = page.getCellPointerArray();

	switch (page.pageType) {
	case Database.BTreePageType.tableInteriorPage: {
			foreach (cp; cpa) {
				uint nextPage = BigEndian!uint(page.page[cp .. cp + uint.sizeof]);
				result ~= pref ~ toQuotedString(nextPage);
				result ~= TreeLayoutToDot(pages[nextPage - 1], pages, nextPage);
			}

			uint nextPage = page.header._rightmostPointer;
			result ~= pref ~ toQuotedString(nextPage);
			result ~= TreeLayoutToDot(pages[nextPage - 1], pages, nextPage);
		}
		break;
	case Database.BTreePageType.tableLeafPage: {

		}
		break;
	default: {
		}

	}
	return result;
}
