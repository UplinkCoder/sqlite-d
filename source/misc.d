module misc;

import sqlited;
import utils;
import std.traits;

struct_type deserialize(struct_type)(Database.Row r) if (is(struct_type == struct)) {
	struct_type instance;
	uint ctr;
	foreach (member; __traits(derivedMembers, struct_type)) {
		alias type = typeof(__traits(getMember, instance, member));
		static if (!is(type == function)) {
			__traits(getMember, instance, member) = r.getAs!(type)(ctr++);
		}
	}
	return instance;
}

import std.typecons;
alias RootPage = Typedef!(uint,uint.init,"rootPage");
struct Table {
	const Database.PageRange pages;
	const RootPage rootPage;

	int opApply(scope int delegate(const Database.Row r) dg) {
		readRowDg!dg(this);
		return 0;
	}

	struct_type[] deserialize(struct_type)() if (is(struct_type == struct)) {
		struct_type[] result;
		foreach(row;this) {
			result ~= row.deserialize!struct_type;
		}
		return result;
	}


}

Table table(const Database db, in string tableName) pure {
	RootPage rootPage;

	readRows!(
		(r) {
		if (r.colum(0).getAs!string == "table" && 
			r.colum(1).getAs!string == tableName) {
			rootPage = r.colum(3).getAs!uint - 1;
		}
	})(db.pages[0], db.pages);
		
		
	return Table(db.pages, rootPage);
}


/// usage : table.select("name","surname").where!("age","sex", (age, sex) => sex.as!Sex == Sex.female, age.as!uint < 40))
/// or table.select("name").where!((type) => type.as!string == "table")("type").as!string;
/// or join().select()
/+
 auto where(S,T...)(S selectResult, T )

 auto where(S,T...)(S selectResult) {
 foreach(i,Element;T) {
 static if (i == 0) {
 static assert(is(Element == delegeate) || is(Element == function),
 "first template argument to where has to be a delegate or a function");
 static assert()
 }
 }
 }

 auto SQL(SQLElements...)(Database db) {
 //	static assert (allStatiesfy(isSQLElement!SQLElements))
 foreach(elem;SQLElements) {
 static if (isSelect!elem) {
 //assert that there is just one select
 } else static if (isWhere!elem) {
 
 }
 }
 }
 +/
/// handlePage is used to itterate over interiorPages transparently
/+
void* handlePageF(Database.BTreePage page,
	Database.PageRange pages,
	void* function(Database.BTreePage, Database.PageRange, void*) pageHandlerF,
	void* initialState = null) { 
	handleRow!(
		(page, pages) => initialState = pageHandlerF(page, pages, initialState)
		)(page, pages);

	return initialState;
}
+/
template pageHandlerTypeP(alias pageHandler) {
	alias pageHandlerTypeP = typeof((cast(const)Database.BTreePage.init));
}

template pageHandlerTypePP(alias pageHandler) {
	alias pageHandlerTypePP = typeof(pageHandler(cast(const)Database.BTreePage.init, cast(const)Database.PageRange.init));
}

template rowHandlerTypeR(alias rowHandler) {
	alias rowHandlerTypeR = typeof(rowHandler(cast(const)Database.BTreePage.Row.init));
}

template rowHandlerTypeRP(alias rowHandler) {
	alias rowHandlerTypeRP = typeof(rowHandler(cast(const)Database.BTreePage.Row.init, cast(const)Database.PageRange.init));
}

template rowHandlerReturnType(alias rowHandler) {
	alias typeR = rowHandlerTypeR!rowHandler;

	static if (is(typeR)) {
		alias rowHandlerReturnType = typeR;
	} else {
		static assert(0, "pageHandler has to be callable with (Row)" ~ typeof(rowHandler).stringof);
	}
}

template pageHandlerRetrunType(alias pageHandler) {
	alias typePP = pageHandlerTypePP!pageHandler;
	alias typeP = pageHandlerTypeP!pageHandler;

	static if (is(typePP)) {
		alias pageHandlerRetrunType = typePP;
	} else static if (is(typeP)) {
		alias pageHandlerRetrunType = typeP;
	} else {
		static assert(0, "pageHandler has to be callable with (BTreePage) or (BTreePage, PageRange)" ~ typeof(pageHandler).stringof);
	}
}
static assert (is(pageHandlerRetrunType!((page, pages) => page)));

auto readRows(alias RowHandler)(const Table table) {
	return readRows!(RowHandler)(table.pages[cast(uint)table.rootPage], table.pages);
}
int readRowDg(alias dg)(const Table table) {
	readRows!((r) {dg(r);})(table);
	return 0;
}

RR readRows(alias rowHandler, RR = rowHandlerReturnType!(rowHandler)[])(const Database.BTreePage page,
	const Database.PageRange pages) {
	RR returnArray;
	enum noReturn = is(RR == void[]);
	enum isPure = is(typeof((){void _() pure {rowHandler(cast(const)Database.Row.init);}}()));

	pragma(msg, isPure);
//	static assert(isPure);
	readRows!rowHandler(page, pages, returnArray);
	return returnArray;
}


/// handlePage is used to itterate over interiorPages transparently
void readRows(alias rowHandler,bool writable = false, RR)(const Database.BTreePage page,
	const Database.PageRange pages,  ref RR returnRange) {
	alias hrt = rowHandlerReturnType!(rowHandler);
	alias defaultReturnRangeType = hrt[];
	enum isPure = is(typeof((){void _() pure {rowHandler(cast(const)Database.Row.init);}}()));
	enum noReturn = is(hrt == void) || is(hrt == typeof(null));

	auto cpa = page.getCellPointerArray();

	switch (page.pageType) with (Database.BTreePage.BTreePageType) {
		
		case tableLeafPage: {
			foreach(cp;cpa) {
				static if (is(hrt)) {
					static if (noReturn) {
						rowHandler(page.getRow(cp, pages));
					} else {
						static if (is (RR == defaultReturnRangeType)) {
							returnRange ~= rowHandler(page.getRow(cp, pages));
						} else {
							returnRange.put(rowHandler(page.getRow(cp, pages)));
						}

					}
				} else {
					static assert(0, "rowHandler has to be callable with (Row)" ~ typeof(rowHandler).stringof);
				}
			}
		} break;
		

		case tableInteriorPage: {
			foreach(cp;cpa) {
				readRows!rowHandler(pages[BigEndian!uint(page.page[cp .. cp + uint.sizeof]) - 1], pages, returnRange);
			}

			readRows!rowHandler(pages[page.header._rightmostPointer - 1], pages, returnRange);

		} break;


		default:
			assert(0, "indexes are not supported by handle Row nor are empty pages");
	}

	return ;

}
