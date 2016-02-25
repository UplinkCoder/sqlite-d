module misc;

import sqlited;
import utils;

struct_type[] deserialize(alias struct_type)(Row r) {
	struct_type instance;
	uint ctr;
	foreach (member; __traits(derivedMembers, struct_type)) {
		alias type = typeof(__traits(getMember, member, instance));
		static if (!is(type == function)) {
			__traits(getMember, member, instance) = r.getAs!(type)(ctr++);
		}
	}
	return [instance];
}

struct_type[] deserialize(alias struct_type)(Row[] ra) {
	struct_type[] result;
	foreach (r; ra) {
		result ~= deserialize(r);
	}
	return result;
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
		import std.conv;
		static assert(0, "pageHandler has to be callable with (BTreePage) or (BTreePage, PageRange)" ~ typeof(pageHandler).stringof);
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
		import std.conv;
		static assert(0, "pageHandler has to be callable with (BTreePage) or (BTreePage, PageRange)" ~ typeof(pageHandler).stringof);
	}
}
static assert (is(pageHandlerRetrunType!((page, pages) => page)));
/// this is often faster. because the PageRange is cached

auto handleRow(alias rowHandler, RR = rowHandlerReturnType!(rowHandler)[])(const Database.BTreePage page,
	const Database.PageRange pages) {
	RR returnArray;
	return handleRow!rowHandler(page, pages, returnArray);
}


/// handlePage is used to itterate over interiorPages transparently
RR handleRow(alias rowHandler, RR)(const Database.BTreePage page,
	const Database.PageRange pages,  ref RR returnRange) {
	alias hrt = rowHandlerReturnType!(rowHandler);
	alias defaultReturnRangeType = hrt[];

	enum nullReturnHandler = is(hrt == void) || is(hrt == typeof(null));
	pragma(msg, nullReturnHandler);
	if (returnRange is RR.init && RR.init == null && !nullReturnHandler) {

	}
	auto cpa = page.getCellPointerArray();

	switch (page.pageType) with (Database.BTreePage.BTreePageType) {

		
		case tableLeafPage: {
		//	if (!__ctfe) returnRange.reserve(cpa.length);

			foreach(cp;cpa) {
				static if (is(hrt)) {
					static if (nullReturnHandler) {
						rowHandler(page.getRow(cp, pages));
						break;
					} else {
						static if (is (RR == defaultReturnRangeType)) {
							returnRange ~= [rowHandler(page.getRow(cp, pages))];
						} else {
							returnRange.put(rowHandler(page.getRow(cp, pages)));
							break;
						}

					}
				} else {
					import std.conv;
					static assert(0, "pageHandler has to be callable with (BTreePage) or (BTreePage, pagesRange)" ~ typeof(rowHandler).stringof);
				}
			}
		}
			break;
		

		case tableInteriorPage: {

			foreach(cp;cpa) {
				static if (nullReturnHandler) {
					handleRow!pageHandler(pages[BigEndian!uint(page.page[cp .. cp + uint.sizeof]) - 1], pages);
				} else {
					handleRow!rowHandler(pages[BigEndian!uint(page.page[cp .. cp + uint.sizeof]) - 1], pages, returnRange);
				}
			}

			static if (nullReturnHandler) {
				handleRow!pageHandler(pages[page.header._rightmostPointer - 1], pages);
			} else {
				handleRow!rowHandler(pages[page.header._rightmostPointer - 1], pages, returnRange);
			}

			break;
		}

		default:
			import std.conv;

			assert(0, "pageType not supported" ~ to!string(page.pageType));
	}

	return returnRange;

}
