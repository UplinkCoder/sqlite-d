module sqlite.misc;

import sqlited;
import sqlite.utils;

struct_type[] deserialize(alias struct_type)(Row r) {
	struct_type[1] instance;
	foreach (member,i; __traits(allMembers, struct_type)) {
		alias type = typeof(__traits(getMember, member, instance[0]));
		static if (!is(type == function)) {
			__traits(getMember, member, instance[0]) = r.getAs!(type)(i);
		}
	}
	return instance;
}

struct_type[] deserialize(alias struct_type)(Row[] ra) {
	struct_type[] result;
	foreach (r; ra) {
		result ~= deserialize(r);
	}
	return result;
}

/// handlePage is used to itterate over interiorPages transparently
/// NOTE handlePageF is slower then handle page. 
void* handlePageF(Database.BTreePage page,
		Database.PageRange pages,
		void* function(Database.BTreePage, Database.PageRange, void*) pageHandlerF,
		void* initialState = null) { 
		handlePage!(
			(page, pages) => initialState = pageHandlerF(page, pages, initialState)
		)(page, pages);

		return initialState;
}

template pageHandlerTypeP(alias pageHandler) {
	alias pageHandlerTypeP = typeof((cast(const)Database.BTreePage.init));
}

template pageHandlerTypePP(alias pageHandler) {
	alias pageHandlerTypePP = typeof(pageHandler(cast(const)Database.BTreePage.init, cast(const)Database.PageRange.init));
}

template handlerRetrunType(alias pageHandler) {
	alias typePP = pageHandlerTypePP!pageHandler;
	alias typeP = pageHandlerTypeP!pageHandler;

	static if (is(typePP)) {
		alias handlerRetrunType = typePP;
	} else static if (is(typeP)) {
		alias handlerRetrunType = typeP;
	} else {
		import std.conv;
		static assert(0, "pageHandler has to be callable with (BTreePage) or (BTreePage, PageRange)" ~ typeof(pageHandler).stringof);
	}
}
static assert (is(handlerRetrunType!((page, pages) => page)));
/// this is often faster. because the PageRange is cached
auto handlePage(alias pageHandler)(const Database db, const uint pageNumber) {
	auto pageRange = db.pages();
	return handlePage!pageHandler(pageRange[pageNumber], pageRange);
}

/// handlePage is used to itterate over interiorPages transparently
RR handlePage(alias pageHandler, RR = handlerRetrunType!(pageHandler)[])(const Database.BTreePage page,
		const Database.PageRange pages,  RR returnRange = RR.init) {
	alias hrt = handlerRetrunType!(pageHandler);
	enum nullReturnHandler = is(hrt == void) || is(hrt == typeof(null));
	pragma(msg, nullReturnHandler);
	if (returnRange is RR.init && RR.init == null && !nullReturnHandler) {

	}

	switch (page.pageType) with (Database.BTreePage.BTreePageType) {

	case tableLeafPage: {
			static if (is(typeof(pageHandler(page, pages)))) {
				static if (nullReturnHandler) {
					pageHandler(page, pages);
					break;
				} else {
					return [pageHandler(page, pages)];
				}
			} else static if (is(typeof(pageHandler(page)))) {
				static if (nullReturnHandler) {
					pageHandler(page);
					break;
				} else {
					return [pageHandler(page)];
				}
			} else {
				import std.conv;
				static assert(0, "pageHandler has to be callable with (BTreePage) or (BTreePage, pagesRange)" ~ typeof(pageHandler).stringof);
			}
		}

	case tableInteriorPage: {
			uint[] pageNumbers;
			auto cpa = page.getCellPointerArray();
		//	pageNumbers.reserve(cpa.length + 1);
			foreach (cp; cpa) {
				BigEndian!uint leftChildPage;
				leftChildPage = (page.page[cp .. cp + uint.sizeof]);
				pageNumbers ~= leftChildPage;
			}
			pageNumbers ~= page.header._rightmostPointer;
			foreach (pageIndex; pageNumbers) {
				auto _page = pages[pageIndex - 1];
				static if (nullReturnHandler) {
					handlePage!pageHandler(_page, pages);
				} else {
					returnRange ~= handlePage!pageHandler(_page, pages);
				}


			}
			break;
		}

	default:
		import std.conv;

		assert(0, "pageType not supported" ~ to!string(page.pageType));
	}

	return returnRange;

}