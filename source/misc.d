module sqlite.misc;

import sqlited;
import sqlite.utils;

struct_type[] deserialize(alias struct_type)(Row r) {
	struct_type[1] instance;
	foreach (member; __traits(allMembers, struct_type)) {
		alias type = typeof(__traits(getMember, member, instance[0]));
		static if (!is(type == function)) {
			__traits(getMember, member, instance[0]) = r.getAs!(type)(member);
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

void handlePage(alias pageHandler)(const Database db, const uint pageNumber) {
	auto pageRange = db.pages();
	return handlePage!pageHandler(pageRange[pageNumber], pageRange);
}

/// handlePage is used to itterate over interiorPages transparently
void handlePage(alias pageHandler)(const Database.BTreePage page,
		const Database.PageRange pages) {
//	typeof(pageHandler(page, pages))[] rv;

	switch (page.pageType) with (Database.BTreePage.BTreePageType) {

	case tableLeafPage: {
			static if (is(typeof(pageHandler(page, pages)))) {
				pageHandler(page, pages);
			} else static if (is(typeof(pageHandler(page)))) {
				pageHandler(page);
			} else {
				import std.conv;
				static assert(0, "pageHandler has to be callable with (BTreePage) or (BTreePage, pagesRange)" ~ typeof(pageHandler).stringof);
			}
			break;
		}

	case tableInteriorPage: {
			uint[] pageNumbers;
			auto cpa = page.getCellPointerArray;

		//	pageNumbers.reserve(cpa.length + 1);
			foreach (cp; cpa) {
				BigEndian!uint leftChildPage;
				debug {
					import std.stdio;
					writeln(page.page.length, " ", cp);
				}
				leftChildPage = (page.page[cp .. cp + 4]);
				pageNumbers ~= leftChildPage;
			}
		//	pageNumbers ~= page.header._rightmostPointer;

			foreach (pageIndex; pageNumbers) {
				auto _page = pages[pageIndex - 1];
				handlePage!pageHandler(_page, pages);
			}
			break;
		}

	default:
		import std.conv;

		assert(0, "pageType not supported" ~ to!string(page.pageType));
	}

}