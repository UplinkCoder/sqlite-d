module sqlite.misc;

import sqlited;
import sqlite.utils;

struct_type[] deserialize(alias struct_type)(Row r) {
	struct_type[1] instance;
	foreach(member;__traits(allMembers, struct_type)) {
		alias type = typeof(__traits(getMember, member, instance[0]));
		static if (!is(type == function)) {
			__traits(getMember, member, instance[0]) = r.getAs!(type)(member);
		}
	}
	return instance;
}

struct_type[] deserialize(alias struct_type)(Row[] ra) {
	struct_type[] result;
	foreach(r;ra) {
		result ~= deserialize(r);
	}
	return result;
}

/// handlePage is used to itterate over interiorPages transparently
void handlePage(Database.BTreePage page, Database.PageRange pages, void* function(Database.BTreePage, Database.PageRange) pageHandler) {
	switch(page.pageType) with (Database.BTreePage.BTreePageType) {

		case tableLeafPage : {
			pageHandler(page, pages);
			break;
		}

		case tableInteriorPage : {
			uint[] pageNumbers;
			auto cpa = page.getCellPointerArray;
			pageNumbers.reserve(cpa.length+1);
			foreach(cp;cpa) {
				BigEndian!uint leftChildPage = *(cast(uint*)(cp + page.base));
				pageNumbers ~= leftChildPage;
			}
			pageNumbers ~= page.header._rightmostPointer;
			
			foreach(pageIndex;pageNumbers) {
				auto _page = pages[pageIndex-1];
				handlePage(_page, pages, pageHandler); 
			}
			break;
		}

		default :
			import std.conv; 
			assert(0, "pageType not supported" ~ to!string(page.pageType));
	}
}

