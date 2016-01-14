module sqlite.misc;

import sqlited;
import sqlite.utils;


	struct_type[] deserialize(alias struct_type)(Row r) {
		struct_type[1] instance;
		foreach(member;__traits(allMembers, struct_type)) {
			alias type = typeof(__traits(getMember, member, instance[0]));
			__traits(getMember, member, instance[0]) = r.getAs!(type)(member);
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
void handlePage(Database.BTreePage page, Database.PageRange pages, void* function(Database.BTreePage, Database.PageRange) pageHandler = ((page,pages){import std.stdio; writeln(page.toString(pages));return null;})) {
		if (page.pageType == Database.BTreePage.BTreePageHeader.BTreePageType.tableLeafPage) {
			pageHandler(page, pages);
		} else

		if (page.pageType == Database.BTreePage.BTreePageHeader.BTreePageType.tableInteriorPage) {
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
		} else 

		assert(0, "pageType not supported");
}

