import sqlited;
import sqlite.utils;
import std.stdio;
import std.algorithm;

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
/*
auto handlePage(Database.PageRange pages, Database.BTreePage page,void* function(Database.BTreePage) pageHandler = ((page){writeln(page);return null;})) {

	if (page.pageType == Database.BTreePage.BTreePageHeader.BTreePageType.leafTablePage) {
			pageHandler(page);
		}

		if (page.pageType == Database.BTreePage.BTreePageHeader.BTreePageType.interiorTablePage) {
			uint[] pageNumbers;
			auto cpa = page.getCellPointerArray;
			pageNumbers.reserve(cpa.length);
			 
			foreach(cp;cpa.map!(cp => cp + page.base)) {
				BigEndian!uint leftChildPage = *(cast(uint*)cp);
				pageNumbers ~= leftChildPage;
			}
			pageNumbers ~= page.header._rightmostPointer;

			foreach(pageIndex;pageNumbers) {
				auto _page = pages[pageIndex-1];
				writeln("page [",pageIndex, "] :\n", page);

				if(_page.hasPayload) {
					foreach(cp;page.getCellPointerArray) {
						auto ps = page.payloadSize(cp);
						if (ps > page.usablePageSize) {
							writeln("found payload bigger then the usable pageSize ", page.usablePageSize, " < ", ps);
						}
					}
				}
			}
		}
}
*/
