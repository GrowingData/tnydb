/* 	Copyright (c) 2012 Terence Siganakis.

 This file is part of tnydb.

 TnyDB is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 TnyDB is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with TnyDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* Contact: Terence Siganakis <terence@siganakis.com> */




#ifndef TNY_PAGE_H_
#define TNY_PAGE_H_

#include "tny.h"
#include "tny_list.h"


typedef struct tny_page_s{
	u64** data;
	int length;
	int depth;
} tny_page;




//////////////////////////////////////////
// Core methods
//////////////////////////////////////////

extern u64 *tny_page_seek(tny_page *page, int value);
extern u64 *tny_page_seek_or(tny_page *page, int value, u64 * restrict result);
extern u64 *tny_page_seek_and(tny_page *page, int value, u64 * restrict result);


extern int tny_page_depth(tny_page *page);
extern u64 * tny_page_depth_data(tny_page *page, int depth);
extern void tny_page_set_data(tny_page *page, int depth, u64* bitmap);


extern int tny_page_access(tny_page *page, int index);

extern tny_page *tny_page_new(int key_count, int allocate);
u64* tny_page_distinct(tny_page *page, u64 *bitmap, int *bit_length) ;


extern u64* tny_page_distinct(tny_page *page, u64 *bitmap, int *length);

extern void tny_page_array_free(int * toFree, int length);

extern int * tny_page_select(tny_page *page, u64 * bitmap, int *value_count);


// Add a reference to the value at "idx" within the columns keys list
extern int tny_page_append(tny_page *page, int idx);


extern int tny_page_access(tny_page *page, int rowIndex) ;



//////////////////////////////////////////
// Aggregate helpers
//////////////////////////////////////////
#endif /* TNY_PAGE_H_ */
